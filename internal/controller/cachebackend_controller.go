package controller

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
)

// conditionTypeReady reports whether the managed backend workload is serving.
const conditionTypeReady = "Ready"

// Event reasons emitted on a CacheBackend.
//
// The cache is an optimization, never a serving dependency: BackendDegraded
// and BackendRecovered narrate transitions of the managed workload's health
// so operators see backend health changes in `kubectl describe`. The
// FailClosedEnabled / FailOpenRestored pair narrates transitions of the
// spec.integration.failOpen toggle — explicitly fail-closed is loud because
// the cache then becomes a serving dependency.
const (
	eventReasonBackendDegraded   = "BackendDegraded"
	eventReasonBackendRecovered  = "BackendRecovered"
	eventReasonFailClosedEnabled = "FailClosedEnabled"
	eventReasonFailOpenRestored  = "FailOpenRestored"
)

// CacheBackendReconciler reconciles a CacheBackend object.
type CacheBackendReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Log      logr.Logger
	Recorder events.EventRecorder
}

// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile drives a CacheBackend toward its desired state. External backends only
// mirror their configured endpoint to status; managed backends (LMCache in Phase 1)
// template a Deployment + Service from the reference stack and own them via owner
// references so they are garbage-collected with the CR.
//
// On every successful reconcile, transitions in the observed backend health
// (entering/leaving Degraded) and in the effective spec.integration.failOpen
// are emitted as Kubernetes Events. Events fire only on transitions — never on
// steady state — so operators see backend outages and fail-closed opt-ins in
// `kubectl describe` without event-stream noise.
func (r *CacheBackendReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := r.Log
	if logger.GetSink() == nil {
		logger = log.FromContext(ctx)
	}

	var backend cachev1alpha1.CacheBackend
	if err := r.Get(ctx, req.NamespacedName, &backend); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	before := snapshotState(&backend)

	result, err := r.dispatch(ctx, logger, &backend)
	if err == nil {
		r.emitTransitionEvents(&backend, before)
	}
	return result, err
}

// dispatch routes a CacheBackend to the right reconcile path. External backends
// only mirror their configured endpoint to status; unsupported / deferred kinds
// shed any previously managed workload; LMCache (Phase 1) templates a
// Deployment + Service.
func (r *CacheBackendReconciler) dispatch(ctx context.Context, logger logr.Logger, backend *cachev1alpha1.CacheBackend) (ctrl.Result, error) {
	if backend.Spec.Type == cachev1alpha1.CacheBackendTypeExternal {
		// A backend switched from a managed type to External must shed its workload.
		if err := r.cleanupOwnedWorkload(ctx, backend); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.reconcileExternal(ctx, backend)
	}

	builder, ok := backendadapter.For(backend.Spec.Type)
	if !ok {
		// Only LMCache is managed in Phase 1 (C2). Other managed types are out of
		// scope here; shed any workload from a previous managed generation.
		logger.V(1).Info("no managed builder for backend type",
			"type", backend.Spec.Type, "namespace", backend.Namespace, "name", backend.Name)
		return ctrl.Result{}, r.reconcileUnmanaged(ctx, backend)
	}

	// StatefulSet-backed (PVC) provisioning + autoscaling is the C3 module. Phase 1
	// manages a Deployment only.
	if backend.Spec.DeploymentKind == cachev1alpha1.CacheBackendDeploymentKindStatefulSet {
		logger.V(1).Info("StatefulSet deploymentKind deferred to C3; skipping",
			"namespace", backend.Namespace, "name", backend.Name)
		return ctrl.Result{}, r.reconcileUnmanaged(ctx, backend)
	}

	return r.reconcileManaged(ctx, logger, backend, builder)
}

// reconcileExternal mirrors a pre-existing backend's configured endpoint to status.
func (r *CacheBackendReconciler) reconcileExternal(ctx context.Context, backend *cachev1alpha1.CacheBackend) error {
	return r.patchStatus(ctx, backend, func() {
		backend.Status.Endpoint = backend.Spec.Endpoint
		backend.Status.Health = ""
		backend.Status.ObservedGeneration = backend.Generation
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeReady)
	})
}

// reconcileUnmanaged sheds any previously owned workload and clears managed status
// for a backend this module no longer provisions (unsupported type or deferred kind).
func (r *CacheBackendReconciler) reconcileUnmanaged(ctx context.Context, backend *cachev1alpha1.CacheBackend) error {
	if err := r.cleanupOwnedWorkload(ctx, backend); err != nil {
		return err
	}
	return r.patchStatus(ctx, backend, func() {
		backend.Status.Endpoint = ""
		backend.Status.Health = ""
		backend.Status.ObservedGeneration = backend.Generation
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeReady)
	})
}

// reconcileManaged templates and applies the child workload, then publishes status.
func (r *CacheBackendReconciler) reconcileManaged(ctx context.Context, logger logr.Logger, backend *cachev1alpha1.CacheBackend, builder backendadapter.Builder) (ctrl.Result, error) {
	workload, err := builder.Build(backend)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("build workload for %s/%s: %w", backend.Namespace, backend.Name, err)
	}

	if err := r.applyDeployment(ctx, backend, workload.Deployment); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.applyService(ctx, backend, workload.Service); err != nil {
		return ctrl.Result{}, err
	}

	var dep appsv1.Deployment
	if err := r.Get(ctx, client.ObjectKeyFromObject(workload.Deployment), &dep); err != nil {
		return ctrl.Result{}, fmt.Errorf("get deployment %s/%s: %w", workload.Deployment.Namespace, workload.Deployment.Name, err)
	}

	if err := r.updateManagedStatus(ctx, backend, workload.Endpoint, &dep); err != nil {
		return ctrl.Result{}, err
	}

	logger.V(1).Info("reconciled managed CacheBackend",
		"namespace", backend.Namespace, "name", backend.Name, "endpoint", workload.Endpoint)
	return ctrl.Result{}, nil
}

// applyDeployment creates or updates the backend Deployment idempotently, owned by the CR.
//
// On create we establish the full templated spec. On update we touch only the
// fields this module owns (replicas + the managed container's image/command/
// args/env) and leave everything else intact — overwriting the whole PodTemplate
// would strip API-server-defaulted fields (port Protocol, RestartPolicy, probe
// thresholds, ...), and since those are re-defaulted on every write it would spin
// a perpetual update loop via the Owns(Deployment) watch.
func (r *CacheBackendReconciler) applyDeployment(ctx context.Context, backend *cachev1alpha1.CacheBackend, desired *appsv1.Deployment) error {
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
		dep.Labels = desired.Labels
		if dep.CreationTimestamp.IsZero() {
			dep.Spec = *desired.Spec.DeepCopy()
		} else {
			dep.Spec.Replicas = desired.Spec.Replicas
			reconcileManagedPodSpec(&dep.Spec.Template.Spec, &desired.Spec.Template.Spec)
		}
		return controllerutil.SetControllerReference(backend, dep, r.Scheme)
	})
	if err != nil {
		return fmt.Errorf("apply deployment %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

// applyService creates or updates the backend Service idempotently, owned by the CR.
// Type, selector, and ports are reconciled (so out-of-band drift is corrected); the
// rendered ports carry Protocol=TCP so they match the API-server-defaulted object,
// and the allocated fields (clusterIP, nodePort) live in separate fields we never
// touch — so reconciling ports does not churn through the Owns(Service) watch.
func (r *CacheBackendReconciler) applyService(ctx context.Context, backend *cachev1alpha1.CacheBackend, desired *corev1.Service) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = desired.Labels
		svc.Spec.Type = desired.Spec.Type
		svc.Spec.Selector = desired.Spec.Selector
		svc.Spec.Ports = desired.Spec.Ports
		return controllerutil.SetControllerReference(backend, svc, r.Scheme)
	})
	if err != nil {
		return fmt.Errorf("apply service %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

// reconcileManagedPodSpec updates the spec-driven fields of the live pod spec in
// place: the managed container's image/command/args/env, plus the pod-level
// override fields. API-server-defaulted fields we don't own (RestartPolicy,
// DNSPolicy, probe thresholds, port Protocol, ...) are left untouched so the
// update does not churn. The desired pod spec already carries the canonical
// defaults for the server-defaulted override fields (schedulerName,
// terminationGracePeriodSeconds), so copying them is idempotent.
func reconcileManagedPodSpec(live *corev1.PodSpec, desired *corev1.PodSpec) {
	reconcileManagedContainer(live, desired)

	// Volumes are builder-owned (e.g. the shm size differs by profile) and are not
	// API-server-defaulted in a Deployment template, so copying them is churn-free.
	live.Volumes = desired.Volumes

	live.NodeSelector = desired.NodeSelector
	live.Affinity = desired.Affinity
	live.Tolerations = desired.Tolerations
	live.TopologySpreadConstraints = desired.TopologySpreadConstraints
	live.ImagePullSecrets = desired.ImagePullSecrets
	live.ServiceAccountName = desired.ServiceAccountName
	live.SecurityContext = desired.SecurityContext
	live.PriorityClassName = desired.PriorityClassName
	live.SchedulerName = desired.SchedulerName
	live.RuntimeClassName = desired.RuntimeClassName
	live.TerminationGracePeriodSeconds = desired.TerminationGracePeriodSeconds
}

// reconcileManagedContainer updates the spec-driven fields of the managed backend
// container in place, leaving API-server-defaulted container fields untouched.
func reconcileManagedContainer(live *corev1.PodSpec, desired *corev1.PodSpec) {
	if len(desired.Containers) == 0 {
		return
	}
	want := desired.Containers[0]
	for i := range live.Containers {
		if live.Containers[i].Name == want.Name {
			live.Containers[i].Image = want.Image
			live.Containers[i].Command = want.Command
			live.Containers[i].Args = want.Args
			live.Containers[i].Env = want.Env
			// Resources are builder-owned (the GPU limit differs by profile) and not
			// API-server-defaulted, so reconciling them is churn-free.
			live.Containers[i].Resources = want.Resources
			return
		}
	}
	// The managed container is missing (e.g. an out-of-band edit) — restore it.
	live.Containers = append(live.Containers, *want.DeepCopy())
}

// cleanupOwnedWorkload best-effort deletes the Deployment + Service this CR owns,
// used when a backend is no longer a managed Deployment (type/kind changed). Normal
// CR deletion is handled by owner-reference garbage collection; this covers the
// in-place mutation case where the CR itself still exists.
func (r *CacheBackendReconciler) cleanupOwnedWorkload(ctx context.Context, backend *cachev1alpha1.CacheBackend) error {
	key := types.NamespacedName{Name: backend.Name, Namespace: backend.Namespace}

	var dep appsv1.Deployment
	if err := r.deleteIfOwned(ctx, key, &dep, backend); err != nil {
		return err
	}
	var svc corev1.Service
	return r.deleteIfOwned(ctx, key, &svc, backend)
}

// deleteIfOwned deletes obj only if it exists and is controller-owned by backend.
func (r *CacheBackendReconciler) deleteIfOwned(ctx context.Context, key types.NamespacedName, obj client.Object, backend *cachev1alpha1.CacheBackend) error {
	if err := r.Get(ctx, key, obj); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(obj, backend) {
		return nil
	}
	if err := r.Delete(ctx, obj); err != nil {
		return client.IgnoreNotFound(err)
	}
	return nil
}

// updateManagedStatus derives health from the Deployment and patches status only when it changes.
func (r *CacheBackendReconciler) updateManagedStatus(ctx context.Context, backend *cachev1alpha1.CacheBackend, endpoint string, dep *appsv1.Deployment) error {
	health, condStatus, reason, message := managedHealth(backend, dep)
	return r.patchStatus(ctx, backend, func() {
		backend.Status.Endpoint = endpoint
		backend.Status.Health = health
		backend.Status.ObservedGeneration = backend.Generation
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeReady,
			Status:             condStatus,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: backend.Generation,
		})
	})
}

// managedHealth maps the Deployment's rollout state to a CacheBackend health.
// Ready requires the Deployment to have observed its current generation and to
// have enough updated + available replicas, so a stale rollout (e.g. mid image
// change) is never reported Ready.
func managedHealth(backend *cachev1alpha1.CacheBackend, dep *appsv1.Deployment) (cachev1alpha1.CacheBackendHealth, metav1.ConditionStatus, string, string) {
	want := int32(1)
	if backend.Spec.Replicas != nil {
		want = *backend.Spec.Replicas
	}

	// A backend scaled to zero is not serving; never report it Ready.
	if want == 0 {
		return cachev1alpha1.CacheBackendHealthPending, metav1.ConditionFalse, "ScaledToZero",
			"backend scaled to zero replicas"
	}

	rolledOut := dep.Status.ObservedGeneration >= dep.Generation
	switch {
	case rolledOut && dep.Status.UpdatedReplicas >= want && dep.Status.AvailableReplicas >= want:
		return cachev1alpha1.CacheBackendHealthReady, metav1.ConditionTrue, "BackendReady",
			fmt.Sprintf("%d/%d replicas available", dep.Status.AvailableReplicas, want)
	case !rolledOut || dep.Status.UpdatedReplicas < want:
		return cachev1alpha1.CacheBackendHealthPending, metav1.ConditionFalse, "RolloutInProgress",
			fmt.Sprintf("%d/%d replicas updated", dep.Status.UpdatedReplicas, want)
	default:
		return cachev1alpha1.CacheBackendHealthDegraded, metav1.ConditionFalse, "ReplicasUnavailable",
			fmt.Sprintf("%d/%d replicas available", dep.Status.AvailableReplicas, want)
	}
}

// patchStatus applies mutate to the backend's status and patches it only when
// it changes. The effective spec.integration.failOpen is always echoed to
// status.failOpen so operators can read the current mode from status alone,
// and so transition detection in [CacheBackendReconciler.emitTransitionEvents]
// has a stable previous-value baseline to compare against.
func (r *CacheBackendReconciler) patchStatus(ctx context.Context, backend *cachev1alpha1.CacheBackend, mutate func()) error {
	before := backend.DeepCopy()
	mutate()
	failOpen := cachev1alpha1.IntegrationFailOpen(backend.Spec.Integration)
	backend.Status.FailOpen = &failOpen
	if equality.Semantic.DeepEqual(before.Status, backend.Status) {
		return nil
	}
	if err := r.Status().Patch(ctx, backend, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("patch CacheBackend status %s/%s: %w", backend.Namespace, backend.Name, err)
	}
	return nil
}

// stateSnapshot captures the prior-status fields that drive transition events.
// Health is the observed phase enum and failOpen is the previously-echoed
// integration.failOpen (nil ⇒ never observed ⇒ treated as the API default of
// true, so an initial apply with failOpen=false correctly fires the warning).
type stateSnapshot struct {
	health   cachev1alpha1.CacheBackendHealth
	failOpen bool
}

// snapshotState captures the prior status values that drive transition events.
// Called at the top of Reconcile before any mutation so emitTransitionEvents
// has a stable baseline to compare the post-reconcile state against.
func snapshotState(cb *cachev1alpha1.CacheBackend) stateSnapshot {
	return stateSnapshot{
		health:   cb.Status.Health,
		failOpen: statusFailOpen(cb.Status.FailOpen),
	}
}

// statusFailOpen treats a missing status.failOpen as the API default (true).
// A first-time reconcile with spec.integration.failOpen=false is then correctly
// observed as a transition true→false and fires the FailClosedEnabled Warning.
func statusFailOpen(v *bool) bool {
	if v == nil {
		return true
	}
	return *v
}

// emitTransitionEvents emits Kubernetes Events on transitions of the observed
// backend health or the effective failOpen toggle. By design events fire only
// on transitions — never on steady state — so a Ready backend reconciling
// every few seconds does not flood the event stream.
//
//   - Entering Degraded → Warning BackendDegraded.
//   - Leaving Degraded for Ready → Normal BackendRecovered.
//   - failOpen flipped true → false → Warning FailClosedEnabled (the cache
//     becomes a serving dependency; advanced opt-in).
//   - failOpen flipped false → true → Normal FailOpenRestored.
func (r *CacheBackendReconciler) emitTransitionEvents(cb *cachev1alpha1.CacheBackend, before stateSnapshot) {
	if r.Recorder == nil {
		return
	}
	after := stateSnapshot{
		health:   cb.Status.Health,
		failOpen: statusFailOpen(cb.Status.FailOpen),
	}

	if before.health != cachev1alpha1.CacheBackendHealthDegraded &&
		after.health == cachev1alpha1.CacheBackendHealthDegraded {
		r.Recorder.Eventf(cb, nil, corev1.EventTypeWarning, eventReasonBackendDegraded, eventReasonBackendDegraded,
			"cache backend is degraded: %s", degradedMessage(cb))
	}
	if before.health == cachev1alpha1.CacheBackendHealthDegraded &&
		after.health == cachev1alpha1.CacheBackendHealthReady {
		r.Recorder.Eventf(cb, nil, corev1.EventTypeNormal, eventReasonBackendRecovered, eventReasonBackendRecovered,
			"cache backend recovered to Ready")
	}

	if before.failOpen && !after.failOpen {
		r.Recorder.Eventf(cb, nil, corev1.EventTypeWarning, eventReasonFailClosedEnabled, eventReasonFailClosedEnabled,
			"fail-closed mode enabled — cache is now a serving dependency; engine requests will fail when the cache is unreachable")
	}
	if !before.failOpen && after.failOpen {
		r.Recorder.Eventf(cb, nil, corev1.EventTypeNormal, eventReasonFailOpenRestored, eventReasonFailOpenRestored,
			"fail-open mode restored — cache is again an optimization, not a serving dependency")
	}
}

// degradedMessage surfaces the Ready=False condition's message (set by
// managedHealth) so the BackendDegraded event names the failure mode (e.g.
// "1/3 replicas available") instead of just announcing the phase change.
func degradedMessage(cb *cachev1alpha1.CacheBackend) string {
	if c := meta.FindStatusCondition(cb.Status.Conditions, conditionTypeReady); c != nil && c.Message != "" {
		return c.Message
	}
	return "backend workload not available"
}

// SetupWithManager sets up the controller with the Manager. Owns(Deployment)
// guarantees that a child's status flipping (e.g. AvailableReplicas dropping
// to zero) re-triggers a Reconcile so emitTransitionEvents observes the change.
func (r *CacheBackendReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("cachebackend-controller")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.CacheBackend{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Complete(r)
}

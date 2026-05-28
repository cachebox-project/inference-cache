package controller

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
)

// Status condition types published on a managed CacheBackend.
//
// Ready reports whether the managed backend workload is currently serving.
// Progressing reports whether the controller is still driving the live state
// toward the desired state (template render, child apply, rollout in flight).
// The two conditions together let consumers tell a still-converging backend
// (Ready=False, Progressing=True) apart from a stuck/degraded one
// (Ready=False, Progressing=False).
const (
	conditionTypeReady       = "Ready"
	conditionTypeProgressing = "Progressing"
)

// Default HPA tuning when the autoscaling spec leaves them unset.
const (
	defaultHPAMinReplicas                 = int32(1)
	defaultHPATargetCPUUtilizationPercent = int32(80)
)

// CacheBackendReconciler reconciles a CacheBackend object.
type CacheBackendReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger
}

// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives a CacheBackend toward its desired state. External backends only
// mirror their configured endpoint to status; managed backends (LMCache in Phase 1)
// template a Deployment + Service (+ optional PVC + HPA) from the reference stack
// and own them via owner references so they are garbage-collected with the CR.
func (r *CacheBackendReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := r.Log
	if logger.GetSink() == nil {
		logger = log.FromContext(ctx)
	}

	var backend cachev1alpha1.CacheBackend
	if err := r.Get(ctx, req.NamespacedName, &backend); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if backend.Spec.Type == cachev1alpha1.CacheBackendTypeExternal {
		// A backend switched from a managed type to External must shed its workload.
		if err := r.cleanupOwnedWorkload(ctx, &backend); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.reconcileExternal(ctx, &backend)
	}

	builder, ok := backendadapter.For(backend.Spec.Type)
	if !ok {
		// Only LMCache is managed in Phase 1 (C2). Other managed types are out of
		// scope here; shed any workload from a previous managed generation.
		logger.V(1).Info("no managed builder for backend type",
			"type", backend.Spec.Type, "namespace", req.Namespace, "name", req.Name)
		return ctrl.Result{}, r.reconcileUnmanaged(ctx, &backend)
	}

	// C3 ships Deployment + optional PVC + optional HPA. StatefulSet (per-replica
	// PVCs via volumeClaimTemplates) is a later module — the LMCache single-pod
	// shape Phase 1 targets is covered by a Deployment + a single shared PVC.
	if backend.Spec.DeploymentKind == cachev1alpha1.CacheBackendDeploymentKindStatefulSet {
		logger.V(1).Info("StatefulSet deploymentKind not yet supported; skipping",
			"namespace", req.Namespace, "name", req.Name)
		return ctrl.Result{}, r.reconcileUnmanaged(ctx, &backend)
	}

	return r.reconcileManaged(ctx, logger, &backend, builder)
}

// reconcileExternal mirrors a pre-existing backend's configured endpoint to status.
func (r *CacheBackendReconciler) reconcileExternal(ctx context.Context, backend *cachev1alpha1.CacheBackend) error {
	return r.patchStatus(ctx, backend, func() {
		backend.Status.Endpoint = backend.Spec.Endpoint
		backend.Status.Health = ""
		backend.Status.Capacity = ""
		backend.Status.ObservedGeneration = backend.Generation
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeReady)
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeProgressing)
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
		backend.Status.Capacity = ""
		backend.Status.ObservedGeneration = backend.Generation
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeReady)
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeProgressing)
	})
}

// reconcileManaged templates and applies the child workload, then publishes status.
func (r *CacheBackendReconciler) reconcileManaged(ctx context.Context, logger logr.Logger, backend *cachev1alpha1.CacheBackend, builder backendadapter.Builder) (ctrl.Result, error) {
	if reason, msg, ok := validateManagedSpec(backend); !ok {
		// Don't tear down whatever's already running — keep the in-flight
		// workload visible to the user while they fix the spec. We just refuse
		// to propagate the unsafe combination further.
		logger.V(1).Info("CacheBackend spec is invalid; not applying children",
			"namespace", backend.Namespace, "name", backend.Name, "reason", reason)
		return ctrl.Result{}, r.markInvalidSpec(ctx, backend, reason, msg)
	}

	workload, err := builder.Build(backend)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("build workload for %s/%s: %w", backend.Namespace, backend.Name, err)
	}

	// PVC first: the Deployment references the PVC by name, so the claim should
	// exist before the workload pod schedules. Apply order doesn't gate
	// reconciliation correctness (k8s tolerates missing PVCs while a pod is
	// pending), but it keeps the time-to-Ready window minimal on first apply.
	if err := r.reconcilePVC(ctx, backend, workload.PVC); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.applyDeployment(ctx, backend, workload.Deployment); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.applyService(ctx, backend, workload.Service); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.reconcileHPA(ctx, backend, workload.Deployment); err != nil {
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
// args/env, plus volume sources for cache-home switching between EmptyDir and
// a PVC) and leave everything else intact — overwriting the whole PodTemplate
// would strip API-server-defaulted fields (port Protocol, RestartPolicy, probe
// thresholds, ...), and since those are re-defaulted on every write it would spin
// a perpetual update loop via the Owns(Deployment) watch.
//
// When an HPA owns scaling (spec.autoscaling set), the reconciler defers to the
// HPA's replica count rather than overwriting it.
func (r *CacheBackendReconciler) applyDeployment(ctx context.Context, backend *cachev1alpha1.CacheBackend, desired *appsv1.Deployment) error {
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
		// Snapshot the HPA-owned field BEFORE we mutate the live spec. When an
		// HPA is configured the controller must never re-assert replicas; doing
		// so would fight the HPA on every reconcile and churn the rollout.
		liveReplicas := dep.Spec.Replicas

		dep.Labels = desired.Labels
		if dep.CreationTimestamp.IsZero() {
			dep.Spec = *desired.Spec.DeepCopy()
		} else {
			dep.Spec.Replicas = desired.Spec.Replicas
			reconcileManagedPodSpec(&dep.Spec.Template.Spec, &desired.Spec.Template.Spec)
		}
		if backend.Spec.Autoscaling != nil && liveReplicas != nil {
			dep.Spec.Replicas = liveReplicas
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

// reconcilePVC creates or updates the PVC backing the cache-home volume when the
// spec requests persistent storage. When the spec drops persistence the existing
// PVC is intentionally NOT deleted — destroying persistent storage in response to
// a spec edit risks silent data loss. The orphaned PVC carries an owner reference,
// so it is still GC'd when the CacheBackend itself is deleted.
//
// On update the requested size is the only mutable spec field we patch
// (StorageClassName is immutable in k8s; access modes are stable for ROW PVCs).
// PVC size shrinks aren't allowed by k8s, so the user must size up over time.
func (r *CacheBackendReconciler) reconcilePVC(ctx context.Context, backend *cachev1alpha1.CacheBackend, desired *corev1.PersistentVolumeClaim) error {
	if desired == nil {
		return nil
	}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pvc, func() error {
		pvc.Labels = desired.Labels
		if pvc.CreationTimestamp.IsZero() {
			// First-time provisioning: write the full desired spec.
			pvc.Spec = *desired.Spec.DeepCopy()
		} else {
			// In-place updates: only the requested storage size is mutable.
			if pvc.Spec.Resources.Requests == nil {
				pvc.Spec.Resources.Requests = corev1.ResourceList{}
			}
			pvc.Spec.Resources.Requests[corev1.ResourceStorage] = desired.Spec.Resources.Requests[corev1.ResourceStorage]
		}
		return controllerutil.SetControllerReference(backend, pvc, r.Scheme)
	})
	if err != nil {
		return fmt.Errorf("apply PVC %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

// reconcileHPA creates, updates, or deletes the HorizontalPodAutoscaler that
// drives the backend Deployment's replica count. The HPA exists iff
// spec.autoscaling is set; otherwise any controller-owned HPA is removed.
func (r *CacheBackendReconciler) reconcileHPA(ctx context.Context, backend *cachev1alpha1.CacheBackend, deployment *appsv1.Deployment) error {
	if backend.Spec.Autoscaling == nil {
		// Autoscaling disabled — clean up any HPA we previously owned.
		return r.deleteOwnedHPA(ctx, backend, deployment.Name)
	}

	desired := buildHPA(backend, deployment)
	hpa := &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, hpa, func() error {
		hpa.Labels = desired.Labels
		hpa.Spec = desired.Spec
		return controllerutil.SetControllerReference(backend, hpa, r.Scheme)
	})
	if err != nil {
		return fmt.Errorf("apply HPA %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

// buildHPA renders the desired HorizontalPodAutoscaler for a CacheBackend whose
// spec.autoscaling is set. Targets the managed Deployment by name. Phase 1 ships
// a CPU-utilization target; cache-aware (custom-metric) HPAs come later.
func buildHPA(backend *cachev1alpha1.CacheBackend, deployment *appsv1.Deployment) *autoscalingv2.HorizontalPodAutoscaler {
	spec := backend.Spec.Autoscaling
	minReplicas := defaultHPAMinReplicas
	if spec.MinReplicas != nil {
		minReplicas = *spec.MinReplicas
	}
	target := defaultHPATargetCPUUtilizationPercent
	if spec.TargetCPUUtilizationPercent != nil {
		target = *spec.TargetCPUUtilizationPercent
	}
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deployment.Name,
			Namespace: deployment.Namespace,
			Labels:    deployment.Labels,
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       deployment.Name,
			},
			MinReplicas: &minReplicas,
			MaxReplicas: spec.MaxReplicas,
			Metrics: []autoscalingv2.MetricSpec{
				{
					Type: autoscalingv2.ResourceMetricSourceType,
					Resource: &autoscalingv2.ResourceMetricSource{
						Name: corev1.ResourceCPU,
						Target: autoscalingv2.MetricTarget{
							Type:               autoscalingv2.UtilizationMetricType,
							AverageUtilization: &target,
						},
					},
				},
			},
		},
	}
}

// deleteOwnedHPA removes a previously-owned HPA (e.g. spec.autoscaling cleared).
// Missing HPA is a no-op.
func (r *CacheBackendReconciler) deleteOwnedHPA(ctx context.Context, backend *cachev1alpha1.CacheBackend, name string) error {
	key := types.NamespacedName{Name: name, Namespace: backend.Namespace}
	var hpa autoscalingv2.HorizontalPodAutoscaler
	if err := r.Get(ctx, key, &hpa); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get HPA %s/%s: %w", backend.Namespace, name, err)
	}
	if !metav1.IsControlledBy(&hpa, backend) {
		return nil
	}
	if err := r.Delete(ctx, &hpa); err != nil {
		return client.IgnoreNotFound(err)
	}
	return nil
}

// reconcileManagedPodSpec updates the spec-driven fields of the live pod spec in
// place: the managed container's image/command/args/env, plus the pod-level
// override fields and the cache-home volume source (which switches between
// EmptyDir and a PVC based on spec.storage). API-server-defaulted fields we
// don't own (RestartPolicy, DNSPolicy, probe thresholds, port Protocol, ...) are
// left untouched so the update does not churn. The desired pod spec already
// carries the canonical defaults for the server-defaulted override fields
// (schedulerName, terminationGracePeriodSeconds), so copying them is idempotent.
func reconcileManagedPodSpec(live *corev1.PodSpec, desired *corev1.PodSpec) {
	reconcileManagedContainer(live, desired)
	reconcileVolumeSources(live, desired)

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
// Resources are reconciled so upgrades from older controller versions adopt the
// new CPU/memory requests (the HPA needs a CPU request to compute utilization).
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
			live.Containers[i].Resources = *want.Resources.DeepCopy()
			return
		}
	}
	// The managed container is missing (e.g. an out-of-band edit) — restore it.
	live.Containers = append(live.Containers, *want.DeepCopy())
}

// reconcileVolumeSources reconciles the source (EmptyDir vs PVC) of volumes
// the controller owns. It only updates volumes that exist on both live and
// desired by name, which keeps it from clobbering volumes the user added.
// In particular this lets cache-home swap between EmptyDir and a PVC when
// the spec gains or loses persistent storage.
func reconcileVolumeSources(live *corev1.PodSpec, desired *corev1.PodSpec) {
	for di := range desired.Volumes {
		want := desired.Volumes[di]
		for li := range live.Volumes {
			if live.Volumes[li].Name == want.Name {
				live.Volumes[li].VolumeSource = want.VolumeSource
				break
			}
		}
	}
}

// cleanupOwnedWorkload best-effort deletes the stateless children this CR
// owns, used when a backend is no longer a managed Deployment (type/kind
// changed). The PVC is intentionally NOT deleted here — destroying persistent
// storage in response to an in-place spec edit risks silent data loss, the
// same reasoning that makes reconcilePVC adopt-and-keep when spec.storage is
// removed. The owner reference still ensures the PVC is GC'd when the
// CacheBackend itself is deleted.
func (r *CacheBackendReconciler) cleanupOwnedWorkload(ctx context.Context, backend *cachev1alpha1.CacheBackend) error {
	key := types.NamespacedName{Name: backend.Name, Namespace: backend.Namespace}

	var dep appsv1.Deployment
	if err := r.deleteIfOwned(ctx, key, &dep, backend); err != nil {
		return err
	}
	var svc corev1.Service
	if err := r.deleteIfOwned(ctx, key, &svc, backend); err != nil {
		return err
	}
	var hpa autoscalingv2.HorizontalPodAutoscaler
	return r.deleteIfOwned(ctx, key, &hpa, backend)
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

// validateManagedSpec catches spec combinations the Phase-1 reconciler cannot
// safely satisfy. It returns false (reason, message) when the spec is unsafe.
// The check is deliberately controller-side rather than CRD-level so it does
// not tighten v1alpha1 schema validation for existing fields.
func validateManagedSpec(backend *cachev1alpha1.CacheBackend) (string, string, bool) {
	if backend.Spec.Storage == nil || backend.Spec.Storage.PVC == nil {
		return "", "", true
	}
	if backend.Spec.Replicas != nil && *backend.Spec.Replicas > 1 {
		return "InvalidStorageConfiguration",
			"spec.storage.pvc currently requires spec.replicas <= 1 (per-replica PVC support deferred)",
			false
	}
	if backend.Spec.Autoscaling != nil && backend.Spec.Autoscaling.MaxReplicas > 1 {
		return "InvalidStorageConfiguration",
			"spec.storage.pvc currently requires spec.autoscaling.maxReplicas <= 1 (per-replica PVC support deferred)",
			false
	}
	return "", "", true
}

// markInvalidSpec records an InvalidStorageConfiguration outcome on status:
// Health=Failed, Ready=False, Progressing=False, both conditions carrying the
// reason/message so kubectl describe shows the user exactly what's wrong.
func (r *CacheBackendReconciler) markInvalidSpec(ctx context.Context, backend *cachev1alpha1.CacheBackend, reason, message string) error {
	return r.patchStatus(ctx, backend, func() {
		backend.Status.Health = cachev1alpha1.CacheBackendHealthFailed
		backend.Status.ObservedGeneration = backend.Generation
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeReady,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: backend.Generation,
		})
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeProgressing,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: backend.Generation,
		})
	})
}

// updateManagedStatus derives health from the Deployment and patches status only when it changes.
func (r *CacheBackendReconciler) updateManagedStatus(ctx context.Context, backend *cachev1alpha1.CacheBackend, endpoint string, dep *appsv1.Deployment) error {
	health, readyStatus, reason, message := managedHealth(backend, dep)
	progressingStatus, progressingReason, progressingMessage := progressingFromHealth(health, reason, message)
	capacity := managedCapacity(backend)
	return r.patchStatus(ctx, backend, func() {
		backend.Status.Endpoint = endpoint
		backend.Status.Health = health
		backend.Status.Capacity = capacity
		backend.Status.ObservedGeneration = backend.Generation
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeReady,
			Status:             readyStatus,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: backend.Generation,
		})
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeProgressing,
			Status:             progressingStatus,
			Reason:             progressingReason,
			Message:            progressingMessage,
			ObservedGeneration: backend.Generation,
		})
	})
}

// managedHealth maps the Deployment's rollout state to a CacheBackend health.
// Ready requires the Deployment to have observed its current generation and to
// have enough updated + available replicas, so a stale rollout (e.g. mid image
// change) is never reported Ready.
//
// When the CacheBackend is autoscaled the HPA owns the desired replica count,
// so the comparison target is the live Deployment's spec.replicas (which the
// HPA writes) rather than the CacheBackend's spec.replicas (which is ignored
// in that mode). This keeps Ready accurate when an HPA decides to run more
// pods than spec.replicas, and avoids a false ScaledToZero when spec.replicas
// happens to be 0 with autoscaling configured.
func managedHealth(backend *cachev1alpha1.CacheBackend, dep *appsv1.Deployment) (cachev1alpha1.CacheBackendHealth, metav1.ConditionStatus, string, string) {
	want := desiredReplicas(backend, dep)

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

// progressingFromHealth derives the Progressing condition from the Ready
// condition's outcome. A Pending backend is still actively converging
// (Progressing=True); a Ready backend has converged (Progressing=False); a
// Degraded backend has finished a rollout but lost replicas (Progressing=False,
// because no rollout is in motion — Ready=False already signals the problem).
func progressingFromHealth(health cachev1alpha1.CacheBackendHealth, reason, message string) (metav1.ConditionStatus, string, string) {
	switch health {
	case cachev1alpha1.CacheBackendHealthReady:
		return metav1.ConditionFalse, "Synced", "rendered children match desired state"
	case cachev1alpha1.CacheBackendHealthPending:
		return metav1.ConditionTrue, reason, message
	case cachev1alpha1.CacheBackendHealthDegraded:
		return metav1.ConditionFalse, "Degraded", message
	default:
		return metav1.ConditionFalse, reason, message
	}
}

// desiredReplicas is the per-reconcile source of truth for "how many replicas
// should this backend be running". With autoscaling enabled the HPA writes
// spec.replicas on the Deployment, so the live value is authoritative; without
// it, the user's spec.replicas (default 1) wins.
func desiredReplicas(backend *cachev1alpha1.CacheBackend, dep *appsv1.Deployment) int32 {
	if backend.Spec.Autoscaling != nil {
		// First reconcile after an HPA spec is added may briefly see
		// dep.Spec.Replicas still set by the controller; the HPA will overwrite
		// it within one cycle. Until then, fall back to the controller value.
		if dep.Spec.Replicas != nil {
			return *dep.Spec.Replicas
		}
		// Fall through to the floor.
	}
	if backend.Spec.Replicas != nil {
		return *backend.Spec.Replicas
	}
	return 1
}

// managedCapacity returns the human-readable capacity surfaced on status.
// When persistent storage is configured this is the requested PVC size; for
// ephemeral-storage backends the field is empty (the EmptyDir size isn't a
// meaningful capacity signal).
func managedCapacity(backend *cachev1alpha1.CacheBackend) string {
	if backend.Spec.Storage == nil || backend.Spec.Storage.PVC == nil {
		return ""
	}
	return backend.Spec.Storage.PVC.Size.String()
}

// patchStatus applies mutate to the backend's status and patches it only when it changes.
func (r *CacheBackendReconciler) patchStatus(ctx context.Context, backend *cachev1alpha1.CacheBackend, mutate func()) error {
	before := backend.DeepCopy()
	mutate()
	if equality.Semantic.DeepEqual(before.Status, backend.Status) {
		return nil
	}
	if err := r.Status().Patch(ctx, backend, client.MergeFrom(before)); err != nil {
		return fmt.Errorf("patch CacheBackend status %s/%s: %w", backend.Namespace, backend.Name, err)
	}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CacheBackendReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.CacheBackend{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Complete(r)
}

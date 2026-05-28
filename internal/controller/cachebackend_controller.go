package controller

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

// conditionTypeReady reports whether the managed backend workload is serving.
const conditionTypeReady = "Ready"

// CacheBackendReconciler reconciles a CacheBackend object.
type CacheBackendReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Log    logr.Logger
	// Registry resolves the runtime adapter to use for a CacheBackend. Nil
	// uses [adapterruntime.DefaultRegistry] — set explicitly only in tests
	// that need a custom adapter set.
	Registry *adapterruntime.Registry
}

// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives a CacheBackend toward its desired state. External backends
// only mirror their configured endpoint to status; managed backends (LMCache
// in Phase 1) ask the registered runtime adapter for the cache-server pod
// spec + service spec, wrap them into a Deployment + Service the controller
// owns, and publish the resolved endpoint.
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

	// StatefulSet-backed (PVC) provisioning + autoscaling is the C3 module. Phase 1
	// manages a Deployment only.
	if backend.Spec.DeploymentKind == cachev1alpha1.CacheBackendDeploymentKindStatefulSet {
		logger.V(1).Info("StatefulSet deploymentKind deferred to C3; skipping",
			"namespace", req.Namespace, "name", req.Name)
		return ctrl.Result{}, r.reconcileUnmanaged(ctx, &backend)
	}

	registry := r.Registry
	if registry == nil {
		registry = adapterruntime.DefaultRegistry()
	}
	runtimeID := resolveRuntimeID(&backend)
	adapter, err := registry.Select(runtimeID, &backend)
	if err != nil {
		// No adapter knows how to wire this (runtime, backend) pair. Shed any
		// previously managed workload and log; the C7 admission validator
		// surfaces the same condition as a user-visible rejection.
		logger.V(1).Info("no runtime adapter for backend",
			"runtime", runtimeID, "type", backend.Spec.Type,
			"namespace", req.Namespace, "name", req.Name, "error", err.Error())
		return ctrl.Result{}, r.reconcileUnmanaged(ctx, &backend)
	}

	return r.reconcileManaged(ctx, logger, &backend, adapter)
}

// resolveRuntimeID picks the [adapterruntime.RuntimeID] the reconciler asks
// the registry to match. The CacheBackend carries the engine name in
// Spec.Integration.Engine; when unset, vLLM is the Phase-1 default (the only
// engine the shipping adapters target).
//
// Engine values are normalised to lower-case so common spellings ("vLLM",
// "VLLM", "SGLang") route to the canonical RuntimeID constants
// ([adapterruntime.RuntimeVLLM] etc.); a free-form string in the CR should
// never silently drop a CacheBackend into the unmanaged path just because of
// case.
func resolveRuntimeID(backend *cachev1alpha1.CacheBackend) adapterruntime.RuntimeID {
	if backend.Spec.Integration != nil && backend.Spec.Integration.Engine != "" {
		return adapterruntime.RuntimeID(strings.ToLower(backend.Spec.Integration.Engine))
	}
	return adapterruntime.RuntimeVLLM
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
// for a backend this module no longer provisions (unsupported runtime/backend or
// deferred kind).
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

// reconcileManaged renders the cache-server PodSpec + Service via the runtime
// adapter, wraps them into a Deployment + Service owned by the CR, and
// publishes the resolved endpoint to status.
func (r *CacheBackendReconciler) reconcileManaged(ctx context.Context, logger logr.Logger, backend *cachev1alpha1.CacheBackend, adapter adapterruntime.KVCacheRuntimeAdapter) (ctrl.Result, error) {
	podSpec, svcSpec, err := adapter.ResolveCacheServer(backend)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolve cache server for %s/%s: %w", backend.Namespace, backend.Name, err)
	}
	if podSpec == nil || svcSpec == nil {
		// An adapter that genuinely needs no cache-server (e.g. an
		// engine-colocated backend) is a valid future case. For Phase 1 it
		// shouldn't happen for managed types — surface as unmanaged.
		logger.V(1).Info("adapter rendered no cache-server; treating as unmanaged",
			"namespace", backend.Namespace, "name", backend.Name)
		return ctrl.Result{}, r.reconcileUnmanaged(ctx, backend)
	}

	dep := r.buildDeployment(backend, podSpec)
	svc := r.buildService(backend, svcSpec)

	if err := r.applyDeployment(ctx, backend, dep); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.applyService(ctx, backend, svc); err != nil {
		return ctrl.Result{}, err
	}

	var live appsv1.Deployment
	if err := r.Get(ctx, client.ObjectKeyFromObject(dep), &live); err != nil {
		return ctrl.Result{}, fmt.Errorf("get deployment %s/%s: %w", dep.Namespace, dep.Name, err)
	}

	endpoint := serviceEndpoint(svc)
	if err := r.updateManagedStatus(ctx, backend, endpoint, &live); err != nil {
		return ctrl.Result{}, err
	}

	logger.V(1).Info("reconciled managed CacheBackend",
		"namespace", backend.Namespace, "name", backend.Name, "endpoint", endpoint)
	return ctrl.Result{}, nil
}

// buildDeployment wraps the adapter-rendered PodSpec into a Deployment the
// controller owns: ObjectMeta + labels + replicas + selector come from the
// CacheBackend identity, not the adapter.
func (r *CacheBackendReconciler) buildDeployment(backend *cachev1alpha1.CacheBackend, podSpec *corev1.PodSpec) *appsv1.Deployment {
	replicas := int32(1)
	if backend.Spec.Replicas != nil {
		replicas = *backend.Spec.Replicas
	}
	selector := selectorLabels(backend.Name)
	podLabels := podTemplateLabels(backend)

	pod := podSpec.DeepCopy()
	applyPodOverrides(pod, backend.Spec.Template)

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      backend.Name,
			Namespace: backend.Namespace,
			Labels:    podLabels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: podLabels},
				Spec:       *pod,
			},
		},
	}
}

// buildService wraps the adapter-rendered Service spec into a Service the
// controller owns: ObjectMeta + Selector come from the CacheBackend identity.
// Adapter-provided fields (Spec.Type, Spec.Ports) are preserved as-is.
func (r *CacheBackendReconciler) buildService(backend *cachev1alpha1.CacheBackend, src *corev1.Service) *corev1.Service {
	selector := selectorLabels(backend.Name)
	labels := podTemplateLabels(backend)
	out := src.DeepCopy()
	out.ObjectMeta = metav1.ObjectMeta{
		Name:      backend.Name,
		Namespace: backend.Namespace,
		Labels:    labels,
	}
	out.Spec.Selector = selector
	if out.Spec.Type == "" {
		out.Spec.Type = corev1.ServiceTypeClusterIP
	}
	return out
}

// selectorLabels are the immutable identity labels for a backend's child objects.
func selectorLabels(name string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "cachebackend",
		"app.kubernetes.io/instance":   name,
		"app.kubernetes.io/managed-by": "inference-cache-controller",
	}
}

// podTemplateLabels add backend-type identity on top of the selector labels.
// The backend-type label is informational (kubectl filtering); the controller
// only relies on the selector labels.
func podTemplateLabels(backend *cachev1alpha1.CacheBackend) map[string]string {
	labels := selectorLabels(backend.Name)
	if t := string(backend.Spec.Type); t != "" {
		labels["inferencecache.io/backend-type"] = t
	}
	return labels
}

// applyPodOverrides copies optional pod-level scheduling/security overrides
// from the spec onto the rendered pod spec. Server-defaulted fields
// (schedulerName, terminationGracePeriodSeconds) are always set to their
// defaults when unset so the rendered template matches the API-server-
// defaulted object and updates don't churn.
func applyPodOverrides(spec *corev1.PodSpec, override *cachev1alpha1.CacheBackendPodSpecOverride) {
	if spec.SchedulerName == "" {
		spec.SchedulerName = "default-scheduler"
	}
	if spec.TerminationGracePeriodSeconds == nil {
		defaultGrace := int64(30)
		spec.TerminationGracePeriodSeconds = &defaultGrace
	}
	if override == nil {
		return
	}
	spec.NodeSelector = override.NodeSelector
	spec.Affinity = override.Affinity
	spec.Tolerations = override.Tolerations
	spec.TopologySpreadConstraints = override.TopologySpreadConstraints
	spec.ImagePullSecrets = override.ImagePullSecrets
	spec.ServiceAccountName = override.ServiceAccountName
	spec.SecurityContext = override.SecurityContext
	spec.PriorityClassName = override.PriorityClassName
	spec.RuntimeClassName = override.RuntimeClassName
	if override.SchedulerName != "" {
		spec.SchedulerName = override.SchedulerName
	}
	if override.TerminationGracePeriodSeconds != nil {
		spec.TerminationGracePeriodSeconds = override.TerminationGracePeriodSeconds
	}
}

// serviceEndpoint formats the published cache endpoint as host:port using the
// service's first port. Engine-protocol prefixes (e.g. lm:// for LMCache) are
// the adapter's responsibility — status.endpoint stays engine-agnostic.
func serviceEndpoint(svc *corev1.Service) string {
	if len(svc.Spec.Ports) == 0 {
		return ""
	}
	return fmt.Sprintf("%s.%s.svc.cluster.local:%d", svc.Name, svc.Namespace, svc.Spec.Ports[0].Port)
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
//
// When the container set changes (e.g. an in-place upgrade from C2's "vllm"
// to the C6 "lmcache-server" container), [reconcileManagedContainer] prunes
// the old containers — and the adapter-owned pod-level Volumes the old
// containers referenced (e.g. C2's cache-home + shm) must be pruned too, or
// the live pod template carries dangling volumes from the previous shape.
// The volume reset is gated on a container-set change so a steady-state
// reconcile never overwrites Volumes (avoiding churn under the Owns watch).
func reconcileManagedPodSpec(live *corev1.PodSpec, desired *corev1.PodSpec) {
	before := containerNameSet(live.Containers)
	reconcileManagedContainer(live, desired)
	after := containerNameSet(live.Containers)
	if !stringSetEqual(before, after) {
		live.Volumes = desired.Volumes
	}

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

// containerNameSet returns the set of container names in cs.
func containerNameSet(cs []corev1.Container) map[string]struct{} {
	out := make(map[string]struct{}, len(cs))
	for i := range cs {
		out[cs[i].Name] = struct{}{}
	}
	return out
}

// stringSetEqual reports whether two name sets contain the same keys.
func stringSetEqual(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

// reconcileManagedContainer updates the spec-driven fields of the managed backend
// container in place, leaving API-server-defaulted container fields untouched.
//
// Containers in live whose names are not in desired are dropped — this is the
// upgrade path from the C2 colocated all-in-one (container name "vllm") to
// the C6 standalone topology (container name "lmcache-server"): an in-place
// upgrade must replace the old managed container, not stack the new one
// alongside it. We never drop containers that match a desired name (we only
// update their managed fields), so a Deployment carrying sidecars in addition
// to the managed container loses the sidecars — sidecars were not a supported
// path in C2 and remain unsupported here.
func reconcileManagedContainer(live *corev1.PodSpec, desired *corev1.PodSpec) {
	if len(desired.Containers) == 0 {
		return
	}
	desiredNames := make(map[string]int, len(desired.Containers))
	for i := range desired.Containers {
		desiredNames[desired.Containers[i].Name] = i
	}

	// First pass: drop any live container whose name isn't desired (the
	// upgrade-from-previous-managed-shape case).
	kept := live.Containers[:0]
	for i := range live.Containers {
		if _, ok := desiredNames[live.Containers[i].Name]; ok {
			kept = append(kept, live.Containers[i])
		}
	}
	live.Containers = kept

	// Second pass: for each desired container, update the matching live one
	// in place (preserving API-server-defaulted fields) or append it.
	for i := range desired.Containers {
		want := desired.Containers[i]
		matched := false
		for j := range live.Containers {
			if live.Containers[j].Name == want.Name {
				// Adapter-owned fields the reconciler propagates from desired:
				// the cache-server's serving port, probes, resource shape, and
				// the connector args/env. Adapters render these explicitly
				// (with API-server-defaulted fields like Port Protocol set in
				// the rendering), so copying them is idempotent and doesn't
				// churn the Owns watch. Leaving Ports/Probes/VolumeMounts
				// untouched would let port drift break the Service's
				// TargetPort lookup or hide a probe regression.
				live.Containers[j].Image = want.Image
				live.Containers[j].ImagePullPolicy = want.ImagePullPolicy
				live.Containers[j].Command = want.Command
				live.Containers[j].Args = want.Args
				live.Containers[j].Env = want.Env
				live.Containers[j].Ports = want.Ports
				live.Containers[j].Resources = want.Resources
				live.Containers[j].VolumeMounts = want.VolumeMounts
				live.Containers[j].ReadinessProbe = want.ReadinessProbe
				live.Containers[j].LivenessProbe = want.LivenessProbe
				live.Containers[j].StartupProbe = want.StartupProbe
				matched = true
				break
			}
		}
		if !matched {
			live.Containers = append(live.Containers, *want.DeepCopy())
		}
	}
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
		Complete(r)
}

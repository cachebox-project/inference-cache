package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
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

// DefaultMatchedEnginePodsRequeueInterval is the steady-state cadence at
// which a CacheBackend with a configured spec.engineSelector self-requeues,
// so the `status.matchedEnginePods` snapshot does not stay stale forever
// between otherwise-unrelated reconcile triggers. The reconciler does not
// Watch Pods by design (see refreshMatchedEnginePods godoc); without a
// self-requeue, the count would only refresh when the CR, the owned
// Deployment, Service, or HPA changed. 30s strikes a balance between
// operator responsiveness and reconcile pressure on a large fleet. Tests
// override via the `MatchedEnginePodsRequeueInterval` reconciler field to
// avoid baking the 30s delay into the suite.
const DefaultMatchedEnginePodsRequeueInterval = 30 * time.Second

// Event reasons emitted on a CacheBackend.
//
// The cache is an optimization, never a serving dependency: BackendDegraded
// and BackendRecovered narrate transitions of the managed workload's
// availability so operators see backend readiness changes in
// `kubectl describe`. The FailClosedEnabled / FailOpenRestored pair
// narrates transitions of the spec.integration.failOpen toggle —
// explicitly fail-closed is loud because the cache then becomes a serving
// dependency.
const (
	eventReasonBackendDegraded   = "BackendDegraded"
	eventReasonBackendRecovered  = "BackendRecovered"
	eventReasonFailClosedEnabled = "FailClosedEnabled"
	eventReasonFailOpenRestored  = "FailOpenRestored"
)

// Condition reasons published on a CacheBackend's Ready condition. Stable
// strings so consumers (the CacheIndex poller, the future readiness gate
// that watches lastEventAt, operator dashboards) can switch on reason
// instead of regexing the message.
const (
	// conditionReasonExternalEndpointAccepted is set when an External
	// CacheBackend's spec.endpoint is non-empty: admission accepted the
	// operator-supplied endpoint and we trust it without probing
	// reachability. A future enhancement could degrade Ready on a
	// connection-probe failure, but that's out of scope for the
	// passthrough adapter today (fail-soft, trust the operator).
	conditionReasonExternalEndpointAccepted = "ExternalEndpointAccepted"
	// conditionReasonExternalEndpointMissing is set defensively when an
	// External CacheBackend has spec.endpoint empty. Admission rejects
	// this at the validating webhook, so reaching this branch means a CR
	// already in etcd from before the webhook was installed.
	conditionReasonExternalEndpointMissing = "ExternalEndpointMissing"
	// conditionReasonExternalEndpointInvalid is set defensively when an
	// External CacheBackend has a non-empty spec.endpoint that fails the
	// shared shape check (bad scheme, no port, embedded whitespace,
	// unbracketed IPv6, …). Current admission rejects all of these at
	// the validating webhook; the reason is reachable only for a CR
	// stored before the relevant shape rule shipped. Status reflects
	// the gap loudly rather than advertising the malformed value as
	// Ready=True (which would let the pod webhook then inject an
	// LMCACHE_REMOTE_URL the engine connector refuses at startup —
	// turning a cache misconfiguration into a serving outage).
	conditionReasonExternalEndpointInvalid = "ExternalEndpointInvalid"
)

// CacheBackendReconciler reconciles a CacheBackend object.
type CacheBackendReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Log      logr.Logger
	Recorder events.EventRecorder
	// APIReader is an uncached live client used for the per-reconcile pod
	// List that backs status.matchedEnginePods. The cached client would
	// register a Pod informer with controller-runtime, which the locked
	// design explicitly rejected (would watch all pods cluster-wide
	// just to count per-CR; the per-reconcile namespaced live List is
	// cheaper at the cluster sizes we target). Production wiring passes
	// mgr.GetAPIReader(); tests that don't exercise the
	// matchedEnginePods writer can leave it nil (a nil APIReader makes
	// refreshMatchedEnginePods fall through to the embedded
	// client.Client so existing fake-client tests still work).
	APIReader client.Reader
	// Registry resolves the runtime adapter to use for a CacheBackend. Nil
	// uses [adapterruntime.DefaultRegistry] — set explicitly only in tests
	// that need a custom adapter set.
	Registry *adapterruntime.Registry
	// MatchedEnginePodsRequeueInterval overrides the self-requeue cadence
	// that keeps status.matchedEnginePods fresh between unrelated reconcile
	// triggers. Zero means "use [DefaultMatchedEnginePodsRequeueInterval]".
	// Production wiring leaves this zero; envtest suites override to a
	// shorter value so they don't bake the 30s production delay into
	// per-test runtime.
	MatchedEnginePodsRequeueInterval time.Duration
}

// matchedEnginePodsRequeueInterval returns the effective cadence for this
// reconciler, honoring the per-reconciler override and falling back to
// [DefaultMatchedEnginePodsRequeueInterval].
func (r *CacheBackendReconciler) matchedEnginePodsRequeueInterval() time.Duration {
	if r.MatchedEnginePodsRequeueInterval > 0 {
		return r.MatchedEnginePodsRequeueInterval
	}
	return DefaultMatchedEnginePodsRequeueInterval
}

// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile drives a CacheBackend toward its desired state. External backends
// only mirror their configured endpoint to status; managed backends (LMCache
// in Phase 1) ask the registered runtime adapter for the cache-server pod
// spec + service spec, wrap them into a Deployment + Service the controller
// owns, optionally reconcile an HPA from spec.autoscaling, and publish the
// resolved endpoint.
//
// On every reconcile — including ones that return an apply error — transitions
// in the observed Ready condition (entering/leaving Ready=False/
// ReplicasUnavailable) and in the effective spec.integration.failOpen are
// emitted as Kubernetes Events. Events fire only on transitions that were
// actually persisted to the apiserver (patchStatus rolls back the in-memory
// mutation on patch failure), and never on steady state — so operators see
// backend outages and fail-closed opt-ins in `kubectl describe` without
// phantom or duplicate events.
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
	// Refresh status.matchedEnginePods regardless of dispatch outcome — the
	// pod-label snapshot is an observation about the engine fleet, not about
	// the cache-server workload dispatch manages, so an apply error must not
	// freeze it. Runs as its own Status().Patch (MergeFrom) so it never
	// fights the status writes dispatch already issued, and is fail-soft on
	// transient List/Patch errors so it never escalates a transient
	// apiserver hiccup into a Reconcile error.
	r.refreshMatchedEnginePods(ctx, &backend)
	// Self-requeue when there's matchedEnginePods work to keep doing on
	// the next tick:
	//
	//   - A non-empty engineSelector is configured. The cadence tracks
	//     pod birth/death between unrelated reconcile triggers. We
	//     deliberately don't Watch Pods (see refreshMatchedEnginePods
	//     godoc); the periodic self-requeue gives a bounded staleness
	//     without the watch's overhead.
	//   - The selector is gone but status.matchedEnginePods is still
	//     populated. That's the operator-just-removed-the-selector +
	//     clear-patch-failed case: without a requeue the stale printer-
	//     column value would persist forever (no Owned watch, no
	//     selector to drive the count to a new value). The retry tries
	//     the clear again on the next tick.
	//
	// Don't shorten an already-shorter RequeueAfter dispatch may have set
	// (none today, but preserve the contract): only fill in the cadence
	// when no other requeue is pending.
	if needsRequeue := (backend.Spec.EngineSelector != nil && len(backend.Spec.EngineSelector.MatchLabels) > 0) ||
		backend.Status.MatchedEnginePods != nil; needsRequeue {
		if result.RequeueAfter == 0 {
			result.RequeueAfter = r.matchedEnginePodsRequeueInterval()
		}
	}
	// Emit transitions whenever dispatch published a status change, even on
	// an apply-error reconcile: the status path runs independently of apply
	// success (so apply churn doesn't freeze the user-visible Ready
	// condition), and the next reconcile's snapshot is taken from the
	// *post-patch* CR. Gating emission on err==nil would mean a transition
	// into Ready=False/ReplicasUnavailable observed during an apply-error
	// pass is permanently lost. emitTransitionEvents only fires when before
	// != after, so an error path that didn't change status (e.g. early
	// return before the status patch) emits nothing.
	r.emitTransitionEvents(&backend, before)
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

	// StatefulSet (per-replica PVCs via volumeClaimTemplates) is a later
	// module. Phase 1 manages a Deployment only.
	if backend.Spec.DeploymentKind == cachev1alpha1.CacheBackendDeploymentKindStatefulSet {
		logger.V(1).Info("StatefulSet deploymentKind not yet supported; skipping",
			"namespace", backend.Namespace, "name", backend.Name)
		return ctrl.Result{}, r.reconcileUnmanaged(ctx, backend)
	}

	registry := r.Registry
	if registry == nil {
		registry = adapterruntime.DefaultRegistry()
	}
	runtimeID := adapterruntime.ResolveRuntimeID(backend)
	adapter, err := registry.Select(runtimeID, backend)
	if err != nil {
		// No adapter knows how to wire this (runtime, backend) pair. The
		// admission validator (M6/C7) rejects this at write time, so
		// reaching this branch means a CR already in etcd from before the
		// webhook was installed (or with a registry that has since
		// shrunk). Shed any previously managed workload and log.
		logger.V(1).Info("no runtime adapter for backend",
			"runtime", runtimeID, "type", backend.Spec.Type,
			"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
		return ctrl.Result{}, r.reconcileUnmanaged(ctx, backend)
	}

	return r.reconcileManaged(ctx, logger, backend, adapter)
}

// reconcileExternal mirrors a pre-existing backend's configured endpoint to
// status and marks the backend Ready: there is no Service to wait on, so
// admission acceptance of spec.endpoint is the only readiness signal the
// controller has. The Ready condition flips to True in lock step so the
// Ready printcolumn (kubectl get cb) reflects the accepted endpoint for
// External CRs that admission has already accepted.
//
// Three terminal states, each driven by the SAME shape rule the
// validating webhook applies on CREATE/UPDATE — so the reconciler is
// honest about CRs that were stored under a laxer rule set:
//
//   - spec.endpoint empty                  → Ready=False/ExternalEndpointMissing
//   - spec.endpoint set but malformed      → Ready=False/ExternalEndpointInvalid
//   - spec.endpoint set and well-formed    → Ready=True/ExternalEndpointAccepted
//
// The "invalid" branch matters because admission's shape rule tightened
// over the life of the CRD (added port-required, bracket-required-IPv6,
// no-embedded-whitespace, etc. as we learned what the engine connector
// rejects). A pre-existing stored CR carrying e.g. `https://...` or a
// portless host would otherwise be marked Ready=True/ExternalEndpointAccepted
// here; the pod webhook would then read spec.endpoint, prepend `lm://`,
// and inject a URL the engine can't parse — turning a cache
// misconfiguration into a serving outage. Publishing Ready=False with a
// specific reason names the gap, and the pod webhook short-circuits on
// the same check (returns no-injection, fail-open).
func (r *CacheBackendReconciler) reconcileExternal(ctx context.Context, backend *cachev1alpha1.CacheBackend) error {
	return r.patchStatus(ctx, backend, func() {
		// TrimSpace before every decision. Admission rejects a
		// whitespace-only endpoint at write time, but a pre-existing
		// CR in etcd from before admission was installed can still
		// carry one. Publishing the trimmed value as status.endpoint
		// means the pod webhook's `endpoint == ""` short-circuit
		// naturally catches whitespace too without a second TrimSpace
		// at the consumer.
		endpoint := strings.TrimSpace(backend.Spec.Endpoint)
		backend.Status.Endpoint = endpoint
		backend.Status.Capacity = ""
		backend.Status.ObservedGeneration = backend.Generation

		// Decide the Ready reason + message in one place so the
		// Progressing/Ready conditions stay in lockstep.
		var (
			readyStatus = metav1.ConditionFalse
			readyReason string
			readyMsg    string
		)
		switch {
		case endpoint == "":
			readyReason = conditionReasonExternalEndpointMissing
			readyMsg = "spec.endpoint is empty; set it to the address of the pre-existing backend"
		default:
			// Use the same shape validator the admission webhook
			// uses; surface the helper's message verbatim so an
			// operator running kubectl describe sees the same
			// shape complaint they would get on a fresh kubectl
			// apply. Pass the raw spec.Endpoint so the helper
			// applies its own TrimSpace consistently.
			if err := adapterruntime.ValidateLMCacheEndpoint(backend.Spec.Endpoint); err != nil {
				readyReason = conditionReasonExternalEndpointInvalid
				readyMsg = "spec." + err.Error()
				break
			}
			readyStatus = metav1.ConditionTrue
			readyReason = conditionReasonExternalEndpointAccepted
			readyMsg = "External endpoint accepted; controller does not provision cache pods for External backends"
		}

		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeProgressing,
			Status:             metav1.ConditionFalse,
			Reason:             readyReason,
			Message:            "External backends complete admission immediately",
			ObservedGeneration: backend.Generation,
		})
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeReady,
			Status:             readyStatus,
			Reason:             readyReason,
			Message:            readyMsg,
			ObservedGeneration: backend.Generation,
		})
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
		backend.Status.Capacity = ""
		backend.Status.ObservedGeneration = backend.Generation
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeReady)
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeProgressing)
	})
}

// reconcileManaged renders the cache-server PodSpec + Service via the runtime
// adapter, wraps them into a Deployment + Service owned by the CR, and
// publishes the resolved endpoint to status.
//
// Apply drives desired state; status reflects observed state. The two must not
// block each other: if a desired-state write fails (e.g. a transient API-server
// conflict or a webhook rejection), we still publish status from whatever the
// live Deployment reports, so the user-visible CR field is never held hostage
// to apply churn. Any apply error is surfaced after the status pass so
// controller-runtime requeues.
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

	// Skip Service + HPA when applyDeployment failed. The HPA targets the
	// Deployment by name, so running it after a foreign-ownership failure
	// could scale another controller's workload; the Service is independent
	// but pointless to expose alongside a Deployment we don't own. Status
	// observation still runs below (it has its own ownership guards) so the
	// CR isn't held hostage to apply churn.
	applyErr := r.applyDeployment(ctx, backend, dep)
	if applyErr == nil {
		if svcErr := r.applyService(ctx, backend, svc); svcErr != nil {
			applyErr = svcErr
		}
		if hpaErr := r.reconcileHPA(ctx, backend, dep); hpaErr != nil && applyErr == nil {
			applyErr = hpaErr
		}
	}

	var live appsv1.Deployment
	if err := r.Get(ctx, client.ObjectKeyFromObject(dep), &live); err != nil {
		if apierrors.IsNotFound(err) && applyErr != nil {
			// Apply failed before creating the Deployment, so there is no
			// observed state to publish — surface the apply error to requeue.
			return ctrl.Result{}, applyErr
		}
		// Either a transient Get error, or NotFound after a successful apply
		// (deleted out-of-band between apply and Get). Both must requeue;
		// silently reporting success here would freeze status at a stale
		// snapshot.
		return ctrl.Result{}, fmt.Errorf("get deployment %s/%s: %w", dep.Namespace, dep.Name, err)
	}
	// Never publish status derived from a foreign workload. The common case
	// is an AlreadyOwned collision during apply (applyErr is set; surface
	// it). The race case is that apply succeeded but the live Deployment's
	// controller ref was changed out-of-band between Update and this Get —
	// applyErr is nil, but we no longer own the object. Returning nil there
	// would silently mark the reconcile successful AND lose the owned-object
	// watch (no future event would re-trigger), so synthesize an explicit
	// error to requeue.
	if !metav1.IsControlledBy(&live, backend) {
		if applyErr != nil {
			return ctrl.Result{}, applyErr
		}
		return ctrl.Result{}, fmt.Errorf("deployment %s/%s lost controller reference after apply", dep.Namespace, dep.Name)
	}

	// Endpoint is derived from the *live* Service, not the desired one: if
	// applyService failed (Forbidden, conflict-budget exhausted, foreign
	// ownership, ...) we must not advertise an endpoint that doesn't exist,
	// has stale ports, or points at a Service we don't own. Empty endpoint
	// when the Service hasn't materialized or is owned by someone else.
	var liveSvc corev1.Service
	endpoint := ""
	if err := r.Get(ctx, client.ObjectKeyFromObject(svc), &liveSvc); err != nil {
		if !apierrors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("get service %s/%s: %w", svc.Namespace, svc.Name, err)
		}
	} else if metav1.IsControlledBy(&liveSvc, backend) {
		endpoint = serviceEndpoint(&liveSvc)
	}

	if err := r.updateManagedStatus(ctx, backend, endpoint, &live, applyErr == nil); err != nil {
		return ctrl.Result{}, err
	}

	if applyErr != nil {
		return ctrl.Result{}, applyErr
	}

	logger.V(1).Info("reconciled managed CacheBackend",
		"namespace", backend.Namespace, "name", backend.Name, "endpoint", endpoint)
	return ctrl.Result{}, nil
}

// buildDeployment wraps the adapter-rendered PodSpec into a Deployment the
// controller owns: ObjectMeta + labels + replicas + selector come from the
// CacheBackend identity, not the adapter.
func (r *CacheBackendReconciler) buildDeployment(backend *cachev1alpha1.CacheBackend, podSpec *corev1.PodSpec) *appsv1.Deployment {
	replicas := initialReplicas(backend)
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
//
// When an HPA owns scaling (spec.autoscaling set), the reconciler defers to the
// HPA's replica count rather than overwriting it — re-asserting replicas on
// every reconcile would fight the HPA and churn the rollout.
//
// Wrapped in RetryOnConflict because the kube Deployment controller writes
// Deployment.Status often during rollout; without retry, the Get/Update inside
// CreateOrUpdate races those writes and surfaces a 409 that aborts the
// reconcile pass and freezes CR status.
func (r *CacheBackendReconciler) applyDeployment(ctx context.Context, backend *cachev1alpha1.CacheBackend, desired *appsv1.Deployment) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
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
				// Preserve the HPA's writes — but clamp to the configured floor so
				// raising autoscaling.minReplicas doesn't briefly publish Ready
				// against the old smaller live count before the HPA catches up.
				preserved := *liveReplicas
				if floor := autoscalingFloor(backend.Spec.Autoscaling); preserved < floor {
					preserved = floor
				}
				dep.Spec.Replicas = &preserved
			}
			return controllerutil.SetControllerReference(backend, dep, r.Scheme)
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("apply deployment %s/%s: %w", desired.Namespace, desired.Name, err)
	}
	return nil
}

// autoscalingFloor is the effective minReplicas value for the HPA — the
// user's setting, or the default floor when unset. Mirrors the resolution
// buildHPA does so the reconciler and the HPA agree on the lower bound.
func autoscalingFloor(spec *cachev1alpha1.CacheBackendAutoscalingSpec) int32 {
	if spec == nil {
		return defaultHPAMinReplicas
	}
	if spec.MinReplicas != nil {
		return *spec.MinReplicas
	}
	return defaultHPAMinReplicas
}

// applyService creates or updates the backend Service idempotently, owned by the CR.
// Type, selector, and ports are reconciled (so out-of-band drift is corrected); the
// rendered ports carry Protocol=TCP so they match the API-server-defaulted object,
// and the allocated fields (clusterIP, nodePort) live in separate fields we never
// touch — so reconciling ports does not churn through the Owns(Service) watch.
func (r *CacheBackendReconciler) applyService(ctx context.Context, backend *cachev1alpha1.CacheBackend, desired *corev1.Service) error {
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: desired.Name, Namespace: desired.Namespace}}
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
			svc.Labels = desired.Labels
			svc.Spec.Type = desired.Spec.Type
			svc.Spec.Selector = desired.Spec.Selector
			svc.Spec.Ports = desired.Spec.Ports
			return controllerutil.SetControllerReference(backend, svc, r.Scheme)
		})
		return err
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
// Volumes are adapter-owned (per [adapterruntime.KVCacheRuntimeAdapter] — the
// adapter fills PodSpec.Containers + PodSpec.Volumes) and are not
// API-server-defaulted in a Deployment template, so the reconciler always
// propagates them from desired. That corrects two cases the previous
// gated-on-container-set-change behaviour missed:
//   - Out-of-band volume drift on a steady-state Deployment.
//   - An adapter update that adds/changes pod-level volumes without changing
//     the container set.
//
// The current LMCache adapter renders no pod-level volumes, so this is a
// no-op for the steady-state path; on an in-place upgrade from the previous
// colocated all-in-one rendering it still prunes the stale cache-home + shm
// volumes that container left behind.
func reconcileManagedPodSpec(live *corev1.PodSpec, desired *corev1.PodSpec) {
	reconcileManagedContainer(live, desired)
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
//
// Containers in live whose names are not in desired are dropped — this is the
// upgrade path from a previous colocated all-in-one rendering (container
// name "vllm") to the standalone topology (container name "lmcache-server"):
// an in-place upgrade must replace the old managed container, not stack the
// new one alongside it. We never drop containers that match a desired name
// (we only update their managed fields), so a Deployment carrying sidecars
// in addition to the managed container loses the sidecars — sidecars were
// not supported in the previous rendering and remain unsupported here.
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
				// TargetPort lookup or hide a probe regression. Resources
				// likewise differ by profile (e.g. GPU vs CPU canary) and
				// aren't API-server-defaulted, so reconciling them is
				// churn-free.
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

// cleanupOwnedWorkload best-effort deletes the Deployment + Service + HPA this
// CR owns, used when a backend is no longer a managed Deployment (type/kind
// changed). Normal CR deletion is handled by owner-reference garbage
// collection; this covers the in-place mutation case where the CR itself
// still exists.
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

// updateManagedStatus derives the Ready + Progressing conditions from the Deployment and patches status only when it changes.
//
// applyOK is the convergence signal from reconcileManaged: when apply failed,
// the live Deployment we read may still reflect a *prior* CR generation, so
// advancing Status.ObservedGeneration to the current CR generation would tell
// clients the controller has caught up when it hasn't. The published
// observedGeneration therefore stays at its prior value until apply succeeds
// for the current generation; the Ready and Progressing conditions carry the
// same generation so callers can tell which spec the observation belongs to.
//
// status.capacity is intentionally left empty here: the field is present on
// the type for forward-compat, but the standalone LMCache server pod has no
// data volume the controller can attach a PVC to today, so reporting a
// requested PVC size as "provisioned capacity" would mislead operators.
// Populating it is left to the follow-up that wires storage end-to-end.
func (r *CacheBackendReconciler) updateManagedStatus(ctx context.Context, backend *cachev1alpha1.CacheBackend, endpoint string, dep *appsv1.Deployment, applyOK bool) error {
	readyStatus, reason, message := managedReadiness(backend, dep)
	progressingStatus, progressingReason, progressingMessage := progressingFromReady(readyStatus, reason, message)
	publishedGen := backend.Status.ObservedGeneration
	if applyOK {
		publishedGen = backend.Generation
	}
	return r.patchStatus(ctx, backend, func() {
		backend.Status.Endpoint = endpoint
		backend.Status.Capacity = ""
		backend.Status.ObservedGeneration = publishedGen
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeReady,
			Status:             readyStatus,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: publishedGen,
		})
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeProgressing,
			Status:             progressingStatus,
			Reason:             progressingReason,
			Message:            progressingMessage,
			ObservedGeneration: publishedGen,
		})
	})
}

// Ready condition reasons published by managedReadiness. Stable strings so
// downstream consumers (transition-event predicates, the Progressing
// derivation, dashboards) can switch on reason instead of regexing the
// message.
const (
	conditionReasonBackendReady        = "BackendReady"
	conditionReasonScaledToZero        = "ScaledToZero"
	conditionReasonRolloutInProgress   = "RolloutInProgress"
	conditionReasonReplicasUnavailable = "ReplicasUnavailable"
)

// managedReadiness maps the Deployment's rollout state to the Ready
// condition (status + reason + message). Ready=True requires the Deployment
// to have observed its current generation and to have enough updated +
// available replicas, so a stale rollout (e.g. mid image change) is never
// reported Ready.
//
// When the CacheBackend is autoscaled the HPA owns the desired replica count,
// so the comparison target is the live Deployment's spec.replicas (which the
// HPA writes) rather than the CacheBackend's spec.replicas (which is ignored
// in that mode). This keeps Ready accurate when an HPA decides to run more
// pods than spec.replicas, and avoids a false ScaledToZero when spec.replicas
// happens to be 0 with autoscaling configured.
func managedReadiness(backend *cachev1alpha1.CacheBackend, dep *appsv1.Deployment) (metav1.ConditionStatus, string, string) {
	want := desiredReplicas(backend, dep)

	// A backend scaled to zero is not serving; never report it Ready.
	if want == 0 {
		return metav1.ConditionFalse, conditionReasonScaledToZero, "backend scaled to zero replicas"
	}

	rolledOut := dep.Status.ObservedGeneration >= dep.Generation
	switch {
	case rolledOut && dep.Status.UpdatedReplicas >= want && dep.Status.AvailableReplicas >= want:
		return metav1.ConditionTrue, conditionReasonBackendReady,
			fmt.Sprintf("%d/%d replicas available", dep.Status.AvailableReplicas, want)
	case !rolledOut || dep.Status.UpdatedReplicas < want:
		return metav1.ConditionFalse, conditionReasonRolloutInProgress,
			fmt.Sprintf("%d/%d replicas updated", dep.Status.UpdatedReplicas, want)
	default:
		return metav1.ConditionFalse, conditionReasonReplicasUnavailable,
			fmt.Sprintf("%d/%d replicas available", dep.Status.AvailableReplicas, want)
	}
}

// progressingFromReady derives the Progressing condition from the Ready
// condition's outcome. A Ready=True backend has converged (Progressing=False,
// Reason=Synced). A Ready=False backend that's actively converging
// (RolloutInProgress) flips Progressing=True; one that has reached a stable
// terminal state (ScaledToZero) or stable failure (ReplicasUnavailable) is
// NOT progressing — no rollout is in motion.
func progressingFromReady(readyStatus metav1.ConditionStatus, reason, message string) (metav1.ConditionStatus, string, string) {
	if readyStatus == metav1.ConditionTrue {
		return metav1.ConditionFalse, "Synced", "rendered children match desired state"
	}
	switch reason {
	case conditionReasonRolloutInProgress:
		return metav1.ConditionTrue, reason, message
	case conditionReasonReplicasUnavailable:
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

// initialReplicas picks the Deployment's initial replica count. With
// autoscaling configured, spec.autoscaling.minReplicas is the source of truth
// (defaulting to 1 when unset), so the workload comes up at or above the HPA
// floor on first apply instead of starting at 1 and waiting for the HPA to
// patch it. Without autoscaling, spec.replicas wins (default 1).
func initialReplicas(backend *cachev1alpha1.CacheBackend) int32 {
	if backend.Spec.Autoscaling != nil {
		if backend.Spec.Autoscaling.MinReplicas != nil {
			return *backend.Spec.Autoscaling.MinReplicas
		}
		return 1
	}
	if backend.Spec.Replicas != nil {
		return *backend.Spec.Replicas
	}
	return 1
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
		// Roll back the in-memory mutation. emitTransitionEvents is called on
		// every Reconcile return and compares the pre-reconcile snapshot to
		// backend.Status; leaving the un-persisted mutation in place would
		// fire a Warning/Normal event for a transition the apiserver never
		// observed, and the same transition would fire again on the next
		// reconcile (when the patch retries) — producing duplicate / phantom
		// events under status-subresource conflict / RBAC / API failures.
		backend.Status = before.Status
		return fmt.Errorf("patch CacheBackend status %s/%s: %w", backend.Namespace, backend.Name, err)
	}
	return nil
}

// refreshMatchedEnginePods refreshes status.matchedEnginePods from the live
// pod-label set in the CacheBackend's namespace. Runs once per reconcile and
// only touches the matchedEnginePods sub-field (via Status().Patch with
// MergeFrom) so it coexists cleanly with the other status writers in this
// reconciler and with any future status writers (e.g. an index-participation
// projector) that touch different sub-fields.
//
// Cadence-by-reconcile, not real-time: counts via a single namespaced
// client.List with the engineSelector — there is no Pod watch, and pod
// births/deaths between reconciles are not reflected until the next pass.
// To keep the count from going indefinitely stale between unrelated
// reconcile triggers, the Reconcile path sets `result.RequeueAfter =
// matchedEnginePodsRequeueInterval` whenever the CR has a non-empty
// EngineSelector, giving the field a bounded staleness without paying
// for a Pod informer. The real-time per-pod signal lives on the engine
// pods themselves (the `InjectedByCacheBackend` Event the
// engine-pod-events controller emits on every annotated pod); this
// status field answers the cluster-wide "is anyone connected at all?"
// question.
//
// Selector resolution mirrors the mutating webhook's policy: a nil or
// empty MatchLabels matches nothing (a broad selector at admission time
// would silently claim every pod). A CR with no selector therefore
// reports no count — and a CR that previously had one and just lost it
// gets its prior value cleared so the printer column doesn't advertise a
// stale match for a CR that no longer claims engine pods.
//
// Fail-soft semantics:
//   - List error → log + skip the tick; keep the existing value.
//   - Status patch error → roll back the in-memory mutation so the rest of
//     the reconcile (transition events, log fields) sees only what the
//     apiserver actually persisted.
//
// Never returns an error: the matchedEnginePods refresh must not escalate
// a transient observation failure into a Reconcile error that retries the
// rest of the reconcile machinery unnecessarily.
func (r *CacheBackendReconciler) refreshMatchedEnginePods(ctx context.Context, backend *cachev1alpha1.CacheBackend) {
	before := backend.DeepCopy()

	sel := backend.Spec.EngineSelector
	if sel == nil || len(sel.MatchLabels) == 0 {
		if backend.Status.MatchedEnginePods == nil {
			return
		}
		backend.Status.MatchedEnginePods = nil
	} else {
		matcher := labels.SelectorFromSet(sel.MatchLabels)
		var pods corev1.PodList
		// Pin the pod read to the uncached APIReader so the controller-
		// runtime cache does NOT register a Pod informer on first use —
		// otherwise the manager would watch every pod cluster-wide just to
		// keep this snapshot count fresh, which the locked design
		// explicitly rejected. Fall back to the cached client only when
		// APIReader is unset (test wiring without a real APIReader); the
		// reconciler still functions, it just uses the cache.
		reader := client.Reader(r.APIReader)
		if reader == nil {
			reader = r.Client
		}
		if err := reader.List(ctx, &pods,
			client.InNamespace(backend.Namespace),
			client.MatchingLabelsSelector{Selector: matcher},
		); err != nil {
			log.FromContext(ctx).V(1).Info("matchedEnginePods refresh skipped: pod list failed",
				"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
			return
		}
		count := int32(len(pods.Items))
		if backend.Status.MatchedEnginePods != nil && *backend.Status.MatchedEnginePods == count {
			return
		}
		backend.Status.MatchedEnginePods = &count
	}

	if err := r.Status().Patch(ctx, backend, client.MergeFrom(before)); err != nil {
		backend.Status.MatchedEnginePods = before.Status.MatchedEnginePods
		log.FromContext(ctx).V(1).Info("matchedEnginePods refresh: status patch failed",
			"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
	}
}

// stateSnapshot captures the prior-status fields that drive transition events.
// degraded / ready are derived from the Ready condition (status + reason) so
// the transition logic doesn't need a separate phase enum on status. failOpen
// is the previously-echoed integration.failOpen (nil ⇒ never observed ⇒
// treated as the API default of true, so an initial apply with
// failOpen=false correctly fires the warning).
type stateSnapshot struct {
	ready    bool
	degraded bool
	failOpen bool
}

// snapshotState captures the prior status values that drive transition events.
// Called at the top of Reconcile before any mutation so emitTransitionEvents
// has a stable baseline to compare the post-reconcile state against.
func snapshotState(cb *cachev1alpha1.CacheBackend) stateSnapshot {
	return stateSnapshot{
		ready:    isReady(cb),
		degraded: isDegraded(cb),
		failOpen: statusFailOpen(cb.Status.FailOpen),
	}
}

// isReady reports whether the Ready condition is currently True.
func isReady(cb *cachev1alpha1.CacheBackend) bool {
	c := meta.FindStatusCondition(cb.Status.Conditions, conditionTypeReady)
	return c != nil && c.Status == metav1.ConditionTrue
}

// isDegraded reports whether the Ready condition is False with the
// "replicas unavailable" reason — the post-rollout failure shape that the
// BackendDegraded event narrates. A rollout in progress or a scaled-to-zero
// backend is not degraded.
func isDegraded(cb *cachev1alpha1.CacheBackend) bool {
	c := meta.FindStatusCondition(cb.Status.Conditions, conditionTypeReady)
	return c != nil && c.Status == metav1.ConditionFalse && c.Reason == conditionReasonReplicasUnavailable
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
// Ready condition (entering/leaving the Ready=False/ReplicasUnavailable
// shape) or the effective failOpen toggle. By design events fire only
// on transitions — never on steady state — so a Ready backend reconciling
// every few seconds does not flood the event stream.
//
//   - Entering Ready=False/ReplicasUnavailable → Warning BackendDegraded.
//   - Leaving Ready=False/ReplicasUnavailable for Ready=True → Normal BackendRecovered.
//   - failOpen flipped true → false → Warning FailClosedEnabled (the cache
//     becomes a serving dependency; advanced opt-in).
//   - failOpen flipped false → true → Normal FailOpenRestored.
func (r *CacheBackendReconciler) emitTransitionEvents(cb *cachev1alpha1.CacheBackend, before stateSnapshot) {
	if r.Recorder == nil {
		return
	}
	after := snapshotState(cb)

	if !before.degraded && after.degraded {
		r.Recorder.Eventf(cb, nil, corev1.EventTypeWarning, eventReasonBackendDegraded, eventReasonBackendDegraded,
			"cache backend is degraded: %s", degradedMessage(cb))
	}
	if before.degraded && after.ready {
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
// managedReadiness) so the BackendDegraded event names the failure mode
// (e.g. "1/3 replicas available") instead of just announcing the transition.
func degradedMessage(cb *cachev1alpha1.CacheBackend) string {
	if c := meta.FindStatusCondition(cb.Status.Conditions, conditionTypeReady); c != nil && c.Message != "" {
		return c.Message
	}
	return "backend workload not available"
}

// SetupWithManager sets up the controller with the Manager. Owns(Deployment)
// guarantees that a child's status flipping (e.g. AvailableReplicas dropping
// to zero) re-triggers a Reconcile so emitTransitionEvents observes the
// change; the HPA is owned so the controller re-reconciles when the
// autoscaler updates spec.replicas or its own status.
func (r *CacheBackendReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("cachebackend-controller")
	}
	if r.APIReader == nil {
		// Default to the manager's uncached APIReader so production
		// wiring doesn't have to thread it explicitly, AND envtest
		// integration tests that boot a real manager still skip the
		// Pod informer per the locked design (the test setup just
		// passes Client, not APIReader).
		r.APIReader = mgr.GetAPIReader()
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.CacheBackend{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Complete(r)
}

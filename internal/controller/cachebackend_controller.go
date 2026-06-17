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
// Ready reports whether the managed backend workload is currently serving
// (gated by the KV-event readiness gate — see evaluateKVEventReadiness).
// Progressing reports whether the controller is still driving the live state
// toward the desired state (template render, child apply, rollout in flight,
// awaiting first KV event). Degraded reports a terminal unhealthy state.
// Ready + Progressing together tell a still-converging backend
// (Ready=False, Progressing=True) apart from a stuck/degraded one
// (Ready=False, Progressing=False); Degraded names the specific failure.
const (
	conditionTypeReady       = "Ready"
	conditionTypeProgressing = "Progressing"
	// Degraded is published alongside Ready. It is True only when the
	// backend is in a genuinely degraded terminal state (rolled out but
	// replicas unavailable, or the workload is Available but no KV events
	// observed within firstEventTimeout).
	conditionTypeDegraded = "Degraded"
)

// KV-event readiness gate.
const (
	// annotationRequireKVEvents opts a CacheBackend OUT of the KV-event
	// readiness gate when set exactly to "false". Absent or any other value
	// leaves the gate enabled (default-on). It is a per-CR annotation rather
	// than a spec field because it is an alpha soft-rollout knob meant to be
	// retired once the gate is trusted (a spec field is harder to retract),
	// and per-CR so one backend can opt out without affecting others.
	annotationRequireKVEvents = "inferencecache.io/require-kv-events"

	// defaultFirstEventTimeout is the fallback when
	// spec.integration.firstEventTimeout is unset and the API-server default
	// ("5m") was not applied (e.g. fake-client unit tests). Mirrors the
	// +kubebuilder:default on the field.
	defaultFirstEventTimeout = 5 * time.Minute

	// Ready/Degraded condition reasons set by the gate. These double as the
	// Event reasons emitted on the corresponding transitions.
	reasonAwaitingFirstKVEvent = "AwaitingFirstKVEvent"
	reasonKVEventsObserved     = "KVEventsObserved"
	reasonNoKVEventsObserved   = "NoKVEventsObserved"

	// reasonNotDegraded is the Degraded=False condition reason.
	reasonNotDegraded = "NotDegraded"
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
	// eventReasonInvalidStorageConfiguration is the Warning fired when a
	// persistent (spec.storage.pvc) backend asks for more than one replica —
	// a single ReadWriteOnce PVC cannot be multi-attached. Mirrors the
	// Ready=False reason so a transition into the gated state is narrated once.
	eventReasonInvalidStorageConfiguration = conditionReasonInvalidStorageConfiguration
	// eventReasonOrphanedPVCRetained is the Warning fired when spec.storage.pvc
	// is removed but the owner-referenced PVC is retained (adopt-and-keep) so
	// the operator knows the storage was kept, not silently dropped. Event
	// aggregation collapses the per-reconcile repeats into one event.
	eventReasonOrphanedPVCRetained = "OrphanedPVCRetained"
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

	// ProbeClient is the controller's POST /probe wrapper.
	// Nil disables the functional-probe gate — the FunctionalProbeOK
	// condition is not written and the Ready gate composition is unchanged
	// from Stage 1's KV-event-only gate. Production wiring always sets it
	// (cmd/controller/main.go); fake-client unit tests that don't exercise
	// the probe gate leave it nil; envtest integration tests inject a
	// httptest-bound client.
	ProbeClient *ProbeClient

	// ProbeRateLimit caps the probe call frequency per CacheBackend. Zero
	// means "use [DefaultProbeRateLimit]" (~30s, matching the ticket's
	// "max once per CacheBackend per ~30s" requirement). Tests override
	// to keep runtime down.
	ProbeRateLimit time.Duration

	// probeLimiter is the per-(namespace, name) "last successful probe call"
	// cache backing the rate limit. Embedded value, so the zero-value
	// sync.Map inside is usable from struct construction — the rate-limit
	// gate works on the first reconcile without explicit initialization.
	probeLimiter probeRateLimiter

	// MinServerRestartCascadeInterval overrides the rate-limit window for
	// the cache-server restart cascade. Zero means "use
	// [DefaultMinServerRestartCascadeInterval]". Production wiring leaves
	// this zero; envtest / unit tests shrink the window to keep per-test
	// runtime cheap.
	MinServerRestartCascadeInterval time.Duration

	// serverInstanceCascade tracks the last cascade-restart time per
	// backend so the rate-limit window is enforced in-process. Lazily
	// initialized in SetupWithManager AND defensively in
	// reconcileServerInstance (the latter so unit tests that bypass
	// SetupWithManager get a working reconciler).
	serverInstanceCascade *serverInstanceCascade
}

// probeRateLimit returns the effective rate-limit for the functional-probe
// gate, honoring the per-reconciler override and falling back to
// [DefaultProbeRateLimit].
func (r *CacheBackendReconciler) probeRateLimit() time.Duration {
	if r.ProbeRateLimit > 0 {
		return r.ProbeRateLimit
	}
	return DefaultProbeRateLimit
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
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch
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
		if apierrors.IsNotFound(err) {
			// The CR was deleted between the watch event and this reconcile.
			// Drop the per-backend rate-limit slot so a long-running
			// controller against a churning fleet doesn't accumulate stale
			// sync.Map entries forever. Safe to call unconditionally — the
			// helper no-ops if the key was never recorded.
			r.probeLimiter.forget(req.NamespacedName.String())
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
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
	// Requeue at the SOONER of the matched-pods cadence and any window
	// dispatch already scheduled. The KV-event gate sets a multi-minute
	// RequeueAfter while AwaitingFirstKVEvent (up to firstEventTimeout); taking
	// the min keeps the matched-pods refresh on its cadence instead of letting
	// the gate window suppress it — otherwise the operator-facing Matched
	// column would go stale for up to firstEventTimeout during the exact "no
	// engine pods attached" diagnosis path. The gate recomputes elapsed on
	// every reconcile, so a shorter requeue only lands its Degraded flip at
	// most one cadence after the deadline.
	if needsRequeue := (backend.Spec.EngineSelector != nil && len(backend.Spec.EngineSelector.MatchLabels) > 0) ||
		backend.Status.MatchedEnginePods != nil; needsRequeue {
		cadence := r.matchedEnginePodsRequeueInterval()
		if result.RequeueAfter == 0 || cadence < result.RequeueAfter {
			result.RequeueAfter = cadence
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
	// Adopt-and-keep visibility: if spec.storage.pvc is absent but we still own a
	// data PVC from a prior persistent generation, warn (once). Done here, before
	// the type/kind routing, so the warning fires regardless of how a now
	// storage-less backend is dispatched — External, StatefulSet/unmanaged, an
	// unsupported (runtime, type) pair, or managed-ephemeral all otherwise bypass
	// the per-path provisioning logic. The PVC itself is retained by its owner
	// reference (reclaimed only on CR delete); warnRetainedPVC no-ops when no
	// owned PVC exists, so this is a cheap cached Get on the common path.
	if backend.Spec.Storage == nil || backend.Spec.Storage.PVC == nil {
		r.warnRetainedPVC(ctx, backend)
	}

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
//
// External backends never enter the KV-event readiness gate, so the
// managed-only Degraded condition is cleared here. Two status fields are
// deliberately NOT reset: firstKVEventObservedAt (a monotonic write-once
// "ever observed a KV event" marker — clearing it would be ineffective
// anyway, since the preserved poller-owned lastEventAt would immediately
// re-satisfy the gate on a return to the managed path) and
// status.indexParticipation (owned by the CacheIndex poller, which converges
// it on its own; an External backend whose engine pods still report KV events
// legitimately keeps it).
func (r *CacheBackendReconciler) reconcileExternal(ctx context.Context, backend *cachev1alpha1.CacheBackend) error {
	// Wipe the in-memory cascade shadow + rate-limit timestamp
	// alongside the on-cluster status clearing below — see
	// clearServerInstanceLatchShadow for why a lingering shadow
	// across managed→External→managed would false-cascade.
	r.clearServerInstanceLatchShadow(backend)
	// Wipe the functional-probe rate-limit entry alongside removing
	// the FunctionalProbeOK condition below. Without this, a CR that
	// flips managed → External → managed within the 30s rate-limit
	// window would have a stale lastCalled timestamp on re-entry, so
	// the first reconcile under the managed path would skip the /probe
	// call (and, because we just removed the condition, find no prior
	// FunctionalProbeOK to re-apply downgrade off of) — Ready=True
	// would be published with no fresh probe verdict.
	r.probeLimiter.forget(client.ObjectKeyFromObject(backend).String())
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
		// Clear the cache-server-instance latch — External backends
		// have no controller-managed cache-server pods, and
		// cleanupOwnedWorkload above has just deleted any prior
		// managed Deployment. Leaving the latch set would expose a
		// stale UID to operators.
		backend.Status.ObservedServerInstance = ""
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
		// Clear any Degraded condition left over from a prior managed state;
		// External readiness is the endpoint check above, not the KV gate.
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeDegraded)
		// FunctionalProbeOK doesn't apply to External backends — the probe
		// gate only fires inside updateManagedStatus, and the controller
		// doesn't drive any cache-plane round-trip for an external endpoint
		// (the gate only applies to managed backends). Clear any
		// FunctionalProbeOK left over from a prior managed state so the
		// External-mode CR doesn't surface a stale condition that no
		// reconcile path will ever update.
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeFunctionalProbeOK)
		// Same for EngineKernelsHealthy — it is published only from the managed
		// path, so clear any left over from a prior managed state (the docs
		// state External backends publish only Ready + Progressing).
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeEngineKernelsHealthy)
	})
}

// reconcileUnmanaged sheds any previously owned workload and clears managed status
// for a backend this module no longer provisions (unsupported runtime/backend or
// deferred kind). The managed conditions are removed; firstKVEventObservedAt and
// status.indexParticipation are left as-is (see reconcileExternal's comment — the
// latch is a monotonic marker and indexParticipation is poller-owned).
func (r *CacheBackendReconciler) reconcileUnmanaged(ctx context.Context, backend *cachev1alpha1.CacheBackend) error {
	if err := r.cleanupOwnedWorkload(ctx, backend); err != nil {
		return err
	}
	// Wipe the in-memory cascade shadow + rate-limit timestamp
	// alongside the on-cluster status clearing below.
	r.clearServerInstanceLatchShadow(backend)
	// Wipe the functional-probe rate-limit entry alongside removing
	// the FunctionalProbeOK condition below. Same reasoning as in
	// reconcileExternal — without this, a managed → Unmanaged →
	// managed cycle inside the 30s rate-limit window would suppress
	// the first /probe call on re-entry and publish Ready=True with
	// no fresh probe verdict.
	r.probeLimiter.forget(client.ObjectKeyFromObject(backend).String())
	return r.patchStatus(ctx, backend, func() {
		backend.Status.Endpoint = ""
		backend.Status.Capacity = ""
		// Clear the cache-server-instance latch — cleanupOwnedWorkload
		// above has just deleted any prior managed Deployment and we
		// no longer provision one, so a retained UID would advertise
		// a stale identifier.
		backend.Status.ObservedServerInstance = ""
		backend.Status.ObservedGeneration = backend.Generation
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeReady)
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeProgressing)
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeDegraded)
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeFunctionalProbeOK)
		// EngineKernelsHealthy is a managed-path-only condition; clear any left
		// over so an unmanaged CR doesn't carry a stale kernel verdict.
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeEngineKernelsHealthy)
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
	resolved, err := adapter.ResolveCacheServer(backend)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("resolve cache server for %s/%s: %w", backend.Namespace, backend.Name, err)
	}
	if resolved == nil || resolved.PodSpec == nil || resolved.Service == nil {
		// An adapter that genuinely needs no cache-server (e.g. an
		// engine-colocated backend) is a valid future case. For Phase 1 it
		// shouldn't happen for managed types — surface as unmanaged.
		logger.V(1).Info("adapter rendered no cache-server; treating as unmanaged",
			"namespace", backend.Namespace, "name", backend.Name)
		return ctrl.Result{}, r.reconcileUnmanaged(ctx, backend)
	}
	podSpec := resolved.PodSpec
	svcSpec := resolved.Service

	// Persistent storage: provision a PVC and mount it when the adapter declares
	// a data volume AND the operator asked for PVC-backed storage. The adapter
	// owns the volume name + mount path (it knows where its data lives); the
	// controller owns PVC identity, owner-ref GC, and the mutable-field reconcile.
	capacity := ""
	if resolved.DataVolume != nil && backend.Spec.Storage != nil && backend.Spec.Storage.PVC != nil {
		// Multi-replica gate: a single ReadWriteOnce PVC cannot be multi-attached.
		// Refuse to provision the workload until the operator scales to 1 or
		// drops spec.storage.pvc. The pod is not created in this state.
		if multiReplicaRequested(backend) {
			return r.reconcileInvalidStorage(ctx, backend)
		}
		pvc, pvcErr := r.applyPVC(ctx, logger, backend)
		if pvcErr != nil {
			return ctrl.Result{}, pvcErr
		}
		// Merge the PVC-backed volume + mount into the rendered cache-server pod
		// before it is wrapped into the Deployment. The apply path already
		// propagates podSpec.Volumes + container VolumeMounts to the live
		// Deployment (see reconcileManagedPodSpec), so no extra apply step is
		// needed.
		mountDataVolume(podSpec, resolved.DataVolume, pvc.Name)
		capacity = boundCapacity(pvc)
	}
	// (When spec.storage.pvc is absent the rendered pod carries no PVC volume, so
	// the workload reverts to ephemeral on the next rollout. The adopt-and-keep
	// Warning for a removed spec.storage.pvc is emitted in dispatch — it covers
	// every routing outcome, not just this managed path.)

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

	requeueAfter, statusErr := r.updateManagedStatus(ctx, backend, endpoint, capacity, &live, applyErr == nil)
	// Do NOT short-circuit on statusErr — the cascade is independent
	// recovery for stale engine sockets and must not be skipped just
	// because the unrelated managed-status patch (matchedEnginePods,
	// Ready / Progressing / Degraded conditions, capacity, …) hit a
	// transient conflict. The cascade has its own patchStatus path
	// for the latch field, gated separately. Capture the error and
	// return it AFTER the cascade has run.

	// Cache-server restart cascade: when the Ready cache-server pod
	// SERVER-INSTANCE IDENTIFIER changes (either a pod UID swap or a
	// restart-sum advance from an in-place kubelet-driven container
	// restart — see currentServerInstanceID's godoc for the shape),
	// cascade-restart every engine Deployment that was injected
	// against this backend so they re-establish their LMCache client
	// socket (the upstream LMServerConnector opens its TCP socket in
	// __init__ only and silently fails every subsequent PUT with EPIPE
	// after a server restart, until the engine pod itself rolls). Always
	// runs (even when applyErr != nil OR updateManagedStatus errored),
	// since the cascade is independent of whether THIS reconcile pass
	// made a successful apply or a successful unrelated status update:
	// a transient apply / status-write churn must not delay engine
	// recovery from a cache-server outage. A non-zero cascadeWait means
	// the rate-limit window suppressed the cascade; honor it on the
	// requeue so we retry exactly at the boundary.
	cascadeWait := r.reconcileServerInstance(ctx, logger, backend)
	if cascadeWait > 0 && (requeueAfter == 0 || cascadeWait < requeueAfter) {
		requeueAfter = cascadeWait
	}
	// Schedule an unconditional periodic re-poll of the cache-server
	// pod set on managed backends. Reason: an in-place container
	// restart (kubelet respawning a crashed cache-server container
	// without bumping pod.UID) does NOT change owned-Deployment status
	// counts, and the controller deliberately does not watch Pods
	// cluster-wide (see refreshMatchedEnginePods godoc). The
	// matched-engine-pods cadence above does not cover this case
	// either: when an operator removes spec.engineSelector after
	// engines were injected, len(matchedEnginePods)→0 and that
	// cadence stops firing, leaving in-place restarts unobservable
	// until something unrelated triggers a reconcile. Pinning a
	// floor at the rate-limit interval bounds the observation
	// latency for in-place restarts at one cadence (cheap: one
	// Pod List + one Deployment Get per backend per cadence).
	pollCadence := r.minServerRestartCascadeInterval()
	if requeueAfter == 0 || pollCadence < requeueAfter {
		requeueAfter = pollCadence
	}

	if applyErr != nil {
		// Return the error so controller-runtime's workqueue
		// rate-limiter requeues the reconcile. Per the
		// sigs.k8s.io/controller-runtime/pkg/reconcile contract, when
		// the error is non-nil the `Result` is ignored — including any
		// RequeueAfter we might set here — so there is no point
		// pretending to schedule the cascade retry at the rate-limit
		// boundary on this path. The rate-limiter's backoff cadence is
		// the actual retry schedule; the next successful reconcile
		// then re-enters the cascade path at its own boundary.
		return ctrl.Result{}, applyErr
	}
	if statusErr != nil {
		// Surface the deferred status-write failure after the cascade
		// has had its chance to recover engine FDs. Same workqueue
		// rate-limiter semantics as the applyErr path: Result is
		// ignored when err != nil.
		return ctrl.Result{}, statusErr
	}

	logger.V(1).Info("reconciled managed CacheBackend",
		"namespace", backend.Namespace, "name", backend.Name, "endpoint", endpoint)
	// requeueAfter is the tighter of two gate-driven schedules (see
	// minNonZero in updateManagedStatus):
	//   * KV-event gate: non-zero while in the AwaitingFirstKVEvent window,
	//     so the automatic Degraded flip fires when firstEventTimeout
	//     elapses without an event — without waiting for the next periodic
	//     resync.
	//   * Functional-probe gate: non-zero on every probe path that did NOT
	//     advance the rate-limit window (rate-limited, HTTP-error) AND on
	//     the success/per-stage-failure paths that DID — schedules the next
	//     /probe call at the rate-limit-window expiry so a quiet stuck
	//     backend re-probes without relying on incidental external watch
	//     events.
	// Either gate's non-zero value (or the smaller of both) lands here.
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
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

// pvcName is the stable name of the data PVC a persistent backend provisions.
// Derived from the CacheBackend name so it is deterministic across reconciles
// and unique within the namespace (one shared PVC per backend on the Deployment
// path; per-replica PVCs via StatefulSet volumeClaimTemplates are a follow-up).
func pvcName(backend *cachev1alpha1.CacheBackend) string {
	return backend.Name + "-data"
}

// multiReplicaRequested reports whether the backend's effective replica ceiling
// exceeds one — the multi-attach hazard for a single ReadWriteOnce PVC, which
// the reconciler gates (see reconcileInvalidStorage). The check mirrors the
// controller's HPA-ownership model: when spec.autoscaling is set the HPA owns
// the live replica count and spec.replicas is ignored for scaling (see
// applyDeployment), so the ceiling is spec.autoscaling.maxReplicas alone — a
// stale spec.replicas left at 3 under maxReplicas=1 is NOT a multi-attach
// hazard and must not be gated. When autoscaling is unset, spec.replicas is the
// ceiling. The ceiling — not the live count — is what matters: the operator's
// intent to ever run more than one pod against the shared PVC is the hazard, and
// admission deliberately surfaces this as Ready=False rather than rejecting
// (per-replica PVCs via StatefulSet are a follow-up).
func multiReplicaRequested(backend *cachev1alpha1.CacheBackend) bool {
	if backend.Spec.Autoscaling != nil {
		return backend.Spec.Autoscaling.MaxReplicas > 1
	}
	return backend.Spec.Replicas != nil && *backend.Spec.Replicas > 1
}

// applyPVC creates or updates the backend's data PVC idempotently, owned by the
// CR so it is garbage-collected when (and only when) the CacheBackend is
// deleted. On create it establishes the full spec; on update it reconciles only
// the mutable field — the requested size, and only upward (k8s rejects shrink;
// equal is a no-op). storageClassName and accessModes are immutable in k8s, so
// a drifted class is a no-op + debug log rather than a doomed patch.
//
// Wrapped in RetryOnConflict because the binder writes PVC.Status during bind;
// without retry the Get/Update inside CreateOrUpdate would race those writes.
func (r *CacheBackendReconciler) applyPVC(ctx context.Context, logger logr.Logger, backend *cachev1alpha1.CacheBackend) (*corev1.PersistentVolumeClaim, error) {
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: pvcName(backend), Namespace: backend.Namespace}}
	want := backend.Spec.Storage.PVC.Size
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		_, e := controllerutil.CreateOrUpdate(ctx, r.Client, pvc, func() error {
			// ResourceVersion is empty only when CreateOrUpdate's Get returned
			// NotFound (we are about to Create); a Get of an existing PVC always
			// populates it. This distinguishes create from update more reliably
			// than CreationTimestamp (which some clients don't stamp on create).
			if pvc.ResourceVersion == "" {
				// First create: establish the (mostly immutable) spec.
				pvc.Labels = podTemplateLabels(backend)
				pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
				pvc.Spec.Resources = corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceStorage: want},
				}
				// Set StorageClassName whenever the operator specified it,
				// INCLUDING an explicit empty string: in Kubernetes "" is not
				// "unset" — it opts out of the default StorageClass (static /
				// no-provisioner binding). Only a nil pointer means "omit, use
				// the cluster default".
				if scn := backend.Spec.Storage.PVC.StorageClassName; scn != nil {
					pvc.Spec.StorageClassName = scn
				}
			} else {
				// Never adopt a PVC we do not own. CreateOrUpdate's Get matched an
				// existing object named <cb>-data; if it is not controller-owned by
				// THIS CacheBackend it may be an operator-created PVC that merely
				// shares the derived name. Adding our controller owner reference
				// here would make that PVC garbage-collected when the CacheBackend
				// is deleted — irreversible data loss. Refuse by returning an error
				// so CreateOrUpdate makes no write; the reconcile surfaces it and
				// requeues until the operator renames the CacheBackend or removes
				// the conflicting PVC.
				if !metav1.IsControlledBy(pvc, backend) {
					return fmt.Errorf("pvc %s/%s already exists and is not owned by this CacheBackend; refusing to adopt it (rename the CacheBackend or remove the conflicting PVC)", pvc.Namespace, pvc.Name)
				}
				// Update: size is expandable (if the StorageClass allows it);
				// patch the request up only — never down (k8s rejects shrink).
				have := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
				if want.Cmp(have) > 0 {
					if pvc.Spec.Resources.Requests == nil {
						pvc.Spec.Resources.Requests = corev1.ResourceList{}
					}
					pvc.Spec.Resources.Requests[corev1.ResourceStorage] = want
				}
				// storageClassName is immutable: a change request can't be
				// honored by k8s, so log it at debug and leave the live class.
				// A nil pointer means the operator did not specify a class on
				// this edit, so there is nothing to compare.
				if scn := backend.Spec.Storage.PVC.StorageClassName; scn != nil {
					live := ""
					if pvc.Spec.StorageClassName != nil {
						live = *pvc.Spec.StorageClassName
					}
					if live != *scn {
						logger.V(1).Info("ignoring spec.storage.pvc.storageClassName change (immutable in k8s)",
							"namespace", backend.Namespace, "name", backend.Name,
							"liveStorageClass", live, "requestedStorageClass", *scn)
					}
				}
			}
			// Re-adoption: clear the "already warned" marker (if any) left by a
			// prior adopt-and-keep orphaning, so a future spec.storage.pvc
			// removal warns again. delete on an absent key is a no-op.
			delete(pvc.Annotations, annotationStorageRetainedWarned)
			return controllerutil.SetControllerReference(backend, pvc, r.Scheme)
		})
		return e
	})
	if err != nil {
		return nil, fmt.Errorf("apply pvc %s/%s: %w", backend.Namespace, pvcName(backend), err)
	}
	return pvc, nil
}

// mountDataVolume merges the PVC-backed volume + mount the adapter declared into
// the rendered cache-server pod. The volume source is the controller-named PVC;
// the mount path is the adapter's (it knows where its data lives). Both upsert
// by name so re-rendering is idempotent and an out-of-band drift is corrected.
//
// The mount is added to every adapter-rendered container: the cache-server pod
// template comes solely from ResolveCacheServer (sidecars are appended to ENGINE
// pods by the pod webhook, never to this pod), so "all rendered containers" is
// exactly the cache-server's own container(s) sharing the one data directory.
func mountDataVolume(podSpec *corev1.PodSpec, dv *adapterruntime.AdapterDataVolume, claimName string) {
	vol := corev1.Volume{
		Name: dv.VolumeName,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claimName},
		},
	}
	replaced := false
	for i := range podSpec.Volumes {
		if podSpec.Volumes[i].Name == dv.VolumeName {
			podSpec.Volumes[i] = vol
			replaced = true
			break
		}
	}
	if !replaced {
		podSpec.Volumes = append(podSpec.Volumes, vol)
	}

	mount := corev1.VolumeMount{Name: dv.VolumeName, MountPath: dv.MountPath}
	for i := range podSpec.Containers {
		podSpec.Containers[i].VolumeMounts = upsertVolumeMount(podSpec.Containers[i].VolumeMounts, mount)
	}
}

// upsertVolumeMount replaces a mount with the same Name (idempotent re-render)
// or appends it, returning the updated slice.
func upsertVolumeMount(mounts []corev1.VolumeMount, m corev1.VolumeMount) []corev1.VolumeMount {
	for i := range mounts {
		if mounts[i].Name == m.Name {
			mounts[i] = m
			return mounts
		}
	}
	return append(mounts, m)
}

// boundCapacity returns the PVC's actual provisioned capacity once bound, or ""
// when the PVC has no capacity yet (still Pending — e.g. a WaitForFirstConsumer
// StorageClass that binds only when the pod schedules). This is the REAL size
// the storage layer provisioned, which can exceed the request (rounding up to a
// provisioner's minimum), so it — not spec.storage.pvc.size — is what status
// reports.
func boundCapacity(pvc *corev1.PersistentVolumeClaim) string {
	// Only report capacity for a PVC that is actually Bound. status.capacity can
	// linger on a claim that has left Bound (e.g. Lost), and a Pending claim has
	// none yet — reporting either would advertise provisioned capacity the
	// backend does not have. The documented contract is "the bound PVC's
	// capacity", so gate on the phase.
	if pvc == nil || pvc.Status.Phase != corev1.ClaimBound {
		return ""
	}
	if q, ok := pvc.Status.Capacity[corev1.ResourceStorage]; ok && !q.IsZero() {
		return q.String()
	}
	return ""
}

// reconcileInvalidStorage publishes the Ready=False/InvalidStorageConfiguration
// terminal state for a persistent backend that asked for more than one replica,
// WITHOUT provisioning the PVC or the workload. A single ReadWriteOnce PVC
// cannot be mounted by multiple pods, so creating a multi-replica Deployment
// that shares it would wedge all but one replica in ContainerCreating
// (multi-attach). The operator must scale to 1 or drop spec.storage.pvc; the
// Warning event is emitted on the state transition (see emitTransitionEvents).
//
// The configuration is invalid, so we refuse to RUN it: any workload from a
// prior valid generation is shed and status.endpoint is cleared, so the
// pod-mutating webhook stops wiring engine pods to a backend the operator must
// fix (leaving a live endpoint advertised while reporting Ready=False would be
// an inconsistent, confusing state). The PVC is NOT deleted —
// cleanupOwnedWorkload only sheds Deployment/Service/HPA, and the PVC stays
// owner-referenced (adopt-and-keep), so persistent data survives the gate and a
// later scale-to-1 re-provisions against it. Fail-open posture means engines
// fall back to local prefill while the backend is gated.
func (r *CacheBackendReconciler) reconcileInvalidStorage(ctx context.Context, backend *cachev1alpha1.CacheBackend) (ctrl.Result, error) {
	if err := r.cleanupOwnedWorkload(ctx, backend); err != nil {
		return ctrl.Result{}, err
	}
	// Wipe the in-memory cascade shadow + rate-limit timestamp
	// alongside the on-cluster status clearing below: a retained
	// shadow would surface a stale pod identifier while no cache-
	// server Deployment exists, and would drive a false cascade
	// when the operator fixes the storage/replicas configuration
	// and the controller flips back to the managed path.
	r.clearServerInstanceLatchShadow(backend)
	// Wipe the functional-probe rate-limit entry too. Same reasoning
	// as in reconcileExternal / reconcileUnmanaged: a managed →
	// InvalidStorage → managed cycle within the 30s rate-limit window
	// would otherwise have a stale lastCalled timestamp on re-entry
	// and the first reconcile under the managed path would skip the
	// /probe call, leaving any prior FunctionalProbeOK=True (or
	// absent) unchallenged.
	r.probeLimiter.forget(client.ObjectKeyFromObject(backend).String())
	err := r.patchStatus(ctx, backend, func() {
		backend.Status.Endpoint = ""
		backend.Status.Capacity = ""
		// Clear the cache-server-instance latch — cleanupOwnedWorkload
		// has just deleted the previously-running Deployment (if any)
		// and we refuse to provision a new one until the operator
		// fixes the configuration. A retained UID would advertise a
		// stale identifier and feed a false cascade on the fix-up
		// transition.
		backend.Status.ObservedServerInstance = ""
		backend.Status.ObservedGeneration = backend.Generation
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeReady,
			Status:             metav1.ConditionFalse,
			Reason:             conditionReasonInvalidStorageConfiguration,
			Message:            invalidStorageMessage,
			ObservedGeneration: backend.Generation,
		})
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeProgressing,
			Status:             metav1.ConditionFalse,
			Reason:             conditionReasonInvalidStorageConfiguration,
			Message:            "controller will not provision a multi-attach workload until storage or replica count is corrected",
			ObservedGeneration: backend.Generation,
		})
		// Not a transient/recoverable workload fault; the Degraded condition
		// (rollout/replica health) does not apply to an unprovisioned workload.
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeDegraded)
		// The kernel gate runs only in updateManagedStatus, which this
		// invalid-storage early-return bypasses; clear EngineKernelsHealthy so a
		// backend that flipped into this gate doesn't keep a stale verdict.
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeEngineKernelsHealthy)
	})
	return ctrl.Result{}, err
}

// warnRetainedPVC emits a Warning ONCE when spec.storage.pvc has been removed
// but the CR still owns a data PVC (adopt-and-keep). The PVC is intentionally
// NOT deleted — destroying persistent storage on a spec edit is irreversible
// data loss, so it stays owner-referenced and is reclaimed only when the
// CacheBackend itself is deleted.
//
// Fire-once: this runs on the steady-state reconcile path, which repeats on
// every resync, so emitting unconditionally would re-fire the Warning forever.
// We stamp the retained PVC with an annotation the first time we warn and skip
// when it is already present, so the event fires once per orphaning rather than
// per reconcile. applyPVC clears the annotation when the backend re-adopts the
// PVC (spec.storage.pvc re-added), so a later removal warns again.
//
// Fail-soft: a NotFound (the common case — no PVC was ever provisioned) or a
// transient Get/Patch error simply emits nothing (the next reconcile retries).
// A PVC we do not own is left entirely alone.
func (r *CacheBackendReconciler) warnRetainedPVC(ctx context.Context, backend *cachev1alpha1.CacheBackend) {
	if r.Recorder == nil {
		return
	}
	pvc := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Name: pvcName(backend), Namespace: backend.Namespace}
	if err := r.Get(ctx, key, pvc); err != nil {
		return
	}
	if !metav1.IsControlledBy(pvc, backend) {
		return
	}
	if pvc.Annotations[annotationStorageRetainedWarned] == "true" {
		return // already warned for this orphaned PVC — stay quiet on resync.
	}
	// Stamp first, then emit: if the stamp write fails we skip the event and
	// retry on the next reconcile, so a persistent Patch failure can't spam.
	patch := client.MergeFrom(pvc.DeepCopy())
	if pvc.Annotations == nil {
		pvc.Annotations = map[string]string{}
	}
	pvc.Annotations[annotationStorageRetainedWarned] = "true"
	if err := r.Patch(ctx, pvc, patch); err != nil {
		return
	}
	r.Recorder.Eventf(backend, nil, corev1.EventTypeWarning, eventReasonOrphanedPVCRetained, eventReasonOrphanedPVCRetained,
		"spec.storage.pvc was removed but PVC %q is retained (owner-referenced; reclaimed only when this CacheBackend is deleted). The workload reverts to ephemeral storage; delete the CacheBackend to reclaim the volume, or re-add spec.storage.pvc to mount it again.", pvc.Name)
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
// status.capacity carries the caller-resolved provisioned capacity: the bound
// PVC's actual status.capacity[storage] when a persistent backend's PVC has
// bound, or "" otherwise (ephemeral backend, or PVC not yet bound). It is the
// REAL provisioned size, not spec.storage.pvc.size (the request) — a pending or
// over-provisioned bind would otherwise be misreported. The Owns(PVC) watch
// re-triggers this reconcile when the PVC binds so the empty→size transition is
// observed promptly.
func (r *CacheBackendReconciler) updateManagedStatus(ctx context.Context, backend *cachev1alpha1.CacheBackend, endpoint, capacity string, dep *appsv1.Deployment, applyOK bool) (time.Duration, error) {
	now := time.Now()
	readyStatus, reason, message := managedReadiness(backend, dep)
	// Resolve the stable timeout anchor: the latched FirstAvailableAt, or — the
	// first time the workload is Available — now. Using a latched value (not the
	// live Deployment Available condition, which resets on a flap) keeps the
	// firstEventTimeout window monotonic so Degraded stays sticky.
	anchor := time.Time{}
	if backend.Status.FirstAvailableAt != nil {
		anchor = backend.Status.FirstAvailableAt.Time
	} else if readyStatus == metav1.ConditionTrue {
		anchor = now
	}
	// Layer the KV-event readiness gate on top of the Deployment-level readiness.
	// Only the workload-Available state is gated; every other Deployment state
	// passes through unchanged.
	gate := evaluateKVEventReadiness(backend, readyStatus, reason, message, anchor, now)
	// Layer the functional-probe gate on top of the
	// KV-event verdict. It only fires when the upstream gate would
	// otherwise say Ready=True — a backend that's already Ready=False for
	// some other reason can't be diagnosed by a downstream probe, and the
	// rate-limit caps a healthy-backend probe to ~once per 30s. The verdict
	// may downgrade Ready to False with a probe-specific reason; the
	// condition itself is published verbatim in the patchStatus closure
	// below so it lands atomically alongside Ready/Progressing/Degraded.
	probeVerdict := evaluateFunctionalProbe(ctx, backend, gate, r.ProbeClient, &r.probeLimiter, r.probeRateLimit(), now)
	gate = downgradeReadyVerdict(gate, probeVerdict)
	// Engine-kernel health gate (lmcache c_ops load). Reads the kernel-check
	// init-container status off matched engine pods; surfaces
	// EngineKernelsHealthy and, in strict mode, downgrades Ready. Uses the
	// uncached APIReader (no Pod informer), fail-soft on list errors.
	kernelReader := client.Reader(r.APIReader)
	if kernelReader == nil {
		kernelReader = r.Client
	}
	kernelPods, kernelListedOK := listMatchedEnginePods(ctx, kernelReader, backend)
	kernelVerdict := evaluateEngineKernelHealth(backend, gate, kernelPods, kernelListedOK)
	gate = downgradeKernelReadyVerdict(gate, kernelVerdict)
	progressingStatus, progressingReason, progressingMessage := progressingFromReady(gate.readyStatus, gate.readyReason, gate.readyMessage)
	publishedGen := backend.Status.ObservedGeneration
	if applyOK {
		publishedGen = backend.Generation
	}
	err := r.patchStatus(ctx, backend, func() {
		backend.Status.Endpoint = endpoint
		backend.Status.Capacity = capacity
		backend.Status.ObservedGeneration = publishedGen
		// Latch the first KV-event observation write-once. The poller can later
		// clear indexParticipation.lastEventAt on a drain, so this durable
		// marker is what keeps lastKVEventSeen true thereafter.
		if backend.Status.FirstKVEventObservedAt == nil {
			if at := currentLastEventAt(backend); at != nil {
				backend.Status.FirstKVEventObservedAt = at.DeepCopy()
			}
		}
		// Latch the first-Available time write-once — the stable, flap-immune
		// anchor for the firstEventTimeout window (see FirstAvailableAt godoc).
		if backend.Status.FirstAvailableAt == nil && readyStatus == metav1.ConditionTrue {
			t := metav1.NewTime(now)
			backend.Status.FirstAvailableAt = &t
		}
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeReady,
			Status:             gate.readyStatus,
			Reason:             gate.readyReason,
			Message:            gate.readyMessage,
			ObservedGeneration: publishedGen,
		})
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeDegraded,
			Status:             gate.degradedStatus,
			Reason:             gate.degradedReason,
			Message:            gate.degradedMessage,
			ObservedGeneration: publishedGen,
		})
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeProgressing,
			Status:             progressingStatus,
			Reason:             progressingReason,
			Message:            progressingMessage,
			ObservedGeneration: publishedGen,
		})
		// Functional-probe condition. The gate decides whether to write,
		// remove, or leave it alone this reconcile. meta.SetStatusCondition
		// / RemoveStatusCondition both honor the write-only-on-change
		// contract — same as the other three conditions above.
		switch {
		case probeVerdict.shouldWriteCondition:
			cond := probeVerdict.condition
			cond.ObservedGeneration = publishedGen
			meta.SetStatusCondition(&backend.Status.Conditions, cond)
		case probeVerdict.removeCondition:
			meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeFunctionalProbeOK)
		}
		// Engine-kernel-health condition (lmcache c_ops load). Same
		// write/remove/leave-alone contract as the functional-probe condition.
		switch {
		case kernelVerdict.shouldWriteCondition:
			kc := kernelVerdict.condition
			kc.ObservedGeneration = publishedGen
			meta.SetStatusCondition(&backend.Status.Conditions, kc)
		case kernelVerdict.removeCondition:
			meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeEngineKernelsHealthy)
		}
	})
	// Commit the rate-limit slot ONLY after the status patch succeeds. A
	// failed patch must not burn a window — the next reconcile retries
	// immediately and re-runs the probe call. (The gate sets commitMark
	// only on a successful probe call, so a rate-limited or HTTP-failed
	// reconcile is a no-op here.)
	if err == nil && probeVerdict.commitMark != nil {
		probeVerdict.commitMark()
	}
	// Take the tighter of the KV-gate requeue and the probe-gate requeue
	// so a stuck-failing backend re-probes when its rate-limit window
	// expires even without an external watch event. A zero from either
	// side means "no requeue requested"; min() must therefore ignore the
	// zero so a non-zero half always wins.
	requeue := minNonZero(gate.requeueAfter, probeVerdict.requeueAfter)
	return requeue, err
}

// minNonZero returns the smaller of two durations, treating a zero on
// either side as "no value" so a non-zero half always wins. Used to merge
// the KV-event and functional-probe requeue requests.
func minNonZero(a, b time.Duration) time.Duration {
	switch {
	case a == 0:
		return b
	case b == 0:
		return a
	case a < b:
		return a
	default:
		return b
	}
}

// kvReadiness is the resolved readiness verdict after layering the KV-event
// gate on top of the Deployment-level readiness. readyStatus/readyReason/
// readyMessage drive Conditions[Ready]; degraded* drive Conditions[Degraded];
// requeueAfter is non-zero only inside the AwaitingFirstKVEvent window, where
// it schedules the automatic Degraded flip once firstEventTimeout elapses
// without an event.
type kvReadiness struct {
	readyStatus     metav1.ConditionStatus
	readyReason     string
	readyMessage    string
	degradedStatus  metav1.ConditionStatus
	degradedReason  string
	degradedMessage string
	requeueAfter    time.Duration
}

// evaluateKVEventReadiness layers the KV-event readiness gate on top of the
// Deployment-derived health. The signal it adds is whether at least one KV
// event has been observed for this backend (status.indexParticipation.
// lastEventAt, written by the CacheIndex poller from engine-pod reports).
//
// Motivation: a backend whose managed workload is up can still be silently
// useless to the cache plane — the inference engine may be serving HTTP while
// its ZMQ KV-event publisher is mis-configured or crashed, so nothing flows
// into the index and LookupRoute keeps returning NO_HINT. The managed
// Deployment's own readiness cannot see that; the first-KV-event signal can.
//
// State machine — the gate only refines the state once the managed
// cache-backend Deployment is Available (managedReadiness Ready=True). Every
// other Deployment state (rollout in progress, scaled to zero, replicas
// unavailable) passes through unchanged:
//
//	Workload Available? | event seen? | within timeout? | Ready | Degraded | reason
//	No                  | -           | -               | (passthrough deployment readiness)
//	Yes                 | No          | Yes             | False | False    | AwaitingFirstKVEvent
//	Yes                 | Yes         | -               | True  | False    | KVEventsObserved
//	Yes                 | No          | No              | False | True     | NoKVEventsObserved
//
// "Event seen" is "have we EVER seen an event for this backend" — a non-nil
// lastEventAt already present on the first reconcile counts (no transition
// through AwaitingFirstKVEvent is required). The gate is opt-out per CR via the
// inferencecache.io/require-kv-events: "false" annotation. External backends
// never reach this code path (reconcileExternal short-circuits dispatch), so
// they are unconditionally exempt.
//
// Timeout anchor: `anchor` is the caller-resolved start of the firstEventTimeout
// clock — the write-once status.firstAvailableAt latch (the time the workload
// was first observed Available). It is deliberately NOT the live Deployment's
// Available condition LastTransitionTime, which resets on an availability flap:
// a latched anchor keeps the elapsed window monotonic, so once a backend
// breaches the timeout (Degraded / NoKVEventsObserved) a later flap cannot move
// it back to AwaitingFirstKVEvent — it stays Degraded until an event arrives.
// (Once firstKVEventObservedAt is latched the gate is satisfied regardless of
// the anchor, since lastKVEventSeen short-circuits.)
func evaluateKVEventReadiness(backend *cachev1alpha1.CacheBackend, readyStatus metav1.ConditionStatus, reason, message string, anchor, now time.Time) kvReadiness {
	// Base verdict mirrors the Deployment-level readiness; the Degraded
	// condition tracks the deployment-level Ready=False/ReplicasUnavailable
	// shape so it is consistent on every path (including the opt-out and
	// not-yet-Available paths below).
	base := kvReadiness{
		readyStatus:  readyStatus,
		readyReason:  reason,
		readyMessage: message,
	}
	if readyStatus == metav1.ConditionFalse && reason == conditionReasonReplicasUnavailable {
		base.degradedStatus = metav1.ConditionTrue
		base.degradedReason = reason
		base.degradedMessage = message
	} else {
		base.degradedStatus = metav1.ConditionFalse
		base.degradedReason = reasonNotDegraded
		base.degradedMessage = "backend is not in a degraded state"
	}

	// Opt-out, or a Deployment that is not yet Available: nothing to gate.
	if !kvEventGateEnabled(backend) || readyStatus != metav1.ConditionTrue {
		return base
	}

	// Workload is Available. Have we ever seen a KV event for this backend?
	if lastKVEventSeen(backend) {
		return kvReadiness{
			readyStatus:     metav1.ConditionTrue,
			readyReason:     reasonKVEventsObserved,
			readyMessage:    "at least one KV event observed; cache is receiving engine state",
			degradedStatus:  metav1.ConditionFalse,
			degradedReason:  reasonNotDegraded,
			degradedMessage: "backend is not in a degraded state",
		}
	}

	// Sticky Degraded: once the timeout has been breached
	// (Conditions[Ready].Reason == NoKVEventsObserved), stay Degraded until an
	// event arrives — never recompute the window. This guards the case where an
	// operator INCREASES spec.integration.firstEventTimeout after the window
	// already elapsed, which would otherwise move the backend back to
	// AwaitingFirstKVEvent (hiding a known publisher outage for another window),
	// contradicting the documented "once Degraded, stays Degraded until an
	// event" contract. (Availability flaps are handled separately by the stable
	// firstAvailableAt anchor; this persisted-reason check survives a no-flap
	// timeout change, where the condition is not overwritten.)
	if readyConditionReason(backend) == reasonNoKVEventsObserved {
		return kvReadiness{
			readyStatus:     metav1.ConditionFalse,
			readyReason:     reasonNoKVEventsObserved,
			readyMessage:    "no KV events observed before the first-event timeout; staying Degraded until an event arrives",
			degradedStatus:  metav1.ConditionTrue,
			degradedReason:  reasonNoKVEventsObserved,
			degradedMessage: "no KV events observed before the first-event timeout",
		}
	}

	// Available but no event yet — still inside the first-event window? The
	// anchor is the latched first-Available time (now on the very first
	// Available reconcile, before the latch is persisted); a zero anchor would
	// only ever delay the Degraded flip, never trigger it prematurely.
	timeout := firstEventTimeout(backend)
	if anchor.IsZero() {
		anchor = now
	}
	if elapsed := now.Sub(anchor); elapsed < timeout {
		return kvReadiness{
			readyStatus:     metav1.ConditionFalse,
			readyReason:     reasonAwaitingFirstKVEvent,
			readyMessage:    fmt.Sprintf("cache-backend workload is Available but no KV events observed yet; waiting up to %s for the engine to report state", timeout),
			degradedStatus:  metav1.ConditionFalse,
			degradedReason:  reasonNotDegraded,
			degradedMessage: "backend is not in a degraded state",
			// Re-reconcile at the deadline so the Degraded flip fires
			// automatically without an external event. No padding is added:
			// RequeueAfter fires no earlier than the requested delay, so the
			// next reconcile observes elapsed >= timeout and flips Degraded —
			// honoring firstEventTimeout as the actual bound rather than
			// overshooting it (which would be visible for small timeouts).
			requeueAfter: timeout - elapsed,
		}
	}
	return kvReadiness{
		readyStatus:     metav1.ConditionFalse,
		readyReason:     reasonNoKVEventsObserved,
		readyMessage:    fmt.Sprintf("no KV events observed within %s of the workload becoming Available; check that engine pods are attached and their --kv-events-config / ZMQ publisher is healthy", timeout),
		degradedStatus:  metav1.ConditionTrue,
		degradedReason:  reasonNoKVEventsObserved,
		degradedMessage: fmt.Sprintf("no KV events observed within %s of the cache-backend workload becoming Available", timeout),
	}
}

// kvEventGateEnabled reports whether the KV-event readiness gate applies to
// this backend. Default-on; only the exact annotation value "false" opts out.
func kvEventGateEnabled(backend *cachev1alpha1.CacheBackend) bool {
	return backend.Annotations[annotationRequireKVEvents] != "false"
}

// lastKVEventSeen reports whether at least one KV event has EVER been observed
// for this backend. The gate's contract is "ever observed", but the poller's
// status.indexParticipation.lastEventAt is only a current-view projection — it
// legitimately clears to nil when a backend's replicas drain (scale-down,
// prefixes TTL'd; see the CacheIndex poller's drain handling). Reading that
// alone would let a backend that already passed the gate regress to
// AwaitingFirstKVEvent → NoKVEventsObserved on a drain. So the durable
// status.firstKVEventObservedAt latch (written write-once below) is consulted
// too: once set it pins the gate satisfied, matching the "first-event startup
// probe, not a liveness check" scope.
func lastKVEventSeen(backend *cachev1alpha1.CacheBackend) bool {
	if backend.Status.FirstKVEventObservedAt != nil {
		return true
	}
	ip := backend.Status.IndexParticipation
	return ip != nil && ip.LastEventAt != nil
}

// currentLastEventAt returns the poller's current-view lastEventAt for the
// backend, or nil. Used to latch firstKVEventObservedAt write-once.
func currentLastEventAt(backend *cachev1alpha1.CacheBackend) *metav1.Time {
	if ip := backend.Status.IndexParticipation; ip != nil {
		return ip.LastEventAt
	}
	return nil
}

// firstEventTimeout resolves the effective first-event timeout, falling back
// to defaultFirstEventTimeout when the spec field is unset or non-positive
// (the API-server applies the 5m kubebuilder default in production; the
// fallback covers fake-client tests and defensively rejects a zero value).
func firstEventTimeout(backend *cachev1alpha1.CacheBackend) time.Duration {
	if i := backend.Spec.Integration; i != nil && i.FirstEventTimeout != nil && i.FirstEventTimeout.Duration > 0 {
		return i.FirstEventTimeout.Duration
	}
	return defaultFirstEventTimeout
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
	// conditionReasonInvalidStorageConfiguration is set Ready=False when a
	// persistent backend (spec.storage.pvc) requests more than one replica
	// (spec.replicas > 1 or spec.autoscaling.maxReplicas > 1). A single
	// ReadWriteOnce PVC cannot be mounted by multiple pods, so the controller
	// refuses to provision the workload until the operator scales to 1 or drops
	// spec.storage.pvc. Per-replica PVCs via StatefulSet volumeClaimTemplates
	// are a separate follow-up.
	conditionReasonInvalidStorageConfiguration = "InvalidStorageConfiguration"
)

// invalidStorageMessage is the Ready=False/InvalidStorageConfiguration message,
// shared by the status condition and the Warning event so kubectl describe and
// the event stream say the same thing.
const invalidStorageMessage = "spec.storage.pvc requires a single replica: a ReadWriteOnce PVC cannot be multi-attached across replicas. Scale to spec.replicas=1 and spec.autoscaling.maxReplicas<=1, or remove spec.storage.pvc. Per-replica persistent storage via StatefulSet is a separate follow-up."

// annotationStorageRetainedWarned marks a retained (adopt-and-keep) PVC for
// which the OrphanedPVCRetained Warning has already been emitted, so the
// steady-state reconcile path warns once per orphaning rather than on every
// resync. Cleared by applyPVC when the backend re-adopts the PVC.
const annotationStorageRetainedWarned = "inferencecache.io/storage-retained-warned"

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
// (RolloutInProgress, or the KV-event gate's AwaitingFirstKVEvent — the
// controller is still driving toward the Ready=True endpoint in both)
// flips Progressing=True; one that has reached a stable terminal state
// (ScaledToZero) or stable failure (ReplicasUnavailable / NoKVEventsObserved)
// is NOT progressing — no rollout is in motion.
func progressingFromReady(readyStatus metav1.ConditionStatus, reason, message string) (metav1.ConditionStatus, string, string) {
	if readyStatus == metav1.ConditionTrue {
		return metav1.ConditionFalse, "Synced", "rendered children match desired state"
	}
	switch reason {
	case conditionReasonRolloutInProgress, reasonAwaitingFirstKVEvent:
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
	// ready is whether Conditions[Ready].Status was True. Pairs with
	// degraded so emitTransitionEvents can narrate the Degraded → Ready
	// recovery transition.
	ready bool
	// degraded is whether Conditions[Degraded].Status was True (covers both
	// the deployment-level ReplicasUnavailable shape AND the KV-event-gate
	// NoKVEventsObserved shape). The readyReason check below splits the two
	// for suppression purposes.
	degraded bool
	failOpen bool
	// readyReason is the prior Conditions[Ready].Reason. The KV-event gate's
	// transitions (AwaitingFirstKVEvent / KVEventsObserved / NoKVEventsObserved)
	// ride on this reason, and the suppression for BackendDegraded /
	// BackendRecovered uses it to distinguish a deployment-caused Degraded
	// from a KV-event-caused one (which has its own dedicated Event).
	readyReason string
	// firstEventLatched is whether status.firstKVEventObservedAt was set. The
	// KVEventsObserved Event keys on the nil→set transition of this latch (not
	// the Ready reason) so it fires exactly once — on the TRUE first event —
	// rather than re-firing every time a rollout takes an already-event-seen
	// backend through RolloutInProgress and back to KVEventsObserved.
	firstEventLatched bool
}

// snapshotState captures the prior status values that drive transition events.
// Called at the top of Reconcile before any mutation so emitTransitionEvents
// has a stable baseline to compare the post-reconcile state against.
func snapshotState(cb *cachev1alpha1.CacheBackend) stateSnapshot {
	return stateSnapshot{
		ready:             isReady(cb),
		degraded:          isDegraded(cb),
		failOpen:          statusFailOpen(cb.Status.FailOpen),
		readyReason:       readyConditionReason(cb),
		firstEventLatched: cb.Status.FirstKVEventObservedAt != nil,
	}
}

// isReady reports whether the Ready condition is currently True.
func isReady(cb *cachev1alpha1.CacheBackend) bool {
	c := meta.FindStatusCondition(cb.Status.Conditions, conditionTypeReady)
	return c != nil && c.Status == metav1.ConditionTrue
}

// isDegraded reports whether Conditions[Degraded] is True — covering both
// the deployment-level ReplicasUnavailable shape and the KV-event-gate
// NoKVEventsObserved shape. BackendDegraded events narrate the former; the
// gate emits its own NoKVEventsObserved event for the latter, so the
// generic event is suppressed by the readyReason check below.
func isDegraded(cb *cachev1alpha1.CacheBackend) bool {
	c := meta.FindStatusCondition(cb.Status.Conditions, conditionTypeDegraded)
	return c != nil && c.Status == metav1.ConditionTrue
}

// readyConditionReason returns the current Conditions[Ready].Reason, or "" when
// the condition is absent.
func readyConditionReason(cb *cachev1alpha1.CacheBackend) string {
	if c := meta.FindStatusCondition(cb.Status.Conditions, conditionTypeReady); c != nil {
		return c.Reason
	}
	return ""
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

// emitTransitionEvents emits Kubernetes Events on transitions of
// Conditions[Ready/Degraded], the KV-event readiness gate state, or the
// effective failOpen toggle. By design events fire only on transitions —
// never on steady state — so a Ready backend reconciling every few seconds
// does not flood the event stream.
//
//   - Entering Conditions[Degraded]=True → Warning BackendDegraded
//     (suppressed for the KV-event-gate flavor — readyReason=NoKVEventsObserved
//     — which emits its own NoKVEventsObserved event instead).
//   - Leaving Conditions[Degraded]=True for Ready=True → Normal
//     BackendRecovered (suppressed when recovering from the KV-event-gate
//     Degraded, which emits KVEventsObserved).
//   - KV-event gate (keyed on the Ready condition reason): Normal
//     AwaitingFirstKVEvent on first reaching it; Normal KVEventsObserved on the
//     first event observed; Warning NoKVEventsObserved on the timeout breach.
//   - failOpen flipped true → false → Warning FailClosedEnabled (the cache
//     becomes a serving dependency; advanced opt-in).
//   - failOpen flipped false → true → Normal FailOpenRestored.
func (r *CacheBackendReconciler) emitTransitionEvents(cb *cachev1alpha1.CacheBackend, before stateSnapshot) {
	if r.Recorder == nil {
		return
	}
	after := snapshotState(cb)

	// Generic Conditions[Degraded] transitions. The KV-event gate's Degraded
	// and Ready flavors carry their own, more specific events below, so
	// suppress the generic event when the transition is a gate flavor —
	// otherwise a KV-event-timeout Degraded would fire BOTH BackendDegraded
	// and NoKVEventsObserved for one transition.
	//
	// Suppression is keyed on the *prior* readyReason for recovery, not the
	// new one: a backend that already saw KV events, then degrades for
	// ReplicasUnavailable, then recovers, comes back with readyReason
	// KVEventsObserved — but that is an ordinary deployment recovery
	// (BackendRecovered), not a first-event observation, so we must not key
	// the suppression on the new reason.
	if !before.degraded && after.degraded &&
		after.readyReason != reasonNoKVEventsObserved {
		r.Recorder.Eventf(cb, nil, corev1.EventTypeWarning, eventReasonBackendDegraded, eventReasonBackendDegraded,
			"cache backend is degraded: %s", degradedMessage(cb))
	}
	if before.degraded && after.ready &&
		before.readyReason != reasonNoKVEventsObserved {
		r.Recorder.Eventf(cb, nil, corev1.EventTypeNormal, eventReasonBackendRecovered, eventReasonBackendRecovered,
			"cache backend recovered to Ready")
	}

	// KV-event readiness gate transitions, keyed on the Ready condition reason
	// so they fire once on entry into each gate state.
	if before.readyReason != reasonAwaitingFirstKVEvent && after.readyReason == reasonAwaitingFirstKVEvent {
		r.Recorder.Eventf(cb, nil, corev1.EventTypeNormal, reasonAwaitingFirstKVEvent, reasonAwaitingFirstKVEvent,
			"cache-backend workload is Available but no KV events observed yet; backend stays Ready=False/AwaitingFirstKVEvent until the first event (check that engine pods are attached and their --kv-events-config is healthy if none arrive)")
	}
	// KVEventsObserved fires exactly once — on the nil→set transition of the
	// firstKVEventObservedAt latch, i.e. the TRUE first event. Keying on the
	// latch (not the Ready reason) means a later rollout that takes an
	// already-event-seen backend through RolloutInProgress and back to
	// KVEventsObserved does NOT re-fire "first KV event observed", and a
	// deployment recovery (ReplicasUnavailable → Ready, events never lost) emits
	// BackendRecovered above instead of a spurious KVEventsObserved.
	if !before.firstEventLatched && after.firstEventLatched {
		r.Recorder.Eventf(cb, nil, corev1.EventTypeNormal, reasonKVEventsObserved, reasonKVEventsObserved,
			"first KV event observed; backend is Ready")
	}
	if before.readyReason != reasonNoKVEventsObserved && after.readyReason == reasonNoKVEventsObserved {
		r.Recorder.Eventf(cb, nil, corev1.EventTypeWarning, reasonNoKVEventsObserved, reasonNoKVEventsObserved,
			"no KV events observed within %s of the workload becoming Available; no engine pods are attached, or the engine's KV-event publisher is mis-configured (--kv-events-config / ZMQ bind)", firstEventTimeout(cb))
	}

	// Multi-replica-on-RWO-PVC gate (Ready=False/InvalidStorageConfiguration),
	// keyed on the Ready reason so it fires once on entry. after.degraded is
	// false in this state (reconcileInvalidStorage removes Degraded), so the
	// generic BackendDegraded/BackendRecovered events above do not double-fire.
	if before.readyReason != conditionReasonInvalidStorageConfiguration &&
		after.readyReason == conditionReasonInvalidStorageConfiguration {
		r.Recorder.Eventf(cb, nil, corev1.EventTypeWarning, eventReasonInvalidStorageConfiguration, eventReasonInvalidStorageConfiguration,
			invalidStorageMessage)
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
	if r.serverInstanceCascade == nil {
		r.serverInstanceCascade = newServerInstanceCascade()
	}
	return ctrl.NewControllerManagedBy(mgr).
		// NOTE: DO NOT add a predicate that filters status-only updates here.
		// The KV-event readiness gate depends on the CacheIndex poller's
		// status.indexParticipation patches triggering a reconcile via this
		// informer (sub-second latency, no explicit cross-controller enqueue).
		// A predicate that filters status updates would silently break the
		// AwaitingFirstKVEvent -> Ready transition.
		For(&cachev1alpha1.CacheBackend{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		// Owns(PVC) so a PVC binding (status.capacity goes from empty to the
		// bound size) or unbinding re-triggers a reconcile and refreshes
		// status.capacity without waiting for the next periodic resync.
		Owns(&corev1.PersistentVolumeClaim{}).
		Complete(r)
}

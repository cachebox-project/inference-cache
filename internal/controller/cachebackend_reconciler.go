// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"time"
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

// DefaultMatchedEnginePodsChurnRequeueInterval is the faster cadence used when
// the observed pod count disagrees with the desired-replica sum of Deployments
// whose pod-template labels match the CacheBackend's engineSelector. It keeps
// rolling restarts and scale churn visible without adding a Pod watch.
const DefaultMatchedEnginePodsChurnRequeueInterval = 5 * time.Second

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
	// Registry resolves the runtime adapter to use for a CacheBackend. The
	// composition root must inject it before reconciliation starts.
	Registry *adapterruntime.Registry
	// BackendRegistry resolves remote provider lifecycle independently from
	// engine/runtime wiring. The composition root must inject it.
	BackendRegistry *backendadapter.Registry
	// MatchedEnginePodsRequeueInterval overrides the self-requeue cadence
	// that keeps status.matchedEnginePods fresh between unrelated reconcile
	// triggers. Zero means "use [DefaultMatchedEnginePodsRequeueInterval]".
	// Production wiring leaves this zero; envtest suites override to a
	// shorter value so they don't bake the 30s production delay into
	// per-test runtime.
	MatchedEnginePodsRequeueInterval time.Duration
	// MatchedEnginePodsChurnRequeueInterval overrides the faster cadence used
	// while observed matching Pods disagree with matching Deployment desired
	// replicas. Zero means "use [DefaultMatchedEnginePodsChurnRequeueInterval]".
	MatchedEnginePodsChurnRequeueInterval time.Duration

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

// matchedEnginePodsChurnRequeueInterval returns the faster churn cadence for
// selector-matched engine pod counts.
func (r *CacheBackendReconciler) matchedEnginePodsChurnRequeueInterval() time.Duration {
	if r.MatchedEnginePodsChurnRequeueInterval > 0 {
		return r.MatchedEnginePodsChurnRequeueInterval
	}
	return DefaultMatchedEnginePodsChurnRequeueInterval
}

// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=inferencecache.io,resources=cachebackends/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=deployments/status,verbs=get
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get
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
	matchedRefresh := r.refreshMatchedEnginePods(ctx, &backend)
	// Typed LMCache PodLocal health comes from the injected native sidecars in
	// engine Pod status, independently from managed/external Redis readiness.
	r.refreshLMCacheMPConnectorStatus(ctx, &backend)
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
		if matchedRefresh.churn {
			cadence = r.matchedEnginePodsChurnRequeueInterval()
		}
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
		Complete(r)
}

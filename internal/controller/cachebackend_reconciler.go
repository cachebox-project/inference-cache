// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"strings"
	"time"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// DefaultMatchedEnginePodsRequeueInterval is the steady-state cadence at
// which a CacheBackend with a configured spec.engineSelector self-requeues,
// so the `status.matchedEnginePods` snapshot does not stay stale forever
// if a Pod watch event is missed or coalesced. Pod events normally trigger an
// immediate reconcile; the 30s safety net balances eventual correction with
// reconcile pressure on a large fleet. Tests override via the
// `MatchedEnginePodsRequeueInterval` reconciler field to avoid baking the 30s
// delay into the suite.
const DefaultMatchedEnginePodsRequeueInterval = 30 * time.Second

// DefaultMatchedEnginePodsChurnRequeueInterval is the faster cadence used when
// the observed pod count disagrees with the desired-replica sum of Deployments
// whose pod-template labels match the CacheBackend's engineSelector. It keeps
// rolling restarts and scale churn visible while Pod watch events converge.
const DefaultMatchedEnginePodsChurnRequeueInterval = 5 * time.Second

// CacheBackendReconciler reconciles a CacheBackend object.
type CacheBackendReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Log      logr.Logger
	Recorder events.EventRecorder
	// APIReader is an uncached live client used for per-reconcile Pod lists that
	// back engine demand and status. The Pod watch supplies prompt reconcile
	// events, while the live reader avoids acting on an informer snapshot that
	// has not yet observed the scheduling, readiness, or deletion event.
	// Production wiring passes mgr.GetAPIReader(); tests that don't exercise the
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
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile drives a CacheBackend toward its desired state. External Redis
// bindings mirror their configured endpoint to remote-storage status; managed
// Redis bindings render a singleton Deployment and Service. LMCache connector
// health is derived independently from the selected engine Pods.
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
	if backend.DeletionTimestamp.IsZero() && isTypedLMCacheNodeLocal(&backend) && !controllerutil.ContainsFinalizer(&backend, nodeLocalShmCleanupFinalizer) {
		before := backend.DeepCopy()
		controllerutil.AddFinalizer(&backend, nodeLocalShmCleanupFinalizer)
		if err := r.Patch(ctx, &backend, client.MergeFrom(before)); err != nil {
			return ctrl.Result{}, err
		}
	}
	if !backend.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&backend, nodeLocalShmCleanupFinalizer) {
			return r.finalizeLMCacheNodeLocal(ctx, &backend)
		}
		return ctrl.Result{}, nil
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
	// Typed LMCache health comes from PodLocal native sidecars or NodeLocal
	// on-demand server Pods and same-node engine coverage, independently from
	// Redis readiness.
	r.refreshLMCacheMPConnectorStatus(ctx, &backend)
	// Self-requeue when there's matchedEnginePods work to keep doing on
	// the next tick:
	//
	//   - A non-empty engineSelector is configured. Pod watches provide the
	//     normal trigger; this cadence is a bounded-staleness fallback for
	//     missed events and selector/annotation transitions.
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

// SetupWithManager sets up the controller with the Manager. Owned provider
// workload changes and mapped engine/NodeLocal-server Pod changes re-trigger a
// Reconcile so lifecycle, coverage, and transition Events track live state.
func (r *CacheBackendReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("cachebackend-controller")
	}
	if r.APIReader == nil {
		// Use the uncached reader for demand/status snapshots. The Pod watch is
		// the event trigger; an authoritative list avoids acting on an informer
		// snapshot that has not yet observed the scheduling or deletion event.
		r.APIReader = mgr.GetAPIReader()
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
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(cacheBackendRequestForPod)).
		Complete(r)
}

// cacheBackendRequestForPod maps controller-owned NodeLocal server Pods and
// webhook-injected engine Pods back to their CacheBackend. Node assignment,
// readiness, deletion, and server scheduling updates therefore drive on-demand
// lifecycle and status immediately.
func cacheBackendRequestForPod(_ context.Context, obj client.Object) []ctrlreconcile.Request {
	if obj == nil {
		return nil
	}
	if owner := metav1.GetControllerOf(obj); owner != nil && owner.Kind == "CacheBackend" && owner.APIVersion == cachev1alpha1.GroupVersion.String() {
		return []ctrlreconcile.Request{{NamespacedName: client.ObjectKey{Namespace: obj.GetNamespace(), Name: owner.Name}}}
	}
	annotations := obj.GetAnnotations()
	ref := annotations[enginebinding.AnnotationInjectedBy]
	if !validCacheBackendRef(ref) || annotations[enginebinding.AnnotationInjectedByUID] == "" {
		return nil
	}
	parts := strings.SplitN(ref, "/", 2)
	return []ctrlreconcile.Request{{NamespacedName: client.ObjectKey{Namespace: parts[0], Name: parts[1]}}}
}

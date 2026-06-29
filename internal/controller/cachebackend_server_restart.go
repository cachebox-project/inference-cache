package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
)

// AnnotationCacheServerRestartTrigger is patched onto an engine Deployment's
// spec.template.metadata.annotations to drive a rolling restart of its pods
// when the controller observes that this backend's cache-server pod has been
// replaced. The value is the same server-instance identifier the controller
// writes to status.observedServerInstance (`<pod-uid>:<restart-sum>` for a
// single-replica backend, or a comma-joined lex-sorted list of those for a
// multi-replica backend), so two quick replacements in succession produce
// distinct annotation values (and distinct rollout revisions). Modeled on
// the kubectl rollout restart pattern
// (kubectl.kubernetes.io/restartedAt), but project-namespaced so an operator
// running both does not see the two trample each other.
//
// Why this annotation triggers a restart: the Deployment controller watches
// spec.template for any change and creates a new ReplicaSet whenever the
// template content (including its annotations) differs from the latest live
// ReplicaSet. The annotation is otherwise inert; it carries no semantics
// beyond "this pod template has been bumped". Annotating the *pod* directly
// would have no effect — the Deployment controller does not reconcile its
// children's annotations, only the template's.
const AnnotationCacheServerRestartTrigger = "inferencecache.io/cache-server-restart-trigger"

// DefaultMinServerRestartCascadeInterval bounds how frequently the
// reconciler will cascade-restart engine Deployments in response to
// cache-server pod replacements, per CacheBackend. It dampens restart
// storms when the cache-server pod is flapping (e.g. crash-loop under
// memory pressure): every restart cascades all engines, so a
// crash-looping cache server would otherwise roll the engine fleet every
// few seconds. 30s is long enough that one full engine rollout is well
// underway before the next cascade is allowed, and short enough that a
// genuine single-restart recovery is not noticeably delayed.
//
// The window is enforced in-memory on the reconciler (see
// serverInstanceCascade.canCascade). A controller restart resets the
// rate-limit window for every backend — the in-memory `lastAt` map is
// lost — which is the intended behavior: the first cascade after
// restart is allowed immediately without waiting up to 30s. Whether
// any cascade actually fires after restart still depends on the
// normal decision (currentID differs from the durable
// status.observedServerInstance baseline AND the convergence /
// strict-superset rules); the restart does not by itself force a
// cascade on every backend.
const DefaultMinServerRestartCascadeInterval = 30 * time.Second

// cascadeRestartReasonServerInstanceChanged is the metric label value
// used whenever the cache-server SERVER-INSTANCE IDENTIFIER differs
// from the value last persisted to status.observedServerInstance.
// This covers both kinds of "the LMCache process is fresh" transition:
// a pod UID swap (replacement, eviction, image roll) AND a restart-
// sum-only advance from an in-place kubelet-driven container restart
// (OOM with restartPolicy=Always reuses pod.UID but resets the
// process). See currentServerInstanceID for the identifier shape. It
// is the only reason today; future non-instance-change triggers
// (e.g. an operator-initiated "force cascade" surface) would add
// their own value. Kept as a constant so the metric label set is
// stable and grep-discoverable.
const cascadeRestartReasonServerInstanceChanged = "server_instance_changed"

// backendServerRestartCascadesTotal counts cascade-restart DECISIONS
// the controller has emitted (NOT raw cache-server pod restarts, and
// NOT engine-Deployment annotate-patch operations). The counter
// advances exactly once per logical cascade event:
//
//   - after the rate-limit window has elapsed for this backend
//     (DefaultMinServerRestartCascadeInterval), and
//   - after the engine-pod scan + Deployment-annotate phase has
//     completed without error.
//
// The increment fires BEFORE the subsequent status patch: by the
// time we get here the engines are already annotated and ready to
// roll, so the metric reflects the recovery the moment it begins —
// the operator-visible counter does NOT lag behind a transient
// status-write failure. Double-counting on retry is prevented by
// the `counted` map in serverInstanceCascade: subsequent
// reconciles for the same (key, currentID) call
// shouldIncrementCascade, which returns false because the pair has
// already been counted.
//
// A cascade with ZERO matched engine Deployments still counts as one
// event — operators want flapping-server symptoms visible even when
// no engines happen to be injected today (e.g. before the engine
// fleet has been deployed, or while the operator is in the middle
// of rewiring spec.engineSelector and matchedEnginePods is empty).
//
// A crash-looping cache-server pod that restarts ten times within
// one cascade window still produces exactly one increment, because
// the rate limit collapses repeated observations into a single
// cascade per window. For raw restart rate, operators should
// compose this metric with the engine fleet's re-roll latency or
// the cache-server pod restartCount metric from kube-state-metrics;
// this Counter is the controller's record of "how many times did I
// decide to emit recovery for this backend", not "how many times
// did the cache-server crash".
//
// Partitioned by namespaced CacheBackend identity and a short reason
// code. Registered into the controller-runtime metrics registry on
// package init so it appears on the manager's /metrics endpoint (no
// per-Service registry — see pkg/server/metrics.go for the
// other-direction posture). Safe to mutate concurrently; tests
// reset its inner state via resetBackendServerRestartCascadesTotalForTest.
var backendServerRestartCascadesTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "inferencecache_backend_server_restart_cascades_total",
		Help: "Cumulative count of cache-server-restart cascades issued by the CacheBackend controller. A single cascade re-annotates every injected engine Deployment for the backend at once; the count is incremented once per cascade, regardless of how many Deployments were touched (zero is a valid cascade — see status.observedServerInstance docs). NOT a raw restart count: rate-limiting collapses repeated observations within one cadence window into a single cascade, so this metric undercounts a flapping cache-server's restart rate by design. Partitioned by the CacheBackend's namespace + name and a short reason code.",
	},
	[]string{"namespace", "backend", "reason"},
)

func init() {
	ctrlmetrics.Registry.MustRegister(backendServerRestartCascadesTotal)
}

// resetBackendServerRestartCascadesTotalForTest resets the cascade
// counter to zero for every label combination. Package-private so
// tests in this package can assert on per-test counts without leaking
// state across runs; intentionally not exported because production
// callers have no reason to zero an operator-visible metric.
func resetBackendServerRestartCascadesTotalForTest() {
	backendServerRestartCascadesTotal.Reset()
}

// cascadeKey keys the per-backend rate-limit map. Includes the
// CacheBackend's metadata.uid alongside namespace/name so a
// delete-recreate-with-same-name does not inherit the deleted
// object's throttle window — the new backend gets a fresh first
// cascade. Without the UID, an operator who deletes and re-creates
// a CacheBackend while a cascade was still inside the rate-limit
// window would silently delay the new backend's first real
// observation by up to MinServerRestartCascadeInterval.
type cascadeKey struct {
	namespace string
	name      string
	uid       string
}

// serverInstanceCascade tracks per-backend in-process state for the
// server-restart cascade: rate-limiting timestamps AND a shadow of
// the most-recently-attempted observedServerInstance value. Used in-
// process by CacheBackendReconciler; a process restart clears it
// (intentional — see DefaultMinServerRestartCascadeInterval).
//
// Why the shadow exists: status.observedServerInstance is written via
// patchStatus, which can fail (conflict / transient apiserver error).
// If the patch silently failed and a real cache-server replacement
// happened before a later successful patch, prior would read as ""
// on the next reconcile and the replacement would be misclassified
// as a first observation (empty→set: no cascade), stranding the
// engines on stale sockets. The shadow records every currentID the
// reconciler attempted to persist so the next reconcile can recover
// the intended baseline even when status is still empty. Authority
// order: a non-empty status.observedServerInstance ALWAYS wins (it's
// the durable on-cluster source of truth); the shadow is only
// consulted when status is empty (controller restart loses the
// shadow but rebuilds it on first observation, so the worst-case
// degenerates to "treat as first observation"). Process-restart
// risk is acceptable because the K8s-resident status field carries
// the value across restarts in the steady-state path.
//
// The maps are not actively pruned: entries are bounded by the
// number of distinct CacheBackends ever observed by the running
// process, each entry costs ~128 bytes, and a typical cluster has
// at most a few hundred CacheBackends over its lifetime. Operator-
// driven churn in the thousands-per-process would warrant adding a
// TTL-based prune; not worth the complexity for the expected scale.
type serverInstanceCascade struct {
	mu     sync.Mutex
	lastAt map[cascadeKey]time.Time
	// shadow records the last currentID the reconciler ATTEMPTED to
	// persist into status.observedServerInstance (whether or not the
	// patch succeeded). Read fallback when the status field is empty.
	shadow map[cascadeKey]string
	// counted records the most recent currentID we have already
	// incremented the cascade counter for. Lets the cascade-fired
	// branch advance the metric exactly once per logical cascade
	// EVENT (identified by (key, currentID)), even when the post-
	// annotate status patch takes multiple retries: the first
	// attempt records the (key, currentID) and Inc()s; subsequent
	// reconciles for the same (key, currentID) see counted ==
	// currentID and skip the increment. Separate from `shadow`
	// because a "baseline" or "converged-superset" persist also
	// updates the shadow but must NOT be counted.
	counted map[cascadeKey]string
	// cleared records that the latch for this key was explicitly
	// cleared (a lifecycle exit from the managed path —
	// reconcileExternal, reconcileUnmanaged, or
	// reconcileInvalidStorage — wiped the shadow and asked the
	// reconciler to publish status.observedServerInstance="").
	// The sentinel survives a transient status-patch failure on
	// that clear: the in-memory cleared bit overrides any stale
	// non-empty status field on the NEXT reconcile, so a
	// managed→External→managed flip in a tight patch-failure
	// window cannot misclassify the new period's first Ready pod
	// as a replacement of the prior-period identifier.
	// recordAttempt clears this flag (a new baseline is being
	// persisted, so we are no longer in the "explicitly cleared"
	// state).
	cleared map[cascadeKey]bool
}

func newServerInstanceCascade() *serverInstanceCascade {
	return &serverInstanceCascade{
		lastAt:  map[cascadeKey]time.Time{},
		shadow:  map[cascadeKey]string{},
		counted: map[cascadeKey]string{},
		cleared: map[cascadeKey]bool{},
	}
}

// recordAttempt stamps the most recent currentID the reconciler
// decided to persist for the given key. Called BEFORE the patch
// attempt so a subsequent reconcile can recover the intended
// baseline even if the patch fails. Also clears the "explicitly
// cleared" sentinel (a new baseline is being recorded, so the
// post-clear gap is closed for this key).
func (s *serverInstanceCascade) recordAttempt(key cascadeKey, currentID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shadow[key] = currentID
	delete(s.cleared, key)
}

// lastAttempt returns the most recent currentID the reconciler
// decided to persist for this key, or "" if no attempt has been
// recorded since process start. Used as the fallback prior when
// status.observedServerInstance is empty.
func (s *serverInstanceCascade) lastAttempt(key cascadeKey) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shadow[key]
}

// shouldIncrementCascade reports whether the cascade counter has
// already been advanced for (key, currentID). If not — i.e. this
// is the first observation of this (key, currentID) since process
// start or since the last clear() — records the pair and returns
// true; the caller is then responsible for Inc()'ing the metric.
// Subsequent calls for the same pair return false, even when the
// post-annotate status patch is retrying across reconciles. This
// is what enforces "one increment per cascade EVENT" against the
// retry-after-failed-persist path.
func (s *serverInstanceCascade) shouldIncrementCascade(key cascadeKey, currentID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.counted[key] == currentID {
		return false
	}
	s.counted[key] = currentID
	return true
}

// clear drops the rate-limit timestamp, the shadow baseline, and
// the counted-cascade ledger for the given key, AND records an
// explicit "cleared" sentinel that overrides any stale non-empty
// status.observedServerInstance on the next reconcile. Called when
// the backend transitions out of the managed path (External,
// unsupported runtime, invalid storage) and its
// status.observedServerInstance is also cleared on the cluster.
//
// The sentinel exists because the in-memory clear runs BEFORE the
// status patch that wipes the cluster-resident field, and that
// patch can fail. Without the sentinel, a tight failure window
// (clear shadow → status patch fails → operator flips back to
// managed before retry) would leave shadow empty + status holding
// the prior period's identifier; the reconciler would then read
// prior = statusField (stale) and misclassify the first new
// managed-period Ready pod as a replacement, triggering an
// unnecessary engine cascade despite the documented "inert/cleared"
// contract. The sentinel overrides that: cleared[key]=true means
// "treat prior as empty regardless of what statusField says".
// recordAttempt deletes the sentinel (a new baseline is now being
// persisted; we are no longer in the cleared state). Controller
// restart loses the sentinel; the External/Unmanaged/Invalid
// path's patchStatus retry on subsequent reconciles is the durable
// backstop for that case.
func (s *serverInstanceCascade) clear(key cascadeKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.lastAt, key)
	delete(s.shadow, key)
	delete(s.counted, key)
	s.cleared[key] = true
}

// isCleared reports whether the latch for this key has been
// explicitly cleared via clear() and not yet recorded a new
// attempt. Used by reconcileServerInstance to override a stale
// non-empty status field on the next managed-period reconcile.
func (s *serverInstanceCascade) isCleared(key cascadeKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cleared[key]
}

// canCascade reports whether enough time has elapsed since the previous
// cascade for the given backend. If true, it stamps now as the most
// recent cascade time. If false, it returns the remaining wait until the
// next cascade is allowed so the caller can RequeueAfter exactly that
// long. The check + stamp are done under one lock so concurrent
// reconciles of the same backend do not double-cascade.
func (s *serverInstanceCascade) canCascade(key cascadeKey, now time.Time, window time.Duration) (bool, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	last, ok := s.lastAt[key]
	if !ok {
		s.lastAt[key] = now
		return true, 0
	}
	elapsed := now.Sub(last)
	if elapsed >= window {
		s.lastAt[key] = now
		return true, 0
	}
	return false, window - elapsed
}

// clearServerInstanceLatchShadow wipes the in-memory shadow + rate-
// limit timestamp for this backend. Called from lifecycle paths that
// intentionally clear the on-cluster status.observedServerInstance
// field (reconcileExternal, reconcileUnmanaged, reconcileInvalidStorage).
// The shadow must follow the cluster-visible field; otherwise a
// later managed→External→managed (or invalid→fixed) transition in
// the same controller process would consult the lingering shadow,
// resolve effectivePrior to the stale prior-period value, and
// misclassify the first new Ready pod as a replacement —
// triggering an unnecessary engine cascade even though the
// documented contract says the latch is "cleared/inert" between
// managed periods.
//
// Safe to call before serverInstanceCascade has been lazy-inited
// (no-op in that case).
func (r *CacheBackendReconciler) clearServerInstanceLatchShadow(backend *cachev1alpha1.CacheBackend) {
	if r.serverInstanceCascade == nil {
		return
	}
	r.serverInstanceCascade.clear(cascadeKey{
		namespace: backend.Namespace,
		name:      backend.Name,
		uid:       string(backend.UID),
	})
}

// minServerRestartCascadeInterval returns the effective rate-limit window
// for this reconciler — the per-reconciler override, or
// DefaultMinServerRestartCascadeInterval when unset.
func (r *CacheBackendReconciler) minServerRestartCascadeInterval() time.Duration {
	if r.MinServerRestartCascadeInterval > 0 {
		return r.MinServerRestartCascadeInterval
	}
	return DefaultMinServerRestartCascadeInterval
}

// reconcileServerInstance observes the current Ready cache-server
// instance identifier for this managed backend and, on a change that
// reflects an actual cache-server replacement (a pod that was Ready
// before is no longer Ready, or a container inside a persisting pod
// has restarted), cascade-restarts every injected engine Deployment
// by patching AnnotationCacheServerRestartTrigger onto each
// Deployment's pod template. Status is patched on every transition;
// cascading is rate-limited per CacheBackend (see
// DefaultMinServerRestartCascadeInterval) and the function returns a
// non-zero requeue when the rate-limit deferred the cascade so the
// caller's reconcile result schedules the retry exactly at the window
// boundary.
//
// "Transient" transitions that do NOT cascade:
//   - empty → set (first observation; there is no prior server-instance
//     to invalidate, so by definition no engine sockets are stale —
//     any engines that connected during the "" window connected to the
//     very pod we are now baselining)
//   - prior set strictly grows AND the owning Deployment is still
//     rolling (a maxSurge midpoint: the old pod is still Ready while
//     the new one comes up; the subsequent transition that drops the
//     old pod IS a cascade). When the Deployment has converged at the
//     wider count instead — operator-driven scale-up — the widened
//     set IS persisted as the new baseline, so a later replacement of
//     any of the added pods cascades correctly.
//
// When no Ready cache-server pod exists at all (currentID = ""), the
// reconciler leaves status.observedServerInstance at its prior value
// rather than clearing it. The latch is intentionally stale-while-
// unavailable: a transient cache-server outage (Deployment scaled to
// 0, all pods Terminating mid-rollout, image pull stuck) must NOT
// look like "everything is fine, no instance" to the next reconcile,
// because the eventual recovery will bring back a fresh-UID pod set
// and that transition IS a real cache-server replacement that
// requires cascading. Clearing the latch in the no-Ready window
// would lose the prior-set memory and turn the recovery's
// "" → "new-uid:0" transition into a first-observation baseline
// (no cascade) — exactly the scenario this whole controller exists
// to prevent. The latch returns to a current-view value as soon as
// a Ready pod is observed.
//
// "Real" transitions that DO cascade:
//   - any prior pod is no longer in the current set (pod replaced)
//   - any persisting pod's restart-count sum advanced (in-place
//     container restart)
//
// Fail-soft: every error path (server-instance observation —
// owned Deployment Get, ReplicaSet owner-chain Get, Pod list;
// engine-cascade observation/annotate; status patch) logs at V(1)
// and returns a positive requeue duration (typically
// minServerRestartCascadeInterval) rather than escalating to the
// caller as a Reconcile error. Cascading is best-effort recovery
// from a known soft-failure mode; a transient apiserver hiccup must
// not back off the rest of the reconcile, but it also must not
// silently strand recovery — the requeue ensures the next reconcile
// retries within the cascade window.
func (r *CacheBackendReconciler) reconcileServerInstance(ctx context.Context, logger logr.Logger, backend *cachev1alpha1.CacheBackend) time.Duration {
	currentID, converged, err := r.currentServerInstanceID(ctx, backend)
	if err != nil {
		// A transient observation failure (owned Deployment Get,
		// ReplicaSet owner-chain Get, or Pod list — see
		// currentServerInstanceID for the chain) leaves us unable
		// to decide whether a cascade is needed. Return the rate-
		// limit interval as the requeue hint so the reconcile
		// retries within the same window we'd cascade in — without
		// this, the only path back is unrelated watch events, which
		// can leave the recovery stranded (especially in the
		// selector-removed-but-still-injected case).
		logger.V(1).Info("server-restart cascade skipped: server-instance observation failed",
			"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
		return r.minServerRestartCascadeInterval()
	}
	if currentID == "" {
		// No Ready cache-server pod yet — nothing to anchor to.
		return 0
	}

	if r.serverInstanceCascade == nil {
		// Defensive lazy-init; the manager wiring should set this in
		// SetupWithManager but unit tests construct the reconciler
		// directly and skip that path.
		r.serverInstanceCascade = newServerInstanceCascade()
	}
	key := cascadeKey{
		namespace: backend.Namespace,
		name:      backend.Name,
		uid:       string(backend.UID),
	}

	// Compute effective prior. Authority order: cleared sentinel →
	// shadow → statusField.
	//
	// 1) The "cleared" sentinel is checked first. A lifecycle exit
	//    from the managed path (External / Unmanaged / InvalidStorage)
	//    sets it via clear(); it survives a transient status-patch
	//    failure on the same clear. If the operator flips back to
	//    managed before the External-side patchStatus retry has
	//    landed, statusField would still hold the prior-period
	//    identifier — but cleared==true forces effectivePrior="",
	//    so the next observation is treated as a clean first-set
	//    (no false cascade against the stale value).
	//
	// 2) The in-memory shadow IS the authority when set: it records
	//    the last currentID the reconciler decided to persist,
	//    whether or not the patch landed. statusField is the durable
	//    projection of that shadow — when they disagree, it is
	//    because the most recent persist failed and has not yet
	//    retried. Trusting statusField over a non-empty shadow would
	//    re-introduce the round-21 regression: a converged scale-up
	//    persist that fails would leave shadow="A:0,B:0" while
	//    status still held the pre-scale "A:0"; a subsequent
	//    replacement of just the added pod ("A:0,C:0") would look
	//    like a strict superset of the status-derived "A:0" and miss
	//    the cascade.
	//
	// 3) Otherwise (cold start, controller restart), statusField is
	//    the durable source — the K8s API survived the restart even
	//    though the in-process state did not.
	statusField := backend.Status.ObservedServerInstance
	var prior string
	switch {
	case r.serverInstanceCascade.isCleared(key):
		prior = ""
	case r.serverInstanceCascade.lastAttempt(key) != "":
		prior = r.serverInstanceCascade.lastAttempt(key)
	default:
		prior = statusField
	}
	if prior == currentID {
		// Logical state is in sync. If the K8s status field is also
		// in sync, we are done. If the field is empty because a prior
		// persist failed (shadow holds the value but status does
		// not), retry the patch idempotently so the operator-visible
		// status field eventually reflects the shadow. Do NOT
		// re-cascade and do NOT re-increment the counter — the
		// recovery already happened on the original observation.
		if statusField == currentID {
			return 0
		}
		if err := r.patchStatus(ctx, backend, func() {
			backend.Status.ObservedServerInstance = currentID
		}); err != nil {
			logger.V(1).Info("server-restart cascade: status-field patch retry failed (shadow holds the in-process baseline; will retry)",
				"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
			return r.minServerRestartCascadeInterval()
		}
		return 0
	}

	// Empty → set: first observation. There is no prior server-
	// instance to invalidate, so engines that connected during the
	// empty window are connecting to the very pod we are baselining.
	// Persist as the baseline and stop. Record the attempt FIRST so
	// a patch failure does not lose the baseline (see shadow godoc).
	if prior == "" {
		r.serverInstanceCascade.recordAttempt(key, currentID)
		if err := r.patchStatus(ctx, backend, func() {
			backend.Status.ObservedServerInstance = currentID
		}); err != nil {
			logger.V(1).Info("server-restart cascade: initial observedServerInstance patch failed (in-memory shadow retains the baseline for the next reconcile)",
				"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
			return r.minServerRestartCascadeInterval()
		}
		return 0
	}

	// Distinguish a real cache-server replacement from a transient
	// rolling-update widening (old + new pod both Ready briefly).
	//
	// Strict-superset transitions split into two cases by the owning
	// Deployment's convergence flag (see currentServerInstanceID):
	//
	//  - NOT converged (rolling-update midpoint): do NOT persist the
	//    widened set. If we did, a failed rollout that gets rolled
	//    back (new pod briefly Ready, then killed by failing
	//    readiness, leaving the original pod alone) would later look
	//    like "the new pod was replaced" and false-cascade — but the
	//    original cache-server process and existing engine sockets
	//    never changed. Keeping the prior latch intact makes the
	//    rollback path a true no-op.
	//
	//  - Converged (steady-state scale-up): persist the widened set
	//    as the new baseline. The operator raised spec.replicas, the
	//    Deployment has reached steady state at the higher count, and
	//    the added pods are real cache-server processes. If we did
	//    not persist, a later replacement of just one of the added
	//    pods would still be a strict superset of the pinned prior
	//    map — instanceChangeRequiresCascade would return false and
	//    no cascade would fire, leaving engines that connected to the
	//    replaced pod with stale sockets. Record the attempt FIRST
	//    so a patch failure does not lose the widened baseline.
	if !instanceChangeRequiresCascade(prior, currentID) {
		if converged {
			r.serverInstanceCascade.recordAttempt(key, currentID)
			if err := r.patchStatus(ctx, backend, func() {
				backend.Status.ObservedServerInstance = currentID
			}); err != nil {
				logger.V(1).Info("server-restart cascade: converged-superset observedServerInstance patch failed (in-memory shadow retains the widened baseline)",
					"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
				return r.minServerRestartCascadeInterval()
			}
			logger.V(1).Info("server-restart cascade skipped: superset at Deployment steady state (scale-up); latch advanced",
				"namespace", backend.Namespace, "name", backend.Name,
				"prior", prior, "current", currentID)
			return 0
		}
		logger.V(1).Info("server-restart cascade skipped: transient rolling-update superset",
			"namespace", backend.Namespace, "name", backend.Name,
			"prior", prior, "current", currentID)
		return 0
	}

	// Rate-limit: a cascade per backend is allowed at most once per
	// minServerRestartCascadeInterval. While the window is open we
	// neither cascade nor advance status.observedServerInstance —
	// leaving the prior value pinned guarantees the next eligible
	// reconcile still sees the change and tries again. The key
	// includes the CacheBackend's UID so a delete-recreate under the
	// same name gets a fresh window.
	ok, wait := r.serverInstanceCascade.canCascade(key, time.Now(), r.minServerRestartCascadeInterval())
	if !ok {
		logger.V(1).Info("server-restart cascade deferred: rate-limited",
			"namespace", backend.Namespace, "name", backend.Name,
			"prior", prior, "current", currentID, "retryAfter", wait.String())
		return wait
	}

	count, err := r.cascadeRestartEngineDeployments(ctx, backend, currentID)
	if err != nil {
		// Soft-fail: log and keep going. The next reconcile (or the
		// matched-pods cadence requeue) will retry. Do NOT advance
		// status.observedServerInstance — leaving it on the prior value
		// keeps the change visible to the retry.
		logger.V(1).Info("server-restart cascade: engine Deployment list/patch failed",
			"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
		return r.minServerRestartCascadeInterval()
	}
	logger.V(1).Info("server-restart cascade: engine Deployments annotated",
		"namespace", backend.Namespace, "name", backend.Name,
		"prior", prior, "current", currentID, "deployments", count)

	// Increment the cascade counter exactly once per cascade EVENT,
	// keyed by (cascadeKey, currentID). shouldIncrementCascade
	// records the pair atomically and returns false on subsequent
	// calls for the same pair; this enforces the "one increment per
	// cascade event" contract even when the post-annotate status
	// patch takes multiple retries to land. The counter advances
	// HERE (not after patchStatus succeeds) because the recovery has
	// already happened — every injected engine Deployment is
	// annotated, ready to roll. Subsequent retries via the shadow
	// short-circuit at the top of this function do not re-enter the
	// cascade path, so the metric stays in sync with the recovery.
	if r.serverInstanceCascade.shouldIncrementCascade(key, currentID) {
		backendServerRestartCascadesTotal.WithLabelValues(backend.Namespace, backend.Name, cascadeRestartReasonServerInstanceChanged).Inc()
	}

	// Persist the new baseline. Record the attempt in the shadow
	// FIRST so a patch failure does not lose the baseline (see the
	// shadow godoc on serverInstanceCascade): on the next reconcile,
	// the shadow short-circuit branch at the top will retry the
	// patch idempotently without re-incrementing the counter (the
	// counted map already holds this currentID).
	r.serverInstanceCascade.recordAttempt(key, currentID)
	if err := r.patchStatus(ctx, backend, func() {
		backend.Status.ObservedServerInstance = currentID
	}); err != nil {
		logger.V(1).Info("server-restart cascade: observedServerInstance patch failed (annotates already issued; shadow + counted retain the event so the next reconcile retries persist without re-counting)",
			"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
		return r.minServerRestartCascadeInterval()
	}
	return 0
}

// instanceChangeRequiresCascade reports whether the prior→current
// transition reflects an actual cache-server replacement that warrants
// rolling the engine fleet, as opposed to a transient
// rolling-update widening (old + new pod both Ready briefly).
//
// Algorithm: parse both strings into (pod-UID → restart-sum) maps. A
// cascade is required iff some pod-UID in prior is either absent in
// current OR has a different restart-sum in current. A current that
// is a strict superset of prior (same UIDs at same restart counts,
// plus extras) is the rolling-update midpoint — no cascade yet.
//
// The caller MUST NOT persist a strict-superset current as the new
// baseline. If it did, a rolled-back rolling update (the new pod
// briefly Ready, then killed by failing readiness, leaving the
// original pod alone) would later look like "the new pod is gone →
// real replacement" and false-cascade. Keeping the latch pinned to
// prior through superset transitions makes the rollback path a
// no-op and the genuine completion path (original pod drops) a
// correctly-detected replacement.
//
// Conservative fallback: if `prior` is non-empty but fails to parse
// (operator hand-edited the field, or a value from a prior schema
// shape survives an upgrade), force a cascade. We cannot reason about
// what the prior set was, so treating any change as a real cascade is
// safer than silently skipping it.
func instanceChangeRequiresCascade(prior, current string) bool {
	pm := parseInstanceMap(prior)
	if len(pm) == 0 && prior != "" {
		return true
	}
	cm := parseInstanceMap(current)
	for uid, priorSum := range pm {
		curSum, ok := cm[uid]
		if !ok {
			return true // prior pod is gone — replacement happened.
		}
		if curSum != priorSum {
			return true // pod persists but container restarted in place.
		}
	}
	return false
}

// parseInstanceMap parses the "<uid1>:<sum1>,<uid2>:<sum2>" identifier
// shape currentServerInstanceID emits into a map keyed by pod-UID.
// Malformed segments are skipped (defensive — the controller is the
// sole writer, so it should never produce bad shapes, but tolerating
// them keeps the cascade decision well-defined if the field is hand-
// edited by an operator).
func parseInstanceMap(s string) map[string]int32 {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make(map[string]int32, len(parts))
	for _, p := range parts {
		i := strings.LastIndexByte(p, ':')
		if i <= 0 || i == len(p)-1 {
			continue
		}
		sum, err := strconv.ParseInt(p[i+1:], 10, 32)
		if err != nil {
			continue
		}
		out[p[:i]] = int32(sum)
	}
	return out
}

// currentServerInstanceID returns a stable identifier representing
// the current set of Ready cache-server pods for the backend, the
// owning Deployment's convergence flag (true when the Deployment
// controller has reached steady state — `spec.replicas` ==
// `status.readyReplicas` == `status.updatedReplicas` and
// `status.observedGeneration` >= `metadata.generation`), or "" when no
// Ready pod exists yet. The candidate set is the owned Deployment's
// pods — pods whose controller-owning ReplicaSet is controller-owned
// by the backend-owned Deployment, identified by both name AND UID so
// a foreign Ready pod that happens to carry the same controller-
// managed labels (or a stale ownerRef name that resolves to a
// different live object) cannot advance observedServerInstance and
// spuriously trigger an engine rollout.
//
// The identifier shape is `<pod-uid>:<restart-sum>` per Ready pod,
// comma-joined and lex-sorted by pod name; for a single-replica
// backend this is one segment, for multi-replica ephemeral backends
// it's a comma-joined list. The restart-sum half (sum of
// pod.status.containerStatuses[].RestartCount) makes in-place
// container restarts observable: an OOM-killed cache-server container
// respawned in the same pod reuses pod.UID, so a UID-only identifier
// would miss it — engines would keep their stale LMCache sockets.
// Including the restart-sum advances the identifier whenever the
// LMCache server process inside the pod is fresh.
//
// The convergence flag lets the caller distinguish a rolling-update
// midpoint (NOT converged: maxSurge has briefly widened the Ready set
// above target) from a steady-state scale-up (converged: the operator
// raised replicas and the new pods are part of the steady state).
// reconcileServerInstance persists a strict-superset baseline only
// when converged is true; otherwise it could pin a transient midpoint
// that a rollback would later misread as a real replacement.
//
// Reads via APIReader (uncached) where possible to avoid registering a
// Pod informer; the controller's design explicitly rejects watching
// all pods cluster-wide (see refreshMatchedEnginePods godoc). Falls
// back to the cached client when APIReader is nil (test wiring).
func (r *CacheBackendReconciler) currentServerInstanceID(ctx context.Context, backend *cachev1alpha1.CacheBackend) (string, bool, error) {
	reader := client.Reader(r.APIReader)
	if reader == nil {
		reader = r.Client
	}

	// Fetch the owned Deployment so we can authenticate candidate pods
	// against its UID. Verify the live Deployment is still controlled by
	// THIS CacheBackend before using it as the ownership anchor — name
	// reuse / race conditions could otherwise let a foreign Deployment
	// re-created under the same name be treated as ours. NotFound is the
	// cold-start case (CR exists, the reconciler hasn't created the
	// Deployment yet, or it was deleted out-of-band): no pods can be
	// authoritatively attributed so report "no instance".
	var ownedDep appsv1.Deployment
	if err := reader.Get(ctx, types.NamespacedName{Namespace: backend.Namespace, Name: backend.Name}, &ownedDep); err != nil {
		if apierrors.IsNotFound(err) {
			return "", false, nil
		}
		return "", false, fmt.Errorf("get owned cache-server deployment: %w", err)
	}
	if !metav1.IsControlledBy(&ownedDep, backend) {
		// Foreign Deployment sharing the backend's name. Refuse to
		// attribute its pods to this CacheBackend.
		return "", false, nil
	}

	matcher := labels.SelectorFromSet(selectorLabels(backend.Name))
	var pods corev1.PodList
	if err := reader.List(ctx, &pods,
		client.InNamespace(backend.Namespace),
		client.MatchingLabelsSelector{Selector: matcher},
	); err != nil {
		return "", false, fmt.Errorf("list cache-server pods: %w", err)
	}
	// Build the set of container names the owned Deployment renders.
	// containerRunSum sums restart counts ONLY for these — sidecars
	// injected by other admission webhooks (Istio's istio-proxy,
	// linkerd's linkerd-proxy, Datadog agents, etc.) are added to the
	// pod's containerStatuses but are absent from the Deployment
	// template, so including them would cascade-restart every engine
	// any time a service-mesh sidecar crash-looped — completely
	// unrelated to the LMCache server's actual state.
	cacheServerContainers := make(map[string]struct{}, len(ownedDep.Spec.Template.Spec.Containers))
	for _, c := range ownedDep.Spec.Template.Spec.Containers {
		cacheServerContainers[c.Name] = struct{}{}
	}

	// Collect every Ready, attributable pod's identifier. A pod that
	// is mid-rollout (Pending / Terminating) does not represent a
	// serving instance — including it would let a rollout's transient
	// state trigger a cascade even though the prior instance is still
	// serving. A pod that is Ready but not transitively controller-
	// owned by THIS backend's Deployment is rejected — see the godoc
	// above for why the ownership check is required.
	//
	// The per-pod identifier is <podUID>:<containerRunSum> where
	// containerRunSum is the sum of restart counts across the
	// cache-server's own containers (filtered by the owned
	// Deployment's template names). pod.UID alone is invariant across
	// in-place container restarts (kubelet restarting a crashed
	// container reuses the pod object), but the LMCache server
	// process inside that pod is fresh and every engine still holds
	// a stale socket. Including the restart-count sum makes that
	// case observable.
	type readyPod struct {
		name string
		id   string
	}
	ready := make([]readyPod, 0, len(pods.Items))
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.DeletionTimestamp != nil {
			continue
		}
		if !podIsReady(p) {
			continue
		}
		owned, err := r.podOwnedByDeployment(ctx, reader, p, &ownedDep)
		if err != nil {
			return "", false, err
		}
		if !owned {
			continue
		}
		ready = append(ready, readyPod{
			name: p.Name,
			id:   fmt.Sprintf("%s:%d", p.UID, containerRunSum(p, cacheServerContainers)),
		})
	}

	// Deployment convergence: spec.replicas == status.readyReplicas
	// == status.updatedReplicas == len(live Ready pods), AND the
	// controller has observed the current generation. When all four
	// hold, the Deployment is in steady state and a strict-superset
	// Ready set against the prior baseline reflects a legitimate
	// scale-up (not a maxSurge midpoint). All four are required:
	//   - readyReplicas == spec.replicas → right number of Ready pods
	//     (rules out maxSurge widening above target)
	//   - updatedReplicas == spec.replicas → all Ready pods run the
	//     latest revision (rules out maxSurge mid-rollout where the
	//     extras are the new revision)
	//   - observedGeneration >= metadata.generation → the controller
	//     has seen the current spec (rules out a just-changed spec
	//     whose effects haven't propagated yet)
	//   - len(live Ready pods) == spec.replicas → the live pod list
	//     we just took for the identifier MATCHES the Deployment
	//     status's claim. Without this clause, a stale Deployment
	//     status (the apps controller hasn't yet observed the
	//     maxSurge new pod) could report readyReplicas==1 while we
	//     observed 2 Ready pods in the same reconcile — the
	//     status counters would lie convergence even though the
	//     midpoint is genuinely transient, and we would persist the
	//     widened latch as a "scale-up" baseline. A later rollback
	//     dropping the new pod would then look like a real
	//     replacement (a UID disappeared from the latch) and
	//     false-cascade. Cross-checking against the live count is
	//     cheap and closes that race.
	// nil spec.Replicas defaults to 1 per the Deployment defaulter
	// (kubebuilder/apiserver default), so we collapse nil to 1.
	wantReplicas := int32(1)
	if ownedDep.Spec.Replicas != nil {
		wantReplicas = *ownedDep.Spec.Replicas
	}
	converged := ownedDep.Status.ObservedGeneration >= ownedDep.Generation &&
		ownedDep.Status.ReadyReplicas == wantReplicas &&
		ownedDep.Status.UpdatedReplicas == wantReplicas &&
		int32(len(ready)) == wantReplicas

	if len(ready) == 0 {
		return "", converged, nil
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].name < ready[j].name })
	ids := make([]string, len(ready))
	for i := range ready {
		ids[i] = ready[i].id
	}
	return strings.Join(ids, ","), converged, nil
}

// containerRunSum returns the sum of restart counts across the cache-
// server's own containers, scoped to the set of container names from
// the owned Deployment's pod template. Used to detect in-place
// restarts of the cache-server container (kubelet respawning a crashed
// container reuses the pod UID but increments restartCount). Sidecars
// outside the owned set are excluded — see currentServerInstanceID's
// godoc for why. init / ephemeral containers are also excluded
// (RestartCount is on containerStatuses, not init/ephemeral surfaces).
func containerRunSum(pod *corev1.Pod, cacheServerContainers map[string]struct{}) int32 {
	var sum int32
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if _, ok := cacheServerContainers[cs.Name]; !ok {
			continue
		}
		sum += cs.RestartCount
	}
	return sum
}

// detectServerOOM returns a human-readable diagnostic when a managed cache-server
// container was OOMKilled — its memory limit is too small for the model's KV
// working set, the silent under-provisioning failure mode — else "". It is
// best-effort: a pod-list error returns "" rather than failing the reconcile, and
// the caller consults it only when the workload is already ReplicasUnavailable, so
// it adds no pod List on the healthy path. The scan is scoped to the owned
// Deployment's container names (so a crashed service-mesh sidecar is never misread
// as the cache server) and reads BOTH the current and last termination state — a
// freshly-OOMKilled container, and one the kubelet has already restarted into
// CrashLoopBackOff (where OOMKilled is on lastState).
func (r *CacheBackendReconciler) detectServerOOM(ctx context.Context, backend *cachev1alpha1.CacheBackend, dep *appsv1.Deployment) string {
	reader := client.Reader(r.APIReader)
	if reader == nil {
		reader = r.Client
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods,
		client.InNamespace(backend.Namespace),
		client.MatchingLabelsSelector{Selector: labels.SelectorFromSet(selectorLabels(backend.Name))},
	); err != nil {
		return ""
	}
	serverContainers := make(map[string]struct{}, len(dep.Spec.Template.Spec.Containers))
	for _, c := range dep.Spec.Template.Spec.Containers {
		serverContainers[c.Name] = struct{}{}
	}
	for i := range pods.Items {
		p := &pods.Items[i]
		for j := range p.Status.ContainerStatuses {
			cs := &p.Status.ContainerStatuses[j]
			if _, ok := serverContainers[cs.Name]; !ok {
				continue
			}
			for _, term := range []*corev1.ContainerStateTerminated{cs.State.Terminated, cs.LastTerminationState.Terminated} {
				if term != nil && term.Reason == "OOMKilled" {
					return fmt.Sprintf("cache-server container %q in pod %q was OOMKilled — its memory limit is too small for the model's KV working set; raise spec.resources.limits.memory", cs.Name, p.Name)
				}
			}
		}
	}
	return ""
}

// podOwnedByDeployment reports whether pod is transitively controller-
// owned (pod → ReplicaSet → Deployment) by the given Deployment,
// matched on both name AND UID at every link. The (owned, err) split
// distinguishes "definitively not owned" (owned=false, err=nil — bare
// pod / non-Deployment / different Deployment) from "couldn't decide"
// (err != nil — transient apiserver/RBAC issue). Callers MUST
// propagate the error so currentServerInstanceID does not advance
// the latch over an incomplete picture; collapsing a transient
// ReplicaSet Get failure to "not owned" would shrink the identifier
// and look like a pod replacement, triggering a false cascade.
//
// A NotFound on the ReplicaSet is treated as "not owned" (the chain
// has been GCd) — the pod cannot be authoritatively attributed.
func (r *CacheBackendReconciler) podOwnedByDeployment(ctx context.Context, reader client.Reader, pod *corev1.Pod, dep *appsv1.Deployment) (bool, error) {
	rsRef := metav1.GetControllerOf(pod)
	if rsRef == nil || rsRef.Kind != "ReplicaSet" || !ownerRefIsAppsV1(rsRef) {
		return false, nil
	}
	var rs appsv1.ReplicaSet
	if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: rsRef.Name}, &rs); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get owning ReplicaSet %s/%s: %w", pod.Namespace, rsRef.Name, err)
	}
	if rs.UID != rsRef.UID {
		return false, nil
	}
	depRef := metav1.GetControllerOf(&rs)
	if depRef == nil || depRef.Kind != "Deployment" || !ownerRefIsAppsV1(depRef) {
		return false, nil
	}
	return depRef.Name == dep.Name && depRef.UID == dep.UID, nil
}

// ownerRefIsAppsV1 reports whether the owner reference points at
// apps/v1. OwnerReference.apiVersion is a required field, so a strict
// equality match is the right shape — an empty value is invalid input
// that we should reject rather than tolerate.
func ownerRefIsAppsV1(ref *metav1.OwnerReference) bool {
	return ref.APIVersion == "apps/v1"
}

// podIsReady reports whether the pod is in the Running phase with its
// Ready condition True. Mirrors the readiness signal kubelet writes into
// the pod's status, which is what the Deployment controller uses to
// decide whether to count a pod toward AvailableReplicas. We deliberately
// do not require a grace period beyond Ready=True: any pod the
// Deployment considers Available is a candidate for "the current server
// instance".
func podIsReady(p *corev1.Pod) bool {
	if p.Status.Phase != corev1.PodRunning {
		return false
	}
	for i := range p.Status.Conditions {
		c := &p.Status.Conditions[i]
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// cascadeRestartEngineDeployments finds every engine pod the webhook
// stamped against this backend, resolves each to its owning Deployment
// via the standard pod→ReplicaSet→Deployment owner chain, and patches
// the trigger annotation onto each unique Deployment's pod template.
// Returns the count of Deployments annotated.
//
// The pod filter is **the injected-by annotation pair**, not the
// EngineSelector. The webhook stamps `inferencecache.io/injected-by`
// AND `inferencecache.io/injected-by-uid` on every successful injection
// — both are required, and the UID half closes the forgery hole that
// `failurePolicy=Ignore` would otherwise leave open (an operator with
// pod-create RBAC could otherwise paste an `injected-by` value
// pointing at any backend and trick the cascade into rolling its
// engines). Filtering on the annotation pair, not the selector, also
// handles the cases the selector-based filter would silently miss:
//   - Operator removed `spec.engineSelector` after pods were injected.
//     The injected-by stamp persists on the pods, but the selector
//     no longer matches them.
//   - Pod labels drifted from the selector after admission (a
//     redeploy with edited labels). The pod still holds the stale
//     LMCache socket, but a label-selector list would miss it.
//
// Why annotate the Deployment's pod template (not the pod): the
// Deployment controller only watches its template; an annotation on a
// child pod has no rolling-restart effect. Patching
// `spec.template.metadata.annotations` is the same mechanism
// `kubectl rollout restart` uses (it stamps
// `kubectl.kubernetes.io/restartedAt`) — we just project-namespace the
// key.
//
// Engine pods owned by non-Deployment workloads (StatefulSet, bare
// Pod, Job, …) are skipped; rolling-restart via `spec.template`
// annotation is a Deployment-shaped contract, and the operator is
// responsible for restarting other workload kinds on a cache-server
// replacement.
//
// The pod List is namespace-scoped (no label selector) because the
// `injected-by` annotation is the authoritative wiring signal and
// annotations cannot be apiserver-side selectors. Namespace-bounded —
// the webhook only stamps `injected-by` on pods in the matched
// backend's namespace.
//
// Trust model: the only authority required to enqueue a cascade-
// restart is the CacheBackendReconciler's own SA, which has the
// `apps/deployments,patch` verb granted via this package's RBAC
// markers. The injected-by + injected-by-uid annotation pair we read
// from pods is normally stamped by the mutating Pod webhook (running
// as the controller SA), so an unprivileged pod-create user cannot
// forge it: when the webhook is reachable it overwrites or strips
// those annotations on every CREATE. The narrow forgery window opens
// only when the webhook is unreachable AT admission time and
// `MutatingWebhookConfiguration.failurePolicy=Ignore` lets the pod
// admit unmodified — in that case a caller who can read live CR /
// ReplicaSet / Deployment UIDs could plant a pod whose annotations +
// ownerRef chain looks legitimate, triggering a cascade-restart of a
// Deployment they do not have direct patch RBAC on. The blast radius
// is bounded to the same namespace as the CacheBackend (pod-list is
// namespace-scoped here, and the webhook only matches CRs in the
// pod's namespace), and a normal cluster keeps the webhook reachable
// — but operators running with hostile namespace tenants should
// either set `failurePolicy=Fail` on the mutating webhook or accept
// that pod-create RBAC in a namespace is elevated to "force-restart
// any in-namespace Deployment whose template the controller can
// patch". The cache plane's existing engine-pod-events controller
// makes the same trust assumption for the same reason.
func (r *CacheBackendReconciler) cascadeRestartEngineDeployments(ctx context.Context, backend *cachev1alpha1.CacheBackend, serverInstanceID string) (int, error) {
	reader := client.Reader(r.APIReader)
	if reader == nil {
		reader = r.Client
	}

	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(backend.Namespace)); err != nil {
		return 0, fmt.Errorf("list engine pods: %w", err)
	}

	wantInjectedBy := backend.Namespace + "/" + backend.Name
	wantInjectedByUID := string(backend.UID)
	// Dedupe targets by (name, UID) — not name alone. The owner-chain
	// walk in podOwningDeploymentName verified the Deployment's UID at
	// the moment of resolution, but a Deployment could be deleted and
	// re-created under the same name between resolution and the patch
	// loop; annotating by name alone in that window would roll an
	// unrelated workload that happens to share the name. Carrying the
	// expected UID lets annotateDeploymentForCascade re-check before
	// patching, closing the TOCTOU window.
	type targetRef struct {
		uid string
	}
	targets := map[string]targetRef{}
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Annotations[podwebhook.AnnotationInjectedBy] != wantInjectedBy {
			continue
		}
		// Require the matching injected-by-uid. The webhook always
		// writes both annotations on a successful injection; a pod
		// carrying only `injected-by` (or `injected-by-uid` with a
		// stale UID) is either a forgery or a survivor from a CR
		// that was deleted and recreated. In both cases the pod is
		// no longer wired to THIS CR's cache-server, so cascading
		// would either roll an unrelated workload or do nothing —
		// neither is helpful.
		if wantInjectedByUID == "" || p.Annotations[podwebhook.AnnotationInjectedByUID] != wantInjectedByUID {
			continue
		}
		depName, depUID, ok, err := r.podOwningDeployment(ctx, reader, p)
		if err != nil {
			return 0, err
		}
		if !ok {
			continue
		}
		// Self-target guard: never cascade-annotate the backend's own
		// cache-server Deployment. The canonical owned Deployment is
		// named after the backend (see buildDeployment in
		// cachebackend_controller.go); podOwningDeployment validates
		// UID at every link in the owner chain, so a depName equal to
		// backend.Name here means we resolved to OUR Deployment, not
		// a foreign Deployment squatting on the same name.
		//
		// Without this guard, a misconfigured spec.engineSelector that
		// overlaps the cache-server pod's labels — combined with a
		// webhook decision to stamp the cache-server pod with this
		// backend's injected-by + injected-by-uid annotations — would
		// pull the cache-server Deployment into the target set. The
		// resulting annotate-patch bumps the pod template, the
		// Deployment rolls a new cache-server pod, the controller
		// observes the new pod's UID, and reconcileServerInstance
		// fires another cascade — an infinite self-induced rollout
		// loop. The cache-server's own recovery is observation-driven
		// (status.observedServerInstance); a forced rollout from
		// this path is never the right answer.
		if depName == backend.Name {
			continue
		}
		// First write wins; if a later pod resolves the same name to a
		// different UID, that means the chain has churned since the
		// pod List — keep the UID we saw first (the patch step will
		// reject if neither identity matches the live Deployment).
		if _, exists := targets[depName]; !exists {
			targets[depName] = targetRef{uid: depUID}
		}
	}

	// Sort for deterministic patch order (helps tests, helps log
	// readability; the apiserver does not care).
	names := make([]string, 0, len(targets))
	for n := range targets {
		names = append(names, n)
	}
	sort.Strings(names)

	annotated := 0
	for _, name := range names {
		patched, err := r.annotateDeploymentForCascade(ctx, backend.Namespace, name, targets[name].uid, serverInstanceID)
		if err != nil {
			return annotated, err
		}
		if patched {
			annotated++
		}
	}
	return annotated, nil
}

// podOwningDeployment walks pod → controller-owning ReplicaSet →
// controller-owning Deployment and returns the Deployment's name AND
// observed UID (in the same namespace as the pod — apps/v1 ownership
// is namespaced). The UID is carried back so the caller can re-verify
// at the moment of patch, closing a TOCTOU window where the resolved
// Deployment is deleted and re-created under the same name between
// resolution and annotate.
//
// The (found, err) split distinguishes "definitively not Deployment-
// owned" (found=false, err=nil — bare pod, StatefulSet, etc.) from
// "couldn't decide" (err != nil — transient apiserver / RBAC issue).
// Callers MUST propagate the error so the cascade doesn't advance
// status.observedServerInstance over an incomplete picture; silently
// collapsing transient errors to "not owned" would let the cascade
// skip a Deployment whose owner chain happened to fail to Get, while
// the latch still moves forward, leaving the engine pods with stale
// sockets that will never be retried.
//
// Each link is checked on both name AND UID: a name-only match would
// resolve a stale or forged ownerRef to the wrong live object (a
// Deployment recreated under the same name is the canonical bad
// case). The (kind, apiVersion) check rejects ownerRefs to non-
// apps/v1 ReplicaSets/Deployments (defensive — a CRD shaped like a
// ReplicaSet/Deployment from another apiGroup must not be picked up).
//
// A genuine NotFound on the ReplicaSet or Deployment (the chain has
// been GCd between our pod List and the Get) returns
// (found=false, err=nil) — the pod's owner chain is gone, so it
// cannot be cascaded anyway. Non-NotFound errors bubble up.
func (r *CacheBackendReconciler) podOwningDeployment(ctx context.Context, reader client.Reader, pod *corev1.Pod) (string, string, bool, error) {
	rsRef := metav1.GetControllerOf(pod)
	if rsRef == nil || rsRef.Kind != "ReplicaSet" || !ownerRefIsAppsV1(rsRef) {
		return "", "", false, nil
	}
	var rs appsv1.ReplicaSet
	if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: rsRef.Name}, &rs); err != nil {
		if apierrors.IsNotFound(err) {
			return "", "", false, nil
		}
		return "", "", false, fmt.Errorf("get owning ReplicaSet %s/%s: %w", pod.Namespace, rsRef.Name, err)
	}
	if rs.UID != rsRef.UID {
		// Name resolved to a different live RS (re-created under the
		// same name). Not the pod's actual owner.
		return "", "", false, nil
	}
	depRef := metav1.GetControllerOf(&rs)
	if depRef == nil || depRef.Kind != "Deployment" || !ownerRefIsAppsV1(depRef) {
		return "", "", false, nil
	}
	// Verify the Deployment named in depRef still exists with the
	// declared UID. A name-only return would let a stale ownerRef
	// resolve to a brand-new Deployment that happens to share the
	// name — which we'd then cascade-restart unrelated work.
	var dep appsv1.Deployment
	if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: depRef.Name}, &dep); err != nil {
		if apierrors.IsNotFound(err) {
			return "", "", false, nil
		}
		return "", "", false, fmt.Errorf("get owning Deployment %s/%s: %w", pod.Namespace, depRef.Name, err)
	}
	if dep.UID != depRef.UID {
		return "", "", false, nil
	}
	return dep.Name, string(dep.UID), true, nil
}

// annotateDeploymentForCascade patches AnnotationCacheServerRestartTrigger
// onto the Deployment's pod template annotations using a JSON merge
// patch, so concurrent writers on other template fields are not
// clobbered. Returns whether a patch was actually issued (false when
// the trigger already carried serverInstanceID — guards against a
// no-op rollout if the reconciler retries on the same identifier).
//
// expectedUID is the Deployment UID observed at owner-chain resolution
// time. The TOCTOU window between resolution and patch is closed at
// TWO points:
//
//  1. The pre-patch Get goes through APIReader (uncached) so the UID
//     we compare against is the live apiserver value, not a stale
//     cached object. Without this, a cache that hadn't yet seen a
//     same-name recreate would pass the UID check against the
//     pre-recreate object and we would then patch the post-recreate
//     unrelated Deployment.
//
//  2. The Patch carries an optimistic-lock precondition derived from
//     the resourceVersion read in step 1 (MergeFromWithOptimisticLock).
//     If the live Deployment has been updated OR deleted-and-recreated
//     between our Get and the Patch, the apiserver returns Conflict
//     and the patch is rejected — even though it addresses by name.
//     We surface the conflict as an error so the cascade retries on
//     the next reconcile against the freshest view.
//
// A NotFound on the live Deployment (the target has been GCd in the
// resolution-to-patch window) returns (false, nil) — there is
// nothing to roll. A UID mismatch likewise returns (false, nil) —
// the same-name workload that exists today is not ours.
func (r *CacheBackendReconciler) annotateDeploymentForCascade(ctx context.Context, namespace, name, expectedUID, serverInstanceID string) (bool, error) {
	// Use the uncached reader for the UID/resourceVersion read; see
	// the godoc above. Fall back to the cached client only when
	// APIReader is unset (unit-test wiring constructs reconcilers
	// without one).
	reader := client.Reader(r.APIReader)
	if reader == nil {
		reader = r.Client
	}
	var dep appsv1.Deployment
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &dep); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get engine deployment %s/%s: %w", namespace, name, err)
	}
	if string(dep.UID) != expectedUID {
		// Live Deployment has a different UID than the one resolved
		// during pod owner-chain walking — same name, different
		// object. Refuse to roll the unrelated workload.
		return false, nil
	}
	if dep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger] == serverInstanceID {
		// Already up to date — somebody (us, on a retry) already
		// patched this round. Skip without bumping the rollout
		// revision.
		return false, nil
	}
	before := dep.DeepCopy()
	if dep.Spec.Template.Annotations == nil {
		dep.Spec.Template.Annotations = map[string]string{}
	}
	dep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger] = serverInstanceID
	// MergeFromWithOptimisticLock embeds before.ResourceVersion as a
	// precondition on the merge patch: if the apiserver-side object
	// has been updated (including delete+recreate, which assigns a
	// fresh resourceVersion) since the Get above, the patch is
	// rejected with Conflict. Surface as an error so the next
	// reconcile retries against the freshest view.
	patch := client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})
	if err := r.Patch(ctx, &dep, patch); err != nil {
		return false, fmt.Errorf("patch engine deployment %s/%s pod-template annotations: %w", namespace, name, err)
	}
	return true, nil
}

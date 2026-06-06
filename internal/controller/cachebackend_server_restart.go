package controller

import (
	"context"
	"fmt"
	"sort"
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
// replaced. The value is the new cache-server pod's metadata.uid, so two
// quick replacements in succession produce distinct annotation values (and
// distinct rollout revisions). Modeled on the kubectl rollout restart pattern
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
// serverInstanceCascade.canCascade). Controller restarts reset the window,
// which is the intended behavior — a controller restart should freely
// cascade once on the first reconcile of each backend, since the engines
// may also hold stale connections.
const DefaultMinServerRestartCascadeInterval = 30 * time.Second

// cascadeRestartReasonServerInstanceChanged is the metric label value used
// when the cache-server pod's UID differs from the value last persisted to
// status.observedServerInstance. It is the only reason today; future
// non-UID-change triggers (e.g. an operator-initiated "force cascade"
// surface) would add their own value. Kept as a constant so the metric
// label set is stable and grep-discoverable.
const cascadeRestartReasonServerInstanceChanged = "server_instance_changed"

// backendServerRestartsTotal counts cascade-restart events the controller
// has issued, partitioned by the namespaced CacheBackend and the cascade
// reason. It is registered into the controller-runtime metrics registry on
// package init so it appears on the manager's /metrics endpoint (no
// per-Service registry — see pkg/server/metrics.go for the
// other-direction posture). The Counter is created once at process start
// and is safe to mutate concurrently; tests reset its inner state by
// resetting the registry (see resetBackendServerRestartsTotalForTest).
var backendServerRestartsTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "inferencecache_backend_server_restarts_total",
		Help: "Cumulative count of cache-server-restart cascades issued by the CacheBackend controller. A single cascade re-annotates every injected engine Deployment for the backend at once; the count is incremented once per cascade, regardless of how many Deployments were touched (zero is a valid cascade — see status.observedServerInstance docs). Partitioned by the CacheBackend's namespace + name and a short reason code.",
	},
	[]string{"namespace", "backend", "reason"},
)

func init() {
	ctrlmetrics.Registry.MustRegister(backendServerRestartsTotal)
}

// resetBackendServerRestartsTotalForTest resets the cascade counter to
// zero for every label combination. Package-private so tests in this
// package can assert on per-test counts without leaking state across
// runs; intentionally not exported because production callers have no
// reason to zero an operator-visible metric.
func resetBackendServerRestartsTotalForTest() {
	backendServerRestartsTotal.Reset()
}

// serverInstanceCascade tracks per-backend rate-limiting state for the
// server-restart cascade. Used in-process by CacheBackendReconciler; a
// process restart clears it (intentional — see DefaultMinServerRestartCascadeInterval).
type serverInstanceCascade struct {
	mu     sync.Mutex
	lastAt map[types.NamespacedName]time.Time
}

func newServerInstanceCascade() *serverInstanceCascade {
	return &serverInstanceCascade{lastAt: map[types.NamespacedName]time.Time{}}
}

// canCascade reports whether enough time has elapsed since the previous
// cascade for the given backend. If true, it stamps now as the most
// recent cascade time. If false, it returns the remaining wait until the
// next cascade is allowed so the caller can RequeueAfter exactly that
// long. The check + stamp are done under one lock so concurrent
// reconciles of the same backend do not double-cascade.
func (s *serverInstanceCascade) canCascade(key types.NamespacedName, now time.Time, window time.Duration) (bool, time.Duration) {
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

// minServerRestartCascadeInterval returns the effective rate-limit window
// for this reconciler — the per-reconciler override, or
// DefaultMinServerRestartCascadeInterval when unset.
func (r *CacheBackendReconciler) minServerRestartCascadeInterval() time.Duration {
	if r.MinServerRestartCascadeInterval > 0 {
		return r.MinServerRestartCascadeInterval
	}
	return DefaultMinServerRestartCascadeInterval
}

// reconcileServerInstance observes the current Ready cache-server pod UID
// for this managed backend and, on a change from the persisted
// status.observedServerInstance, cascade-restarts every injected engine
// Deployment by patching AnnotationCacheServerRestartTrigger onto each
// Deployment's pod template. Status is patched only when the observed UID
// changes; the cascade is rate-limited per CacheBackend (see
// DefaultMinServerRestartCascadeInterval) and the function returns a
// non-zero requeue when the rate-limit deferred the cascade so the
// caller's reconcile result schedules the retry exactly at the window
// boundary.
//
// Empty→set transitions of the UID NEVER cascade: there are no engines
// holding a stale connection to a not-yet-existed server. The first
// observation only persists the UID as the baseline; subsequent
// transitions cascade.
//
// Fail-soft: every error path (Pod list, Deployment list, Patch failure)
// returns nil and only logs at V(1). Cascading is best-effort recovery
// from a known soft-failure mode; it must never escalate a transient
// apiserver hiccup into a Reconcile error that backs off the rest of
// the reconcile.
func (r *CacheBackendReconciler) reconcileServerInstance(ctx context.Context, logger logr.Logger, backend *cachev1alpha1.CacheBackend) time.Duration {
	currentUID, err := r.currentServerInstanceUID(ctx, backend)
	if err != nil {
		logger.V(1).Info("server-restart cascade skipped: cache-server pod list failed",
			"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
		return 0
	}
	if currentUID == "" {
		// No Ready cache-server pod yet — nothing to anchor to.
		return 0
	}

	prior := backend.Status.ObservedServerInstance
	if prior == currentUID {
		return 0
	}

	// Empty → set: first observation. Persist as the baseline and stop;
	// there are no engines depending on a not-yet-existed server.
	if prior == "" {
		if err := r.patchStatus(ctx, backend, func() {
			backend.Status.ObservedServerInstance = currentUID
		}); err != nil {
			logger.V(1).Info("server-restart cascade: initial observedServerInstance patch failed",
				"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
		}
		return 0
	}

	// Rate-limit: a cascade per backend is allowed at most once per
	// minServerRestartCascadeInterval. While the window is open we
	// neither cascade nor advance status.observedServerInstance — leaving
	// the prior value pinned guarantees the next eligible reconcile still
	// sees the change and tries again.
	key := types.NamespacedName{Namespace: backend.Namespace, Name: backend.Name}
	if r.serverInstanceCascade == nil {
		// Defensive lazy-init; the manager wiring should set this in
		// SetupWithManager but unit tests construct the reconciler
		// directly and skip that path.
		r.serverInstanceCascade = newServerInstanceCascade()
	}
	ok, wait := r.serverInstanceCascade.canCascade(key, time.Now(), r.minServerRestartCascadeInterval())
	if !ok {
		logger.V(1).Info("server-restart cascade deferred: rate-limited",
			"namespace", backend.Namespace, "name", backend.Name,
			"priorUID", prior, "currentUID", currentUID, "retryAfter", wait.String())
		return wait
	}

	count, err := r.cascadeRestartEngineDeployments(ctx, backend, currentUID)
	if err != nil {
		// Soft-fail: log and keep going. The next reconcile (or the
		// matched-pods cadence requeue) will retry. Do NOT advance
		// status.observedServerInstance — leaving it on the prior value
		// keeps the change visible to the retry.
		logger.V(1).Info("server-restart cascade: engine Deployment list/patch failed",
			"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
		return r.minServerRestartCascadeInterval()
	}
	backendServerRestartsTotal.WithLabelValues(backend.Namespace, backend.Name, cascadeRestartReasonServerInstanceChanged).Inc()
	logger.V(1).Info("server-restart cascade: engine Deployments annotated",
		"namespace", backend.Namespace, "name", backend.Name,
		"priorUID", prior, "currentUID", currentUID, "deployments", count)

	if err := r.patchStatus(ctx, backend, func() {
		backend.Status.ObservedServerInstance = currentUID
	}); err != nil {
		logger.V(1).Info("server-restart cascade: observedServerInstance patch failed",
			"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
	}
	return 0
}

// currentServerInstanceUID returns a stable identifier representing the
// current set of Ready cache-server pods for the backend, or "" when no
// Ready pod exists yet. The candidate set is the owned Deployment's
// pods — pods whose controller-owning ReplicaSet is controller-owned by
// the backend-owned Deployment, identified by both name AND UID so a
// foreign Ready pod that happens to carry the same controller-managed
// labels (or a stale ownerRef name that resolves to a different live
// object) cannot advance observedServerInstance and spuriously trigger
// an engine rollout.
//
// The identifier is a comma-joined, lex-sorted list of every Ready
// pod's UID — NOT just the lex-smallest. Multi-replica managed
// backends (`spec.replicas > 1` on non-PVC backends) replace pods
// independently; tracking only one would let a restart of any other
// replica slip past observedServerInstance, so engines bound to that
// replica would keep their stale LMCache socket forever. Including
// every Ready pod's UID means a single replacement always changes the
// identifier and cascades.
//
// Reads via APIReader (uncached) where possible to avoid registering a
// Pod informer; the controller's design explicitly rejects watching
// all pods cluster-wide (see refreshMatchedEnginePods godoc). Falls
// back to the cached client when APIReader is nil (test wiring).
func (r *CacheBackendReconciler) currentServerInstanceUID(ctx context.Context, backend *cachev1alpha1.CacheBackend) (string, error) {
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
			return "", nil
		}
		return "", fmt.Errorf("get owned cache-server deployment: %w", err)
	}
	if !metav1.IsControlledBy(&ownedDep, backend) {
		// Foreign Deployment sharing the backend's name. Refuse to
		// attribute its pods to this CacheBackend.
		return "", nil
	}

	matcher := labels.SelectorFromSet(selectorLabels(backend.Name))
	var pods corev1.PodList
	if err := reader.List(ctx, &pods,
		client.InNamespace(backend.Namespace),
		client.MatchingLabelsSelector{Selector: matcher},
	); err != nil {
		return "", fmt.Errorf("list cache-server pods: %w", err)
	}
	// Collect every Ready, attributable pod's UID. A pod that is
	// mid-rollout (Pending / Terminating) does not represent a serving
	// instance — including it would let a rollout's transient state
	// trigger a cascade even though the prior instance is still
	// serving. A pod that is Ready but not transitively controller-
	// owned by THIS backend's Deployment is rejected — see the godoc
	// above for why the ownership check is required.
	type readyPod struct {
		name string
		uid  string
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
		if !r.podOwnedByDeployment(ctx, reader, p, &ownedDep) {
			continue
		}
		ready = append(ready, readyPod{name: p.Name, uid: string(p.UID)})
	}
	if len(ready) == 0 {
		return "", nil
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].name < ready[j].name })
	uids := make([]string, len(ready))
	for i := range ready {
		uids[i] = ready[i].uid
	}
	return strings.Join(uids, ","), nil
}

// podOwnedByDeployment reports whether pod is transitively controller-
// owned (pod → ReplicaSet → Deployment) by the given Deployment, matched
// on both name AND UID at every link. The UID check is what protects
// against a stale ownerRef name resolving to a different live object
// (a name-only check would be fooled by a Deployment recreated under
// the same name). apiVersion is checked at the (kind, apiGroup) level
// to tolerate `apps/v1` written as `apps/v1` vs the rare bare `v1`.
func (r *CacheBackendReconciler) podOwnedByDeployment(ctx context.Context, reader client.Reader, pod *corev1.Pod, dep *appsv1.Deployment) bool {
	rsRef := metav1.GetControllerOf(pod)
	if rsRef == nil || rsRef.Kind != "ReplicaSet" || !ownerRefIsAppsV1(rsRef) {
		return false
	}
	var rs appsv1.ReplicaSet
	if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: rsRef.Name}, &rs); err != nil {
		return false
	}
	if rs.UID != rsRef.UID {
		return false
	}
	depRef := metav1.GetControllerOf(&rs)
	if depRef == nil || depRef.Kind != "Deployment" || !ownerRefIsAppsV1(depRef) {
		return false
	}
	return depRef.Name == dep.Name && depRef.UID == dep.UID
}

// ownerRefIsAppsV1 reports whether the owner reference's APIVersion
// resolves to apps/v1. We tolerate the rare case of an empty APIVersion
// when the apiserver normalizes it away on read.
func ownerRefIsAppsV1(ref *metav1.OwnerReference) bool {
	return ref.APIVersion == "" || ref.APIVersion == "apps/v1"
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
func (r *CacheBackendReconciler) cascadeRestartEngineDeployments(ctx context.Context, backend *cachev1alpha1.CacheBackend, newUID string) (int, error) {
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
	deploymentNames := map[string]struct{}{}
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
		depName, ok := r.podOwningDeploymentName(ctx, reader, p)
		if !ok {
			continue
		}
		deploymentNames[depName] = struct{}{}
	}

	// Sort for deterministic patch order (helps tests, helps log
	// readability; the apiserver does not care).
	names := make([]string, 0, len(deploymentNames))
	for n := range deploymentNames {
		names = append(names, n)
	}
	sort.Strings(names)

	annotated := 0
	for _, name := range names {
		patched, err := r.annotateDeploymentForCascade(ctx, backend.Namespace, name, newUID)
		if err != nil {
			return annotated, err
		}
		if patched {
			annotated++
		}
	}
	return annotated, nil
}

// podOwningDeploymentName walks pod → controller-owning ReplicaSet →
// controller-owning Deployment and returns the Deployment's name (in
// the same namespace as the pod — apps/v1 ownership is namespaced).
// Returns false when any link in the chain is missing or the pod is
// not owned by a Deployment-shaped workload. Each link is checked on
// both name AND UID: a name-only match would resolve a stale or
// forged ownerRef to the wrong live object (a Deployment recreated
// under the same name is the canonical bad case). The
// (kind, apiVersion) check rejects ownerRefs to non-apps/v1
// ReplicaSets/Deployments (rare; defensive — a CRD shaped like a
// ReplicaSet/Deployment from another apiGroup must not be picked up).
func (r *CacheBackendReconciler) podOwningDeploymentName(ctx context.Context, reader client.Reader, pod *corev1.Pod) (string, bool) {
	rsRef := metav1.GetControllerOf(pod)
	if rsRef == nil || rsRef.Kind != "ReplicaSet" || !ownerRefIsAppsV1(rsRef) {
		return "", false
	}
	var rs appsv1.ReplicaSet
	if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: rsRef.Name}, &rs); err != nil {
		// Transient NotFound is common (RS could have been GCd between
		// pod creation and our List). Fail-soft.
		return "", false
	}
	if rs.UID != rsRef.UID {
		// Name resolved to a different live RS (re-created under the
		// same name). Not the pod's actual owner — fail-soft.
		return "", false
	}
	depRef := metav1.GetControllerOf(&rs)
	if depRef == nil || depRef.Kind != "Deployment" || !ownerRefIsAppsV1(depRef) {
		return "", false
	}
	// Verify the Deployment named in depRef still exists with the
	// declared UID. A name-only return would let a stale ownerRef
	// resolve to a brand-new Deployment that happens to share the
	// name — which we'd then cascade-restart unrelated work.
	var dep appsv1.Deployment
	if err := reader.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: depRef.Name}, &dep); err != nil {
		return "", false
	}
	if dep.UID != depRef.UID {
		return "", false
	}
	return dep.Name, true
}

// annotateDeploymentForCascade patches AnnotationCacheServerRestartTrigger
// onto the Deployment's pod template annotations using a JSON merge
// patch, so concurrent writers on other template fields are not
// clobbered. Returns whether a patch was actually issued (false when the
// trigger already carried newUID — guards against a no-op rollout if the
// reconciler retries on the same UID). A NotFound on the Deployment is
// treated as a successful no-op.
func (r *CacheBackendReconciler) annotateDeploymentForCascade(ctx context.Context, namespace, name, newUID string) (bool, error) {
	var dep appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &dep); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get engine deployment %s/%s: %w", namespace, name, err)
	}
	if dep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger] == newUID {
		// Already up to date — somebody (us, on a retry) already
		// patched this round. Skip without bumping the rollout
		// revision.
		return false, nil
	}
	before := dep.DeepCopy()
	if dep.Spec.Template.Annotations == nil {
		dep.Spec.Template.Annotations = map[string]string{}
	}
	dep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger] = newUID
	if err := r.Patch(ctx, &dep, client.MergeFrom(before)); err != nil {
		return false, fmt.Errorf("patch engine deployment %s/%s pod-template annotations: %w", namespace, name, err)
	}
	return true, nil
}

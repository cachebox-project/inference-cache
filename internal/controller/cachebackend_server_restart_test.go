package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
)

// cascadeRestartFixture builds a fake-client-backed reconciler plus a fully
// wired managed CacheBackend with one Ready cache-server pod (transitively
// owned by the CacheBackend-owned Deployment+ReplicaSet, the way the apps
// controller stack would create them) and one engine
// Deployment+ReplicaSet+Pod that the webhook has injected against the
// backend. Shared by every cascade test so each scenario only expresses
// what's different (UID, status, rate-limit window, …) and the boring
// setup stays terse.
type cascadeRestartFixture struct {
	r          *CacheBackendReconciler
	backend    *cachev1alpha1.CacheBackend
	serverDep  *appsv1.Deployment
	serverRS   *appsv1.ReplicaSet
	serverPod  *corev1.Pod
	engineDep  *appsv1.Deployment
	engineRS   *appsv1.ReplicaSet
	enginePod  *corev1.Pod
	engineNS   string
	cacheNS    string
	cacheName  string
	engineDepN string
	enginePodN string
}

func newCascadeRestartFixture(t *testing.T, opts ...func(*cascadeRestartFixture)) *cascadeRestartFixture {
	t.Helper()
	f := &cascadeRestartFixture{
		cacheNS:    "team-a",
		cacheName:  "cache",
		engineNS:   "team-a",
		engineDepN: "vllm-engine",
		enginePodN: "vllm-engine-abc",
	}
	for _, o := range opts {
		o(f)
	}

	scheme := newScheme(t)

	f.backend = &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{
			Name:       f.cacheName,
			Namespace:  f.cacheNS,
			UID:        "cache-uid-1",
			Generation: 1,
		},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type: cachev1alpha1.CacheBackendTypeLMCache,
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app": "vllm-engine"},
			},
		},
	}

	tru := true
	// Cache-server Deployment+ReplicaSet the reconciler "owns" — the
	// transitive owner chain currentServerInstanceID's strengthened
	// ownership check requires to attribute a Ready pod to this backend.
	// The Deployment's controller-owner reference points at the
	// CacheBackend, which is what IsControlledBy expects.
	f.serverDep = &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      f.cacheName,
			Namespace: f.cacheNS,
			UID:       "cache-dep-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: cachev1alpha1.GroupVersion.String(),
				Kind:       "CacheBackend",
				Name:       f.backend.Name,
				UID:        f.backend.UID,
				Controller: &tru,
			}},
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: selectorLabels(f.cacheName)},
				// containerRunSum scopes its restart-count sum to
				// container names from THIS template, so the test
				// must enumerate the cache-server's container name
				// (lmcache-server) — sidecars added to the pod by
				// other admission webhooks would not be included.
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "lmcache-server", Image: "lmcache:test"}},
				},
			},
		},
	}
	f.serverRS = &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      f.cacheName + "-rs",
			Namespace: f.cacheNS,
			UID:       "cache-rs-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       f.cacheName,
				UID:        f.serverDep.UID,
				Controller: &tru,
			}},
		},
	}
	// The "current Ready" cache-server pod the controller observes.
	// Labeled with the exact selectorLabels() set and owner-referenced
	// up the chain to serverDep so currentServerInstanceID's transitive
	// ownership check admits it.
	f.serverPod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cache-pod-aaa",
			Namespace: f.cacheNS,
			UID:       "cache-pod-uid-1",
			Labels:    selectorLabels(f.cacheName),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       f.serverRS.Name,
				UID:        f.serverRS.UID,
				Controller: &tru,
			}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}

	// Engine Deployment, the cascade target.
	f.engineDep = &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      f.engineDepN,
			Namespace: f.engineNS,
			UID:       "engine-dep-uid",
		},
		Spec: appsv1.DeploymentSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": "vllm-engine"},
				},
			},
		},
	}
	f.engineRS = &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      f.engineDepN + "-rs",
			Namespace: f.engineNS,
			UID:       "engine-rs-uid",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       f.engineDepN,
				UID:        f.engineDep.UID,
				Controller: &tru,
			}},
		},
	}
	f.enginePod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      f.enginePodN,
			Namespace: f.engineNS,
			UID:       "engine-pod-uid",
			Labels:    map[string]string{"app": "vllm-engine"},
			Annotations: map[string]string{
				podwebhook.AnnotationInjectedBy:    f.cacheNS + "/" + f.cacheName,
				podwebhook.AnnotationInjectedByUID: string(f.backend.UID),
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       f.engineRS.Name,
				UID:        f.engineRS.UID,
				Controller: &tru,
			}},
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheBackend{}, &appsv1.Deployment{}).
		WithObjects(f.backend, f.serverDep, f.serverRS, f.serverPod, f.engineDep, f.engineRS, f.enginePod).
		Build()
	f.r = &CacheBackendReconciler{
		Client:                          c,
		Scheme:                          scheme,
		Log:                             logr.Discard(),
		MinServerRestartCascadeInterval: 50 * time.Millisecond, // tests run with a tiny window
		serverInstanceCascade:           newServerInstanceCascade(),
	}

	resetBackendServerRestartCascadesTotalForTest()
	return f
}

func (f *cascadeRestartFixture) reload(t *testing.T) {
	t.Helper()
	cb := &cachev1alpha1.CacheBackend{}
	if err := f.r.Get(context.Background(), types.NamespacedName{Name: f.cacheName, Namespace: f.cacheNS}, cb); err != nil {
		t.Fatalf("reload backend: %v", err)
	}
	f.backend = cb
}

func (f *cascadeRestartFixture) reloadEngineDep(t *testing.T) {
	t.Helper()
	dep := &appsv1.Deployment{}
	if err := f.r.Get(context.Background(), types.NamespacedName{Name: f.engineDepN, Namespace: f.engineNS}, dep); err != nil {
		t.Fatalf("reload engine dep: %v", err)
	}
	f.engineDep = dep
}

// serverInstanceID is the per-pod identifier currentServerInstanceID
// computes: <pod.UID>:<containerRunSum>. Shared by the assertion
// helpers so tests build the expected observedServerInstance value
// without duplicating the format. Mirrors containerRunSum in
// cachebackend_server_restart.go.
func serverInstanceID(p *corev1.Pod) string {
	var sum int32
	for i := range p.Status.ContainerStatuses {
		sum += p.Status.ContainerStatuses[i].RestartCount
	}
	return fmt.Sprintf("%s:%d", p.UID, sum)
}

func cascadeRestartsCount(t *testing.T, namespace, backend, reason string) float64 {
	t.Helper()
	m, err := backendServerRestartCascadesTotal.GetMetricWithLabelValues(namespace, backend, reason)
	if err != nil {
		t.Fatalf("get counter %s/%s/%s: %v", namespace, backend, reason, err)
	}
	var pb dto.Metric
	if err := m.(prometheus.Counter).Write(&pb); err != nil {
		t.Fatalf("write counter: %v", err)
	}
	return pb.GetCounter().GetValue()
}

func TestReconcileServerInstance_FirstObservationStampsBaseline(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Status starts empty (no ObservedServerInstance). The first call
	// should persist the UID baseline and NOT cascade-restart.
	if got := f.backend.Status.ObservedServerInstance; got != "" {
		t.Fatalf("precondition: ObservedServerInstance = %q, want empty", got)
	}

	wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend)
	if wait != 0 {
		t.Fatalf("wait = %v, want 0 (first observation never rate-limits)", wait)
	}

	f.reload(t)
	if got := f.backend.Status.ObservedServerInstance; got != serverInstanceID(f.serverPod) {
		t.Fatalf("ObservedServerInstance = %q, want %q", got, serverInstanceID(f.serverPod))
	}
	f.reloadEngineDep(t)
	if _, ok := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; ok {
		t.Fatalf("engine deployment got cascade annotation on first observation; want no cascade until a UID transition")
	}
	if got := cascadeRestartsCount(t, f.cacheNS, f.cacheName, cascadeRestartReasonServerInstanceChanged); got != 0 {
		t.Fatalf("cascade counter = %v, want 0 (first observation never cascades)", got)
	}
}

func TestReconcileServerInstance_UIDChangeCascadesEngineDeployment(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Seed status with the prior UID so the call observes a transition.
	f.backend.Status.ObservedServerInstance = "previous-server-uid"
	if err := f.r.Status().Update(context.Background(), f.backend); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	f.reload(t)

	wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend)
	if wait != 0 {
		t.Fatalf("wait = %v, want 0 (rate-limit window has not been used before)", wait)
	}

	f.reloadEngineDep(t)
	got := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]
	if got != serverInstanceID(f.serverPod) {
		t.Fatalf("cascade annotation = %q, want %q (the new cache-server pod UID)", got, serverInstanceID(f.serverPod))
	}

	f.reload(t)
	if got := f.backend.Status.ObservedServerInstance; got != serverInstanceID(f.serverPod) {
		t.Fatalf("ObservedServerInstance = %q, want %q", got, serverInstanceID(f.serverPod))
	}
	if got := cascadeRestartsCount(t, f.cacheNS, f.cacheName, cascadeRestartReasonServerInstanceChanged); got != 1 {
		t.Fatalf("cascade counter = %v, want 1", got)
	}
}

func TestReconcileServerInstance_RateLimitedSecondCascadeIsDeferred(t *testing.T) {
	f := newCascadeRestartFixture(t)
	f.r.MinServerRestartCascadeInterval = 1 * time.Hour // make the window effectively block any second cascade

	// Seed prior UID and a fresh ready pod with the current UID; first call cascades.
	f.backend.Status.ObservedServerInstance = "previous-server-uid"
	if err := f.r.Status().Update(context.Background(), f.backend); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	f.reload(t)
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("first cascade wait = %v, want 0", wait)
	}

	// Now simulate a second UID flip: replace the server pod with a fresh one carrying a new UID.
	if err := f.r.Delete(context.Background(), f.serverPod); err != nil {
		t.Fatalf("delete first server pod: %v", err)
	}
	newPod := f.serverPod.DeepCopy()
	newPod.ResourceVersion = ""
	newPod.Name = "cache-pod-bbb"
	newPod.UID = "cache-pod-uid-2"
	if err := f.r.Create(context.Background(), newPod); err != nil {
		t.Fatalf("create second server pod: %v", err)
	}
	if err := f.r.Status().Update(context.Background(), newPod); err != nil {
		t.Fatalf("update second server pod status: %v", err)
	}

	f.reload(t)
	wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend)
	if wait <= 0 {
		t.Fatalf("wait = %v, want > 0 (rate-limit must defer the second cascade)", wait)
	}
	if wait > f.r.MinServerRestartCascadeInterval {
		t.Fatalf("wait = %v, want <= window %v", wait, f.r.MinServerRestartCascadeInterval)
	}

	f.reload(t)
	// Status MUST stay pinned to the first cascade's UID — advancing it
	// inside the rate-limit window would lose the missed cascade.
	if got := f.backend.Status.ObservedServerInstance; got != serverInstanceID(f.serverPod) {
		t.Fatalf("ObservedServerInstance = %q, want pinned to first-cascade UID %q", got, serverInstanceID(f.serverPod))
	}
	if got := cascadeRestartsCount(t, f.cacheNS, f.cacheName, cascadeRestartReasonServerInstanceChanged); got != 1 {
		t.Fatalf("cascade counter = %v, want 1 (rate-limited second cascade must not increment)", got)
	}
}

func TestReconcileServerInstance_NotReadyPodGivesNoBaseline(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Flip the server pod to NOT Ready (Pending), simulating mid-rollout.
	notReady := f.serverPod.DeepCopy()
	notReady.Status.Phase = corev1.PodPending
	notReady.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
	if err := f.r.Status().Update(context.Background(), notReady); err != nil {
		t.Fatalf("flip pod to not-ready: %v", err)
	}

	wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend)
	if wait != 0 {
		t.Fatalf("wait = %v, want 0", wait)
	}
	f.reload(t)
	if got := f.backend.Status.ObservedServerInstance; got != "" {
		t.Fatalf("ObservedServerInstance = %q, want empty (no Ready pod to anchor to)", got)
	}
}

// TestReconcileServerInstance_SelectorRemovedButPodStillInjectedCascades
// asserts that an operator clearing spec.engineSelector AFTER engine
// pods were already injected does not silently break recovery. The
// pods' injected-by annotations persist, their LMCache sockets are
// still stale on a cache-server restart, and the cascade MUST still
// roll them. Selector match is an apiserver-side perf optimization
// for other reconciler paths; the cascade authoritatively filters on
// the injected-by annotation pair, so removing the selector does not
// disable recovery.
func TestReconcileServerInstance_SelectorRemovedButPodStillInjectedCascades(t *testing.T) {
	f := newCascadeRestartFixture(t)
	f.backend.Spec.EngineSelector = nil
	if err := f.r.Update(context.Background(), f.backend); err != nil {
		t.Fatalf("update backend: %v", err)
	}
	f.reload(t)

	// Seed prior UID so this is a transition, not the first observation.
	f.backend.Status.ObservedServerInstance = "previous-server-uid"
	if err := f.r.Status().Update(context.Background(), f.backend); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	f.reload(t)

	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("wait = %v, want 0", wait)
	}

	f.reloadEngineDep(t)
	got := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]
	if got != serverInstanceID(f.serverPod) {
		t.Fatalf("cascade annotation = %q, want %q (already-injected pods must still cascade after selector removal)", got, serverInstanceID(f.serverPod))
	}
	f.reload(t)
	if got := f.backend.Status.ObservedServerInstance; got != serverInstanceID(f.serverPod) {
		t.Fatalf("ObservedServerInstance = %q, want %q", got, serverInstanceID(f.serverPod))
	}
}

// TestReconcileServerInstance_StaleInjectedByUIDIsRejected asserts
// that the cascade rejects a pod whose injected-by name matches but
// whose injected-by-uid does not (CR deleted and recreated under the
// same name, or an operator with pod-create RBAC forging the
// annotation). The pod is NOT actually wired to the live CR's cache-
// server socket — annotating its Deployment would roll unrelated
// work or do nothing useful.
func TestReconcileServerInstance_StaleInjectedByUIDIsRejected(t *testing.T) {
	f := newCascadeRestartFixture(t)
	// Forge a name-match / UID-mismatch on the engine pod.
	enginePod := f.enginePod.DeepCopy()
	enginePod.Annotations[podwebhook.AnnotationInjectedByUID] = "stale-uid-from-deleted-cr"
	if err := f.r.Update(context.Background(), enginePod); err != nil {
		t.Fatalf("stale UID annotation: %v", err)
	}

	f.backend.Status.ObservedServerInstance = "previous-server-uid"
	if err := f.r.Status().Update(context.Background(), f.backend); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	f.reload(t)

	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("wait = %v, want 0", wait)
	}
	f.reloadEngineDep(t)
	if _, ok := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; ok {
		t.Fatalf("engine deployment cascaded against a pod with a stale injected-by-uid; want no cascade")
	}
}

// TestReconcileServerInstance_ForeignReadyPodIgnoredForServerInstance
// asserts that a Ready pod carrying the controller-managed labels but
// NOT controller-owned by THIS backend's Deployment must not advance
// observedServerInstance — otherwise a transition would spuriously
// trigger an engine rollout.
func TestReconcileServerInstance_ForeignReadyPodIgnoredForServerInstance(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Pre-seed status to the existing real pod's UID so the next
	// observation is a no-op transition rather than first-observation.
	f.backend.Status.ObservedServerInstance = serverInstanceID(f.serverPod)
	if err := f.r.Status().Update(context.Background(), f.backend); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	f.reload(t)

	// Foreign pod: same labels, NOT owned via the cache-server
	// Deployment chain. A name lex-smaller than the legit cache pod
	// so a label-only picker would prefer it.
	foreign := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "aaaa-foreign-pod",
			Namespace: f.cacheNS,
			UID:       "foreign-uid",
			Labels:    selectorLabels(f.cacheName),
			// No ownerRefs — looks like a bare pod from another tool.
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			}},
		},
	}
	if err := f.r.Create(context.Background(), foreign); err != nil {
		t.Fatalf("create foreign pod: %v", err)
	}
	if err := f.r.Status().Update(context.Background(), foreign); err != nil {
		t.Fatalf("set foreign ready: %v", err)
	}

	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("wait = %v, want 0", wait)
	}
	f.reload(t)
	if got := f.backend.Status.ObservedServerInstance; got != serverInstanceID(f.serverPod) {
		t.Fatalf("ObservedServerInstance = %q, want pinned to the legit pod %q (foreign pod must not advance the latch)", got, serverInstanceID(f.serverPod))
	}
	f.reloadEngineDep(t)
	if _, ok := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; ok {
		t.Fatalf("foreign pod triggered a cascade; want no cascade")
	}
}

// TestReconcileServerInstance_MultiReplicaTracksEveryReadyPod asserts
// that a backend with multiple Ready cache-server pods (the ephemeral
// `spec.replicas > 1` shape) encodes every Ready pod's UID into
// observedServerInstance. Replacing ANY one of the replicas must
// advance the identifier and cascade — a tracker that watched only
// one pod would silently miss restarts of the others.
func TestReconcileServerInstance_MultiReplicaTracksEveryReadyPod(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Add a second Ready cache-server pod owned by the same RS.
	tru := true
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cache-pod-bbb",
			Namespace: f.cacheNS,
			UID:       "cache-pod-uid-2",
			Labels:    selectorLabels(f.cacheName),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       f.serverRS.Name,
				UID:        f.serverRS.UID,
				Controller: &tru,
			}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	if err := f.r.Create(context.Background(), pod2); err != nil {
		t.Fatalf("create pod2: %v", err)
	}
	if err := f.r.Status().Update(context.Background(), pod2); err != nil {
		t.Fatalf("set pod2 ready: %v", err)
	}

	// First observation should encode BOTH pods, sorted by name. With
	// pod-aaa < pod-bbb, the lex-sorted order is uid-1, uid-2.
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("first observation wait = %v, want 0", wait)
	}
	f.reload(t)
	wantInitial := serverInstanceID(f.serverPod) + "," + serverInstanceID(pod2)
	if got := f.backend.Status.ObservedServerInstance; got != wantInitial {
		t.Fatalf("initial ObservedServerInstance = %q, want %q (both Ready pod UIDs, lex-sorted by name)", got, wantInitial)
	}

	// Replace ONLY the second replica (the one whose UID would be
	// silently missed by a single-pod tracker).
	if err := f.r.Delete(context.Background(), pod2); err != nil {
		t.Fatalf("delete pod2: %v", err)
	}
	pod2b := pod2.DeepCopy()
	pod2b.ResourceVersion = ""
	pod2b.UID = "cache-pod-uid-2-replacement"
	if err := f.r.Create(context.Background(), pod2b); err != nil {
		t.Fatalf("create pod2 replacement: %v", err)
	}
	if err := f.r.Status().Update(context.Background(), pod2b); err != nil {
		t.Fatalf("ready pod2 replacement: %v", err)
	}

	// The identifier must now advance to include the replacement UID.
	// Wait a frame for the rate-limit window — fixture's
	// MinServerRestartCascadeInterval is 50ms.
	time.Sleep(60 * time.Millisecond)
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("replacement wait = %v, want 0", wait)
	}
	f.reload(t)
	wantAfter := serverInstanceID(f.serverPod) + "," + serverInstanceID(pod2b)
	if got := f.backend.Status.ObservedServerInstance; got != wantAfter {
		t.Fatalf("ObservedServerInstance after replacement = %q, want %q", got, wantAfter)
	}
	f.reloadEngineDep(t)
	if got := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; got != wantAfter {
		t.Fatalf("cascade annotation = %q, want %q (non-first replica's restart must still cascade)", got, wantAfter)
	}
}

// TestReconcileServerInstance_RollingUpdateSupersetDoesNotCascade
// asserts that a Deployment rolling-update midpoint — when the old
// pod is still Ready while the new one comes up (maxSurge=1) —
// does NOT trigger a cascade AND does NOT advance
// observedServerInstance. The cascade fires on the NEXT transition
// that drops the old pod, and the latch stays pinned at the prior
// baseline through the midpoint so a rollback (see
// TestReconcileServerInstance_RollingUpdateRollbackDoesNotCascade)
// is a true no-op. Without this debounce a normal single-replica
// rollout would roll the engine fleet twice.
func TestReconcileServerInstance_RollingUpdateSupersetDoesNotCascade(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Baseline: only one pod, observedServerInstance gets stamped.
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("baseline wait = %v, want 0", wait)
	}
	f.reload(t)
	baseline := f.backend.Status.ObservedServerInstance
	if baseline != serverInstanceID(f.serverPod) {
		t.Fatalf("baseline = %q, want %q", baseline, serverInstanceID(f.serverPod))
	}

	// Simulate the rolling-update midpoint: add a second Ready pod
	// owned by the same RS (a new replica from maxSurge=1).
	tru := true
	newer := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cache-pod-zzz", // sorts AFTER the original
			Namespace: f.cacheNS,
			UID:       "cache-pod-uid-new",
			Labels:    selectorLabels(f.cacheName),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       f.serverRS.Name,
				UID:        f.serverRS.UID,
				Controller: &tru,
			}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	if err := f.r.Create(context.Background(), newer); err != nil {
		t.Fatalf("create newer pod: %v", err)
	}
	if err := f.r.Status().Update(context.Background(), newer); err != nil {
		t.Fatalf("ready newer pod: %v", err)
	}

	// Mid-rollout transition: prior strictly grows. Must NOT cascade
	// AND must NOT advance the latch — keeping prior pinned is what
	// makes a subsequent rollback a no-op.
	time.Sleep(60 * time.Millisecond)
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("midpoint wait = %v, want 0", wait)
	}
	f.reload(t)
	if got := f.backend.Status.ObservedServerInstance; got != baseline {
		t.Fatalf("midpoint ObservedServerInstance = %q, want %q (must stay pinned to baseline through strict-superset; advancing here would make a rollback look like a replacement)", got, baseline)
	}
	f.reloadEngineDep(t)
	if _, ok := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; ok {
		t.Fatalf("rolling-update midpoint triggered a cascade; the old pod is still Ready so the cascade must wait")
	}
	if got := cascadeRestartsCount(t, f.cacheNS, f.cacheName, cascadeRestartReasonServerInstanceChanged); got != 0 {
		t.Fatalf("cascade counter at midpoint = %v, want 0", got)
	}

	// Drop the old pod (rolling update finished). NOW the cascade
	// must fire — the new pod is what serves traffic, the old pod's
	// LMCache sockets are unreachable.
	if err := f.r.Delete(context.Background(), f.serverPod); err != nil {
		t.Fatalf("delete old pod: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("post-rollout wait = %v, want 0", wait)
	}
	f.reload(t)
	wantFinal := serverInstanceID(newer)
	if got := f.backend.Status.ObservedServerInstance; got != wantFinal {
		t.Fatalf("final ObservedServerInstance = %q, want %q", got, wantFinal)
	}
	f.reloadEngineDep(t)
	if got := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; got != wantFinal {
		t.Fatalf("cascade annotation = %q, want %q (cascade fires once, on the drop of the old pod)", got, wantFinal)
	}
	if got := cascadeRestartsCount(t, f.cacheNS, f.cacheName, cascadeRestartReasonServerInstanceChanged); got != 1 {
		t.Fatalf("cascade counter = %v, want exactly 1 (one rolling update = one cascade)", got)
	}
}

// TestReconcileServerInstance_RollingUpdateRollbackDoesNotCascade
// drives the rollback-of-a-rolling-update scenario: the NEW pod
// becomes Ready briefly (strict-superset midpoint) and is then
// rolled back (new pod killed by failing readiness, leaving the
// ORIGINAL pod alone). This must NOT cascade — the original cache-
// server process and its sockets never changed. The contract that
// makes this work is "do not persist the strict-superset midpoint
// while the Deployment is rolling"; the rollback path then becomes
// a true no-op (prior=current after the rollback completes).
func TestReconcileServerInstance_RollingUpdateRollbackDoesNotCascade(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Baseline observation: latch = original pod's identifier.
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("baseline wait = %v, want 0", wait)
	}
	f.reload(t)
	baseline := f.backend.Status.ObservedServerInstance
	if baseline != serverInstanceID(f.serverPod) {
		t.Fatalf("baseline = %q, want %q", baseline, serverInstanceID(f.serverPod))
	}

	// Simulate the rolling-update midpoint: a second Ready pod appears
	// (maxSurge=1). Strict superset → no cascade AND no latch advance.
	tru := true
	newer := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cache-pod-zzz", // sorts AFTER the original
			Namespace: f.cacheNS,
			UID:       "cache-pod-uid-newer-but-doomed",
			Labels:    selectorLabels(f.cacheName),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       f.serverRS.Name,
				UID:        f.serverRS.UID,
				Controller: &tru,
			}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	if err := f.r.Create(context.Background(), newer); err != nil {
		t.Fatalf("create newer pod: %v", err)
	}
	if err := f.r.Status().Update(context.Background(), newer); err != nil {
		t.Fatalf("ready newer pod: %v", err)
	}

	time.Sleep(60 * time.Millisecond)
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("midpoint wait = %v, want 0", wait)
	}
	f.reload(t)
	if got := f.backend.Status.ObservedServerInstance; got != baseline {
		t.Fatalf("midpoint ObservedServerInstance = %q, want %q (must stay pinned to baseline; advancing here makes the rollback look like a replacement)", got, baseline)
	}

	// Now the rollback: the new pod fails readiness / image-pull / etc.
	// and is killed, leaving ONLY the original pod alone. Pre-fix this
	// looked like "the new pod was replaced" (prior contained newer,
	// current does not) and false-cascaded. Post-fix, since the latch
	// never advanced past baseline, the rollback is prior=current,
	// no-op.
	if err := f.r.Delete(context.Background(), newer); err != nil {
		t.Fatalf("delete rolled-back newer pod: %v", err)
	}

	time.Sleep(60 * time.Millisecond)
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("post-rollback wait = %v, want 0", wait)
	}
	f.reload(t)
	if got := f.backend.Status.ObservedServerInstance; got != baseline {
		t.Fatalf("post-rollback ObservedServerInstance = %q, want %q (rolled-back rolling update must be a no-op; original pod and its sockets never changed)", got, baseline)
	}
	f.reloadEngineDep(t)
	if _, ok := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; ok {
		t.Fatalf("rolled-back rolling update triggered a cascade; the cache-server process never changed, every engine still holds a live socket")
	}
	if got := cascadeRestartsCount(t, f.cacheNS, f.cacheName, cascadeRestartReasonServerInstanceChanged); got != 0 {
		t.Fatalf("cascade counter = %v, want 0 (no real cache-server replacement happened)", got)
	}
}

// TestReconcileServerInstance_ConvergedScaleUpPersistsBaseline drives
// the operator-scale-up scenario: a strict-superset transition where
// the owning Deployment has reached steady state at the wider count
// is a legitimate scale-up, NOT a rolling-update midpoint. The latch
// must advance to include the added pod(s) so a later replacement of
// any of the added pods cascades correctly.
//
// The regression this guards against: if the reconciler unconditionally
// refused to persist superset midpoints, a converged scale-up would
// leave the new pod outside the latch forever; a subsequent OOM-kill
// of just the added pod would be observable to engines (their sockets
// to that pod would die) but not to the controller (prior strictly
// containing current's UIDs at same restart-sums →
// instanceChangeRequiresCascade returns false). The engines would
// be stranded on stale sockets with no recovery path.
func TestReconcileServerInstance_ConvergedScaleUpPersistsBaseline(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Baseline: 1 Ready pod. Mark Deployment converged at replicas=1
	// (the fixture defaults to nil-replicas → 1).
	if err := setOwnedDeploymentConverged(f, 1); err != nil {
		t.Fatalf("set baseline Deployment converged: %v", err)
	}
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("baseline wait = %v, want 0", wait)
	}
	f.reload(t)
	baseline := f.backend.Status.ObservedServerInstance
	if baseline != serverInstanceID(f.serverPod) {
		t.Fatalf("baseline = %q, want %q", baseline, serverInstanceID(f.serverPod))
	}

	// Operator scales the backend up to 2 replicas. A second Ready pod
	// arrives, owned by the same RS chain. Mark Deployment converged
	// at replicas=2 (replicas==readyReplicas==updatedReplicas, with
	// observedGeneration current).
	tru := true
	added := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cache-pod-zzz",
			Namespace: f.cacheNS,
			UID:       "cache-pod-uid-added",
			Labels:    selectorLabels(f.cacheName),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       f.serverRS.Name,
				UID:        f.serverRS.UID,
				Controller: &tru,
			}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	if err := f.r.Create(context.Background(), added); err != nil {
		t.Fatalf("create added pod: %v", err)
	}
	if err := f.r.Status().Update(context.Background(), added); err != nil {
		t.Fatalf("ready added pod: %v", err)
	}
	if err := setOwnedDeploymentConverged(f, 2); err != nil {
		t.Fatalf("set converged Deployment at replicas=2: %v", err)
	}

	time.Sleep(60 * time.Millisecond)
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("post-scale-up wait = %v, want 0", wait)
	}
	f.reload(t)
	wantBaseline := serverInstanceID(f.serverPod) + "," + serverInstanceID(added)
	if got := f.backend.Status.ObservedServerInstance; got != wantBaseline {
		t.Fatalf("post-scale-up ObservedServerInstance = %q, want %q (Deployment converged at the wider count → latch must advance so a later replacement of the added pod cascades)", got, wantBaseline)
	}
	// No cascade should have fired — adding a pod is not a replacement.
	f.reloadEngineDep(t)
	if _, ok := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; ok {
		t.Fatalf("scale-up triggered a cascade; only replacements/restarts should cascade")
	}
	if got := cascadeRestartsCount(t, f.cacheNS, f.cacheName, cascadeRestartReasonServerInstanceChanged); got != 0 {
		t.Fatalf("cascade counter after scale-up = %v, want 0", got)
	}

	// Now replace ONLY the added pod (different UID). Without the
	// converged-superset persist, this would stay a strict superset
	// of the original baseline and miss the cascade. With the
	// persist, the baseline includes the added pod's UID, so its
	// disappearance is a real replacement and the cascade fires.
	if err := f.r.Delete(context.Background(), added); err != nil {
		t.Fatalf("delete added pod (replacement step 1): %v", err)
	}
	replacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cache-pod-zzz", // same name, different UID
			Namespace: f.cacheNS,
			UID:       "cache-pod-uid-replacement",
			Labels:    selectorLabels(f.cacheName),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       f.serverRS.Name,
				UID:        f.serverRS.UID,
				Controller: &tru,
			}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	if err := f.r.Create(context.Background(), replacement); err != nil {
		t.Fatalf("create replacement pod: %v", err)
	}
	if err := f.r.Status().Update(context.Background(), replacement); err != nil {
		t.Fatalf("ready replacement pod: %v", err)
	}
	// Stay converged at replicas=2 throughout (the rate-limit + the
	// rolling-update test's pattern of 60ms sleep applies).
	if err := setOwnedDeploymentConverged(f, 2); err != nil {
		t.Fatalf("re-affirm converged Deployment: %v", err)
	}

	time.Sleep(60 * time.Millisecond)
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("post-replacement wait = %v, want 0", wait)
	}
	f.reload(t)
	wantFinal := serverInstanceID(f.serverPod) + "," + serverInstanceID(replacement)
	if got := f.backend.Status.ObservedServerInstance; got != wantFinal {
		t.Fatalf("post-replacement ObservedServerInstance = %q, want %q", got, wantFinal)
	}
	f.reloadEngineDep(t)
	if got := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; got != wantFinal {
		t.Fatalf("cascade annotation = %q, want %q (replacement of an added scale-up pod must cascade — without the converged-superset persist, the missing-UID transition would still look like a strict superset and miss the cascade)", got, wantFinal)
	}
	if got := cascadeRestartsCount(t, f.cacheNS, f.cacheName, cascadeRestartReasonServerInstanceChanged); got != 1 {
		t.Fatalf("cascade counter after replacement = %v, want exactly 1", got)
	}
}

// TestReconcileServerInstance_ClearedSentinelOverridesStaleStatus
// drives the failure window in the managed→External→managed
// transition: the in-memory clear ran (shadow gone, cleared
// sentinel set) but the on-cluster status patch FAILED, so the
// status field still holds the prior managed period's identifier.
// Without the sentinel, the next managed-period reconcile would
// read prior = statusField (stale) and misclassify the first new
// Ready pod as a replacement → false-cascade. The sentinel forces
// effectivePrior = "" so the new period starts with a clean
// empty→set baseline.
func TestReconcileServerInstance_ClearedSentinelOverridesStaleStatus(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Plant a stale prior-period status value directly on the CR —
	// what a managed→External patch-failure window would leave
	// behind. The actual current pod is f.serverPod with a
	// different identifier; pre-fix the reconciler would see
	// stale != current and cascade.
	f.backend.Status.ObservedServerInstance = "stale-prior-period:0"
	if err := f.r.Status().Update(context.Background(), f.backend); err != nil {
		t.Fatalf("plant stale status: %v", err)
	}
	f.reload(t)

	// Simulate the in-memory side of the failed-clear scenario:
	// External path's clearServerInstanceLatchShadow ran (which
	// sets the cleared sentinel), the status patch errored, then
	// the operator flipped back to managed before the retry. The
	// in-memory state for the cascade key is:
	//   shadow: empty
	//   cleared: true
	//   lastAt: empty
	key := cascadeKey{namespace: f.backend.Namespace, name: f.backend.Name, uid: string(f.backend.UID)}
	f.r.serverInstanceCascade.clear(key)
	if !f.r.serverInstanceCascade.isCleared(key) {
		t.Fatalf("precondition: cleared sentinel not set after clear()")
	}

	// Reconcile in the new managed period. currentID =
	// serverInstanceID(f.serverPod) != "stale-prior-period:0".
	// With the sentinel: effectivePrior="" → empty→set → no
	// cascade, persist new baseline.
	// Without the sentinel (the bug): effectivePrior=stale →
	// real-change branch → cascade fires.
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("wait = %v, want 0", wait)
	}

	f.reload(t)
	want := serverInstanceID(f.serverPod)
	if got := f.backend.Status.ObservedServerInstance; got != want {
		t.Fatalf("status.observedServerInstance = %q, want %q (sentinel should have driven a clean empty→set persist over the stale value)", got, want)
	}
	f.reloadEngineDep(t)
	if _, ok := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; ok {
		t.Fatalf("cascade fired despite the cleared sentinel; this is the false-cascade scenario the sentinel must prevent (managed→External patch-fail + flip-back-to-managed)")
	}
	if got := cascadeRestartsCount(t, f.cacheNS, f.cacheName, cascadeRestartReasonServerInstanceChanged); got != 0 {
		t.Fatalf("cascade counter = %v, want 0", got)
	}

	// recordAttempt happened above (during the empty→set persist),
	// which should have cleared the sentinel — verify so the next
	// real change DOES cascade correctly.
	if f.r.serverInstanceCascade.isCleared(key) {
		t.Fatalf("cleared sentinel still set after a successful empty→set baseline persist; recordAttempt must clear it so subsequent changes are detected normally")
	}
}

// TestReconcileServerInstance_ShadowWinsOverStaleStatusOnScaleUpPersistFailure
// drives the scenario where a converged scale-up's baseline persist
// fails: the shadow records the widened pod set ("A:0,B:0") but the
// K8s-resident status field still holds the pre-scale baseline
// ("A:0") because the patch never landed. If the prior were taken
// from the status field (stale) instead of the shadow (current),
// a subsequent replacement of just the added pod ("A:0,B:0" →
// "A:0,C:0") would look like a strict superset of "A:0" and miss
// the cascade — engines that connected to B would be stranded on
// stale sockets forever.
func TestReconcileServerInstance_ShadowWinsOverStaleStatusOnScaleUpPersistFailure(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Step 1: baseline (1 Ready pod, converged, persists cleanly).
	if err := setOwnedDeploymentConverged(f, 1); err != nil {
		t.Fatalf("set baseline Deployment converged: %v", err)
	}
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("baseline wait = %v, want 0", wait)
	}
	f.reload(t)
	baseline := f.backend.Status.ObservedServerInstance
	if baseline != serverInstanceID(f.serverPod) {
		t.Fatalf("baseline = %q, want %q", baseline, serverInstanceID(f.serverPod))
	}

	// Step 2: scale up to replicas=2. New pod becomes Ready. Make the
	// converged-superset baseline persist FAIL. The shadow should
	// record "A:0,B:0" while status stays at "A:0".
	tru := true
	added := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cache-pod-added",
			Namespace: f.cacheNS,
			UID:       "cache-pod-uid-added",
			Labels:    selectorLabels(f.cacheName),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       f.serverRS.Name,
				UID:        f.serverRS.UID,
				Controller: &tru,
			}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	if err := f.r.Create(context.Background(), added); err != nil {
		t.Fatalf("create added pod: %v", err)
	}
	if err := f.r.Status().Update(context.Background(), added); err != nil {
		t.Fatalf("ready added pod: %v", err)
	}
	if err := setOwnedDeploymentConverged(f, 2); err != nil {
		t.Fatalf("set converged at replicas=2: %v", err)
	}

	failOnce := &statusPatchFailingClient{Client: f.r.Client, remaining: 1}
	f.r.Client = failOnce
	time.Sleep(60 * time.Millisecond)
	// Scale-up reconcile: status patch fails. Shadow should advance.
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait <= 0 {
		t.Fatalf("scale-up reconcile wait = %v, want positive (persist failed)", wait)
	}
	f.reload(t)
	if got := f.backend.Status.ObservedServerInstance; got != baseline {
		t.Fatalf("scale-up status field = %q after FAILED persist, want still %q", got, baseline)
	}
	wantShadow := serverInstanceID(f.serverPod) + "," + serverInstanceID(added)
	key := cascadeKey{namespace: f.backend.Namespace, name: f.backend.Name, uid: string(f.backend.UID)}
	if got := f.r.serverInstanceCascade.lastAttempt(key); got != wantShadow {
		t.Fatalf("shadow after scale-up = %q, want %q (shadow must record the widened baseline so a subsequent replacement is detectable)", got, wantShadow)
	}

	// Step 3: replace the added pod (B → C, e.g. OOM-kill of B).
	// The shadow holds "A:0,B:0"; if the reconciler trusted the
	// (stale) status field "A:0" as prior instead, current
	// "A:0,C:0" would be a strict superset of "A:0" — no cascade.
	// With the shadow as authoritative prior, prior="A:0,B:0" and
	// the missing B IS detected as a replacement → cascade fires.
	if err := f.r.Delete(context.Background(), added); err != nil {
		t.Fatalf("delete added pod (OOM-kill): %v", err)
	}
	replacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cache-pod-replacement-2",
			Namespace: f.cacheNS,
			UID:       "cache-pod-uid-replacement-2",
			Labels:    selectorLabels(f.cacheName),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       f.serverRS.Name,
				UID:        f.serverRS.UID,
				Controller: &tru,
			}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	if err := f.r.Create(context.Background(), replacement); err != nil {
		t.Fatalf("create replacement: %v", err)
	}
	if err := f.r.Status().Update(context.Background(), replacement); err != nil {
		t.Fatalf("ready replacement: %v", err)
	}
	// Keep Deployment converged at replicas=2 throughout.
	if err := setOwnedDeploymentConverged(f, 2); err != nil {
		t.Fatalf("re-affirm converged at replicas=2: %v", err)
	}

	time.Sleep(60 * time.Millisecond)
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("post-replacement wait = %v, want 0", wait)
	}
	f.reloadEngineDep(t)
	wantFinal := serverInstanceID(f.serverPod) + "," + serverInstanceID(replacement)
	if got := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; got != wantFinal {
		t.Fatalf("cascade annotation = %q, want %q (shadow must override stale status as prior — without the override the missing-B transition would look like a strict superset and miss the cascade)", got, wantFinal)
	}
	if got := cascadeRestartsCount(t, f.cacheNS, f.cacheName, cascadeRestartReasonServerInstanceChanged); got != 1 {
		t.Fatalf("cascade counter = %v, want exactly 1", got)
	}
}

// TestReconcileServerInstance_StaleDeploymentStatusDoesNotPersistMidpoint
// drives the race where the Deployment.Status counters lie about
// convergence: spec.replicas=1, status.readyReplicas=1,
// status.updatedReplicas=1, observedGeneration current — but the
// LIVE pod list has 2 Ready pods (a maxSurge mid-rollout where the
// apps/v1 Deployment controller has not yet observed the new pod).
// If the convergence check trusted only the Status counters, the
// strict-superset midpoint would be persisted as a "scale-up"
// baseline; a subsequent rollback dropping the new pod would then
// look like a real replacement and false-cascade the engine fleet.
// The cross-check against len(live Ready pods) closes that race.
func TestReconcileServerInstance_StaleDeploymentStatusDoesNotPersistMidpoint(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Baseline: 1 Ready pod, Deployment converged at replicas=1.
	if err := setOwnedDeploymentConverged(f, 1); err != nil {
		t.Fatalf("set baseline Deployment converged: %v", err)
	}
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("baseline wait = %v, want 0", wait)
	}
	f.reload(t)
	baseline := f.backend.Status.ObservedServerInstance
	if baseline != serverInstanceID(f.serverPod) {
		t.Fatalf("baseline = %q, want %q", baseline, serverInstanceID(f.serverPod))
	}

	// Simulate a rolling-update midpoint where the apps controller's
	// Status counters are STALE: they still claim readyReplicas=1
	// (matching spec.replicas=1) while the live pod list has 2 Ready
	// pods. This is what stale-status convergence looks like to our
	// reconciler. Leave Deployment.Status unchanged — it already
	// reports {ReadyReplicas: 1, UpdatedReplicas: 1, replicas=1}
	// from setOwnedDeploymentConverged(f, 1) above.
	tru := true
	newer := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cache-pod-zzz",
			Namespace: f.cacheNS,
			UID:       "cache-pod-uid-newer-stale-status",
			Labels:    selectorLabels(f.cacheName),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       f.serverRS.Name,
				UID:        f.serverRS.UID,
				Controller: &tru,
			}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	if err := f.r.Create(context.Background(), newer); err != nil {
		t.Fatalf("create newer pod (stale-status midpoint): %v", err)
	}
	if err := f.r.Status().Update(context.Background(), newer); err != nil {
		t.Fatalf("ready newer pod: %v", err)
	}

	time.Sleep(60 * time.Millisecond)
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("midpoint wait = %v, want 0", wait)
	}
	f.reload(t)
	// CRITICAL ASSERTION: the latch must NOT advance. Without the
	// len(ready)==wantReplicas clause in the convergence check, the
	// stale-status counters (which look converged at replicas=1)
	// would convince the reconciler that the {old,new} pair is a
	// steady-state scale-up rather than a transient midpoint, and
	// the widened latch would get persisted.
	if got := f.backend.Status.ObservedServerInstance; got != baseline {
		t.Fatalf("midpoint ObservedServerInstance = %q, want %q (stale Deployment.Status counters reported convergence, but the live pod count (%d) > spec.replicas (1) is a midpoint, not a scale-up — the latch must NOT advance or a rollback would false-cascade)", got, baseline, 2)
	}
	f.reloadEngineDep(t)
	if _, ok := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; ok {
		t.Fatalf("stale-status midpoint triggered a cascade; only converged transitions or real replacements should cascade")
	}

	// Now simulate the rollback: the new pod is killed. The latch
	// is still on the baseline, so prior == currentID == baseline,
	// and the rollback is a true no-op.
	if err := f.r.Delete(context.Background(), newer); err != nil {
		t.Fatalf("delete rolled-back new pod: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("post-rollback wait = %v, want 0", wait)
	}
	f.reload(t)
	if got := f.backend.Status.ObservedServerInstance; got != baseline {
		t.Fatalf("post-rollback ObservedServerInstance = %q, want %q (rolled-back rolling update must be a no-op; the original pod's process never changed)", got, baseline)
	}
	if got := cascadeRestartsCount(t, f.cacheNS, f.cacheName, cascadeRestartReasonServerInstanceChanged); got != 0 {
		t.Fatalf("cascade counter = %v, want 0 (no real cache-server replacement happened)", got)
	}
}

// setOwnedDeploymentConverged mutates the fixture's serverDep so its
// Status reflects a converged Deployment at the given replica count.
// Returns the apply error if any. Used by ConvergedScaleUp test.
func setOwnedDeploymentConverged(f *cascadeRestartFixture, replicas int32) error {
	// Fetch a live copy (the fake client tracks resource versions).
	live := &appsv1.Deployment{}
	if err := f.r.Get(context.Background(), types.NamespacedName{Name: f.serverDep.Name, Namespace: f.serverDep.Namespace}, live); err != nil {
		return fmt.Errorf("get live serverDep: %w", err)
	}
	r := replicas
	live.Spec.Replicas = &r
	if err := f.r.Update(context.Background(), live); err != nil {
		return fmt.Errorf("update serverDep.Spec.Replicas: %w", err)
	}
	// Status subresource requires a separate update.
	if err := f.r.Get(context.Background(), types.NamespacedName{Name: f.serverDep.Name, Namespace: f.serverDep.Namespace}, live); err != nil {
		return fmt.Errorf("reload live serverDep: %w", err)
	}
	live.Status.ReadyReplicas = replicas
	live.Status.UpdatedReplicas = replicas
	live.Status.Replicas = replicas
	live.Status.ObservedGeneration = live.Generation
	if err := f.r.Status().Update(context.Background(), live); err != nil {
		return fmt.Errorf("update serverDep.Status: %w", err)
	}
	return nil
}

// TestReconcileServerInstance_InPlaceContainerRestartCascades asserts
// that an in-place container restart inside the cache-server pod
// (kubelet respawning a crashed container, e.g. on OOM with
// restartPolicy=Always — pod.UID stays the same) still advances
// observedServerInstance and triggers the cascade. The per-pod
// identifier sums containerStatuses[].restartCount, so a bump in any
// container's restart count changes the identifier without needing
// the pod to be replaced.
func TestReconcileServerInstance_InPlaceContainerRestartCascades(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Baseline observation pins the identifier.
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("baseline wait = %v, want 0", wait)
	}
	f.reload(t)
	baseline := f.backend.Status.ObservedServerInstance
	if baseline != serverInstanceID(f.serverPod) {
		t.Fatalf("baseline ObservedServerInstance = %q, want %q", baseline, serverInstanceID(f.serverPod))
	}

	// Simulate the kubelet bumping the lmcache-server container's
	// restart count from 0 to 1 (e.g. OOM-killed container respawned
	// in-place; same pod, same pod.UID, fresh LMCache process).
	live := &corev1.Pod{}
	if err := f.r.Get(context.Background(), types.NamespacedName{Name: f.serverPod.Name, Namespace: f.cacheNS}, live); err != nil {
		t.Fatalf("get serverPod: %v", err)
	}
	live.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:         "lmcache-server",
		Ready:        true,
		RestartCount: 1,
	}}
	if err := f.r.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("bump container restart count: %v", err)
	}

	// Wait past the rate-limit window (fixture sets 50ms).
	time.Sleep(60 * time.Millisecond)
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("post-restart wait = %v, want 0", wait)
	}
	f.reload(t)
	want := fmt.Sprintf("%s:1", f.serverPod.UID)
	if got := f.backend.Status.ObservedServerInstance; got != want {
		t.Fatalf("ObservedServerInstance after container restart = %q, want %q (the restart-count bump must advance the identifier)", got, want)
	}
	f.reloadEngineDep(t)
	if got := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; got != want {
		t.Fatalf("cascade annotation after container restart = %q, want %q", got, want)
	}
}

// TestReconcileServerInstance_SidecarRestartIgnored asserts that
// containerRunSum is scoped to the cache-server's own containers (per
// the owned Deployment's pod template), so a restart of an externally-
// injected sidecar (service mesh, Datadog, etc. — present in the
// pod's containerStatuses but absent from the Deployment template)
// does NOT advance observedServerInstance and does NOT cascade. A
// cascade for every Istio sidecar crash-loop would be a serious
// operator-facing regression.
func TestReconcileServerInstance_SidecarRestartIgnored(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Baseline observation.
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("baseline wait = %v, want 0", wait)
	}
	f.reload(t)
	baseline := f.backend.Status.ObservedServerInstance

	// Inject a sidecar restart event into the pod's containerStatuses.
	// The sidecar is NOT in the owned Deployment's template, so the
	// reconciler must ignore its restart count.
	live := &corev1.Pod{}
	if err := f.r.Get(context.Background(), types.NamespacedName{Name: f.serverPod.Name, Namespace: f.cacheNS}, live); err != nil {
		t.Fatalf("get serverPod: %v", err)
	}
	live.Status.ContainerStatuses = []corev1.ContainerStatus{
		{Name: "lmcache-server", Ready: true, RestartCount: 0},
		{Name: "istio-proxy", Ready: true, RestartCount: 7},
	}
	if err := f.r.Status().Update(context.Background(), live); err != nil {
		t.Fatalf("inject sidecar restarts: %v", err)
	}

	time.Sleep(60 * time.Millisecond)
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("post-sidecar-restart wait = %v, want 0", wait)
	}
	f.reload(t)
	if got := f.backend.Status.ObservedServerInstance; got != baseline {
		t.Fatalf("ObservedServerInstance changed despite only sidecar restart: %q → %q", baseline, got)
	}
	f.reloadEngineDep(t)
	if _, ok := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; ok {
		t.Fatalf("sidecar restart triggered a cascade; only the cache-server's own containers should advance the identifier")
	}
}

// TestReconcileServerInstance_ForeignDeploymentSameNameIgnored asserts
// that when the backend's CacheBackend.UID does not control the live
// Deployment named after it (a foreign Deployment recreated under the
// same name, or operator drift), the reconciler refuses to attribute
// its pods to the backend and observedServerInstance stays empty.
func TestReconcileServerInstance_ForeignDeploymentSameNameIgnored(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Rewrite the cache-server Deployment's controller-owner ref to
	// point at some OTHER CacheBackend (a foreign UID).
	dep := f.serverDep.DeepCopy()
	dep.OwnerReferences[0].UID = "foreign-cb-uid"
	if err := f.r.Update(context.Background(), dep); err != nil {
		t.Fatalf("rewrite owner ref: %v", err)
	}

	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("wait = %v, want 0", wait)
	}
	f.reload(t)
	if got := f.backend.Status.ObservedServerInstance; got != "" {
		t.Fatalf("ObservedServerInstance = %q, want empty (foreign Deployment must not be attributed to this backend)", got)
	}
}

func TestReconcileServerInstance_NonInjectedPodsDoNotCascade(t *testing.T) {
	f := newCascadeRestartFixture(t)
	// Drop the injected-by annotation on the engine pod (e.g. webhook was
	// unreachable at admission time). The Deployment matches the selector
	// but the pod is NOT actually wired to this backend, so no cascade.
	enginePod := f.enginePod.DeepCopy()
	delete(enginePod.Annotations, podwebhook.AnnotationInjectedBy)
	delete(enginePod.Annotations, podwebhook.AnnotationInjectedByUID)
	if err := f.r.Update(context.Background(), enginePod); err != nil {
		t.Fatalf("strip injected-by: %v", err)
	}

	f.backend.Status.ObservedServerInstance = "previous-server-uid"
	if err := f.r.Status().Update(context.Background(), f.backend); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	f.reload(t)

	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("wait = %v, want 0", wait)
	}

	f.reloadEngineDep(t)
	if _, ok := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; ok {
		t.Fatalf("engine deployment cascaded despite no injected-by annotation")
	}
	if got := cascadeRestartsCount(t, f.cacheNS, f.cacheName, cascadeRestartReasonServerInstanceChanged); got != 1 {
		// We still count the cascade-call as one occurrence (an
		// operator could otherwise miss flapping-server symptoms when
		// no engines happen to be injected). Zero touched Deployments
		// is documented in the metric Help text as a valid cascade.
		t.Fatalf("cascade counter = %v, want 1 (a transition with zero matched Deployments is still one cascade event)", got)
	}
}

// TestReconcileServerInstance_SelfTargetGuardSkipsOwnDeployment
// drives the self-induced-rollout-loop scenario: an over-broad
// spec.engineSelector overlaps the cache-server pod's labels AND
// the pod webhook stamps the cache-server pod with the backend's
// injected-by + injected-by-uid annotations. Without the
// self-target guard, the cascade would pull the cache-server's own
// Deployment into the target set; annotating it would roll the
// cache-server, the controller would observe the new pod's UID,
// fire another cascade, and loop forever. The guard recognizes the
// canonical name (backend.Name == owned-Deployment name) and skips
// it. The engine Deployment must still be annotated — the guard
// must be narrow.
func TestReconcileServerInstance_SelfTargetGuardSkipsOwnDeployment(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Establish baseline.
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("baseline wait = %v, want 0", wait)
	}

	// Stamp the cache-server pod with this backend's injected-by
	// annotations — simulating the misconfiguration the guard
	// defends against (webhook decided to inject the cache-server
	// pod because its labels matched spec.engineSelector).
	live := &corev1.Pod{}
	if err := f.r.Get(context.Background(), types.NamespacedName{Name: f.serverPod.Name, Namespace: f.cacheNS}, live); err != nil {
		t.Fatalf("get cache-server pod: %v", err)
	}
	if live.Annotations == nil {
		live.Annotations = map[string]string{}
	}
	live.Annotations[podwebhook.AnnotationInjectedBy] = f.cacheNS + "/" + f.cacheName
	live.Annotations[podwebhook.AnnotationInjectedByUID] = string(f.backend.UID)
	if err := f.r.Update(context.Background(), live); err != nil {
		t.Fatalf("stamp cache-server pod with injected-by annotations: %v", err)
	}

	// Replace the cache-server pod to trigger a cascade. Use a new
	// UID so the cascade decision fires.
	if err := f.r.Delete(context.Background(), f.serverPod); err != nil {
		t.Fatalf("delete old cache-server pod: %v", err)
	}
	tru := true
	replacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cache-pod-replacement",
			Namespace: f.cacheNS,
			UID:       "cache-pod-uid-replacement",
			Labels:    selectorLabels(f.cacheName),
			Annotations: map[string]string{
				// Replacement also carries the misconfigured
				// injected-by stamp.
				podwebhook.AnnotationInjectedBy:    f.cacheNS + "/" + f.cacheName,
				podwebhook.AnnotationInjectedByUID: string(f.backend.UID),
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       f.serverRS.Name,
				UID:        f.serverRS.UID,
				Controller: &tru,
			}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	if err := f.r.Create(context.Background(), replacement); err != nil {
		t.Fatalf("create replacement cache-server pod: %v", err)
	}
	if err := f.r.Status().Update(context.Background(), replacement); err != nil {
		t.Fatalf("ready replacement: %v", err)
	}

	// Wait past the rate-limit window.
	time.Sleep(60 * time.Millisecond)
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("post-replacement wait = %v, want 0", wait)
	}

	// Self-target guard: the cache-server Deployment must NOT have
	// been annotated, even though its pod carried the matching
	// injected-by stamp.
	ownDep := &appsv1.Deployment{}
	if err := f.r.Get(context.Background(), types.NamespacedName{Name: f.cacheName, Namespace: f.cacheNS}, ownDep); err != nil {
		t.Fatalf("get cache-server Deployment: %v", err)
	}
	if _, ok := ownDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; ok {
		t.Fatalf("cache-server's own Deployment was annotated for cascade — self-induced rollout loop would follow")
	}

	// The engine Deployment SHOULD still have been annotated — the
	// guard must be narrow (only skip the backend's own Deployment).
	f.reloadEngineDep(t)
	want := serverInstanceID(replacement)
	if got := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; got != want {
		t.Fatalf("engine Deployment cascade annotation = %q, want %q (guard must be narrow — engine cascades still fire)", got, want)
	}
}

// TestReconcileServerInstance_ShadowRecoversBaselineFromPatchFailure
// drives the patch-failure-window scenario: a transient
// status-subresource patch failure on the first observation leaves
// status.observedServerInstance empty. Without the in-process shadow,
// a subsequent real cache-server replacement during that window would
// read prior="" and misclassify the replacement as another first
// observation (empty→set, no cascade) — engines stuck on stale
// sockets. With the shadow, the reconciler recovers the intended
// baseline from in-memory state and the replacement cascades.
func TestReconcileServerInstance_ShadowRecoversBaselineFromPatchFailure(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Wrap the client so the FIRST status-subresource patch returns a
	// synthetic error (simulating a conflict / transient apiserver
	// hiccup), then becomes transparent on subsequent attempts.
	failOnce := &statusPatchFailingClient{Client: f.r.Client, remaining: 1}
	f.r.Client = failOnce

	// First observation: status patch fails, latch stays "", but the
	// shadow records the attempt.
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait <= 0 {
		t.Fatalf("first-observation patch failure should return a requeue duration; got %v", wait)
	}
	f.reload(t)
	if got := f.backend.Status.ObservedServerInstance; got != "" {
		t.Fatalf("status.observedServerInstance = %q after simulated patch failure; want empty", got)
	}

	// Now replace the cache-server pod with a new one (different UID)
	// — simulating a server restart in the patch-failure window.
	if err := f.r.Delete(context.Background(), f.serverPod); err != nil {
		t.Fatalf("delete old server pod: %v", err)
	}
	tru := true
	replacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cache-pod-replacement",
			Namespace: f.cacheNS,
			UID:       "cache-pod-uid-replacement",
			Labels:    selectorLabels(f.cacheName),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       f.serverRS.Name,
				UID:        f.serverRS.UID,
				Controller: &tru,
			}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	if err := f.r.Create(context.Background(), replacement); err != nil {
		t.Fatalf("create replacement pod: %v", err)
	}
	if err := f.r.Status().Update(context.Background(), replacement); err != nil {
		t.Fatalf("ready replacement pod: %v", err)
	}

	// Wait past the rate-limit (fixture is 50ms) so the cascade is
	// eligible on the next reconcile.
	time.Sleep(60 * time.Millisecond)
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("post-replacement wait = %v, want 0", wait)
	}

	// The cascade must have fired. Without the shadow this would be a
	// false-empty→set first-observation case (engines stranded).
	f.reloadEngineDep(t)
	want := serverInstanceID(replacement)
	if got := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; got != want {
		t.Fatalf("cascade annotation = %q, want %q (shadow must recover the lost baseline so the replacement cascades)", got, want)
	}
	if got := cascadeRestartsCount(t, f.cacheNS, f.cacheName, cascadeRestartReasonServerInstanceChanged); got != 1 {
		t.Fatalf("cascade counter = %v, want 1 (replacement after a swallowed baseline-patch must still count as one cascade event)", got)
	}
}

// TestReconcileServerInstance_CounterIncrementsExactlyOncePerEvent
// drives the "one increment per cascade EVENT" contract through a
// failed-persist retry cycle. The cascade fires (annotates the
// engine) on the first attempt, and the counter advances at that
// point — engines are already recovering, so the metric should
// reflect the recovery regardless of whether the latch persist
// succeeded yet. On the subsequent retry the persist finally
// succeeds via the shadow short-circuit; the counter must NOT
// double-count because the `counted` map already holds this
// (key, currentID).
func TestReconcileServerInstance_CounterIncrementsExactlyOncePerEvent(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Baseline observation persists normally.
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("baseline wait = %v, want 0", wait)
	}
	f.reload(t)
	if baseline := f.backend.Status.ObservedServerInstance; baseline == "" {
		t.Fatalf("baseline observedServerInstance is empty; expected the first observation to persist")
	}

	// Replace the server pod. The next reconcile should cascade.
	// Inject a status-patch-failing wrapper so the FIRST cascade-
	// follow-up patch fails; the cascade itself (annotate engines)
	// runs successfully, the counter advances, but the latch persist
	// returns an error.
	if err := f.r.Delete(context.Background(), f.serverPod); err != nil {
		t.Fatalf("delete server pod: %v", err)
	}
	tru := true
	replacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cache-pod-rep2",
			Namespace: f.cacheNS,
			UID:       "cache-pod-uid-rep2",
			Labels:    selectorLabels(f.cacheName),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       f.serverRS.Name,
				UID:        f.serverRS.UID,
				Controller: &tru,
			}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	if err := f.r.Create(context.Background(), replacement); err != nil {
		t.Fatalf("create replacement: %v", err)
	}
	if err := f.r.Status().Update(context.Background(), replacement); err != nil {
		t.Fatalf("ready replacement: %v", err)
	}
	failOnce := &statusPatchFailingClient{Client: f.r.Client, remaining: 1}
	f.r.Client = failOnce

	time.Sleep(60 * time.Millisecond)
	// First cascade attempt: annotates the engine + advances the
	// counter, then the status patch fails. The counter MUST be at
	// 1 by the end of this reconcile — engines are recovering, so
	// the operator-visible metric should reflect the event even if
	// the on-cluster latch could not be advanced yet.
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait <= 0 {
		t.Fatalf("post-replacement first-attempt wait = %v, want positive (persist failed → request retry)", wait)
	}
	if got := cascadeRestartsCount(t, f.cacheNS, f.cacheName, cascadeRestartReasonServerInstanceChanged); got != 1 {
		t.Fatalf("cascade counter after first attempt = %v, want 1 (engines were annotated; the metric must reflect the cascade event regardless of persist success)", got)
	}
	f.reloadEngineDep(t)
	want := serverInstanceID(replacement)
	if got := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; got != want {
		t.Fatalf("engine annotation = %q, want %q (annotate must precede persist; engines should be recovering)", got, want)
	}

	// Wait past the rate-limit window so canCascade lets the retry
	// through. The status field still holds the PRE-cascade baseline
	// (the first persist failed), so prior != currentID and the
	// reconcile re-enters the cascade branch (not the shadow short-
	// circuit). The cascade then runs idempotently: annotates are
	// no-ops (the trigger already matches currentID), the counter
	// does NOT advance (shouldIncrementCascade returns false because
	// `counted` already holds (key, currentID)), and the persist
	// finally succeeds.
	time.Sleep(60 * time.Millisecond)
	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("retry-reconcile wait = %v, want 0", wait)
	}
	if got := cascadeRestartsCount(t, f.cacheNS, f.cacheName, cascadeRestartReasonServerInstanceChanged); got != 1 {
		t.Fatalf("cascade counter after persist retry = %v, want exactly 1 (one cascade event = one increment, regardless of how many persist retries were required)", got)
	}
	// And the status field is now in sync with the in-process baseline.
	f.reload(t)
	if got := f.backend.Status.ObservedServerInstance; got != want {
		t.Fatalf("status.observedServerInstance after retry = %q, want %q (retry must reconcile the field)", got, want)
	}
}

// statusPatchFailingClient wraps a client.Client and returns a
// synthetic error from the FIRST `remaining` calls to Status().Patch
// on a CacheBackend, then passes calls through. Used to simulate a
// transient status-subresource patch failure window.
type statusPatchFailingClient struct {
	client.Client
	remaining int
}

func (c *statusPatchFailingClient) Status() client.SubResourceWriter {
	return &statusPatchFailingSubResource{
		SubResourceWriter: c.Client.Status(),
		owner:             c,
	}
}

type statusPatchFailingSubResource struct {
	client.SubResourceWriter
	owner *statusPatchFailingClient
}

func (s *statusPatchFailingSubResource) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
	if _, ok := obj.(*cachev1alpha1.CacheBackend); ok && s.owner.remaining > 0 {
		s.owner.remaining--
		return fmt.Errorf("synthetic status-subresource patch failure for test")
	}
	return s.SubResourceWriter.Patch(ctx, obj, p, opts...)
}

func TestReconcileServerInstance_AnnotateIdempotent(t *testing.T) {
	f := newCascadeRestartFixture(t)
	// Pre-seed the engine Deployment's pod template with the trigger
	// annotation set to the CURRENT cache-server UID. The cascade should
	// detect that and skip a no-op Patch (which would otherwise bump the
	// rollout revision and pointlessly recycle engine pods).
	dep := f.engineDep.DeepCopy()
	if dep.Spec.Template.Annotations == nil {
		dep.Spec.Template.Annotations = map[string]string{}
	}
	dep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger] = serverInstanceID(f.serverPod)
	if err := f.r.Update(context.Background(), dep); err != nil {
		t.Fatalf("preseed annotation: %v", err)
	}

	f.backend.Status.ObservedServerInstance = "previous-server-uid"
	if err := f.r.Status().Update(context.Background(), f.backend); err != nil {
		t.Fatalf("seed status: %v", err)
	}
	f.reload(t)

	// Use a tracking client to confirm no second Deployment write occurs.
	patches := 0
	tracked := &countingClient{Client: f.r.Client, patchCount: &patches}
	f.r.Client = tracked
	defer func() { f.r.Client = tracked.Client }()

	if wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend); wait != 0 {
		t.Fatalf("wait = %v, want 0", wait)
	}
	if patches != 0 {
		t.Fatalf("Deployment patch count = %d, want 0 (already up to date)", patches)
	}
}

func TestPodOwningDeployment_ResolvesViaReplicaSet(t *testing.T) {
	f := newCascadeRestartFixture(t)
	name, uid, ok, err := f.r.podOwningDeployment(context.Background(), f.r.Client, f.enginePod)
	if err != nil {
		t.Fatalf("podOwningDeployment err = %v, want nil", err)
	}
	if !ok {
		t.Fatalf("podOwningDeployment ok = false, want true")
	}
	if name != f.engineDepN {
		t.Fatalf("Deployment name = %q, want %q", name, f.engineDepN)
	}
	if uid == "" {
		t.Fatalf("Deployment UID is empty; the owner-chain walk must return a non-empty UID so the cascade patch can re-verify identity")
	}
}

func TestPodOwningDeployment_NoOwnerReturnsFalse(t *testing.T) {
	f := newCascadeRestartFixture(t)
	bare := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bare-pod",
			Namespace: f.engineNS,
		},
	}
	if _, _, ok, err := f.r.podOwningDeployment(context.Background(), f.r.Client, bare); err != nil || ok {
		t.Fatalf("podOwningDeployment for an unowned pod returned (ok=%v, err=%v); want (false, nil)", ok, err)
	}
}

// TestAnnotateDeploymentForCascade_TOCTOUDifferentUIDSkipsPatch locks
// the TOCTOU contract: if the live Deployment's UID does not match
// the UID observed during owner-chain resolution, the patch is
// skipped (the resolved target was deleted and re-created under the
// same name between resolution and annotate). A name-only patch in
// that window would roll an unrelated workload.
func TestAnnotateDeploymentForCascade_TOCTOUDifferentUIDSkipsPatch(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Look up the live Deployment UID so we can pass a *different* one.
	live := &appsv1.Deployment{}
	if err := f.r.Get(context.Background(), types.NamespacedName{Name: f.engineDepN, Namespace: f.engineNS}, live); err != nil {
		t.Fatalf("get live engine Deployment: %v", err)
	}
	bogus := string(live.UID) + "-stale"

	beforeTrigger := live.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]

	patched, err := f.r.annotateDeploymentForCascade(
		context.Background(),
		f.engineNS,
		f.engineDepN,
		bogus,                 // expectedUID — deliberately stale
		"new-instance-id-xyz", // serverInstanceID
	)
	if err != nil {
		t.Fatalf("annotateDeploymentForCascade returned err = %v; want nil (TOCTOU skip is not an error)", err)
	}
	if patched {
		t.Fatalf("annotateDeploymentForCascade patched = true; want false (UID mismatch should refuse the patch)")
	}

	// Confirm the Deployment was NOT modified.
	after := &appsv1.Deployment{}
	if err := f.r.Get(context.Background(), types.NamespacedName{Name: f.engineDepN, Namespace: f.engineNS}, after); err != nil {
		t.Fatalf("get post-call engine Deployment: %v", err)
	}
	if got := after.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; got != beforeTrigger {
		t.Fatalf("Deployment annotation was modified despite UID mismatch: before=%q after=%q", beforeTrigger, got)
	}
}

// TestReconcileServerInstance_ObservationErrorRequeues asserts that when
// currentServerInstanceID fails (transient apiserver/RBAC hiccup),
// reconcileServerInstance returns a positive requeue duration so the
// reconcile retries within the rate-limit window — without this, an
// observation failure would silently skip the cascade and the only
// path back is unrelated watch events.
func TestReconcileServerInstance_ObservationErrorRequeues(t *testing.T) {
	f := newCascadeRestartFixture(t)

	// Inject a Client whose Pod List errors. The pod List in
	// currentServerInstanceID is the first apiserver call on the
	// reconciler's hot path, so any error here exercises the
	// "observation failed" branch.
	f.r.Client = &erroringPodListClient{Client: f.r.Client}

	wait := f.r.reconcileServerInstance(context.Background(), logr.Discard(), f.backend)
	if wait <= 0 {
		t.Fatalf("reconcileServerInstance wait = %v on observation failure; want a positive requeue (rate-limit interval)", wait)
	}
	if want := f.r.minServerRestartCascadeInterval(); wait != want {
		t.Fatalf("reconcileServerInstance wait = %v on observation failure; want %v (the rate-limit interval)", wait, want)
	}
}

// erroringPodListClient wraps a client.Client and returns a synthetic
// error from List() when the target is a PodList. Used to exercise the
// reconcileServerInstance observation-failure path.
type erroringPodListClient struct {
	client.Client
}

func (c *erroringPodListClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*corev1.PodList); ok {
		return fmt.Errorf("synthetic pod-list failure for test")
	}
	return c.Client.List(ctx, list, opts...)
}

// countingClient counts Patch calls on Deployments. Used by
// TestReconcileServerInstance_AnnotateIdempotent to confirm a no-op
// cascade does not bump the rollout revision.
type countingClient struct {
	client.Client
	patchCount *int
}

func (c *countingClient) Patch(ctx context.Context, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
	if _, ok := obj.(*appsv1.Deployment); ok {
		*c.patchCount++
	}
	return c.Client.Patch(ctx, obj, p, opts...)
}

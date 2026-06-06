package controller

import (
	"context"
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
// wired managed CacheBackend with one Ready cache-server pod and one
// engine Deployment+ReplicaSet+Pod that the webhook has injected against
// the backend. Shared by every cascade test so each scenario only
// expresses what's different (UID, status, rate-limit window, …) and the
// boring setup stays terse.
type cascadeRestartFixture struct {
	r          *CacheBackendReconciler
	backend    *cachev1alpha1.CacheBackend
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

	// The "current Ready" cache-server pod the controller observes. Labeled
	// with the exact selectorLabels() set so currentServerInstanceUID's
	// List finds it.
	f.serverPod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cache-pod-aaa",
			Namespace: f.cacheNS,
			UID:       "cache-pod-uid-1",
			Labels: map[string]string{
				"app.kubernetes.io/name":       "cachebackend",
				"app.kubernetes.io/instance":   f.cacheName,
				"app.kubernetes.io/managed-by": "inference-cache-controller",
			},
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
	tru := true
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
		WithStatusSubresource(&cachev1alpha1.CacheBackend{}, &appsv1.Deployment{}, &corev1.PersistentVolumeClaim{}).
		WithObjects(f.backend, f.serverPod, f.engineDep, f.engineRS, f.enginePod).
		Build()
	f.r = &CacheBackendReconciler{
		Client:                          c,
		Scheme:                          scheme,
		Log:                             logr.Discard(),
		MinServerRestartCascadeInterval: 50 * time.Millisecond, // tests run with a tiny window
		serverInstanceCascade:           newServerInstanceCascade(),
	}

	ResetBackendServerRestartsTotalForTest()
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

func cascadeRestartsCount(t *testing.T, namespace, backend, reason string) float64 {
	t.Helper()
	m, err := backendServerRestartsTotal.GetMetricWithLabelValues(namespace, backend, reason)
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
	if got := f.backend.Status.ObservedServerInstance; got != string(f.serverPod.UID) {
		t.Fatalf("ObservedServerInstance = %q, want %q", got, f.serverPod.UID)
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
	if got != string(f.serverPod.UID) {
		t.Fatalf("cascade annotation = %q, want %q (the new cache-server pod UID)", got, f.serverPod.UID)
	}

	f.reload(t)
	if got := f.backend.Status.ObservedServerInstance; got != string(f.serverPod.UID) {
		t.Fatalf("ObservedServerInstance = %q, want %q", got, f.serverPod.UID)
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
	if got := f.backend.Status.ObservedServerInstance; got != string(f.serverPod.UID) {
		t.Fatalf("ObservedServerInstance = %q, want pinned to first-cascade UID %q", got, f.serverPod.UID)
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

func TestReconcileServerInstance_NoEngineSelectorSkipsCascadeButStillTracksUID(t *testing.T) {
	f := newCascadeRestartFixture(t)
	// Strip the engine selector — a backend with no claimed engines has no
	// fleet to cascade-restart, but its observedServerInstance still tracks
	// the cache-server pod (so a future engine attach inherits the
	// baseline correctly).
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
	if _, ok := f.engineDep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger]; ok {
		t.Fatalf("engine deployment unexpectedly cascaded with no engine selector configured")
	}
	f.reload(t)
	if got := f.backend.Status.ObservedServerInstance; got != string(f.serverPod.UID) {
		t.Fatalf("ObservedServerInstance = %q, want %q (still tracked even without engines)", got, f.serverPod.UID)
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
	dep.Spec.Template.Annotations[AnnotationCacheServerRestartTrigger] = string(f.serverPod.UID)
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

func TestPodOwningDeploymentName_ResolvesViaReplicaSet(t *testing.T) {
	f := newCascadeRestartFixture(t)
	got, ok := f.r.podOwningDeploymentName(context.Background(), f.r.Client, f.enginePod)
	if !ok {
		t.Fatalf("podOwningDeploymentName ok = false, want true")
	}
	if got != f.engineDepN {
		t.Fatalf("Deployment name = %q, want %q", got, f.engineDepN)
	}
}

func TestPodOwningDeploymentName_NoOwnerReturnsFalse(t *testing.T) {
	f := newCascadeRestartFixture(t)
	bare := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bare-pod",
			Namespace: f.engineNS,
		},
	}
	if _, ok := f.r.podOwningDeploymentName(context.Background(), f.r.Client, bare); ok {
		t.Fatalf("podOwningDeploymentName for an unowned pod returned ok=true; want false")
	}
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

package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
)

// newEnginePodEventsReconciler builds the reconciler under test with a
// fake client seeded with objs and a buffered FakeRecorder. The buffer is
// generous so a test never blocks on emit; assertions drain non-blockingly.
func newEnginePodEventsReconciler(t *testing.T, objs ...client.Object) (*EnginePodEventsReconciler, *events.FakeRecorder) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("clientgoscheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("cachev1alpha1: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	rec := events.NewFakeRecorder(16)
	return &EnginePodEventsReconciler{Client: c, Log: logr.Discard(), Recorder: rec}, rec
}

func injectedPod(name, namespace, cbRef string, podLabels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			UID:         types.UID(name + "-uid"),
			Labels:      podLabels,
			Annotations: map[string]string{podwebhook.AnnotationInjectedBy: cbRef},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "engine", Image: "registry.example.com/vllm:test"}},
		},
	}
}

// injectedPodWithUID returns a pod stamped with BOTH the injected-by
// annotation AND the matching injected-by-uid annotation that the webhook
// writes on a successful injection. Used by tests that simulate the
// happy-path "real webhook actually ran" scenario.
func injectedPodWithUID(name, namespace, cbRef, cbUID string, podLabels map[string]string) *corev1.Pod {
	p := injectedPod(name, namespace, cbRef, podLabels)
	p.Annotations[podwebhook.AnnotationInjectedByUID] = cbUID
	return p
}

func skippedPod(name, namespace string, podLabels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(name + "-uid"),
			Labels:    podLabels,
			Annotations: map[string]string{
				podwebhook.AnnotationSkip:          "true",
				podwebhook.AnnotationInjectSkipped: podwebhook.InjectSkippedReasonSkipAnnotation,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "engine", Image: "registry.example.com/vllm:test"}},
		},
	}
}

func reconcilePod(t *testing.T, r *EnginePodEventsReconciler, namespace, name string) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: name, Namespace: namespace},
	}); err != nil {
		t.Fatalf("reconcile pod %s/%s: %v", namespace, name, err)
	}
}

func drainRecorder(rec *events.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func TestEnginePodEvents_EmitsInjectedEventOnAnnotatedPod(t *testing.T) {
	const ns = "engines"
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: ns, UID: "primary-uid-1"},
	}
	pod := injectedPodWithUID("engine-a", ns, ns+"/"+cb.Name, string(cb.UID), map[string]string{"app": "vllm"})
	r, rec := newEnginePodEventsReconciler(t, cb, pod)

	reconcilePod(t, r, ns, pod.Name)

	got := drainRecorder(rec)
	want := "Normal " + eventReasonEngineInjected
	found := false
	for _, e := range got {
		if strings.Contains(e, want) && strings.Contains(e, ns+"/"+cb.Name) {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected event containing %q and %q; got %v", want, ns+"/"+cb.Name, got)
	}
}

func TestEnginePodEvents_NoEventOnPodWithoutAnnotation(t *testing.T) {
	// A pod without the injected-by annotation is unrelated to this
	// cache plane. The controller must skip it — no event, no error.
	const ns = "engines"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "unrelated",
			Namespace: ns,
			UID:       "unrelated-uid",
			Labels:    map[string]string{"app": "anything"},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "x", Image: "x"}}},
	}
	r, rec := newEnginePodEventsReconciler(t, pod)

	reconcilePod(t, r, ns, pod.Name)

	if got := drainRecorder(rec); len(got) != 0 {
		t.Fatalf("expected no events on unannotated pod, got %v", got)
	}
}

func TestEnginePodEvents_EmitsSkippedEventOnSkippedPod(t *testing.T) {
	const ns = "engines"
	pod := skippedPod("engine-skip", ns, map[string]string{"app": "vllm"})
	r, rec := newEnginePodEventsReconciler(t, pod)

	reconcilePod(t, r, ns, pod.Name)

	got := drainRecorder(rec)
	expect := "Normal " + eventReasonSkippedByOperator
	found := false
	for _, e := range got {
		if strings.Contains(e, expect) && strings.Contains(e, podwebhook.InjectSkippedReasonSkipAnnotation) {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected event containing %q and %q; got %v", expect, podwebhook.InjectSkippedReasonSkipAnnotation, got)
	}
}

func TestEnginePodEventsPredicateIncludesInjectedAndSkippedPods(t *testing.T) {
	const ns = "engines"
	if !enginePodEventCandidate(injectedPod("engine-a", ns, ns+"/primary", nil)) {
		t.Fatalf("injected pod was not accepted by predicate")
	}
	if !enginePodEventCandidate(skippedPod("engine-skip", ns, nil)) {
		t.Fatalf("skipped pod was not accepted by predicate")
	}
	if enginePodEventCandidate(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "plain", Namespace: ns}}) {
		t.Fatalf("plain pod was accepted by predicate")
	}
}

func TestEnginePodEvents_SkipsWhenCacheBackendNotFound(t *testing.T) {
	// Without a live CR, the controller cannot verify the injected-by-uid
	// annotation. The pod's injected-by annotation alone is user-
	// controllable, so an absent CR could mean either "CR was deleted
	// after the webhook stamped this pod" (real injection) or "user
	// forged the annotation while the webhook was unreachable"
	// (failurePolicy=Ignore). Without the UID check we cannot tell
	// these apart — be conservative and skip the event. The signal
	// stays authoritative at the cost of missing it in the rare
	// CR-deleted-mid-reconcile case.
	const ns = "engines"
	pod := injectedPodWithUID("engine-a", ns, ns+"/missing-cb", "doesnt-matter-uid", nil)
	r, rec := newEnginePodEventsReconciler(t, pod) // no CB seeded

	reconcilePod(t, r, ns, pod.Name)

	if got := drainRecorder(rec); len(got) != 0 {
		t.Fatalf("expected no event when CacheBackend is missing; got %v", got)
	}
}

func TestEnginePodEvents_SkipsWhenInjectedByUIDAnnotationMissing(t *testing.T) {
	// A pod carrying only the injected-by annotation but NOT the
	// injected-by-uid companion is the failurePolicy=Ignore forgery
	// scenario: the user supplied injected-by in the pod template; the
	// webhook never ran, so the UID annotation is absent. Skip emission.
	const ns = "engines"
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: ns, UID: "primary-uid-1"},
	}
	// Use injectedPod (NOT injectedPodWithUID) so the UID annotation is absent.
	pod := injectedPod("engine-forger", ns, ns+"/"+cb.Name, nil)
	r, rec := newEnginePodEventsReconciler(t, cb, pod)

	reconcilePod(t, r, ns, pod.Name)

	if got := drainRecorder(rec); len(got) != 0 {
		t.Fatalf("expected no event when injected-by-uid annotation is missing; got %v", got)
	}
}

func TestEnginePodEvents_EmitsWhenForgedAnnotationsCarryCurrentLiveUID(t *testing.T) {
	// Pins the documented LIMIT of the injected-by-uid check: it
	// reduces the failurePolicy=Ignore forgery surface (catches
	// copy/paste from a different CR or a stale UID) but does NOT
	// cryptographically authenticate the webhook. metadata.uid is not
	// secret; a pod creator with `get` RBAC on CacheBackends can read
	// the live UID and stamp the pair correctly, and the controller
	// then emits the InjectedByCacheBackend Event as if the webhook
	// had actually injected. This test exists so a future "we closed
	// the hole" claim trips the assertion below and forces a real
	// authentication mechanism (webhook-authored signature) rather
	// than a documentation update.
	const ns = "engines"
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: ns, UID: "primary-uid-1"},
	}
	// Forger stamps the LIVE UID — exactly what a user with API read
	// access would do under failurePolicy=Ignore with the webhook
	// unreachable.
	pod := injectedPodWithUID("engine-forger", ns, ns+"/"+cb.Name, string(cb.UID), nil)
	r, rec := newEnginePodEventsReconciler(t, cb, pod)

	reconcilePod(t, r, ns, pod.Name)

	got := drainRecorder(rec)
	found := false
	for _, e := range got {
		if strings.Contains(e, eventReasonEngineInjected) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected InjectedByCacheBackend event when forged annotations carry the live UID (documented limit); got %v", got)
	}
}

func TestEnginePodEvents_UsesAPIReaderForCacheBackendLookup(t *testing.T) {
	// Pin the structural invariant: the CacheBackend lookup MUST go
	// through APIReader (uncached live client), not the embedded
	// Client (cached). The cached client's informer can be momentarily
	// stale — especially right at controller startup or right after a
	// CR's first apply — so a NotFound from the cache may be a real
	// deletion OR a cache miss. NotFound is treated as a permanent
	// skip per the conservative contract; if we honored cache misses
	// we would silently drop the one-shot event. Live reads remove
	// the ambiguity. A regression that flips the lookup back to the
	// cached client would fail this test because the two clients hold
	// disjoint objects.
	const ns = "engines"
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: ns, UID: "primary-uid-1"},
	}
	pod := injectedPodWithUID("engine-a", ns, ns+"/"+cb.Name, string(cb.UID), nil)
	// Cached client: knows the POD (which the reconciler Gets first)
	// but NOT the CacheBackend. If the controller used Client for the
	// CR lookup, it would see NotFound and skip.
	cached := newEnginePodEventsClient(t, pod)
	// APIReader: knows the CacheBackend. The reconciler should consult
	// it for the lookup and find the CR there.
	apireader := newEnginePodEventsClient(t, cb)
	rec := events.NewFakeRecorder(16)
	r := &EnginePodEventsReconciler{
		Client:    cached,
		Log:       logr.Discard(),
		Recorder:  rec,
		APIReader: apireader,
	}

	reconcilePod(t, r, ns, pod.Name)

	got := drainRecorder(rec)
	want := "Normal " + eventReasonEngineInjected
	found := false
	for _, e := range got {
		if strings.Contains(e, want) {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected event from APIReader-backed lookup; got %v", got)
	}
}

// newEnginePodEventsClient builds a fresh fake client preloaded with
// objs, sharing the controller scheme.
func newEnginePodEventsClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add cache scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

func TestEnginePodEvents_TransientLookupErrorRetries(t *testing.T) {
	// A non-NotFound CacheBackend Get failure (RBAC blip, transient API
	// error, informer cache miss) must NOT be swallowed as a skip:
	// admission is CREATE-only, so a one-shot drop loses the event for
	// the affected pod permanently. The reconciler must surface the
	// error so controller-runtime requeues with backoff.
	const ns = "engines"
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: ns, UID: "primary-uid-1"},
	}
	pod := injectedPodWithUID("engine-a", ns, ns+"/"+cb.Name, string(cb.UID), nil)
	r, rec := newEnginePodEventsReconciler(t, cb, pod)
	wantErr := errors.New("synthetic transient apiserver error")
	// Wrap the embedded client so the controller's Get on CacheBackends
	// returns a transient error. We can't use interceptor.Funcs directly
	// on the builder because the test helper already built the fake;
	// shadow the Client field with a wrapper instead.
	r.Client = &transientCBGetClient{Client: r.Client, kind: "CacheBackend", err: wantErr}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: pod.Name, Namespace: ns},
	})
	if err == nil {
		t.Fatalf("expected reconcile error for transient lookup failure; got nil")
	}
	if !strings.Contains(err.Error(), wantErr.Error()) {
		t.Fatalf("expected reconcile error to wrap %q; got %v", wantErr, err)
	}
	if got := drainRecorder(rec); len(got) != 0 {
		t.Fatalf("expected no event on transient lookup failure (it'll fire on the retry); got %v", got)
	}
}

// transientCBGetClient injects err into the Get path for CacheBackend
// objects only; all other reads/writes pass through unchanged. Pod Gets
// (which the reconciler does first) still work.
type transientCBGetClient struct {
	client.Client
	kind string
	err  error
}

func (c *transientCBGetClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*cachev1alpha1.CacheBackend); ok {
		return c.err
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

func TestEnginePodEvents_SkipsWhenInjectedByUIDDoesNotMatchLiveCR(t *testing.T) {
	// The user-supplied UID could match a previous incarnation of the
	// CR (recreated under the same name) or be guessed. The check is:
	// the annotation UID must equal the LIVE CR's UID.
	const ns = "engines"
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: ns, UID: "primary-uid-CURRENT"},
	}
	pod := injectedPodWithUID("engine-stale-uid", ns, ns+"/"+cb.Name, "primary-uid-OLD", nil)
	r, rec := newEnginePodEventsReconciler(t, cb, pod)

	reconcilePod(t, r, ns, pod.Name)

	if got := drainRecorder(rec); len(got) != 0 {
		t.Fatalf("expected no event when injected-by-uid does not match the live CR; got %v", got)
	}
}

func TestEnginePodEvents_NoEventOnDeletedPod(t *testing.T) {
	// A reconcile fired against a pod that was deleted between enqueue
	// and reconcile must short-circuit cleanly (NotFound → nil error,
	// no event, no requeue).
	const ns = "engines"
	r, rec := newEnginePodEventsReconciler(t /* no pod seeded */)

	reconcilePod(t, r, ns, "engine-gone")

	if got := drainRecorder(rec); len(got) != 0 {
		t.Fatalf("expected no events for deleted pod, got %v", got)
	}
}

func TestEnginePodEvents_MalformedAnnotationDoesNotEmit(t *testing.T) {
	// The webhook always stamps `<namespace>/<name>`. A pod whose
	// injected-by annotation does not parse cleanly therefore can
	// ONLY be stale or manually-tampered: the webhook didn't write
	// it in this shape. Emitting a Normal "Injected engine config"
	// for that would falsely claim the webhook had done work it
	// never did. The controller skips emission and logs the reason.
	const ns = "engines"
	cases := []struct {
		name    string
		annoVal string
	}{
		{name: "no slash separator", annoVal: "no-slash-here"},
		{name: "empty namespace half", annoVal: "/primary"},
		{name: "empty name half", annoVal: "engines/"},
		{name: "empty string", annoVal: ""},
		{name: "extra slash (third segment)", annoVal: "engines/primary/extra"},
		// Slash-shaped but Kubernetes-invalid refs. Without the
		// validation check these slip past validCacheBackendRef and
		// hit the apiserver as a BadRequest, which the controller's
		// retry-on-non-NotFound branch would hot-loop.
		{name: "uppercase namespace", annoVal: "ENGINES/primary"},
		{name: "uppercase name", annoVal: "engines/PRIMARY"},
		{name: "underscore in namespace", annoVal: "bad_ns/cb"},
		{name: "leading hyphen in name", annoVal: "engines/-cb"},
		{name: "dots in namespace (namespaces are DNS labels)", annoVal: "ns.with.dots/cb"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := injectedPod("engine-weird", ns, tc.annoVal, nil)
			r, rec := newEnginePodEventsReconciler(t, pod)

			reconcilePod(t, r, ns, pod.Name)

			got := drainRecorder(rec)
			for _, e := range got {
				if strings.Contains(e, eventReasonEngineInjected) {
					t.Fatalf("expected no InjectedByCacheBackend event for malformed ref %q; got %q", tc.annoVal, e)
				}
			}
		})
	}
}

func TestEnginePodEvents_NilRecorderIsSafe(t *testing.T) {
	// SetupWithManager wires a Recorder, but a directly-constructed
	// reconciler in a test that doesn't care about events must not
	// panic.
	const ns = "engines"
	pod := injectedPod("engine-a", ns, ns+"/primary", nil)
	r, _ := newEnginePodEventsReconciler(t, pod)
	r.Recorder = nil

	reconcilePod(t, r, ns, pod.Name)
}

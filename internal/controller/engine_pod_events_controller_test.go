package controller

import (
	"context"
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
		ObjectMeta: metav1.ObjectMeta{Name: "primary", Namespace: ns},
	}
	pod := injectedPod("engine-a", ns, ns+"/"+cb.Name, map[string]string{"app": "vllm"})
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

func TestEnginePodEvents_EmitsEvenWhenCacheBackendNotFound(t *testing.T) {
	// The annotation is the source of truth for the binding identity;
	// the lookup of the named CacheBackend only enriches the event with
	// the Related reference. A missing CR (e.g. deleted between
	// admission and our reconcile) must NOT suppress the event — the
	// pod was wired and that's what the operator needs to know.
	const ns = "engines"
	pod := injectedPod("engine-a", ns, ns+"/missing-cb", nil)
	r, rec := newEnginePodEventsReconciler(t, pod) // no CB seeded

	reconcilePod(t, r, ns, pod.Name)

	got := drainRecorder(rec)
	want := "Normal " + eventReasonEngineInjected
	found := false
	for _, e := range got {
		if strings.Contains(e, want) && strings.Contains(e, "missing-cb") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected event containing %q and %q; got %v", want, "missing-cb", got)
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

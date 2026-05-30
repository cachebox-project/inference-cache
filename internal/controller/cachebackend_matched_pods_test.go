package controller

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// matchedSelector is the canonical selector all matched-pods tests use so
// the helpers can keep producing matching/non-matching pods without taking
// a selector argument.
var matchedSelector = map[string]string{"app": "engine"}

func lmcacheBackendWithSelector(name, namespace string, matchLabels map[string]string) *cachev1alpha1.CacheBackend {
	cb := lmcacheBackend(name, namespace)
	cb.Spec.EngineSelector = &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: matchLabels}
	return cb
}

// engineLikePod returns a minimal Pod object usable as a label-bearing fake
// for the reconciler's pod-count query. It omits container spec because the
// match is by labels alone.
func engineLikePod(name, namespace string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "engine", Image: "registry.example.com/vllm:test"}},
		},
	}
}

func TestReconcileMatchedEnginePodsCountsMatchingPods(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackendWithSelector("cache", "ns1", matchedSelector)
	objs := []client.Object{
		cb,
		engineLikePod("engine-a", "ns1", matchedSelector),
		engineLikePod("engine-b", "ns1", matchedSelector),
		engineLikePod("engine-c", "ns1", matchedSelector),
		// Different label set — must not count.
		engineLikePod("other", "ns1", map[string]string{"app": "router"}),
		// Same label set but in a different namespace — must not count.
		engineLikePod("engine-cross", "ns2", matchedSelector),
	}
	r := newReconciler(scheme, objs...)

	reconcile(t, r, "cache", "ns1")

	got := getBackend(t, r, "cache", "ns1").Status.MatchedEnginePods
	if got == nil {
		t.Fatalf("status.matchedEnginePods = nil, want 3 (3 matching pods in ns1)")
	}
	if *got != 3 {
		t.Fatalf("status.matchedEnginePods = %d, want 3", *got)
	}
}

func TestReconcileMatchedEnginePodsZeroWhenSelectorMatchesNothing(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackendWithSelector("cache", "ns1", matchedSelector)
	// A pod that does NOT match the selector — the count must be a
	// meaningful observed 0, not nil (nil = "not yet computed").
	r := newReconciler(scheme, cb, engineLikePod("other", "ns1", map[string]string{"app": "router"}))

	reconcile(t, r, "cache", "ns1")

	got := getBackend(t, r, "cache", "ns1").Status.MatchedEnginePods
	if got == nil {
		t.Fatalf("status.matchedEnginePods = nil, want 0 (selector configured, no matches)")
	}
	if *got != 0 {
		t.Fatalf("status.matchedEnginePods = %d, want 0", *got)
	}
}

func TestReconcileMatchedEnginePodsNilWhenSelectorAbsent(t *testing.T) {
	scheme := newScheme(t)
	// A CacheBackend without an engineSelector matches no pods by
	// design (the webhook ignores empty selectors to avoid claiming
	// every pod in the namespace). The status field stays nil so the
	// printer column renders blank rather than advertising a 0-match.
	cb := lmcacheBackend("cache", "ns1")
	r := newReconciler(scheme, cb, engineLikePod("anything", "ns1", matchedSelector))

	reconcile(t, r, "cache", "ns1")

	if got := getBackend(t, r, "cache", "ns1").Status.MatchedEnginePods; got != nil {
		t.Fatalf("status.matchedEnginePods = %v (*=%d), want nil for CB without selector", got, *got)
	}
}

func TestReconcileMatchedEnginePodsClearsWhenSelectorRemoved(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackendWithSelector("cache", "ns1", matchedSelector)
	r := newReconciler(scheme, cb, engineLikePod("e1", "ns1", matchedSelector))

	reconcile(t, r, "cache", "ns1")
	if got := getBackend(t, r, "cache", "ns1").Status.MatchedEnginePods; got == nil || *got != 1 {
		t.Fatalf("baseline: status.matchedEnginePods = %v, want 1", got)
	}

	// Operator removes the selector. The count must go back to nil so
	// the printer column does not advertise a stale match for a CR that
	// no longer claims any engine pods.
	live := getBackend(t, r, "cache", "ns1")
	live.Spec.EngineSelector = nil
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("clear selector: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	if got := getBackend(t, r, "cache", "ns1").Status.MatchedEnginePods; got != nil {
		t.Fatalf("after clearing selector, status.matchedEnginePods = %v (*=%d), want nil", got, *got)
	}
}

func TestReconcileMatchedEnginePodsWriteOnlyOnChange(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add cache scheme: %v", err)
	}

	cb := lmcacheBackendWithSelector("cache", "ns1", matchedSelector)
	pods := []client.Object{
		engineLikePod("e1", "ns1", matchedSelector),
		engineLikePod("e2", "ns1", matchedSelector),
	}

	// Count only the SubResourcePatch calls that touch the CacheBackend
	// status — those are the writes the refresher would issue when the
	// count drifts. Other status patches (Endpoint, Health, ...) flow
	// through the same SubResourcePatch interceptor so we filter by
	// MergePatch payload; the simplest pin is the resource type +
	// sub-resource name, with the patch contents inspected only in the
	// "did it move?" assertion below.
	var (
		cbStatusPatches int32
	)
	funcs := interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if sub == "status" {
				if _, ok := obj.(*cachev1alpha1.CacheBackend); ok {
					atomic.AddInt32(&cbStatusPatches, 1)
				}
			}
			return c.Status().Patch(ctx, obj, patch, opts...)
		},
	}
	objs := append([]client.Object{cb}, pods...)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheBackend{}, &appsv1.Deployment{}).
		WithObjects(objs...).
		WithInterceptorFuncs(funcs).
		Build()
	r := &CacheBackendReconciler{Client: c, Scheme: scheme, Log: logr.Discard()}

	// First reconcile establishes status (Endpoint, Health, FailOpen) AND
	// the matchedEnginePods count — both go through SubResourcePatch.
	reconcile(t, r, "cache", "ns1")
	firstPasses := atomic.LoadInt32(&cbStatusPatches)
	if firstPasses == 0 {
		t.Fatalf("first reconcile issued no CacheBackend status patches; want at least one")
	}
	if got := getBackend(t, r, "cache", "ns1").Status.MatchedEnginePods; got == nil || *got != 2 {
		t.Fatalf("after first reconcile: matchedEnginePods = %v, want 2", got)
	}

	// Second reconcile sees an identical world. The matchedEnginePods
	// writer must skip its patch entirely (count unchanged). Other
	// status fields are also at steady state, so the SubResourcePatch
	// counter must not advance at all.
	reconcile(t, r, "cache", "ns1")
	if got := atomic.LoadInt32(&cbStatusPatches); got != firstPasses {
		t.Fatalf("steady-state reconcile patched CacheBackend status %d more time(s); want 0", got-firstPasses)
	}
}

func TestReconcileMatchedEnginePodsFailSoftOnListError(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add cache scheme: %v", err)
	}

	cb := lmcacheBackendWithSelector("cache", "ns1", matchedSelector)
	// Seed an existing observed value: a previous reconcile observed 5.
	cb.Status.MatchedEnginePods = ptrInt32(5)

	// Fail every Pod List with a synthetic error so the refresher must
	// take its fail-soft branch. The rest of the reconcile must still
	// proceed (no error return), and the pre-existing count must
	// survive (clearing on a transient apiserver hiccup would dump
	// false "0 matches" alarms into kubectl get cb output).
	listErr := errors.New("synthetic apiserver list failure")
	funcs := interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				return listErr
			}
			return c.List(ctx, list, opts...)
		},
	}
	objs := []client.Object{cb}
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheBackend{}, &appsv1.Deployment{}).
		WithObjects(objs...).
		WithInterceptorFuncs(funcs).
		Build()
	r := &CacheBackendReconciler{Client: fc, Scheme: scheme, Log: logr.Discard()}

	reconcile(t, r, "cache", "ns1") // no Fatal because reconcile must not surface the list error

	got := getBackend(t, r, "cache", "ns1").Status.MatchedEnginePods
	if got == nil {
		t.Fatalf("status.matchedEnginePods = nil after transient list error; want preserved (5)")
	}
	if *got != 5 {
		t.Fatalf("status.matchedEnginePods = %d after transient list error; want preserved (5)", *got)
	}
}

// TestReconcileMatchedEnginePodsCoexistsWithOtherStatusWriters confirms the
// matchedEnginePods writer does not stomp on the other status writers in the
// same Reconcile (Endpoint, Health, FailOpen, ObservedGeneration) — both run
// in the same pass and must read each other's writes via the merge patch.
// Pins the coexistence invariant any future status writer must also satisfy.
func TestReconcileMatchedEnginePodsCoexistsWithOtherStatusWriters(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackendWithSelector("cache", "ns1", matchedSelector)
	r := newReconciler(scheme, cb,
		engineLikePod("e1", "ns1", matchedSelector),
		engineLikePod("e2", "ns1", matchedSelector),
	)

	reconcile(t, r, "cache", "ns1")

	got := getBackend(t, r, "cache", "ns1")
	if got.Status.MatchedEnginePods == nil || *got.Status.MatchedEnginePods != 2 {
		t.Fatalf("matchedEnginePods = %v, want 2", got.Status.MatchedEnginePods)
	}
	if got.Status.Endpoint == "" {
		t.Fatalf("status.endpoint dropped — the matchedEnginePods patch should not stomp on the other status writers")
	}
	if got.Status.ObservedGeneration == 0 {
		t.Fatalf("status.observedGeneration = 0 — the matchedEnginePods patch should not stomp on dispatch's status writes")
	}
	if got.Status.FailOpen == nil || !*got.Status.FailOpen {
		t.Fatalf("status.failOpen = %v, want true echo preserved across the matchedEnginePods patch", got.Status.FailOpen)
	}
}

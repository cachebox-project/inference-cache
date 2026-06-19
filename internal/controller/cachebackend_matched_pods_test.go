package controller

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
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
// for the reconciler's pod-count query. The container is a placeholder
// (matching is purely by labels), but the apiserver/fake-client rejects
// an empty Containers list, so one minimal entry is included.
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
	msg := getBackend(t, r, "cache", "ns1").Status.EngineSelectorMessage
	if !strings.Contains(msg, "spec.engineSelector.matchLabels={app:engine}") ||
		!strings.Contains(msg, "no Pods in namespace match") {
		t.Fatalf("status.engineSelectorMessage = %q, want selector echo and no-match diagnosis", msg)
	}
}

func TestReconcileMatchedEnginePodsNoUnmatchedDiagnosticWhenDeploymentScaledToZero(t *testing.T) {
	const ns = "ns1"
	cb := lmcacheBackendWithSelector("cache", ns, matchedSelector)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "engine", Namespace: ns},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptrInt32(0),
			Selector: &metav1.LabelSelector{MatchLabels: matchedSelector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: matchedSelector},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "engine", Image: "registry.example.com/vllm:test"}},
				},
			},
		},
	}
	r, rec := newReconcilerWithRecorder(t, cb, dep)

	reconcile(t, r, "cache", ns)

	got := getBackend(t, r, "cache", ns)
	if got.Status.MatchedEnginePods == nil || *got.Status.MatchedEnginePods != 0 {
		t.Fatalf("status.matchedEnginePods = %v, want observed 0", got.Status.MatchedEnginePods)
	}
	if got.Status.EngineSelectorMessage != "" {
		t.Fatalf("status.engineSelectorMessage = %q, want empty for intentionally scaled-to-zero Deployment", got.Status.EngineSelectorMessage)
	}
	expectNoEvent(t, drainEvents(rec), eventReasonEngineSelectorUnmatched)
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
	if msg := getBackend(t, r, "cache", "ns1").Status.EngineSelectorMessage; msg != "" {
		t.Fatalf("after clearing selector, status.engineSelectorMessage = %q, want cleared", msg)
	}
}

func TestReconcileMatchedEnginePodsClearsMessageWhenPodsReturn(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackendWithSelector("cache", "ns1", matchedSelector)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")
	if msg := getBackend(t, r, "cache", "ns1").Status.EngineSelectorMessage; msg == "" {
		t.Fatalf("status.engineSelectorMessage empty after zero-match observation, want diagnosis")
	}

	if err := r.Create(context.Background(), engineLikePod("e1", "ns1", matchedSelector)); err != nil {
		t.Fatalf("create matching pod: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	got := getBackend(t, r, "cache", "ns1")
	if got.Status.MatchedEnginePods == nil || *got.Status.MatchedEnginePods != 1 {
		t.Fatalf("matchedEnginePods = %v, want 1", got.Status.MatchedEnginePods)
	}
	if got.Status.EngineSelectorMessage != "" {
		t.Fatalf("status.engineSelectorMessage = %q, want cleared after a pod matches", got.Status.EngineSelectorMessage)
	}
}

func TestReconcileMatchedEnginePodsEmitsUnmatchedEventOnInitialZeroAndTransition(t *testing.T) {
	cb := lmcacheBackendWithSelector("cache", "ns1", matchedSelector)
	r, rec := newReconcilerWithRecorder(t, cb)

	reconcile(t, r, "cache", "ns1")
	events := drainEvents(rec)
	expectEvent(t, events, "Normal "+eventReasonEngineSelectorUnmatched)
	expectEvent(t, events, "spec.engineSelector.matchLabels={app:engine}")

	reconcile(t, r, "cache", "ns1")
	expectNoEvent(t, drainEvents(rec), eventReasonEngineSelectorUnmatched)

	if err := r.Create(context.Background(), engineLikePod("e1", "ns1", matchedSelector)); err != nil {
		t.Fatalf("create matching pod: %v", err)
	}
	reconcile(t, r, "cache", "ns1")
	expectNoEvent(t, drainEvents(rec), eventReasonEngineSelectorUnmatched)

	if err := r.Delete(context.Background(), engineLikePod("e1", "ns1", matchedSelector)); err != nil {
		t.Fatalf("delete matching pod: %v", err)
	}
	reconcile(t, r, "cache", "ns1")
	events = drainEvents(rec)
	expectEvent(t, events, "Normal "+eventReasonEngineSelectorUnmatched)
	expectEvent(t, events, "no Pods in namespace match")
}

func TestReconcileMatchedEnginePodsNoUnmatchedEventWithoutSelector(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	r, rec := newReconcilerWithRecorder(t, cb)

	reconcile(t, r, "cache", "ns1")

	expectNoEvent(t, drainEvents(rec), eventReasonEngineSelectorUnmatched)
}

// TestReconcileSchedulesRequeueWhenSelectorRemovedButStatusStillSet pins
// the retry path for the operator-removed-the-selector + clear-patch-failed
// scenario. Without a scheduled requeue the stale printer-column value would
// persist forever (no Owned watch covers Pods, and the selector is gone, so
// no event ever drives the count to a new value). The fix ties the requeue
// schedule to `status.matchedEnginePods != nil`, not just the spec selector.
//
// Simulates the clear-patch-failure by intercepting the matchedEnginePods
// status patch (discriminated by the merge-patch payload) so the in-memory
// rollback restores the seeded value — exactly the state Reconcile would
// see at return-time if the apiserver rejected the clear patch.
func TestReconcileSchedulesRequeueWhenSelectorRemovedButStatusStillSet(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add cache scheme: %v", err)
	}

	cb := lmcacheBackend("cache", "ns1") // no selector configured
	cb.Status.MatchedEnginePods = ptrInt32(3)

	patchErr := errors.New("synthetic apiserver patch failure")
	funcs := interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if sub == "status" {
				if _, ok := obj.(*cachev1alpha1.CacheBackend); ok {
					data, _ := patch.Data(obj)
					if strings.Contains(string(data), "matchedEnginePods") {
						return patchErr
					}
				}
			}
			return c.Status().Patch(ctx, obj, patch, opts...)
		},
	}
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheBackend{}, &appsv1.Deployment{}).
		WithObjects(cb).
		WithInterceptorFuncs(funcs).
		Build()
	r := &CacheBackendReconciler{Client: fc, Scheme: scheme, Log: logr.Discard()}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cache", Namespace: "ns1"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Fatalf("expected RequeueAfter > 0 to retry the stale-status clear; got %v", res.RequeueAfter)
	}
	if got := getBackend(t, r, "cache", "ns1").Status.MatchedEnginePods; got == nil {
		t.Fatalf("expected status.matchedEnginePods to remain non-nil after a failed clear (rollback contract); got nil")
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
	// count drifts. Other status patches (Endpoint, Conditions, ...) flow
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

	// First reconcile establishes status (Endpoint, Conditions, FailOpen) AND
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

// TestReconcileMatchedEnginePodsFailSoftOnStatusPatchError pins the second
// half of refreshMatchedEnginePods' fail-soft contract: when the namespaced
// pod List succeeds but the follow-up Status().Patch to write the new count
// errors out (transient apiserver hiccup, RBAC blip, status-subresource
// race), the reconciler must NOT surface the patch failure as a reconcile
// error AND must roll back the in-memory mutation so the rest of the
// reconcile sees only what the apiserver actually persisted. The companion
// list-error test covers the read side; this covers the write side.
func TestReconcileMatchedEnginePodsFailSoftOnStatusPatchError(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add cache scheme: %v", err)
	}

	cb := lmcacheBackendWithSelector("cache", "ns1", matchedSelector)
	// Seed an existing observed value so we can distinguish "patch failed +
	// rollback worked" (stays at 5) from "patch succeeded silently" (moves
	// to the new count) and from "patch failed and we leaked the in-memory
	// mutation" (moves but is not durable).
	cb.Status.MatchedEnginePods = ptrInt32(5)

	// Discriminate the matchedEnginePods patch from the dispatch
	// patchStatus by inspecting the merge-patch payload — the refresh
	// is the only writer whose patch carries a "matchedEnginePods"
	// key (dispatch writes Endpoint/Conditions/FailOpen/ObservedGeneration
	// but never touches the count sub-field).
	patchErr := errors.New("synthetic apiserver patch failure")
	var patchCalls int32
	funcs := interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if sub == "status" {
				if _, ok := obj.(*cachev1alpha1.CacheBackend); ok {
					data, _ := patch.Data(obj)
					if strings.Contains(string(data), "matchedEnginePods") {
						atomic.AddInt32(&patchCalls, 1)
						return patchErr
					}
				}
			}
			return c.Status().Patch(ctx, obj, patch, opts...)
		},
	}
	objs := []client.Object{
		cb,
		engineLikePod("e1", "ns1", matchedSelector),
		engineLikePod("e2", "ns1", matchedSelector),
		engineLikePod("e3", "ns1", matchedSelector),
	}
	fc := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheBackend{}, &appsv1.Deployment{}).
		WithObjects(objs...).
		WithInterceptorFuncs(funcs).
		Build()
	r := &CacheBackendReconciler{Client: fc, Scheme: scheme, Log: logr.Discard()}

	// reconcile() t.Fatal's on any reconcile error → if we get here, the
	// reconciler swallowed the patch error as designed.
	reconcile(t, r, "cache", "ns1")

	if got := atomic.LoadInt32(&patchCalls); got == 0 {
		t.Fatalf("expected the matchedEnginePods status patch to fire at least once; got 0")
	}

	// In-memory rollback: the persisted CR still reports the prior
	// observed 5, NOT the un-persisted new count of 3.
	got := getBackend(t, r, "cache", "ns1").Status.MatchedEnginePods
	if got == nil {
		t.Fatalf("status.matchedEnginePods = nil after patch error; want rolled-back prior value 5")
	}
	if *got != 5 {
		t.Fatalf("status.matchedEnginePods = %d after patch error; want rolled-back prior value 5", *got)
	}
}

// TestReconcileMatchedEnginePodsCoexistsWithOtherStatusWriters confirms the
// matchedEnginePods writer does not stomp on the other status writers in the
// same Reconcile (Endpoint, Conditions, FailOpen, ObservedGeneration) — both run
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

// TestReconcileMatchedEnginePodsUsesAPIReaderForPods pins the structural
// invariant the locked design called out: the pod List backing
// matchedEnginePods MUST go through the manager's APIReader (uncached live
// client), not the manager's cached Client. Using Client would make
// controller-runtime register a cluster-wide Pod informer just to maintain
// this snapshot count — exactly what the "no Pod watch" rule rejects.
//
// The test plumbs Client and APIReader to two DIFFERENT fake clients (the
// Client carries the CB but ZERO pods; the APIReader carries ZERO CBs but
// the matching pods). A pass means matchedEnginePods reflects the
// APIReader's pod count. Any future refactor that flips the pod List back
// to r.Client would fail here.
func TestReconcileMatchedEnginePodsUsesAPIReaderForPods(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackendWithSelector("cache", "ns1", matchedSelector)
	// Cached client: has the CB, but no pods. If refresh used this
	// client, matchedEnginePods would be 0.
	cached := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheBackend{}, &appsv1.Deployment{}).
		WithObjects(cb).
		Build()
	// APIReader: has only the matching pods, no CB. If refresh uses
	// THIS reader, matchedEnginePods is the pod count.
	apireader := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			engineLikePod("e1", "ns1", matchedSelector),
			engineLikePod("e2", "ns1", matchedSelector),
		).
		Build()
	r := &CacheBackendReconciler{
		Client:    cached,
		Scheme:    scheme,
		Log:       logr.Discard(),
		APIReader: apireader,
	}

	reconcile(t, r, "cache", "ns1")

	got := getBackend(t, r, "cache", "ns1").Status.MatchedEnginePods
	if got == nil {
		t.Fatalf("status.matchedEnginePods = nil; expected APIReader to be consulted (count from APIReader's 2 pods)")
	}
	if *got != 2 {
		t.Fatalf("status.matchedEnginePods = %d, want 2 (APIReader's pods, not Client's)", *got)
	}
}

func TestReconcileMatchedEnginePodsUsesChurnCadenceWhenDeploymentDesiredDisagrees(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackendWithSelector("cache", "ns1", matchedSelector)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "engine", Namespace: "ns1"},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptrInt32(2),
			Selector: &metav1.LabelSelector{MatchLabels: matchedSelector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: matchedSelector},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "engine", Image: "registry.example.com/vllm:test"}},
				},
			},
		},
	}
	r := newReconciler(scheme, cb, dep, engineLikePod("e1", "ns1", matchedSelector))
	r.MatchedEnginePodsRequeueInterval = 30 * time.Second
	r.MatchedEnginePodsChurnRequeueInterval = 5 * time.Second

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cache", Namespace: "ns1"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != 5*time.Second {
		t.Fatalf("RequeueAfter = %s, want 5s while desired replicas (2) != matched pods (1)", res.RequeueAfter)
	}

	if err := r.Create(context.Background(), engineLikePod("e2", "ns1", matchedSelector)); err != nil {
		t.Fatalf("create second matching pod: %v", err)
	}
	res, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cache", Namespace: "ns1"},
	})
	if err != nil {
		t.Fatalf("reconcile after convergence: %v", err)
	}
	if res.RequeueAfter != 30*time.Second {
		t.Fatalf("RequeueAfter = %s, want steady 30s once desired replicas match observed pods", res.RequeueAfter)
	}
}

func TestReconcileMatchedEnginePodsUsesSteadyCadenceWithoutMatchingDeployment(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackendWithSelector("cache", "ns1", matchedSelector)
	r := newReconciler(scheme, cb,
		engineLikePod("e1", "ns1", matchedSelector),
		engineLikePod("e2", "ns1", matchedSelector),
	)
	r.MatchedEnginePodsRequeueInterval = 30 * time.Second
	r.MatchedEnginePodsChurnRequeueInterval = 5 * time.Second

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cache", Namespace: "ns1"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != 30*time.Second {
		t.Fatalf("RequeueAfter = %s, want steady 30s for matching pods without a matching Deployment", res.RequeueAfter)
	}
}

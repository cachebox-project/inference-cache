// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
)

// newReconcilerWithRecorder builds a reconciler wired with a buffered fake
// recorder. The buffer is intentionally generous so a test never blocks on
// emit; assertions read from rec.Events with a select+default to avoid hanging
// when an expected event is missing.
func newReconcilerWithRecorder(t *testing.T, objs ...client.Object) (*CacheBackendReconciler, *events.FakeRecorder) {
	return newReconcilerWithRecorderOptions(t, false, objs...)
}

func newReconcilerWithRecorderOptions(t *testing.T, addReadyEngine bool, objs ...client.Object) (*CacheBackendReconciler, *events.FakeRecorder) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add cache scheme: %v", err)
	}
	if addReadyEngine {
		for _, obj := range objs {
			cb, ok := obj.(*cachev1alpha1.CacheBackend)
			if !ok || !isTypedLMCachePodLocal(cb) {
				continue
			}
			objs = append(objs, readyEnginePodForBackend(cb))
		}
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheBackend{}, &appsv1.Deployment{}).
		WithObjects(objs...).
		Build()
	rec := events.NewFakeRecorder(16)
	r := &CacheBackendReconciler{Client: c, Scheme: scheme, Log: logr.Discard(), Recorder: rec}
	configureTestRegistries(r)
	return r, rec
}

func readyEnginePodForBackend(cb *cachev1alpha1.CacheBackend) *corev1.Pod {
	if cb.UID == "" {
		cb.UID = types.UID(fmt.Sprintf("uid-%s", cb.Name))
	}
	if cb.Spec.EngineSelector == nil || len(cb.Spec.EngineSelector.MatchLabels) == 0 {
		cb.Spec.EngineSelector = &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: map[string]string{"app": cb.Name + "-engine"}}
	}
	labels := make(map[string]string, len(cb.Spec.EngineSelector.MatchLabels))
	for key, value := range cb.Spec.EngineSelector.MatchLabels {
		labels[key] = value
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: cb.Name + "-engine", Namespace: cb.Namespace, Labels: labels,
			Annotations: map[string]string{
				enginebinding.AnnotationInjectedBy:         cb.Namespace + "/" + cb.Name,
				enginebinding.AnnotationInjectedByUID:      string(cb.UID),
				enginebinding.AnnotationInjectedGeneration: strconv.FormatInt(cb.Generation, 10),
			},
		},
		Status: corev1.PodStatus{
			InitContainerStatuses: []corev1.ContainerStatus{{Name: lmCacheMPServerStatusContainerName, Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}},
			Conditions:            []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func requireRemoteStorageForReadiness(cb *cachev1alpha1.CacheBackend) {
	failOpen := false
	if cb.Spec.Integration == nil {
		cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	}
	cb.Spec.Integration.FailOpen = &failOpen
}

// drainEvents pulls every event currently on the recorder channel. The channel
// is non-blocking; absence of an expected event is detected by length, not by
// blocking, so tests fail fast instead of timing out.
func drainEvents(rec *events.FakeRecorder) []string {
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

// expectEvent fails the test if no recorded event contains the substring.
func expectEvent(t *testing.T, events []string, substr string) {
	t.Helper()
	for _, e := range events {
		if strings.Contains(e, substr) {
			return
		}
	}
	t.Fatalf("expected event containing %q; got %v", substr, events)
}

// expectNoEvent fails the test if any recorded event contains the substring.
func expectNoEvent(t *testing.T, events []string, substr string) {
	t.Helper()
	for _, e := range events {
		if strings.Contains(e, substr) {
			t.Fatalf("did not expect event containing %q; got %v", substr, events)
		}
	}
}

// markDeploymentReady mutates the child Deployment's status so managedReadiness
// observes it as Ready (rolled out + all replicas available).
func markDeploymentReady(t *testing.T, r *CacheBackendReconciler, name, namespace string, want int32) {
	t.Helper()
	dep := getDeployment(t, r, name, namespace)
	dep.Status.ObservedGeneration = dep.Generation
	dep.Status.Replicas = want
	dep.Status.UpdatedReplicas = want
	dep.Status.AvailableReplicas = want
	dep.Status.ReadyReplicas = want
	if err := r.Status().Update(context.Background(), dep); err != nil {
		t.Fatalf("update deployment status to ready: %v", err)
	}
}

// markDeploymentDegraded mutates the child Deployment's status into the
// post-rollout "replicas unavailable" state managedReadiness reports as
// Ready=False/ReplicasUnavailable.
func markDeploymentDegraded(t *testing.T, r *CacheBackendReconciler, name, namespace string, want int32) {
	t.Helper()
	dep := getDeployment(t, r, name, namespace)
	dep.Status.ObservedGeneration = dep.Generation
	dep.Status.Replicas = want
	dep.Status.UpdatedReplicas = want
	dep.Status.AvailableReplicas = 0
	dep.Status.ReadyReplicas = 0
	if err := r.Status().Update(context.Background(), dep); err != nil {
		t.Fatalf("update deployment status to degraded: %v", err)
	}
}

// reconcileN runs Reconcile n times for the given backend (used to confirm
// steady-state reconciles do not flood the event stream).
func reconcileN(t *testing.T, r *CacheBackendReconciler, name, namespace string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Name: name, Namespace: namespace},
		}); err != nil {
			t.Fatalf("reconcile %s/%s [iter %d]: %v", namespace, name, i, err)
		}
	}
}

func TestReconcileEmitsBackendDegradedOnTransition(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	requireRemoteStorageForReadiness(cb)
	r, rec := newReconcilerWithRecorderOptions(t, true, cb)

	// Cold start: Pending → no event yet.
	reconcile(t, r, "cache", "ns1")
	expectNoEvent(t, drainEvents(rec), eventReasonBackendDegraded)

	// Drive to Ready: no event for Pending → Ready (only Degraded entry/exit
	// is loud enough to deserve an event by design).
	markDeploymentReady(t, r, "cache", "ns1", 1)
	reconcile(t, r, "cache", "ns1")
	if !isReady(getBackend(t, r, "cache", "ns1")) {
		t.Fatalf("Ready condition not True before degrading")
	}
	expectNoEvent(t, drainEvents(rec), eventReasonBackendDegraded)

	// Backend dies under load: AvailableReplicas drops to 0 with the rollout
	// already observed → managedReadiness reports Ready=False/ReplicasUnavailable.
	markDeploymentDegraded(t, r, "cache", "ns1", 1)
	reconcile(t, r, "cache", "ns1")

	updated := getBackend(t, r, "cache", "ns1")
	cond := findCondition(updated.Status.Conditions, conditionTypeReady)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != reasonRemoteStorageUnavailable {
		t.Fatalf("Ready condition = %+v, want False/RemoteStorageUnavailable", cond)
	}
	events := drainEvents(rec)
	expectEvent(t, events, "Warning "+reasonRemoteStorageUnavailable)
	expectEvent(t, events, "0/1 replicas available")
}

// TestReconcileEmitsTransitionEventEvenWhenApplyErrors guards the
// event-emission ↔ status-decoupling interaction: status can now be patched
// from the live Deployment even when reconcile returns an apply error, so
// emitting events only on the happy path would lose any Degraded transition
// observed during an apply-error reconcile (the next reconcile's snapshot is
// taken from the already-updated status). Events must therefore be emitted on
// every observed transition regardless of dispatch error.
func TestReconcileEmitsTransitionEventEvenWhenApplyErrors(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add cache scheme: %v", err)
	}

	cb := lmcacheBackend("cache", "ns1")
	requireRemoteStorageForReadiness(cb)
	enginePod := readyEnginePodForBackend(cb)

	// Block Deployment Updates after the first reconcile so the second pass
	// returns an apply error while the live Deployment status drives a
	// Ready → Degraded transition.
	var blockUpdate atomic.Bool
	funcs := interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*appsv1.Deployment); ok && blockUpdate.Load() {
				return apierrors.NewForbidden(
					schema.GroupResource{Group: "apps", Resource: "deployments"},
					obj.GetName(),
					errors.New("denied by admission webhook"),
				)
			}
			return c.Update(ctx, obj, opts...)
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheBackend{}, &appsv1.Deployment{}).
		WithObjects(cb, enginePod).
		WithInterceptorFuncs(funcs).
		Build()
	rec := events.NewFakeRecorder(16)
	r := &CacheBackendReconciler{Client: c, Scheme: scheme, Log: logr.Discard(), Recorder: rec}
	configureTestRegistries(r)

	// First pass establishes the Deployment + drives Ready (no events; only
	// Degraded transitions are loud by design).
	reconcile(t, r, "cache", "ns1")
	markDeploymentReady(t, r, "cache", "ns1", 1)
	reconcile(t, r, "cache", "ns1")
	_ = drainEvents(rec)

	// Now drive a Ready=True → Ready=False/ReplicasUnavailable transition
	// while apply errors. Mutating the CR forces applyDeployment to issue
	// an Update; degrading the live Deployment status (AvailableReplicas=0)
	// drives the readiness transition.
	blockUpdate.Store(true)
	live := getBackend(t, r, "cache", "ns1")
	live.Spec.RemoteStorage.Redis.Image = "example.com/redis:v9"
	live.Generation = 2
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("update CR: %v", err)
	}
	enginePod.Annotations[enginebinding.AnnotationInjectedGeneration] = "2"
	if err := r.Update(context.Background(), enginePod); err != nil {
		t.Fatalf("update engine Pod: %v", err)
	}
	r.refreshLMCacheMPConnectorStatus(context.Background(), getBackend(t, r, "cache", "ns1"))
	markDeploymentDegraded(t, r, "cache", "ns1", 1)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cache", Namespace: "ns1"},
	}); err == nil {
		t.Fatalf("reconcile returned nil, want error (apply was blocked)")
	}

	if ready := findCondition(getBackend(t, r, "cache", "ns1").Status.Conditions, conditionTypeReady); ready == nil || ready.Reason != reasonRemoteStorageUnavailable {
		t.Fatalf("Ready condition = %+v, want RemoteStorageUnavailable (status path runs independently of apply error)", ready)
	}
	events := drainEvents(rec)
	expectEvent(t, events, "Warning "+reasonRemoteStorageUnavailable)
}

// TestReconcileNoPhantomEventOnStatusPatchFailure pins the rollback semantics
// of patchStatus: if the status sub-resource Patch fails, the in-memory
// mutation must be rolled back so emitTransitionEvents does not see a
// transition the apiserver never observed. Otherwise an apiserver hiccup
// would surface as a Warning/Normal event for a transition that didn't
// persist, and would fire again on the next reconcile that retries the patch
// — duplicate events that look real but reflect nothing in cluster state.
func TestReconcileNoPhantomEventOnStatusPatchFailure(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add cache scheme: %v", err)
	}

	cb := lmcacheBackend("cache", "ns1")
	requireRemoteStorageForReadiness(cb)
	enginePod := readyEnginePodForBackend(cb)

	var blockStatusPatch atomic.Bool
	funcs := interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if sub == "status" && blockStatusPatch.Load() {
				if _, ok := obj.(*cachev1alpha1.CacheBackend); ok {
					return apierrors.NewInternalError(errors.New("simulated apiserver hiccup on status patch"))
				}
			}
			return c.Status().Patch(ctx, obj, patch, opts...)
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheBackend{}, &appsv1.Deployment{}).
		WithObjects(cb, enginePod).
		WithInterceptorFuncs(funcs).
		Build()
	rec := events.NewFakeRecorder(16)
	r := &CacheBackendReconciler{Client: c, Scheme: scheme, Log: logr.Discard(), Recorder: rec}
	configureTestRegistries(r)

	// Drive to Ready first. Pending → Ready emits no event by design (only
	// Degraded entry/exit are loud).
	reconcile(t, r, "cache", "ns1")
	markDeploymentReady(t, r, "cache", "ns1", 1)
	reconcile(t, r, "cache", "ns1")
	_ = drainEvents(rec)

	// Block the status patch, then degrade the live Deployment. Reconcile
	// computes Degraded internally, fails to patch status, returns the
	// error — and must emit NO event, because the apiserver never saw the
	// transition.
	blockStatusPatch.Store(true)
	markDeploymentDegraded(t, r, "cache", "ns1", 1)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cache", Namespace: "ns1"},
	}); err == nil {
		t.Fatalf("reconcile returned nil, want error (status patch was blocked)")
	}
	if got := drainEvents(rec); len(got) != 0 {
		t.Fatalf("emitted events for un-persisted transition: %v (want none)", got)
	}
	// The live CR status must still report the pre-transition Ready, since
	// the patch was rejected.
	if !isReady(getBackend(t, r, "cache", "ns1")) {
		t.Fatalf("Ready condition not True (the failed patch must not be visible)")
	}

	// Unblock and reconcile again. The same transition now lands cleanly and
	// fires the BackendDegraded warning exactly once (no carry-over from
	// the blocked pass).
	blockStatusPatch.Store(false)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "cache", Namespace: "ns1"},
	}); err != nil {
		t.Fatalf("reconcile after unblock: %v", err)
	}
	if ready := findCondition(getBackend(t, r, "cache", "ns1").Status.Conditions, conditionTypeReady); ready == nil || ready.Reason != reasonRemoteStorageUnavailable {
		t.Fatalf("Ready condition = %+v, want RemoteStorageUnavailable after unblock", ready)
	}
	got := drainEvents(rec)
	expectEvent(t, got, "Warning "+reasonRemoteStorageUnavailable)
	count := 0
	for _, e := range got {
		if strings.Contains(e, reasonRemoteStorageUnavailable) {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("RemoteStorageUnavailable event count = %d, want exactly 1: %v", count, got)
	}
}

func TestReconcileEmitsBackendRecoveredOnReadyTransition(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	requireRemoteStorageForReadiness(cb)
	r, rec := newReconcilerWithRecorderOptions(t, true, cb)

	reconcile(t, r, "cache", "ns1")
	markDeploymentReady(t, r, "cache", "ns1", 1)
	reconcile(t, r, "cache", "ns1")

	// Backend dies (Degraded), then comes back (Ready) — exercise the full
	// chaos→recovery path the ticket calls out in the test plan.
	markDeploymentDegraded(t, r, "cache", "ns1", 1)
	reconcile(t, r, "cache", "ns1")
	_ = drainEvents(rec) // discard the Degraded warning emitted above

	markDeploymentReady(t, r, "cache", "ns1", 1)
	reconcile(t, r, "cache", "ns1")

	if !isReady(getBackend(t, r, "cache", "ns1")) {
		t.Fatalf("Ready condition not True after recovery")
	}
	events := drainEvents(rec)
	expectEvent(t, events, "Normal "+reasonRemoteStorageReady)
	// No spurious second warning during recovery.
	expectNoEvent(t, events, eventReasonBackendDegraded)
}

func TestReconcileSteadyStateDoesNotFloodEvents(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	requireRemoteStorageForReadiness(cb)
	r, rec := newReconcilerWithRecorderOptions(t, true, cb)

	reconcile(t, r, "cache", "ns1")
	markDeploymentReady(t, r, "cache", "ns1", 1)
	reconcile(t, r, "cache", "ns1")
	_ = drainEvents(rec)

	// Five steady-state reconciles after Ready is established must not emit
	// any events — Ready→Ready is the no-op transition that mattered most for
	// the "no event spam" gate in the ticket.
	reconcileN(t, r, "cache", "ns1", 5)

	if events := drainEvents(rec); len(events) != 0 {
		t.Fatalf("expected no events on steady-state reconcile, got %v", events)
	}
}

func TestReconcileEmitsFailClosedWarningOnApply(t *testing.T) {
	failOpen := false
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{FailOpen: &failOpen}
	r, rec := newReconcilerWithRecorderOptions(t, true, cb)

	// First reconcile: previous status.failOpen is nil (effective true), spec
	// is false → transition fires the FailClosedEnabled Warning. The status
	// echoes the observed value so a steady-state reconcile is a no-op.
	reconcile(t, r, "cache", "ns1")

	updated := getBackend(t, r, "cache", "ns1")
	if updated.Status.FailOpen == nil || *updated.Status.FailOpen {
		t.Fatalf("status.failOpen = %v, want explicit false echo", updated.Status.FailOpen)
	}
	events := drainEvents(rec)
	expectEvent(t, events, "Warning "+eventReasonFailClosedEnabled)
	expectEvent(t, events, "serving dependency")

	// Idempotency: a second reconcile must not re-fire the warning, even
	// though the spec is still fail-closed — the transition is the trigger,
	// not the value.
	reconcile(t, r, "cache", "ns1")
	if events := drainEvents(rec); len(events) != 0 {
		t.Fatalf("expected no additional events on steady-state fail-closed reconcile, got %v", events)
	}
}

func TestReconcileEmitsFailOpenRestoredWhenFlippedBack(t *testing.T) {
	failOpen := false
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{FailOpen: &failOpen}
	r, rec := newReconcilerWithRecorderOptions(t, true, cb)

	reconcile(t, r, "cache", "ns1")
	_ = drainEvents(rec) // discard the FailClosedEnabled warning emitted above

	live := getBackend(t, r, "cache", "ns1")
	trueV := true
	live.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{FailOpen: &trueV}
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("flip failOpen back to true: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	updated := getBackend(t, r, "cache", "ns1")
	if updated.Status.FailOpen == nil || !*updated.Status.FailOpen {
		t.Fatalf("status.failOpen = %v, want explicit true echo", updated.Status.FailOpen)
	}
	events := drainEvents(rec)
	expectEvent(t, events, "Normal "+eventReasonFailOpenRestored)
	expectNoEvent(t, events, eventReasonFailClosedEnabled)
}

func TestReconcileDefaultFailOpenIsSilent(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	r, rec := newReconcilerWithRecorder(t, cb)

	reconcile(t, r, "cache", "ns1")

	updated := getBackend(t, r, "cache", "ns1")
	if updated.Status.FailOpen == nil || !*updated.Status.FailOpen {
		t.Fatalf("status.failOpen = %v, want default-true echo when integration spec is absent", updated.Status.FailOpen)
	}
	// No event when the spec leaves failOpen at the default true.
	events := drainEvents(rec)
	expectNoEvent(t, events, eventReasonFailClosedEnabled)
	expectNoEvent(t, events, eventReasonFailOpenRestored)
}

func TestReconcileNilRecorderIsSafe(t *testing.T) {
	// SetupWithManager guarantees the Recorder is wired, but defense in depth:
	// a nil Recorder must never panic, since the reconciler is constructed
	// directly in tests and may be in tests that don't care about events.
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	r := newReconciler(scheme, cb) // no Recorder
	reconcile(t, r, "cache", "ns1")
	markDeploymentDegraded(t, r, "cache", "ns1", 1)
	reconcile(t, r, "cache", "ns1")
}

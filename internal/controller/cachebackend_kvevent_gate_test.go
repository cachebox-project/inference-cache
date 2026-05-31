package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// gatedLMCacheBackend is a managed LMCache backend WITH the KV-event readiness
// gate enabled (no opt-out annotation), used by the KV-event gate tests. The
// shared lmcacheBackend fixture opts out, so gate tests use this instead.
func gatedLMCacheBackend(name, ns string) *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Generation: 1},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:     cachev1alpha1.CacheBackendTypeLMCache,
			Replicas: ptrInt32(1),
		},
	}
}

// setDeploymentHTTPReady drives a managed backend's Deployment to the
// engine-HTTP-Ready state managedHealth keys on (rolled out + all replicas
// available) and stamps the Available condition LastTransitionTime — the gate's
// firstEventTimeout anchor — to availableSince.
func setDeploymentHTTPReady(t *testing.T, cl client.Client, name, ns string, availableSince time.Time) {
	t.Helper()
	var dep appsv1.Deployment
	if err := cl.Get(context.Background(), types.NamespacedName{Name: name, Namespace: ns}, &dep); err != nil {
		t.Fatalf("get deployment %s/%s: %v", ns, name, err)
	}
	want := int32(1)
	if dep.Spec.Replicas != nil {
		want = *dep.Spec.Replicas
	}
	dep.Status.ObservedGeneration = dep.Generation
	dep.Status.Replicas = want
	dep.Status.UpdatedReplicas = want
	dep.Status.AvailableReplicas = want
	dep.Status.ReadyReplicas = want
	dep.Status.Conditions = []appsv1.DeploymentCondition{{
		Type:               appsv1.DeploymentAvailable,
		Status:             corev1.ConditionTrue,
		Reason:             "MinimumReplicasAvailable",
		LastTransitionTime: metav1.NewTime(availableSince),
	}}
	if err := cl.Status().Update(context.Background(), &dep); err != nil {
		t.Fatalf("update deployment status %s/%s: %v", ns, name, err)
	}
}

// patchLastEventAt simulates the CacheIndex poller writing a fresh KV-event
// timestamp into status.indexParticipation via the status subresource — the
// exact path the poller uses, and the signal the gate reads.
func patchLastEventAt(t *testing.T, cl client.Client, name, ns string, at time.Time) {
	t.Helper()
	var cb cachev1alpha1.CacheBackend
	if err := cl.Get(context.Background(), types.NamespacedName{Name: name, Namespace: ns}, &cb); err != nil {
		t.Fatalf("get CacheBackend %s/%s: %v", ns, name, err)
	}
	before := cb.DeepCopy()
	tm := metav1.NewTime(at)
	cb.Status.IndexParticipation = &cachev1alpha1.CacheBackendIndexParticipation{PrefixCount: 1, LastEventAt: &tm}
	if err := cl.Status().Patch(context.Background(), &cb, client.MergeFrom(before)); err != nil {
		t.Fatalf("patch indexParticipation %s/%s: %v", ns, name, err)
	}
}

// TestIntegrationKVEventReadinessGate exercises the KV-event gate state machine
// via the single-shot reconcile helper against a real apiserver.
func TestIntegrationKVEventReadinessGate(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, _ := startEnv(t)
	r := &CacheBackendReconciler{Client: k8s, Scheme: scheme, Log: logr.Discard()}
	ctx := context.Background()

	t.Run("HTTPReadyNoEventIsPendingAwaitingFirstKVEvent", func(t *testing.T) {
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, gatedLMCacheBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		setDeploymentHTTPReady(t, k8s, "cache", ns, time.Now())
		reconcile(t, r, "cache", ns)

		cb := getBackend(t, r, "cache", ns)
		if cb.Status.Health != cachev1alpha1.CacheBackendHealthPending {
			t.Fatalf("health = %q, want Pending", cb.Status.Health)
		}
		ready := findCondition(cb.Status.Conditions, conditionTypeReady)
		if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != reasonAwaitingFirstKVEvent {
			t.Fatalf("Ready = %+v, want False/AwaitingFirstKVEvent", ready)
		}
		if deg := findCondition(cb.Status.Conditions, conditionTypeDegraded); deg == nil || deg.Status != metav1.ConditionFalse {
			t.Fatalf("Degraded = %+v, want False", deg)
		}
	})

	t.Run("EventSeenIsReadyKVEventsObserved", func(t *testing.T) {
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, gatedLMCacheBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		setDeploymentHTTPReady(t, k8s, "cache", ns, time.Now())
		patchLastEventAt(t, k8s, "cache", ns, time.Now())
		reconcile(t, r, "cache", ns)

		cb := getBackend(t, r, "cache", ns)
		if cb.Status.Health != cachev1alpha1.CacheBackendHealthReady {
			t.Fatalf("health = %q, want Ready", cb.Status.Health)
		}
		ready := findCondition(cb.Status.Conditions, conditionTypeReady)
		if ready == nil || ready.Status != metav1.ConditionTrue || ready.Reason != reasonKVEventsObserved {
			t.Fatalf("Ready = %+v, want True/KVEventsObserved", ready)
		}
		if deg := findCondition(cb.Status.Conditions, conditionTypeDegraded); deg == nil || deg.Status != metav1.ConditionFalse {
			t.Fatalf("Degraded = %+v, want False", deg)
		}
	})

	t.Run("StaysReadyAfterPollerClearsLastEventAtOnDrain", func(t *testing.T) {
		// Regression: the poller's lastEventAt is a current-view projection it
		// clears on a replica drain. A backend that already passed the gate
		// must NOT regress to AwaitingFirstKVEvent — the durable
		// firstKVEventObservedAt latch keeps it Ready.
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, gatedLMCacheBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		setDeploymentHTTPReady(t, k8s, "cache", ns, time.Now())
		patchLastEventAt(t, k8s, "cache", ns, time.Now())
		reconcile(t, r, "cache", ns)

		cb := getBackend(t, r, "cache", ns)
		if cb.Status.FirstKVEventObservedAt == nil {
			t.Fatalf("firstKVEventObservedAt = nil, want latched after first event")
		}
		if cb.Status.Health != cachev1alpha1.CacheBackendHealthReady {
			t.Fatalf("pre-drain health = %q, want Ready", cb.Status.Health)
		}

		// Simulate the poller draining the projection: prefixCount 0, lastEventAt nil.
		before := cb.DeepCopy()
		cb.Status.IndexParticipation = &cachev1alpha1.CacheBackendIndexParticipation{PrefixCount: 0}
		if err := k8s.Status().Patch(ctx, cb, client.MergeFrom(before)); err != nil {
			t.Fatalf("patch drained participation: %v", err)
		}
		reconcile(t, r, "cache", ns)

		got := getBackend(t, r, "cache", ns)
		if got.Status.Health != cachev1alpha1.CacheBackendHealthReady {
			t.Fatalf("post-drain health = %q, want Ready (latched)", got.Status.Health)
		}
		ready := findCondition(got.Status.Conditions, conditionTypeReady)
		if ready == nil || ready.Status != metav1.ConditionTrue || ready.Reason != reasonKVEventsObserved {
			t.Fatalf("post-drain Ready = %+v, want True/KVEventsObserved", ready)
		}
	})

	t.Run("TimeoutBreachedIsDegradedNoKVEventsObserved", func(t *testing.T) {
		ns := freshNS(t, k8s)
		cb := gatedLMCacheBackend("cache", ns)
		cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
			FirstEventTimeout: &metav1.Duration{Duration: time.Second},
		}
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		// Engine became HTTP-Ready well before the 1s timeout window.
		setDeploymentHTTPReady(t, k8s, "cache", ns, time.Now().Add(-5*time.Second))
		reconcile(t, r, "cache", ns)

		got := getBackend(t, r, "cache", ns)
		if got.Status.Health != cachev1alpha1.CacheBackendHealthDegraded {
			t.Fatalf("health = %q, want Degraded", got.Status.Health)
		}
		ready := findCondition(got.Status.Conditions, conditionTypeReady)
		if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != reasonNoKVEventsObserved {
			t.Fatalf("Ready = %+v, want False/NoKVEventsObserved", ready)
		}
		deg := findCondition(got.Status.Conditions, conditionTypeDegraded)
		if deg == nil || deg.Status != metav1.ConditionTrue || deg.Reason != reasonNoKVEventsObserved {
			t.Fatalf("Degraded = %+v, want True/NoKVEventsObserved", deg)
		}
	})

	t.Run("AnnotationOptOutGoesReadyWithoutEvent", func(t *testing.T) {
		ns := freshNS(t, k8s)
		cb := gatedLMCacheBackend("cache", ns)
		cb.Annotations = map[string]string{annotationRequireKVEvents: "false"}
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		setDeploymentHTTPReady(t, k8s, "cache", ns, time.Now())
		reconcile(t, r, "cache", ns)

		got := getBackend(t, r, "cache", ns)
		if got.Status.Health != cachev1alpha1.CacheBackendHealthReady {
			t.Fatalf("opted-out health = %q, want Ready without any KV event", got.Status.Health)
		}
		ready := findCondition(got.Status.Conditions, conditionTypeReady)
		if ready == nil || ready.Status != metav1.ConditionTrue || ready.Reason == reasonKVEventsObserved {
			t.Fatalf("Ready = %+v, want True via the Deployment-rollout reason (not the gate)", ready)
		}
	})

	t.Run("RaceEventBeforeFirstEvaluationGoesReadyDirectly", func(t *testing.T) {
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, gatedLMCacheBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		// lastEventAt is already populated by the time the gate first sees the
		// backend HTTP-Ready: must go straight to Ready, never through
		// AwaitingFirstKVEvent.
		setDeploymentHTTPReady(t, k8s, "cache", ns, time.Now())
		patchLastEventAt(t, k8s, "cache", ns, time.Now())
		reconcile(t, r, "cache", ns)

		got := getBackend(t, r, "cache", ns)
		ready := findCondition(got.Status.Conditions, conditionTypeReady)
		if ready == nil || ready.Status != metav1.ConditionTrue || ready.Reason != reasonKVEventsObserved {
			t.Fatalf("Ready = %+v, want True/KVEventsObserved with no Pending transition", ready)
		}
	})

	t.Run("ExternalBypassesGateEntirely", func(t *testing.T) {
		ns := freshNS(t, k8s)
		cb := &cachev1alpha1.CacheBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "ext", Namespace: ns},
			Spec: cachev1alpha1.CacheBackendSpec{
				Type:     cachev1alpha1.CacheBackendTypeExternal,
				Endpoint: "external.example.svc:6379",
			},
		}
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "ext", ns)

		got := getBackend(t, r, "ext", ns)
		if got.Status.Endpoint != "external.example.svc:6379" {
			t.Fatalf("endpoint = %q, want mirrored external endpoint", got.Status.Endpoint)
		}
		// External never enters the KV-event gate: readiness comes from
		// admission accepting the endpoint (reason ExternalEndpointAccepted),
		// not from a KV event. The gate's reasons and latch must never appear.
		ready := findCondition(got.Status.Conditions, conditionTypeReady)
		if ready == nil || ready.Status != metav1.ConditionTrue || ready.Reason != "ExternalEndpointAccepted" {
			t.Fatalf("Ready = %+v, want True/ExternalEndpointAccepted (endpoint-driven, not gated)", ready)
		}
		if got.Status.Health != cachev1alpha1.CacheBackendHealthReady {
			t.Fatalf("health = %q, want Ready (endpoint accepted)", got.Status.Health)
		}
		if deg := findCondition(got.Status.Conditions, conditionTypeDegraded); deg != nil {
			t.Fatalf("Degraded = %+v, want absent for External", deg)
		}
		if got.Status.FirstKVEventObservedAt != nil {
			t.Fatalf("firstKVEventObservedAt = %v, want nil (External never enters the gate)", got.Status.FirstKVEventObservedAt)
		}
	})

	t.Run("BackwardCompatDefaultsTimeoutTo5m", func(t *testing.T) {
		ns := freshNS(t, k8s)
		cb := gatedLMCacheBackend("cache", ns)
		// Provide integration but omit firstEventTimeout: the apiserver applies
		// the +kubebuilder:default of 5m.
		cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "vllm"}
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		got := getBackend(t, r, "cache", ns)
		ft := got.Spec.Integration.FirstEventTimeout
		if ft == nil || ft.Duration != 5*time.Minute {
			t.Fatalf("firstEventTimeout = %v, want defaulted 5m", ft)
		}
	})
}

// TestIntegrationKVEventGateAutoReconcileOnPollerWrite is the regression test
// for the watch-driven coupling: a CacheIndex-poller-style write to
// status.indexParticipation.lastEventAt must trigger a C2 reconcile via the
// For(&CacheBackend{}) informer — WITHOUT any explicit cross-controller enqueue
// and WITHOUT the test calling Reconcile directly. If a future predicate change
// filters status-only updates off that informer, this test fails immediately.
func TestIntegrationKVEventGateAutoReconcileOnPollerWrite(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, cfg := startEnv(t)

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:     scheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: ptrBool(true)},
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	if err := (&CacheBackendReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Log:    logr.Discard(),
	}).SetupWithManager(mgr); err != nil {
		t.Fatalf("setup with manager: %v", err)
	}

	mgrCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		if err := mgr.Start(mgrCtx); err != nil {
			t.Logf("manager stopped: %v", err)
		}
	}()
	if !mgr.GetCache().WaitForCacheSync(mgrCtx) {
		t.Fatalf("cache did not sync")
	}

	ns := freshNS(t, k8s)
	if err := k8s.Create(context.Background(), gatedLMCacheBackend("cache", ns)); err != nil {
		t.Fatalf("create CacheBackend: %v", err)
	}
	key := types.NamespacedName{Name: "cache", Namespace: ns}
	pollDeployment(t, k8s, key, "be created by the manager")

	// Engine becomes HTTP-Ready: the manager reconciles via the Owns(Deployment)
	// watch and the gate should park the backend at AwaitingFirstKVEvent.
	setDeploymentHTTPReady(t, k8s, "cache", ns, time.Now())
	waitForReadyReason(t, k8s, key, metav1.ConditionFalse, reasonAwaitingFirstKVEvent)

	// Now simulate the poller writing the first KV event. We do NOT call
	// Reconcile — the For(&CacheBackend{}) informer must drive it.
	patchLastEventAt(t, k8s, "cache", ns, time.Now())
	waitForReadyReason(t, k8s, key, metav1.ConditionTrue, reasonKVEventsObserved)
}

// TestKVEventGateEmitsTransitionEvents asserts the gate's operator-facing
// Events reach the recorder on each transition: AwaitingFirstKVEvent (Normal),
// KVEventsObserved (Normal), NoKVEventsObserved (Warning). Uses the fake-client
// recorder so it runs without envtest.
func TestKVEventGateEmitsTransitionEvents(t *testing.T) {
	cb := gatedLMCacheBackend("cache", "ns1")
	r, rec := newReconcilerWithRecorder(t, cb)

	// Cold start: deployment not ready → no gate event yet.
	reconcile(t, r, "cache", "ns1")
	drainEvents(rec)

	// HTTP-Ready, no event → AwaitingFirstKVEvent.
	markDeploymentReady(t, r, "cache", "ns1", 1)
	reconcile(t, r, "cache", "ns1")
	expectEvent(t, drainEvents(rec), reasonAwaitingFirstKVEvent)

	// First KV event observed → KVEventsObserved, backend Ready.
	patchLastEventAt(t, r.Client, "cache", "ns1", time.Now())
	reconcile(t, r, "cache", "ns1")
	expectEvent(t, drainEvents(rec), reasonKVEventsObserved)
	if got := getBackend(t, r, "cache", "ns1").Status.Health; got != cachev1alpha1.CacheBackendHealthReady {
		t.Fatalf("health = %q, want Ready", got)
	}
}

// TestKVEventGateEmitsNoKVEventsObservedOnTimeout asserts the Warning Event
// fires when the first-event window elapses with no event.
func TestKVEventGateEmitsNoKVEventsObservedOnTimeout(t *testing.T) {
	cb := gatedLMCacheBackend("cache", "ns1")
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
		FirstEventTimeout: &metav1.Duration{Duration: time.Second},
	}
	r, rec := newReconcilerWithRecorder(t, cb)
	reconcile(t, r, "cache", "ns1")

	// Ready + Available since well before the 1s window.
	dep := getDeployment(t, r, "cache", "ns1")
	dep.Status.ObservedGeneration = dep.Generation
	dep.Status.Replicas = 1
	dep.Status.UpdatedReplicas = 1
	dep.Status.AvailableReplicas = 1
	dep.Status.ReadyReplicas = 1
	dep.Status.Conditions = []appsv1.DeploymentCondition{{
		Type:               appsv1.DeploymentAvailable,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-5 * time.Second)),
	}}
	if err := r.Status().Update(context.Background(), dep); err != nil {
		t.Fatalf("update deployment status: %v", err)
	}
	drainEvents(rec)

	reconcile(t, r, "cache", "ns1")
	expectEvent(t, drainEvents(rec), reasonNoKVEventsObserved)
	if got := getBackend(t, r, "cache", "ns1").Status.Health; got != cachev1alpha1.CacheBackendHealthDegraded {
		t.Fatalf("health = %q, want Degraded", got)
	}
}

// TestKVEventGateDeploymentRecoveryIsBackendRecovered is the regression guard
// for the event-suppression fix: a backend that already saw KV events, then
// degrades for ReplicasUnavailable, then recovers must emit BackendRecovered —
// NOT a misleading second KVEventsObserved (events never stopped flowing).
func TestKVEventGateDeploymentRecoveryIsBackendRecovered(t *testing.T) {
	cb := gatedLMCacheBackend("cache", "ns1")
	r, rec := newReconcilerWithRecorder(t, cb)
	reconcile(t, r, "cache", "ns1")

	// Become Ready with events flowing.
	markDeploymentReady(t, r, "cache", "ns1", 1)
	patchLastEventAt(t, r.Client, "cache", "ns1", time.Now())
	reconcile(t, r, "cache", "ns1")
	if got := getBackend(t, r, "cache", "ns1").Status.Health; got != cachev1alpha1.CacheBackendHealthReady {
		t.Fatalf("setup health = %q, want Ready", got)
	}
	drainEvents(rec) // discard the initial KVEventsObserved

	// Lose replicas → Degraded (deployment cause, not KV).
	markDeploymentDegraded(t, r, "cache", "ns1", 1)
	reconcile(t, r, "cache", "ns1")
	expectEvent(t, drainEvents(rec), eventReasonBackendDegraded)

	// Replicas recover; lastEventAt is still set (events never lost).
	markDeploymentReady(t, r, "cache", "ns1", 1)
	reconcile(t, r, "cache", "ns1")
	evs := drainEvents(rec)
	expectEvent(t, evs, eventReasonBackendRecovered)
	expectNoEvent(t, evs, reasonKVEventsObserved)
}

// waitForReadyReason polls until the CacheBackend's Ready condition matches the
// wanted status+reason, or fails after a bounded deadline.
func waitForReadyReason(t *testing.T, cl client.Client, key types.NamespacedName, status metav1.ConditionStatus, reason string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last *metav1.Condition
	for time.Now().Before(deadline) {
		var cb cachev1alpha1.CacheBackend
		if err := cl.Get(context.Background(), key, &cb); err == nil {
			last = findCondition(cb.Status.Conditions, conditionTypeReady)
			if last != nil && last.Status == status && last.Reason == reason {
				return
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("Ready condition did not reach %s/%s within deadline; last = %+v", status, reason, last)
}

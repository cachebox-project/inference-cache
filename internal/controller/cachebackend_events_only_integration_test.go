package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// eventsOnlyBackend is an events-only (tier-1 routing) LMCache backend: it
// provisions no server workload and is gated on the first KV event exactly as a
// managed backend, but with no Deployment/Service/endpoint. The KV-event gate
// is left ON (no opt-out annotation) so the AwaitingFirstKVEvent →
// KVEventsObserved transition is observable.
func eventsOnlyBackend(name, ns string) *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Generation: 1},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type: cachev1alpha1.CacheBackendTypeLMCache,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Engine: "vllm",
				Mode:   cachev1alpha1.CacheBackendIntegrationModeEventsOnly,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app": "vllm"},
			},
			BackendConfig: map[string]string{"model": "Qwen/Qwen2.5-0.5B-Instruct"},
		},
	}
}

// countOwnedWorkloads returns the number of Deployments and Services in ns
// owned (controller ref) by the named CacheBackend. An events-only backend must
// own zero of each — it provisions no server workload.
func countOwnedWorkloads(t *testing.T, cl client.Client, cbName, ns string) (deployments, services int) {
	t.Helper()
	var deps appsv1.DeploymentList
	if err := cl.List(context.Background(), &deps, client.InNamespace(ns)); err != nil {
		t.Fatalf("list deployments in %s: %v", ns, err)
	}
	for i := range deps.Items {
		if owner := metav1.GetControllerOf(&deps.Items[i]); owner != nil && owner.Kind == "CacheBackend" && owner.Name == cbName {
			deployments++
		}
	}
	var svcs corev1.ServiceList
	if err := cl.List(context.Background(), &svcs, client.InNamespace(ns)); err != nil {
		t.Fatalf("list services in %s: %v", ns, err)
	}
	for i := range svcs.Items {
		if owner := metav1.GetControllerOf(&svcs.Items[i]); owner != nil && owner.Kind == "CacheBackend" && owner.Name == cbName {
			services++
		}
	}
	return deployments, services
}

// TestIntegrationEventsOnlyMode exercises the events-only reconcile path against
// a real apiserver: no provisioned workload, empty status.endpoint, the same
// KV-event readiness gate as a managed backend, and the managed-only advisory
// conditions (FunctionalProbeOK / EngineKernelsHealthy / T2Degraded /
// EngineCompatibility) absent. The transition subtests prove a backend that
// previously provisioned a Deployment+Service in Offload mode sheds them on the
// flip to EventsOnly (cleanupOwnedWorkload) and re-anchors its first-event
// window rather than inheriting the stale Offload availability time.
func TestIntegrationEventsOnlyMode(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, _ := startEnv(t)
	r := &CacheBackendReconciler{Client: k8s, Scheme: scheme, Log: logr.Discard()}
	ctx := context.Background()

	t.Run("NoWorkloadEmptyEndpointGatedOnFirstKVEvent", func(t *testing.T) {
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, eventsOnlyBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)

		// No owned Deployment, no owned Service — events-only provisions nothing.
		if deps, svcs := countOwnedWorkloads(t, k8s, "cache", ns); deps != 0 || svcs != 0 {
			t.Fatalf("events-only owned workloads = %d deployments / %d services, want 0/0", deps, svcs)
		}

		cb := getBackend(t, r, "cache", ns)
		if cb.Status.Endpoint != "" {
			t.Fatalf("status.endpoint = %q, want empty (no provisioned server)", cb.Status.Endpoint)
		}

		// Before any KV event, with the default 5m firstEventTimeout still
		// open, the gate parks the backend at Ready=False/AwaitingFirstKVEvent.
		ready := findCondition(cb.Status.Conditions, conditionTypeReady)
		if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != reasonAwaitingFirstKVEvent {
			t.Fatalf("pre-event Ready = %+v, want False/AwaitingFirstKVEvent", ready)
		}
		if deg := findCondition(cb.Status.Conditions, conditionTypeDegraded); deg == nil || deg.Status != metav1.ConditionFalse {
			t.Fatalf("pre-event Degraded = %+v, want False (timeout not yet breached)", deg)
		}

		// The managed-only advisory conditions must be ABSENT — there is no
		// server to functionally probe and no offload tier to mark degraded.
		if c := findCondition(cb.Status.Conditions, conditionTypeFunctionalProbeOK); c != nil {
			t.Fatalf("FunctionalProbeOK = %+v, want absent for events-only", c)
		}
		if c := findCondition(cb.Status.Conditions, conditionTypeT2Degraded); c != nil {
			t.Fatalf("T2Degraded = %+v, want absent for events-only", c)
		}

		// Inject the first KV event the same way the kvevent-gate test does:
		// the poller writes status.indexParticipation.lastEventAt. Reconcile →
		// Ready=True/KVEventsObserved.
		patchLastEventAt(t, k8s, "cache", ns, time.Now())
		reconcile(t, r, "cache", ns)

		got := getBackend(t, r, "cache", ns)
		ready = findCondition(got.Status.Conditions, conditionTypeReady)
		if ready == nil || ready.Status != metav1.ConditionTrue || ready.Reason != reasonKVEventsObserved {
			t.Fatalf("post-event Ready = %+v, want True/KVEventsObserved", ready)
		}
		if deg := findCondition(got.Status.Conditions, conditionTypeDegraded); deg == nil || deg.Status != metav1.ConditionFalse {
			t.Fatalf("post-event Degraded = %+v, want False", deg)
		}
		// Still no workload after the event, endpoint still empty, advisories
		// still absent.
		if deps, svcs := countOwnedWorkloads(t, k8s, "cache", ns); deps != 0 || svcs != 0 {
			t.Fatalf("post-event owned workloads = %d/%d, want 0/0", deps, svcs)
		}
		if got.Status.Endpoint != "" {
			t.Fatalf("post-event status.endpoint = %q, want empty", got.Status.Endpoint)
		}
		if c := findCondition(got.Status.Conditions, conditionTypeFunctionalProbeOK); c != nil {
			t.Fatalf("post-event FunctionalProbeOK = %+v, want absent", c)
		}
		if c := findCondition(got.Status.Conditions, conditionTypeT2Degraded); c != nil {
			t.Fatalf("post-event T2Degraded = %+v, want absent", c)
		}
	})

	t.Run("NoEventBeforeTimeoutIsDegradedNoKVEventsObserved", func(t *testing.T) {
		// Events-only has no workload to wait on, so the firstEventTimeout clock
		// starts at the first reconcile (status.firstAvailableAt is latched
		// immediately). Drive a short window past its deadline with no KV event
		// and the gate must flip the backend to Ready=False/NoKVEventsObserved,
		// Degraded=True — exactly like a managed backend, but with no Deployment.
		ns := freshNS(t, k8s)
		cb := eventsOnlyBackend("cache", ns)
		cb.Spec.Integration.FirstEventTimeout = &metav1.Duration{Duration: time.Second}
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		// First reconcile latches status.firstAvailableAt (no workload — the
		// clock starts now) and parks at AwaitingFirstKVEvent inside the window.
		reconcile(t, r, "cache", ns)
		latched := getBackend(t, r, "cache", ns)
		if latched.Status.FirstAvailableAt == nil {
			t.Fatalf("firstAvailableAt = nil after first reconcile, want latched immediately (no workload to wait on)")
		}
		if ready := findCondition(latched.Status.Conditions, conditionTypeReady); ready == nil ||
			ready.Status != metav1.ConditionFalse || ready.Reason != reasonAwaitingFirstKVEvent {
			t.Fatalf("pre-timeout Ready = %+v, want False/AwaitingFirstKVEvent", ready)
		}

		// Backdate the latched anchor well before the 1s window so the next
		// reconcile observes the timeout breached without sleeping.
		setFirstAvailableAt(t, k8s, "cache", ns, time.Now().Add(-5*time.Second))
		reconcile(t, r, "cache", ns)

		got := getBackend(t, r, "cache", ns)
		ready := findCondition(got.Status.Conditions, conditionTypeReady)
		if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != reasonNoKVEventsObserved {
			t.Fatalf("post-timeout Ready = %+v, want False/NoKVEventsObserved", ready)
		}
		deg := findCondition(got.Status.Conditions, conditionTypeDegraded)
		if deg == nil || deg.Status != metav1.ConditionTrue || deg.Reason != reasonNoKVEventsObserved {
			t.Fatalf("post-timeout Degraded = %+v, want True/NoKVEventsObserved", deg)
		}
		// Still server-less: no workload provisioned, no endpoint published.
		if deps, svcs := countOwnedWorkloads(t, k8s, "cache", ns); deps != 0 || svcs != 0 {
			t.Fatalf("post-timeout owned workloads = %d/%d, want 0/0 (events-only provisions nothing)", deps, svcs)
		}
		if got.Status.Endpoint != "" {
			t.Fatalf("post-timeout status.endpoint = %q, want empty", got.Status.Endpoint)
		}
	})

	t.Run("OffloadToEventsOnlyShedsPriorWorkload", func(t *testing.T) {
		// A backend provisioned in Offload mode (Deployment + Service), then
		// flipped to EventsOnly, must shed the prior owned workload on the next
		// reconcile (cleanupOwnedWorkload), end with empty status.endpoint, and
		// drop the managed-only advisory conditions.
		ns := freshNS(t, k8s)
		cb := &cachev1alpha1.CacheBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: ns, Generation: 1},
			Spec: cachev1alpha1.CacheBackendSpec{
				Type: cachev1alpha1.CacheBackendTypeLMCache,
				Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
					Engine: "vllm",
					Mode:   cachev1alpha1.CacheBackendIntegrationModeOffload,
				},
			},
		}
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create Offload backend: %v", err)
		}
		reconcile(t, r, "cache", ns)

		// Offload provisioned a Deployment + Service owned by the backend.
		if deps, svcs := countOwnedWorkloads(t, k8s, "cache", ns); deps != 1 || svcs != 1 {
			t.Fatalf("Offload owned workloads = %d deployments / %d services, want 1/1", deps, svcs)
		}

		// Seed a stale managed-only EngineKernelsHealthy condition (as a prior
		// Offload generation would publish from the lmcache-kernel-check init
		// container). The flip to EventsOnly must clear it — events-only loads no
		// connector and injects no kernel-check, so a lingering kernel verdict
		// would be a stale, server-less-irrelevant advisory.
		seedManaged := getBackend(t, r, "cache", ns)
		beforeSeed := seedManaged.DeepCopy()
		meta.SetStatusCondition(&seedManaged.Status.Conditions, metav1.Condition{
			Type:               conditionTypeEngineKernelsHealthy,
			Status:             metav1.ConditionTrue,
			Reason:             reasonKernelsHealthy,
			Message:            "seeded prior-Offload kernel verdict",
			ObservedGeneration: seedManaged.Generation,
		})
		// Also seed a stale managed-only EngineCompatibility verdict (as a prior
		// Offload generation would publish when an injected engine crash-looped
		// after the connector was wired). Events-only injects no connector, so the
		// flip must shed this too — otherwise it strands a connector-incompatibility
		// advisory on a backend that never wires a connector.
		meta.SetStatusCondition(&seedManaged.Status.Conditions, metav1.Condition{
			Type:               conditionTypeEngineCompatibility,
			Status:             metav1.ConditionFalse,
			Reason:             reasonInjectedEngineCrashLooping,
			Message:            "seeded prior-Offload incompatibility verdict",
			ObservedGeneration: seedManaged.Generation,
		})
		if err := k8s.Status().Patch(ctx, seedManaged, client.MergeFrom(beforeSeed)); err != nil {
			t.Fatalf("seed managed-only conditions: %v", err)
		}
		seeded := getBackend(t, r, "cache", ns).Status.Conditions
		if c := findCondition(seeded, conditionTypeEngineKernelsHealthy); c == nil {
			t.Fatalf("EngineKernelsHealthy seed did not take; want present before the flip")
		}
		if c := findCondition(seeded, conditionTypeEngineCompatibility); c == nil {
			t.Fatalf("EngineCompatibility seed did not take; want present before the flip")
		}

		// Flip the live object to EventsOnly. (Validation admits this: type is
		// LMCache, no autoscaling.)
		live := getBackend(t, r, "cache", ns)
		live.Spec.Integration.Mode = cachev1alpha1.CacheBackendIntegrationModeEventsOnly
		if err := k8s.Update(ctx, live); err != nil {
			t.Fatalf("update to EventsOnly: %v", err)
		}
		reconcile(t, r, "cache", ns)

		// The prior Deployment + Service are deleted by cleanupOwnedWorkload.
		if deps, svcs := countOwnedWorkloads(t, k8s, "cache", ns); deps != 0 || svcs != 0 {
			t.Fatalf("post-flip owned workloads = %d deployments / %d services, want 0/0 (cleanupOwnedWorkload)", deps, svcs)
		}
		// The named Deployment/Service are gone (the controller owns the name).
		if _, err := getOptionalDeployment(t, r, "cache", ns); err == nil {
			t.Fatalf("Deployment cache/%s still exists after flip to EventsOnly", ns)
		}

		got := getBackend(t, r, "cache", ns)
		if got.Status.Endpoint != "" {
			t.Fatalf("post-flip status.endpoint = %q, want empty", got.Status.Endpoint)
		}
		if c := findCondition(got.Status.Conditions, conditionTypeFunctionalProbeOK); c != nil {
			t.Fatalf("post-flip FunctionalProbeOK = %+v, want absent", c)
		}
		if c := findCondition(got.Status.Conditions, conditionTypeEngineKernelsHealthy); c != nil {
			t.Fatalf("post-flip EngineKernelsHealthy = %+v, want absent (events-only sheds the managed-only kernel condition)", c)
		}
		if c := findCondition(got.Status.Conditions, conditionTypeT2Degraded); c != nil {
			t.Fatalf("post-flip T2Degraded = %+v, want absent", c)
		}
		if c := findCondition(got.Status.Conditions, conditionTypeEngineCompatibility); c != nil {
			t.Fatalf("post-flip EngineCompatibility = %+v, want absent (events-only sheds the managed-only incompatibility advisory)", c)
		}
	})

	t.Run("OffloadToEventsOnlyReanchorsFirstEventWindow", func(t *testing.T) {
		// A backend that ran in Offload long enough to TIME OUT (Ready=False/
		// NoKVEventsObserved, Degraded=True) and never observed a KV event, then
		// flips to EventsOnly, must get a FRESH firstEventTimeout window measured
		// from the flip. Two things would otherwise strand it Degraded the instant
		// it becomes events-only: (a) reusing the hour-old firstAvailableAt anchor,
		// and (b) the gate's sticky-NoKVEventsObserved short-circuit inheriting the
		// prior mode's timed-out Ready reason before the fresh anchor is consulted.
		// This is the primary remediation events-only exists for — an Offload
		// backend that never got events, flipped to routing-only — so both must be
		// bypassed on the transition.
		ns := freshNS(t, k8s)
		cb := eventsOnlyBackend("cache", ns)
		if err := k8s.Create(ctx, cb); err != nil {
			t.Fatalf("create: %v", err)
		}
		// Seed the status a timed-out prior Offload generation would leave behind:
		// a server endpoint (which events-only clears, and which the reconcile
		// reads as the "transitioned from a server-bearing mode" signal), a
		// firstAvailableAt latched an hour ago, no KV event ever observed, AND the
		// sticky Ready=False/NoKVEventsObserved + Degraded=True verdict.
		live := getBackend(t, r, "cache", ns)
		beforeSeed := live.DeepCopy()
		stale := metav1.NewTime(time.Now().Add(-time.Hour))
		live.Status.Endpoint = "cache." + ns + ".svc.cluster.local:8080"
		live.Status.FirstAvailableAt = &stale
		meta.SetStatusCondition(&live.Status.Conditions, metav1.Condition{
			Type: conditionTypeReady, Status: metav1.ConditionFalse,
			Reason: reasonNoKVEventsObserved, Message: "seeded prior-Offload timeout",
			ObservedGeneration: live.Generation,
		})
		meta.SetStatusCondition(&live.Status.Conditions, metav1.Condition{
			Type: conditionTypeDegraded, Status: metav1.ConditionTrue,
			Reason: reasonNoKVEventsObserved, Message: "seeded prior-Offload timeout",
			ObservedGeneration: live.Generation,
		})
		if err := k8s.Status().Patch(ctx, live, client.MergeFrom(beforeSeed)); err != nil {
			t.Fatalf("seed prior-Offload status: %v", err)
		}
		// Precondition: the sticky timed-out verdict is really present before the flip.
		if c := findCondition(getBackend(t, r, "cache", ns).Status.Conditions, conditionTypeReady); c == nil || c.Reason != reasonNoKVEventsObserved {
			t.Fatalf("seed precondition: Ready = %+v, want False/NoKVEventsObserved before the flip", c)
		}

		reconcile(t, r, "cache", ns)
		got := getBackend(t, r, "cache", ns)

		// The flip re-anchored the window to now, so the default 5m timeout has
		// NOT elapsed: the backend parks at AwaitingFirstKVEvent, not the
		// stale-anchor NoKVEventsObserved.
		ready := findCondition(got.Status.Conditions, conditionTypeReady)
		if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != reasonAwaitingFirstKVEvent {
			t.Fatalf("post-flip Ready = %+v, want False/AwaitingFirstKVEvent (re-anchored window, not stale NoKVEventsObserved)", ready)
		}
		if deg := findCondition(got.Status.Conditions, conditionTypeDegraded); deg == nil || deg.Status != metav1.ConditionFalse {
			t.Fatalf("post-flip Degraded = %+v, want False (fresh window not yet breached)", deg)
		}
		// firstAvailableAt was overwritten with the flip moment, and the server
		// endpoint cleared.
		if got.Status.FirstAvailableAt == nil || got.Status.FirstAvailableAt.Time.Before(time.Now().Add(-time.Minute)) {
			t.Fatalf("post-flip firstAvailableAt = %v, want re-anchored to ~now (stale Offload anchor reused)", got.Status.FirstAvailableAt)
		}
		if got.Status.Endpoint != "" {
			t.Fatalf("post-flip status.endpoint = %q, want empty", got.Status.Endpoint)
		}
	})
}

package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
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
// conditions (FunctionalProbeOK / T2Degraded) absent. The transition subtest
// proves a backend that previously provisioned a Deployment+Service in Offload
// mode sheds them on the flip to EventsOnly (cleanupOwnedWorkload).
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
		if c := findCondition(got.Status.Conditions, conditionTypeT2Degraded); c != nil {
			t.Fatalf("post-flip T2Degraded = %+v, want absent", c)
		}
	})
}

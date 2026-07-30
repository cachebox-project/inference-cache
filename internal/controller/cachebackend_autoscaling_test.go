package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// ---- HPA reconciliation -----------------------------------------------------

func autoscalingBackend(name, namespace string, min, max int32, targetCPU *int32) *cachev1alpha1.CacheBackend {
	cb := lmcacheBackend(name, namespace)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{
		MinReplicas:                 ptrInt32(min),
		MaxReplicas:                 max,
		TargetCPUUtilizationPercent: targetCPU,
	}
	return cb
}

func getHPA(t *testing.T, r *CacheBackendReconciler, name, namespace string) *autoscalingv2.HorizontalPodAutoscaler {
	t.Helper()
	var hpa autoscalingv2.HorizontalPodAutoscaler
	if err := r.Get(context.Background(), types.NamespacedName{Name: name, Namespace: namespace}, &hpa); err != nil {
		t.Fatalf("get HPA %s/%s: %v", namespace, name, err)
	}
	return &hpa
}

func TestReconcileHPACreated(t *testing.T) {
	scheme := newScheme(t)
	cb := autoscalingBackend("cache", "ns1", 2, 5, ptrInt32(60))
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	hpa := getHPA(t, r, "cache", "ns1")
	if hpa.Spec.ScaleTargetRef.Kind != "Deployment" || hpa.Spec.ScaleTargetRef.Name != "cache" {
		t.Fatalf("HPA target = %+v, want Deployment/cache", hpa.Spec.ScaleTargetRef)
	}
	if hpa.Spec.MinReplicas == nil || *hpa.Spec.MinReplicas != 2 || hpa.Spec.MaxReplicas != 5 {
		t.Fatalf("HPA min/max = %v/%d, want 2/5", hpa.Spec.MinReplicas, hpa.Spec.MaxReplicas)
	}
	if owner := metav1.GetControllerOf(hpa); owner == nil || owner.Name != "cache" {
		t.Fatalf("HPA controller owner = %+v, want CacheBackend/cache", owner)
	}
	if len(hpa.Spec.Metrics) != 1 {
		t.Fatalf("HPA metrics = %d, want 1", len(hpa.Spec.Metrics))
	}
	m := hpa.Spec.Metrics[0]
	if m.Type != autoscalingv2.ResourceMetricSourceType || m.Resource == nil || m.Resource.Name != corev1.ResourceCPU {
		t.Fatalf("HPA metric = %+v, want CPU resource metric", m)
	}
	if m.Resource.Target.AverageUtilization == nil || *m.Resource.Target.AverageUtilization != 60 {
		t.Fatalf("HPA target CPU = %v, want 60", m.Resource.Target.AverageUtilization)
	}
}

func TestReconcileHPADefaults(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 3}
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	hpa := getHPA(t, r, "cache", "ns1")
	if hpa.Spec.MinReplicas == nil || *hpa.Spec.MinReplicas != defaultHPAMinReplicas {
		t.Fatalf("default min replicas = %v, want %d", hpa.Spec.MinReplicas, defaultHPAMinReplicas)
	}
	target := hpa.Spec.Metrics[0].Resource.Target.AverageUtilization
	if target == nil || *target != defaultHPATargetCPUUtilizationPercent {
		t.Fatalf("default target CPU = %v, want %d", target, defaultHPATargetCPUUtilizationPercent)
	}
}

func TestReconcileHPAUpdated(t *testing.T) {
	scheme := newScheme(t)
	cb := autoscalingBackend("cache", "ns1", 1, 3, ptrInt32(50))
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	live := getBackend(t, r, "cache", "ns1")
	live.Spec.Autoscaling.MaxReplicas = 10
	live.Spec.Autoscaling.TargetCPUUtilizationPercent = ptrInt32(80)
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("update autoscaling: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	hpa := getHPA(t, r, "cache", "ns1")
	if hpa.Spec.MaxReplicas != 10 {
		t.Fatalf("HPA max after update = %d, want 10", hpa.Spec.MaxReplicas)
	}
	if got := hpa.Spec.Metrics[0].Resource.Target.AverageUtilization; got == nil || *got != 80 {
		t.Fatalf("HPA target CPU after update = %v, want 80", got)
	}
}

func TestReconcileHPADeletedWhenAutoscalingCleared(t *testing.T) {
	scheme := newScheme(t)
	cb := autoscalingBackend("cache", "ns1", 1, 3, nil)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")
	_ = getHPA(t, r, "cache", "ns1")

	live := getBackend(t, r, "cache", "ns1")
	live.Spec.Autoscaling = nil
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("clear autoscaling: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	var hpas autoscalingv2.HorizontalPodAutoscalerList
	if err := r.List(context.Background(), &hpas); err != nil {
		t.Fatalf("list HPAs: %v", err)
	}
	if len(hpas.Items) != 0 {
		t.Fatalf("HPAs = %d, want 0 after autoscaling cleared", len(hpas.Items))
	}
}

func TestReconcileHPACleanedUpOnSwitchToExternal(t *testing.T) {
	// Switching to an External backend sheds all managed children, including
	// the HPA.
	scheme := newScheme(t)
	cb := autoscalingBackend("cache", "ns1", 1, 3, nil)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")
	_ = getHPA(t, r, "cache", "ns1")

	live := getBackend(t, r, "cache", "ns1")
	live.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
	live.Spec.Endpoint = "external.ns1.svc:8080"
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("switch to external: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	var hpas autoscalingv2.HorizontalPodAutoscalerList
	if err := r.List(context.Background(), &hpas); err != nil {
		t.Fatalf("list HPAs: %v", err)
	}
	if len(hpas.Items) != 0 {
		t.Fatalf("HPAs = %d, want 0 after switch to External", len(hpas.Items))
	}
}

func TestReconcileInitialReplicasFromAutoscalingMin(t *testing.T) {
	// With autoscaling configured, the Deployment must come up at the HPA's
	// minReplicas — otherwise it briefly runs below the HPA floor on first
	// apply (and may publish ScaledToZero status if spec.replicas defaults to
	// zero on a different shape).
	scheme := newScheme(t)
	cb := autoscalingBackend("cache", "ns1", 3, 6, nil)
	// Even with spec.replicas explicitly set, the HPA's floor wins on init.
	cb.Spec.Replicas = ptrInt32(1)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	dep := getDeployment(t, r, "cache", "ns1")
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 3 {
		t.Fatalf("initial deployment replicas = %v, want 3 (autoscaling.minReplicas)", dep.Spec.Replicas)
	}
}

func TestReconcileInitialReplicasDefaultsToOneWithAutoscaling(t *testing.T) {
	// Autoscaling without minReplicas → default 1 (matching the HPA default
	// the controller renders).
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 5}
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	dep := getDeployment(t, r, "cache", "ns1")
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 1 {
		t.Fatalf("initial deployment replicas = %v, want 1 (default autoscaling floor)", dep.Spec.Replicas)
	}
}

func TestReconcileDeploymentClampsToRaisedHPAFloor(t *testing.T) {
	// When the user raises autoscaling.minReplicas above the current live
	// replica count, the reconciler must NOT preserve the stale lower value —
	// otherwise managedReadiness would report Ready against the old count
	// before the HPA controller catches up, briefly publishing a Ready that
	// does not satisfy the new minimum.
	scheme := newScheme(t)
	cb := autoscalingBackend("cache", "ns1", 1, 5, nil)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")
	dep := getDeployment(t, r, "cache", "ns1")
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 1 {
		t.Fatalf("initial deployment replicas = %v, want 1", dep.Spec.Replicas)
	}

	// Raise the HPA floor; live replicas (set by us above) lags behind.
	live := getBackend(t, r, "cache", "ns1")
	*live.Spec.Autoscaling.MinReplicas = 4
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("raise minReplicas: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	dep = getDeployment(t, r, "cache", "ns1")
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas < 4 {
		t.Fatalf("deployment replicas = %v, want >= 4 (clamped to raised HPA floor)", dep.Spec.Replicas)
	}
}

func TestReconcileDeploymentRespectsHPAReplicas(t *testing.T) {
	// When an HPA owns the replica count, the reconciler must not overwrite
	// dep.Spec.Replicas back to spec.Replicas — that would let the controller
	// and the HPA fight, churning the rollout.
	scheme := newScheme(t)
	cb := autoscalingBackend("cache", "ns1", 1, 5, nil)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	// HPA scales the Deployment to 4 replicas (simulated).
	dep := getDeployment(t, r, "cache", "ns1")
	scaled := int32(4)
	dep.Spec.Replicas = &scaled
	if err := r.Update(context.Background(), dep); err != nil {
		t.Fatalf("update deployment replicas: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	dep = getDeployment(t, r, "cache", "ns1")
	if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 4 {
		t.Fatalf("deployment replicas = %v, want 4 (HPA-managed, not reset by reconciler)", dep.Spec.Replicas)
	}
}

// ---- Status (Progressing, observedGeneration) -------------------------------

func TestStatusProgressingTrueWhilePending(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	updated := getBackend(t, r, "cache", "ns1")
	ready := findCondition(updated.Status.Conditions, conditionTypeReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != conditionReasonRolloutInProgress {
		t.Fatalf("Ready condition = %+v, want False/RolloutInProgress right after create", ready)
	}
	prog := findCondition(updated.Status.Conditions, conditionTypeProgressing)
	if prog == nil || prog.Status != metav1.ConditionTrue {
		t.Fatalf("Progressing condition = %+v, want True while Pending", prog)
	}
}

func TestStatusProgressingFalseOnceReady(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(1)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	dep := getDeployment(t, r, "cache", "ns1")
	dep.Status.ObservedGeneration = dep.Generation
	dep.Status.Replicas = 1
	dep.Status.UpdatedReplicas = 1
	dep.Status.AvailableReplicas = 1
	dep.Status.ReadyReplicas = 1
	if err := r.Status().Update(context.Background(), dep); err != nil {
		t.Fatalf("update deployment status: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	updated := getBackend(t, r, "cache", "ns1")
	ready := findCondition(updated.Status.Conditions, conditionTypeReady)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Fatalf("Ready condition = %+v, want True", ready)
	}
	prog := findCondition(updated.Status.Conditions, conditionTypeProgressing)
	if prog == nil || prog.Status != metav1.ConditionFalse || prog.Reason != "Synced" {
		t.Fatalf("Progressing condition = %+v, want False/Synced once Ready", prog)
	}
}

func TestStatusProgressingFalseWhenDegraded(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(2)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	// Simulate a rolled-out Deployment that has lost some replicas: rollout
	// has finished (Progressing should be False) but Ready is False because
	// not enough replicas are available.
	dep := getDeployment(t, r, "cache", "ns1")
	dep.Status.ObservedGeneration = dep.Generation
	dep.Status.Replicas = 2
	dep.Status.UpdatedReplicas = 2
	dep.Status.AvailableReplicas = 1
	dep.Status.ReadyReplicas = 1
	if err := r.Status().Update(context.Background(), dep); err != nil {
		t.Fatalf("update deployment status: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	updated := getBackend(t, r, "cache", "ns1")
	ready := findCondition(updated.Status.Conditions, conditionTypeReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != conditionReasonReplicasUnavailable {
		t.Fatalf("Ready condition = %+v, want False/ReplicasUnavailable", ready)
	}
	prog := findCondition(updated.Status.Conditions, conditionTypeProgressing)
	if prog == nil || prog.Status != metav1.ConditionFalse || prog.Reason != "Degraded" {
		t.Fatalf("Progressing condition = %+v, want False/Degraded", prog)
	}
}

func TestStatusProgressingFalseAtScaledToZero(t *testing.T) {
	// A backend with spec.replicas: 0 is in a stable terminal state — no
	// rollout is in motion. Progressing must be False (Reason=ScaledToZero),
	// not True, so consumers don't see "still converging" forever.
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(0)
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")

	updated := getBackend(t, r, "cache", "ns1")
	prog := findCondition(updated.Status.Conditions, conditionTypeProgressing)
	if prog == nil || prog.Status != metav1.ConditionFalse || prog.Reason != "ScaledToZero" {
		t.Fatalf("Progressing condition = %+v, want False/ScaledToZero at zero replicas", prog)
	}
}

func TestStatusObservedGenerationTracksSpec(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1")
	r := newReconciler(scheme, cb)

	reconcile(t, r, "cache", "ns1")
	if got := getBackend(t, r, "cache", "ns1").Status.ObservedGeneration; got != 1 {
		t.Fatalf("initial observedGeneration = %d, want 1", got)
	}

	// Bump the spec → bump generation.
	live := getBackend(t, r, "cache", "ns1")
	live.Generation = 5
	live.Spec.Replicas = ptrInt32(3)
	if err := r.Update(context.Background(), live); err != nil {
		t.Fatalf("update spec: %v", err)
	}
	reconcile(t, r, "cache", "ns1")

	if got := getBackend(t, r, "cache", "ns1").Status.ObservedGeneration; got != 5 {
		t.Fatalf("status.observedGeneration after update = %d, want 5", got)
	}
}

// ---- Pure-function coverage -------------------------------------------------

func TestProgressingFromReadyExhaustive(t *testing.T) {
	cases := []struct {
		name        string
		readyStatus metav1.ConditionStatus
		reason      string
		wantStatus  metav1.ConditionStatus
		wantReason  string
	}{
		{"Ready", metav1.ConditionTrue, conditionReasonBackendReady, metav1.ConditionFalse, "Synced"},
		{"Pending-rollout", metav1.ConditionFalse, conditionReasonRolloutInProgress, metav1.ConditionTrue, conditionReasonRolloutInProgress},
		{"Pending-scaled-to-zero", metav1.ConditionFalse, conditionReasonScaledToZero, metav1.ConditionFalse, conditionReasonScaledToZero},
		{"Degraded", metav1.ConditionFalse, conditionReasonReplicasUnavailable, metav1.ConditionFalse, "Degraded"},
		// The KV-event gate's AwaitingFirstKVEvent is still-converging; without
		// this case the controller would advertise a non-degraded wait window
		// as stuck (Progressing=False), contradicting the documented contract.
		{"Awaiting-first-kv-event", metav1.ConditionFalse, reasonAwaitingFirstKVEvent, metav1.ConditionTrue, reasonAwaitingFirstKVEvent},
		// The gate's NoKVEventsObserved is a stable failure (Degraded);
		// already not progressing — same shape as ReplicasUnavailable.
		{"No-kv-events-observed", metav1.ConditionFalse, reasonNoKVEventsObserved, metav1.ConditionFalse, reasonNoKVEventsObserved},
		{"Unknown-reason-passthrough", metav1.ConditionFalse, "WedgedExternalEndpoint", metav1.ConditionFalse, "WedgedExternalEndpoint"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, reason, _ := progressingFromReady(tc.readyStatus, tc.reason, "msg")
			if status != tc.wantStatus {
				t.Fatalf("status = %v, want %v", status, tc.wantStatus)
			}
			if reason != tc.wantReason {
				t.Fatalf("reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}

func TestDesiredReplicasPrefersHPAWhenAutoscalingSet(t *testing.T) {
	cb := autoscalingBackend("cache", "ns1", 1, 5, nil)
	// User-set spec.replicas should be ignored once autoscaling is in charge —
	// the HPA's writes to dep.spec.replicas are authoritative.
	cb.Spec.Replicas = ptrInt32(1)
	dep := newDep(4)
	if got := desiredReplicas(cb, dep); got != 4 {
		t.Fatalf("desiredReplicas = %d, want 4 (HPA-driven)", got)
	}
}

func TestDesiredReplicasFallbackToSpecWhenNoAutoscaling(t *testing.T) {
	cb := lmcacheBackend("cache", "ns1")
	cb.Spec.Replicas = ptrInt32(3)
	dep := newDep(7) // out-of-band edit; not HPA-managed.
	if got := desiredReplicas(cb, dep); got != 3 {
		t.Fatalf("desiredReplicas = %d, want 3 (spec.replicas wins without autoscaling)", got)
	}
}

func TestDesiredReplicasReflectsSingletonClamp(t *testing.T) {
	// A singleton cache-server is clamped to one replica at deploy time. desiredReplicas
	// — the readiness expectation — must reflect the clamp, or a grandfathered
	// spec.replicas:3 (written before admission rejected it) deploys one pod but
	// expects three and reports RolloutInProgress forever.
	t.Run("sglang Redis L2 (pair-driven) clamps to 1", func(t *testing.T) {
		cb := lmcacheBackend("cache", "ns1")
		cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "sglang"}
		cb.Spec.Replicas = ptrInt32(3)
		if got := desiredReplicas(cb, newDep(3)); got != 1 {
			t.Fatalf("desiredReplicas = %d, want 1 (singleton readiness must match the clamp)", got)
		}
	})
	t.Run("host-network master (hostNetwork-driven) clamps to 1", func(t *testing.T) {
		cb := mooncakeBackend("cache", "ns1")
		cb.Spec.Replicas = ptrInt32(3)
		dep := newDep(3)
		dep.Spec.Template.Spec.HostNetwork = true
		if got := desiredReplicas(cb, dep); got != 1 {
			t.Fatalf("desiredReplicas = %d, want 1 (host-network singleton)", got)
		}
	})
	t.Run("disabled (0) is preserved, not clamped up", func(t *testing.T) {
		cb := lmcacheBackend("cache", "ns1")
		cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Engine: "sglang"}
		cb.Spec.Replicas = ptrInt32(0)
		if got := desiredReplicas(cb, newDep(0)); got != 0 {
			t.Fatalf("desiredReplicas = %d, want 0 (disabled preserved)", got)
		}
	})
	t.Run("EventsOnly is NOT a singleton — no cache-server is rendered", func(t *testing.T) {
		cb := lmcacheBackend("cache", "ns1")
		cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
			Engine: "sglang",
			Mode:   cachev1alpha1.CacheBackendIntegrationModeEventsOnly,
		}
		cb.Spec.Replicas = ptrInt32(3)
		if got := desiredReplicas(cb, newDep(3)); got != 3 {
			t.Fatalf("desiredReplicas = %d, want 3 (EventsOnly provisions no Redis, so nothing to clamp)", got)
		}
	})
	t.Run("vllm+LMCache is NOT a singleton — spec.replicas honored", func(t *testing.T) {
		cb := lmcacheBackend("cache", "ns1") // engine defaults to vllm
		cb.Spec.Replicas = ptrInt32(3)
		if got := desiredReplicas(cb, newDep(3)); got != 3 {
			t.Fatalf("desiredReplicas = %d, want 3 (vLLM lm:// server scales, not a singleton)", got)
		}
	})
}

func TestManagedReadinessIgnoresSpecReplicasUnderHPA(t *testing.T) {
	// spec.replicas=0 with autoscaling set must NOT trip the ScaledToZero
	// guard — the HPA owns the count, and minReplicas>=1 is enforced by the
	// kubebuilder validation on autoscaling.minReplicas.
	cb := autoscalingBackend("cache", "ns1", 1, 3, nil)
	cb.Spec.Replicas = ptrInt32(0)
	dep := newDep(2)
	dep.Status.ObservedGeneration = dep.Generation
	dep.Status.UpdatedReplicas = 2
	dep.Status.AvailableReplicas = 2

	status, reason, _ := managedReadiness(cb, dep)
	if status != metav1.ConditionTrue || reason != conditionReasonBackendReady {
		t.Fatalf("managedReadiness = %v/%q, want True/BackendReady under HPA with 2/2 replicas", status, reason)
	}
}

func newDep(replicas int32) *appsv1.Deployment {
	r := replicas
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: &r},
	}
}

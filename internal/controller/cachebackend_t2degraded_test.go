package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// TestIntegrationT2DegradedCondition exercises the advisory T2Degraded
// condition the reconciler derives from status.indexParticipation.t2HitRate
// (written out-of-band by the CacheIndex poller):
//   - "0"  (queried, zero reloads) -> True/T2ZeroHitRate
//   - ">0" (serving)               -> False/T2Serving
//   - nil  (tier-2 not exercised)  -> condition absent
//
// It also pins the fail-open invariant: T2Degraded never makes Ready a
// T2-derived failure (tier-2 is an optimization, not a serving dependency).
func TestIntegrationT2DegradedCondition(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, scheme, _ := startEnv(t)
	r := &CacheBackendReconciler{Client: k8s, Scheme: scheme, Log: logr.Discard()}
	ctx := context.Background()

	mkReady := func(t *testing.T) string {
		t.Helper()
		ns := freshNS(t, k8s)
		if err := k8s.Create(ctx, gatedLMCacheBackend("cache", ns)); err != nil {
			t.Fatalf("create: %v", err)
		}
		reconcile(t, r, "cache", ns)
		setDeploymentHTTPReady(t, k8s, "cache", ns, time.Now())
		reconcile(t, r, "cache", ns)
		return ns
	}

	// setT2 patches status.indexParticipation.t2HitRate, reconciles, and returns
	// the resulting T2Degraded + Ready conditions (nil when absent).
	setT2 := func(t *testing.T, ns string, t2 *string) (t2deg, ready *metav1.Condition) {
		t.Helper()
		var cb cachev1alpha1.CacheBackend
		if err := k8s.Get(ctx, types.NamespacedName{Name: "cache", Namespace: ns}, &cb); err != nil {
			t.Fatalf("get: %v", err)
		}
		before := cb.DeepCopy()
		// Seed a recent KV event alongside t2HitRate so the KV-event gate is
		// satisfied and the backend reaches Ready=True — that lets the
		// fail-open assertion below prove T2Degraded does not flip Ready.
		now := metav1.NewTime(time.Now())
		cb.Status.IndexParticipation = &cachev1alpha1.CacheBackendIndexParticipation{PrefixCount: 1, LastEventAt: &now, T2HitRate: t2}
		if err := k8s.Status().Patch(ctx, &cb, client.MergeFrom(before)); err != nil {
			t.Fatalf("patch indexParticipation: %v", err)
		}
		reconcile(t, r, "cache", ns)
		got := getBackend(t, r, "cache", ns)
		return findCondition(got.Status.Conditions, conditionTypeT2Degraded),
			findCondition(got.Status.Conditions, conditionTypeReady)
	}

	t.Run("zeroHitRateIsDegradedAndReadyNotT2Derived", func(t *testing.T) {
		ns := mkReady(t)
		zero := "0"
		deg, ready := setT2(t, ns, &zero)
		if deg == nil || deg.Status != metav1.ConditionTrue || deg.Reason != reasonT2ZeroHitRate {
			t.Fatalf("T2Degraded = %+v, want True/T2ZeroHitRate", deg)
		}
		// Fail-open: the backend is Ready=True (Available + a seeded KV event);
		// T2Degraded=True must NOT flip it to NotReady — tier-2 is an
		// optimization, not a serving dependency.
		if ready == nil || ready.Status != metav1.ConditionTrue {
			t.Fatalf("Ready = %+v, want True (T2Degraded must not gate Ready)", ready)
		}
	})

	t.Run("servingHitRateNotDegraded", func(t *testing.T) {
		ns := mkReady(t)
		rate := "0.75"
		deg, _ := setT2(t, ns, &rate)
		if deg == nil || deg.Status != metav1.ConditionFalse || deg.Reason != reasonT2Serving {
			t.Fatalf("T2Degraded = %+v, want False/T2Serving", deg)
		}
	})

	t.Run("notExercisedHasNoCondition", func(t *testing.T) {
		ns := mkReady(t)
		deg, _ := setT2(t, ns, nil)
		if deg != nil {
			t.Fatalf("T2Degraded should be absent when t2HitRate is nil, got %+v", deg)
		}
	})

	t.Run("clearedOnTransitionToExternal", func(t *testing.T) {
		ns := mkReady(t)
		zero := "0"
		if deg, _ := setT2(t, ns, &zero); deg == nil || deg.Status != metav1.ConditionTrue {
			t.Fatalf("precondition: T2Degraded should be True, got %+v", deg)
		}
		// Flip to External: reconcileExternal must clear the managed-only
		// T2Degraded advisory (else a stale condition lingers on a CR no
		// managed path will ever update again).
		var cb cachev1alpha1.CacheBackend
		if err := k8s.Get(ctx, types.NamespacedName{Name: "cache", Namespace: ns}, &cb); err != nil {
			t.Fatalf("get: %v", err)
		}
		cb.Spec.Type = cachev1alpha1.CacheBackendTypeExternal
		cb.Spec.Endpoint = "shared.svc.cluster.local:9000"
		if err := k8s.Update(ctx, &cb); err != nil {
			t.Fatalf("flip to External: %v", err)
		}
		reconcile(t, r, "cache", ns)
		if c := findCondition(getBackend(t, r, "cache", ns).Status.Conditions, conditionTypeT2Degraded); c != nil {
			t.Fatalf("T2Degraded should be cleared after External transition, got %+v", c)
		}
	})
}

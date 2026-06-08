package server

import (
	"math"
	"testing"
)

// PolicyStore.RoutingFloorScore is the resolver the LookupRoute handler reads
// to decide whether a PREFIX_MATCH score has cleared the per-namespace floor.
// These tests pin the three operator-meaningful shapes the resolver promises:
// "no policy" → server-wide default; explicit value → respected as-is
// (including the "0" opt-out); negative leak-through → clamped to 0.

const floorTolerance = 1e-6

func approxFloorEq(a, b float32) bool { return math.Abs(float64(a-b)) <= floorTolerance }

// TestPolicyStoreRoutingFloorScoreFallsBackToDefault pins the most important
// case: an unconfigured namespace must see the safety floor. Otherwise the
// trivial-match-as-PREFIX_MATCH bug returns for every tenant without an
// installed CachePolicy, which is the common case (most namespaces have no
// CachePolicy CR — server defaults are deliberately sane).
func TestPolicyStoreRoutingFloorScoreFallsBackToDefault(t *testing.T) {
	store := NewPolicyStore()
	if got := store.RoutingFloorScore("never-configured"); !approxFloorEq(got, DefaultRoutingFloorScore) {
		t.Fatalf("RoutingFloorScore(no-policy) = %v, want DefaultRoutingFloorScore (%v)", got, DefaultRoutingFloorScore)
	}
}

// TestPolicyStoreRoutingFloorScoreRespectsPolicyValue exercises the per-
// namespace knob. An explicit value wins as-is, AND the explicit 0 opt-out
// reaches the store as 0 — NOT the server default. Without this carve-out
// the disable-the-floor primitive would be lost (every namespace clamped to
// >= DefaultRoutingFloorScore), and operators couldn't reproduce the
// pre-floor every-match-is-PREFIX_MATCH baseline for benchmarking or
// regression-testing the ranker.
func TestPolicyStoreRoutingFloorScoreRespectsPolicyValue(t *testing.T) {
	store := NewPolicyStore()
	store.Replace([]ResolvedPolicy{
		{Namespace: "ns-strict", RoutingFloorScore: 5.0},
		{Namespace: "ns-disabled", RoutingFloorScore: 0},
	})
	if got := store.RoutingFloorScore("ns-strict"); !approxFloorEq(got, 5.0) {
		t.Fatalf("strict floor = %v, want 5.0", got)
	}
	if got := store.RoutingFloorScore("ns-disabled"); got != 0 {
		t.Fatalf("disabled floor = %v, want exactly 0 (explicit opt-out, not DefaultRoutingFloorScore)", got)
	}
}

// TestPolicyStoreRoutingFloorScoreClampsNegative defends against a hand-
// crafted /policy POST that bypasses the CRD pattern validator: a negative
// floor would silently disable enforcement (the comparison s < floor with
// floor<0 never fires) instead of the safest interpretation. Clamp to 0.
func TestPolicyStoreRoutingFloorScoreClampsNegative(t *testing.T) {
	store := NewPolicyStore()
	store.Replace([]ResolvedPolicy{{Namespace: "ns-bad", RoutingFloorScore: -1.0}})
	if got := store.RoutingFloorScore("ns-bad"); got != 0 {
		t.Fatalf("negative floor = %v, want 0 (clamped)", got)
	}
}

package server

import (
	"math"
	"testing"
)

func f32Ptr(v float32) *float32 { return &v }

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
		{Namespace: "ns-strict", RoutingFloorScore: f32Ptr(5.0)},
		{Namespace: "ns-disabled", RoutingFloorScore: f32Ptr(0)},
	})
	if got := store.RoutingFloorScore("ns-strict"); !approxFloorEq(got, 5.0) {
		t.Fatalf("strict floor = %v, want 5.0", got)
	}
	if got := store.RoutingFloorScore("ns-disabled"); got != 0 {
		t.Fatalf("disabled floor = %v, want exactly 0 (explicit opt-out, not DefaultRoutingFloorScore)", got)
	}
}

// TestPolicyStoreRoutingFloorScoreNilFieldFallsBackToDefault pins the
// pointer-disambiguation rule: a CachePolicy present in the store but
// without the RoutingFloorScore field set (nil pointer, e.g. a legacy /
// hand-crafted /policy POST body that omits the field) must fall back to
// DefaultRoutingFloorScore, NOT silently disable the floor. The kubebuilder
// default normally ensures every CR routed through admission has a present
// value, so this resolver branch fires only for the apiserver-bypass case
// — but that case must apply the safety floor, not the opt-out, since the
// operator never expressed an opt-out intent.
func TestPolicyStoreRoutingFloorScoreNilFieldFallsBackToDefault(t *testing.T) {
	store := NewPolicyStore()
	store.Replace([]ResolvedPolicy{
		// CachePolicy present in the store but with RoutingFloorScore=nil
		// (the wire body omitted the field — pre-defaulting body, manual
		// crafter, legacy CR).
		{Namespace: "ns-bare", RoutingFloorScore: nil},
	})
	if got := store.RoutingFloorScore("ns-bare"); !approxFloorEq(got, DefaultRoutingFloorScore) {
		t.Fatalf("bare-field-present ns = %v, want DefaultRoutingFloorScore (%v) — absent field MUST NOT be inferred as opt-out",
			got, DefaultRoutingFloorScore)
	}
}

// TestPolicyStoreRoutingFloorScoreClampsNegative defends against a hand-
// crafted /policy POST that bypasses the CRD pattern validator: a negative
// floor would silently disable enforcement (the comparison s < floor with
// floor<0 never fires) instead of the safest interpretation. Clamp to 0.
func TestPolicyStoreRoutingFloorScoreClampsNegative(t *testing.T) {
	store := NewPolicyStore()
	store.Replace([]ResolvedPolicy{{Namespace: "ns-bad", RoutingFloorScore: f32Ptr(-1.0)}})
	if got := store.RoutingFloorScore("ns-bad"); got != 0 {
		t.Fatalf("negative floor = %v, want 0 (clamped)", got)
	}
}

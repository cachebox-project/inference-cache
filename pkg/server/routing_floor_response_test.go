package server

import (
	"context"
	"testing"

	"github.com/cachebox-project/inference-cache/pkg/index"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// LookupRoute-level tests for the routing floor score. The PolicyStore
// resolver is covered in routing_floor_score_test.go; these tests pin the
// observable wire-side behavior the gateway sees: which reason_code
// surfaces for the operator-meaningful workload shapes.

// TestLookupRouteRoutingFloorScoreDowngradesWhenAllReplicasHoldPrefix is
// the canonical case the routing-floor-score design targets — the case
// the matched-tokens floor CANNOT catch: three replicas all hold the
// SAME LONG shared prefix (1500 tokens, e.g. a RAG corpus header or a
// custom system prompt). With matched_tokens=1500, the per-replica
// matched-tokens floor (default 64) is cleared by every replica — so
// none get filtered. But distinguishing_power = 1 - 3/3 = 0, every score
// = 0, and the post-score floor (DefaultRoutingFloorScore = 0.1) is what
// downgrades the response off the PREFIX_MATCH path. The final wire
// code is then affinity-toggle-dependent (AFFINITY_HINT with the
// kubebuilder default and a usable seed + serving replica; NO_HINT
// otherwise) — the tests below disable affinity to keep pinning the
// historic NO_HINT shape; the AFFINITY_HINT side is covered by
// affinity_routing_test.go.
//
// This is the test that proves the new floor catches the workload shape
// the existing matched-tokens floor was scoped to miss. Using a 1500-
// token prefix (above the default 64-token matched-tokens floor) is
// load-bearing here — a shorter prefix would be downgraded by the
// matched-tokens floor before this code path runs, and the assertion
// would pass for the wrong reason.
func TestLookupRouteRoutingFloorScoreDowngradesWhenAllReplicasHoldPrefix(t *testing.T) {
	svc := newTestService()
	// Isolate this test to the routing-floor downgrade invariant. The
	// affinity-routing fallback would otherwise rewrite the downgraded
	// StrategyNone into AFFINITY_HINT — that's the right behavior for
	// the affinity path (covered in affinity_routing_test.go) but
	// orthogonal to the routing-floor → NO_HINT invariant this test
	// pins.
	fal := false
	svc.policies.Replace([]ResolvedPolicy{{Namespace: "no-policy", AffinityRouting: &fal}})
	for _, rid := range []string{"r0", "r1", "r2"} {
		svc.index.Ingest(index.Update{
			ReplicaID: rid, Model: "m", Tenant: "no-policy", HashScheme: "vllm",
			// 1500 tokens — clears the default matched-tokens floor (64)
			// comfortably; the only thing that can downgrade now is the
			// distinguishing-power → score → routing-floor path.
			Prefixes: []index.PrefixRef{{PrefixHash: []byte("rag-corpus-header"), TokenCount: 1500}},
		})
	}
	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "no-policy", HashScheme: "vllm",
		PrefixHash: []byte("rag-corpus-header"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("reason = %q, want NO_HINT — all replicas hold the prefix, 1500 tokens clears the matched-tokens floor, distinguishing_power=0, score=0 < routing floor (so ONLY the routing-floor-score path can produce this downgrade)", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 0 {
		t.Fatalf("scores must be empty on downgrade, got %+v", resp.GetReplicaScores())
	}
}

// TestLookupRouteRoutingFloorScoreKeepsUniqueMatch: only one of three
// replicas holds a 64-token unique prefix. distinguishing_power = 1 - 1/3
// ≈ 0.667, score ≈ 42.7 (well above the 0.1 default floor) → PREFIX_MATCH
// preserved. The other two replicas held a different prefix (counted in
// total_replicas but not in this lookup), so they don't appear in scores.
func TestLookupRouteRoutingFloorScoreKeepsUniqueMatch(t *testing.T) {
	svc := newTestService()
	svc.index.Ingest(index.Update{
		ReplicaID: "r0", Model: "m", Tenant: "no-policy", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("unique"), TokenCount: 64}},
	})
	for _, rid := range []string{"r1", "r2"} {
		svc.index.Ingest(index.Update{
			ReplicaID: rid, Model: "m", Tenant: "no-policy", HashScheme: "vllm",
			Prefixes: []index.PrefixRef{{PrefixHash: []byte("other"), TokenCount: 32}},
		})
	}
	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "no-policy", HashScheme: "vllm",
		PrefixHash: []byte("unique"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("reason = %q, want PREFIX_MATCH (1-of-3 unique match clears default floor)", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 1 || resp.GetReplicaScores()[0].GetReplicaId() != "r0" {
		t.Fatalf("scores = %+v, want exactly r0", resp.GetReplicaScores())
	}
}

// TestLookupRouteRoutingFloorScorePolicyOverride confirms the per-namespace
// knob fires: a CR setting routingFloorScore=100 must downgrade even an
// otherwise-reasonable unique match (64 × 0.667 ≈ 42.7 < 100). Without
// this assertion the field could be silently ignored and the smoke tests
// would still pass.
func TestLookupRouteRoutingFloorScorePolicyOverride(t *testing.T) {
	svc := newTestService()
	// Disable affinity so the routing-floor downgrade lands as NO_HINT,
	// the historic shape this test pins. See the sibling
	// TestLookupRouteRoutingFloorScoreDowngradesWhenAllReplicasHoldPrefix
	// comment for the rationale.
	fal := false
	svc.policies.Replace([]ResolvedPolicy{{Namespace: "strict", RoutingFloorScore: f32Ptr(100), AffinityRouting: &fal}})
	svc.index.Ingest(index.Update{
		ReplicaID: "r0", Model: "m", Tenant: "strict", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("unique"), TokenCount: 64}},
	})
	for _, rid := range []string{"r1", "r2"} {
		svc.index.Ingest(index.Update{
			ReplicaID: rid, Model: "m", Tenant: "strict", HashScheme: "vllm",
			Prefixes: []index.PrefixRef{{PrefixHash: []byte("other"), TokenCount: 32}},
		})
	}
	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "strict", HashScheme: "vllm", PrefixHash: []byte("unique"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v (the hot path must fail open with an empty result, not an RPC error)", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("reason = %q, want NO_HINT (strict floor 100 > unique-match score)", resp.GetReasonCode())
	}
}

// TestLookupRouteRoutingFloorScoreZeroDisablesFloor exercises the explicit
// opt-out: an operator setting RoutingFloorScore=0 disables this floor.
// The test scaffolding constructs a ResolvedPolicy directly with both
// RoutingFloorScore=0 AND the implicit MinimumMatchedTokens=0 (the struct
// zero value, since Replace doesn't go through the apiserver and so
// doesn't get the kubebuilder default-64 fill), so BOTH floors are off
// here — every match surfaces as PREFIX_MATCH regardless of how trivial.
// In production a real CR would still default `minimumMatchedTokens: 64`
// at admission, so disabling the routing-floor alone would not suffice
// to surface every trivial match — the operator would have to set both
// to 0 / "0". The point of the assertion here is the routing-floor opt-
// out semantics in isolation; the matched-tokens opt-out is tested by
// the matched-tokens floor suite (pkg/server/matched_tokens_floor_test.go).
func TestLookupRouteRoutingFloorScoreZeroDisablesFloor(t *testing.T) {
	svc := newTestService()
	// MinimumMatchedTokens defaults to 0 in the struct, so this Replace
	// installs a policy with both floors off — exercising the routing-
	// floor opt-out in isolation.
	svc.policies.Replace([]ResolvedPolicy{{Namespace: "raw", RoutingFloorScore: f32Ptr(0)}})
	for _, rid := range []string{"r0", "r1", "r2"} {
		svc.index.Ingest(index.Update{
			ReplicaID: rid, Model: "m", Tenant: "raw", HashScheme: "vllm",
			Prefixes: []index.PrefixRef{{PrefixHash: []byte("chat"), TokenCount: 16}},
		})
	}
	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "raw", HashScheme: "vllm", PrefixHash: []byte("chat"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v (hot path must fail open, not error)", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("reason = %q, want PREFIX_MATCH (floor opt-out — every match surfaces)", resp.GetReasonCode())
	}
}

// TestLookupRouteRoutingFloorScoreSingleReplicaSurvives: with one replica
// serving the (tenant, model, scheme), distinguishingPower degrades to 1.0
// inside the index, so the score equals matched_tokens × freshness ×
// pressure × slo_bias. That is above the default floor 0.1 by orders of
// magnitude for any non-trivial matched_tokens, so the single-replica
// deployment must keep its PREFIX_MATCH — the simplest cluster shape
// MUST NOT break.
func TestLookupRouteRoutingFloorScoreSingleReplicaSurvives(t *testing.T) {
	svc := newTestService()
	svc.index.Ingest(index.Update{
		ReplicaID: "solo", Model: "m", Tenant: "no-policy", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 64}},
	})
	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "no-policy", HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v (hot path must fail open, not error)", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("reason = %q, want PREFIX_MATCH (single-replica must preserve baseline)", resp.GetReasonCode())
	}
}

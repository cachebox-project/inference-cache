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

// TestLookupRouteRoutingFloorScoreDowngradesWhenAllReplicasHoldPrefix is the
// canonical case the design targets: three replicas all hold the
// chat-template head, distinguishing_power = 0, every Score = 0. The
// default floor catches the score=0 case and downgrades to NO_HINT — the
// gateway sees an honest "no useful routing decision" instead of a
// PREFIX_MATCH credit on content every replica already had.
func TestLookupRouteRoutingFloorScoreDowngradesWhenAllReplicasHoldPrefix(t *testing.T) {
	svc := newTestService()
	for _, rid := range []string{"r0", "r1", "r2"} {
		svc.index.Ingest(index.Update{
			ReplicaID: rid, Model: "m", Tenant: "no-policy", HashScheme: "vllm",
			Prefixes: []index.PrefixRef{{PrefixHash: []byte("chat"), TokenCount: 16}},
		})
	}
	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "no-policy", HashScheme: "vllm",
		PrefixHash: []byte("chat"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("reason = %q, want NO_HINT — all replicas hold the prefix, distinguishing_power=0, score=0 < default floor", resp.GetReasonCode())
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
	svc.policies.Replace([]ResolvedPolicy{{Namespace: "strict", RoutingFloorScore: 100}})
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
	resp, _ := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "strict", HashScheme: "vllm", PrefixHash: []byte("unique"),
	})
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("reason = %q, want NO_HINT (strict floor 100 > unique-match score)", resp.GetReasonCode())
	}
}

// TestLookupRouteRoutingFloorScoreZeroDisablesFloor exercises the explicit
// opt-out: an operator setting RoutingFloorScore=0 wants raw recall back
// — every match surfaces as PREFIX_MATCH, regardless of how trivial. This
// is the primitive operators need for regression testing the ranker
// without the floor masking the result.
func TestLookupRouteRoutingFloorScoreZeroDisablesFloor(t *testing.T) {
	svc := newTestService()
	svc.policies.Replace([]ResolvedPolicy{{Namespace: "raw", RoutingFloorScore: 0}})
	for _, rid := range []string{"r0", "r1", "r2"} {
		svc.index.Ingest(index.Update{
			ReplicaID: rid, Model: "m", Tenant: "raw", HashScheme: "vllm",
			Prefixes: []index.PrefixRef{{PrefixHash: []byte("chat"), TokenCount: 16}},
		})
	}
	resp, _ := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "raw", HashScheme: "vllm", PrefixHash: []byte("chat"),
	})
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
	resp, _ := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "no-policy", HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("reason = %q, want PREFIX_MATCH (single-replica must preserve baseline)", resp.GetReasonCode())
	}
}

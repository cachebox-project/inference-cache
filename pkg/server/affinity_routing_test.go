package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cachebox-project/inference-cache/pkg/index"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// newServiceWithReplicas builds the service against a fresh index that
// has each replica REGISTERED AS SERVING the (tenant, model, "vllm")
// engine domain. Each replica is seeded with a distinct PrefixHash so
// it appears in servingByScope (the scheme-aware accelerator
// AffinityHint reads from). All affinity-routing tests use hash_scheme
// = "vllm".
func newServiceWithReplicas(t *testing.T, tenant, model string, replicas []string) (*inferenceCacheService, *index.Index, *PolicyStore) {
	t.Helper()
	idx := index.New(index.WithTTL(time.Hour), index.WithSweepInterval(time.Hour))
	for n, rid := range replicas {
		idx.Ingest(index.Update{
			Tenant: tenant, Model: model, ReplicaID: rid, HashScheme: "vllm",
			Prefixes: []index.PrefixRef{{PrefixHash: []byte{byte(n + 1)}, TokenCount: 32}},
			Stats:    &index.ReplicaStats{ReplicaID: rid},
		})
	}
	store := NewPolicyStore()
	svc := newInferenceCacheService(idx, newServerMetrics(), store)
	return svc, idx, store
}

func TestLookupRouteAffinityHintOnNoMatchEnabled(t *testing.T) {
	svc, _, _ := newServiceWithReplicas(t, "tenantA", "modelX", []string{"r-1", "r-2", "r-3"})
	// Default policy store → AffinityRoutingEnabled=true (server default).

	req := &icpb.LookupRouteRequest{
		TenantId:         "tenantA",
		ModelId:          "modelX",
		HashScheme:       "vllm",
		BlockHashes:      [][]byte{[]byte("prompt-block-1"), []byte("prompt-block-2")},
		BlockTokenCounts: []int32{16, 16},
	}
	resp, err := svc.LookupRoute(context.Background(), req)
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "AFFINITY_HINT" {
		t.Fatalf("reason_code: expected AFFINITY_HINT, got %q", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 1 {
		t.Fatalf("expected 1 replica score, got %d", len(resp.GetReplicaScores()))
	}
	if !strings.HasPrefix(resp.GetReplicaScores()[0].GetReplicaId(), "r-") {
		t.Fatalf("expected replica from the known set, got %q", resp.GetReplicaScores()[0].GetReplicaId())
	}
}

func TestLookupRouteAffinityHintDisabledReturnsNoHint(t *testing.T) {
	svc, _, store := newServiceWithReplicas(t, "tenantA", "modelX", []string{"r-1", "r-2"})
	fal := false
	store.Replace([]ResolvedPolicy{{Namespace: "tenantA", AffinityRouting: &fal}})

	req := &icpb.LookupRouteRequest{
		TenantId:         "tenantA",
		ModelId:          "modelX",
		HashScheme:       "vllm",
		BlockHashes:      [][]byte{[]byte("block-A")},
		BlockTokenCounts: []int32{16},
	}
	resp, _ := svc.LookupRoute(context.Background(), req)
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("expected NO_HINT, got %q", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 0 {
		t.Fatalf("expected 0 replica scores, got %d", len(resp.GetReplicaScores()))
	}
}

func TestLookupRouteAffinityHintNoReplicasReturnsNoHint(t *testing.T) {
	idx := index.New(index.WithTTL(time.Hour), index.WithSweepInterval(time.Hour))
	store := NewPolicyStore()
	svc := newInferenceCacheService(idx, newServerMetrics(), store)

	req := &icpb.LookupRouteRequest{
		TenantId:         "tenantA",
		ModelId:          "modelX",
		HashScheme:       "vllm",
		BlockHashes:      [][]byte{[]byte("block")},
		BlockTokenCounts: []int32{16},
	}
	resp, _ := svc.LookupRoute(context.Background(), req)
	// With no replicas known for (tenantA, modelX), AffinityHint returns
	// ok=false and the handler falls through to whatever the index's
	// miss-classifier produced. Since the index is globally empty,
	// classifyMiss returns StrategyNone → NO_HINT.
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("expected NO_HINT (no replicas known), got %q", resp.GetReasonCode())
	}
}

func TestLookupRouteAffinityHintNoSeedReturnsNoHint(t *testing.T) {
	svc, _, _ := newServiceWithReplicas(t, "tenantA", "modelX", []string{"r-1"})

	// No block_hashes and no prefix_hash → no seed → fall through to
	// NO_HINT even with affinity Enabled.
	req := &icpb.LookupRouteRequest{
		TenantId:   "tenantA",
		ModelId:    "modelX",
		HashScheme: "vllm",
	}
	resp, _ := svc.LookupRoute(context.Background(), req)
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("expected NO_HINT (no seed), got %q", resp.GetReasonCode())
	}
}

func TestLookupRouteAffinityHintStableAcrossRepeats(t *testing.T) {
	svc, _, _ := newServiceWithReplicas(t, "tenantA", "modelX", []string{"r-1", "r-2", "r-3", "r-4"})

	req := &icpb.LookupRouteRequest{
		TenantId:         "tenantA",
		ModelId:          "modelX",
		HashScheme:       "vllm",
		BlockHashes:      [][]byte{[]byte("identical-prompt-block")},
		BlockTokenCounts: []int32{16},
	}
	resp1, _ := svc.LookupRoute(context.Background(), req)
	if resp1.GetReasonCode() != "AFFINITY_HINT" || len(resp1.GetReplicaScores()) != 1 {
		t.Fatalf("setup: expected AFFINITY_HINT with 1 score, got %q / %d", resp1.GetReasonCode(), len(resp1.GetReplicaScores()))
	}
	want := resp1.GetReplicaScores()[0].GetReplicaId()

	for i := 0; i < 10; i++ {
		resp, _ := svc.LookupRoute(context.Background(), req)
		if resp.GetReasonCode() != "AFFINITY_HINT" || len(resp.GetReplicaScores()) != 1 {
			t.Fatalf("repeat %d: lost AFFINITY_HINT shape (%q / %d)", i, resp.GetReasonCode(), len(resp.GetReplicaScores()))
		}
		if got := resp.GetReplicaScores()[0].GetReplicaId(); got != want {
			t.Fatalf("repeat %d: expected stable replica %q, got %q", i, want, got)
		}
	}
}

func TestLookupRouteAffinityHintScoreFieldsZero(t *testing.T) {
	svc, _, _ := newServiceWithReplicas(t, "tenantA", "modelX", []string{"r-1", "r-2"})
	req := &icpb.LookupRouteRequest{
		TenantId:         "tenantA",
		ModelId:          "modelX",
		HashScheme:       "vllm",
		BlockHashes:      [][]byte{[]byte("block")},
		BlockTokenCounts: []int32{16},
	}
	resp, _ := svc.LookupRoute(context.Background(), req)
	if resp.GetReasonCode() != "AFFINITY_HINT" {
		t.Fatalf("setup: expected AFFINITY_HINT, got %q", resp.GetReasonCode())
	}
	rs := resp.GetReplicaScores()[0]
	if rs.GetScore() != 0 || rs.GetMatchedTokens() != 0 || rs.GetEstimatedCacheHitProb() != 0 {
		t.Fatalf("AFFINITY_HINT scoring fields not zero: score=%v matched=%v hit=%v",
			rs.GetScore(), rs.GetMatchedTokens(), rs.GetEstimatedCacheHitProb())
	}
}

// TestLookupRouteAffinityHintBypassesMinimumPrefixTokens verifies that the
// CRD-documented behavior holds: even when the per-namespace
// minimumPrefixTokens gate would short-circuit a tiny prompt to NO_HINT
// before the index is touched, the affinity fallback STILL fires. This is
// the canonical diffuse-single-turn-with-short-prompts case (chatbot,
// single-message workloads) where the prefix-match path has no signal but
// stable replica pinning is essentially free and still warms T1.
func TestLookupRouteAffinityHintBypassesMinimumPrefixTokens(t *testing.T) {
	svc, _, store := newServiceWithReplicas(t, "tenantA", "modelX", []string{"r-1", "r-2", "r-3"})
	// Set a high minimumPrefixTokens floor that would short-circuit a
	// small request. Affinity should still fire.
	store.Replace([]ResolvedPolicy{{Namespace: "tenantA", MinimumPrefixTokens: 1024}})

	req := &icpb.LookupRouteRequest{
		TenantId:         "tenantA",
		ModelId:          "modelX",
		HashScheme:       "vllm",
		BlockHashes:      [][]byte{[]byte("tiny-prompt")},
		BlockTokenCounts: []int32{8}, // 8 << 1024 floor
	}
	resp, err := svc.LookupRoute(context.Background(), req)
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "AFFINITY_HINT" {
		t.Fatalf("reason_code: expected AFFINITY_HINT (affinity must bypass minimumPrefixTokens), got %q", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 1 {
		t.Fatalf("expected 1 replica score, got %d", len(resp.GetReplicaScores()))
	}
}

// TestLookupRouteMinimumPrefixTokensStillFiltersWhenAffinityDisabled
// verifies the other side of the bypass: when affinityRouting is Disabled,
// minimumPrefixTokens behaves exactly as before (short-circuit to NO_HINT).
// Catches a regression where the bypass refactor silently removed the
// short-circuit even for the disabled-affinity case.
func TestLookupRouteMinimumPrefixTokensStillFiltersWhenAffinityDisabled(t *testing.T) {
	svc, _, store := newServiceWithReplicas(t, "tenantA", "modelX", []string{"r-1", "r-2"})
	fal := false
	store.Replace([]ResolvedPolicy{{Namespace: "tenantA", MinimumPrefixTokens: 1024, AffinityRouting: &fal}})

	req := &icpb.LookupRouteRequest{
		TenantId:         "tenantA",
		ModelId:          "modelX",
		HashScheme:       "vllm",
		BlockHashes:      [][]byte{[]byte("tiny-prompt")},
		BlockTokenCounts: []int32{8},
	}
	resp, _ := svc.LookupRoute(context.Background(), req)
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("expected NO_HINT (affinity disabled), got %q", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 0 {
		t.Fatalf("expected no replica scores, got %d", len(resp.GetReplicaScores()))
	}
}

// TestLookupRouteAffinityHintPreservesUnknownHashSchemePrecedence verifies
// that affinity never papers over UNKNOWN_HASH_SCHEME, even on the
// minimumPrefixTokens-bypass path. The (tenant, model) is populated
// under one scheme; a small request comes in under a different scheme.
// AffinityHint is scheme-aware (servingByScope), so a direct call
// would correctly refuse to pin a wrong-scheme replica — but the
// precedence-violating failure mode is subtler: bypassing the lookup
// entirely would lose the operator-facing UNKNOWN_HASH_SCHEME signal
// and surface as bare NO_HINT, hiding the gateway misconfiguration.
// With the precedence guard the request goes through the full lookup,
// the index classifies it as StrategyUnknownHashScheme, and the
// response code stays UNKNOWN_HASH_SCHEME.
func TestLookupRouteAffinityHintPreservesUnknownHashSchemePrecedence(t *testing.T) {
	// Seed (tenantA, modelX) ONLY under hash_scheme "vllm" so a request
	// under "sglang" is a genuine UNKNOWN_HASH_SCHEME case.
	idx := index.New(index.WithTTL(time.Hour), index.WithSweepInterval(time.Hour))
	idx.Ingest(index.Update{
		Tenant: "tenantA", Model: "modelX", ReplicaID: "r-vllm-1", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 64}},
		Stats:    &index.ReplicaStats{ReplicaID: "r-vllm-1"},
	})
	store := NewPolicyStore()
	// minimumPrefixTokens=1024 means the request below would short-circuit
	// pre-lookup if it weren't for the precedence guard.
	store.Replace([]ResolvedPolicy{{Namespace: "tenantA", MinimumPrefixTokens: 1024}})
	svc := newInferenceCacheService(idx, newServerMetrics(), store)

	req := &icpb.LookupRouteRequest{
		TenantId:         "tenantA",
		ModelId:          "modelX",
		HashScheme:       "sglang", // wrong scheme — no entries here
		BlockHashes:      [][]byte{[]byte("tiny-prompt")},
		BlockTokenCounts: []int32{8}, // 8 << 1024 floor
	}
	resp, err := svc.LookupRoute(context.Background(), req)
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "UNKNOWN_HASH_SCHEME" {
		t.Fatalf("reason_code: expected UNKNOWN_HASH_SCHEME (diagnostic precedence over AFFINITY_HINT), got %q with %d replica scores",
			resp.GetReasonCode(), len(resp.GetReplicaScores()))
	}
}

// TestLookupRouteMinimumPrefixTokensDowngradesPrefixMatchWhenAffinityEnabled
// verifies the operator-facing intent of minimumPrefixTokens still holds with
// affinity Enabled: a tiny request that happens to deep-match a seeded prefix
// must NOT surface PREFIX_MATCH. The pre-lookup short-circuit is gone (it
// would hide UNKNOWN_HASH_SCHEME), so the gate fires as a result-side
// downgrade — the request runs the full lookup, PREFIX_MATCH is detected,
// then downgraded to StrategyNone, and AFFINITY_HINT takes over.
func TestLookupRouteMinimumPrefixTokensDowngradesPrefixMatchWhenAffinityEnabled(t *testing.T) {
	idx := index.New(index.WithTTL(time.Hour), index.WithSweepInterval(time.Hour))
	idx.Ingest(index.Update{
		Tenant: "tenantA", Model: "modelX", ReplicaID: "r-1", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("deep"), TokenCount: 64}},
		Stats:    &index.ReplicaStats{ReplicaID: "r-1"},
	})
	store := NewPolicyStore()
	// minimumPrefixTokens=1024 — the request below is well under the gate.
	// minimumMatchedTokens=0 — disable the matched-tokens floor so the only
	// thing in the way of PREFIX_MATCH is the minimumPrefixTokens downgrade.
	store.Replace([]ResolvedPolicy{{Namespace: "tenantA", MinimumPrefixTokens: 1024, MinimumMatchedTokens: 0}})
	svc := newInferenceCacheService(idx, newServerMetrics(), store)

	req := &icpb.LookupRouteRequest{
		TenantId:         "tenantA",
		ModelId:          "modelX",
		HashScheme:       "vllm",
		PrefixHash:       []byte("deep"),
		PrefixTokenCount: 1, // 1 << 1024 floor
	}
	resp, _ := svc.LookupRoute(context.Background(), req)
	if resp.GetReasonCode() != "AFFINITY_HINT" {
		t.Fatalf("reason_code: expected AFFINITY_HINT (PREFIX_MATCH downgraded by minimumPrefixTokens, affinity fires), got %q",
			resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 1 || resp.GetReplicaScores()[0].GetReplicaId() != "r-1" {
		t.Fatalf("expected single replica r-1, got %+v", resp.GetReplicaScores())
	}
	rs := resp.GetReplicaScores()[0]
	if rs.GetScore() != 0 || rs.GetMatchedTokens() != 0 || rs.GetEstimatedCacheHitProb() != 0 {
		t.Fatalf("AFFINITY_HINT scoring fields not zero (the downgraded PREFIX_MATCH scores leaked through): score=%v matched=%v hit=%v",
			rs.GetScore(), rs.GetMatchedTokens(), rs.GetEstimatedCacheHitProb())
	}
}

// TestLookupRouteMinimumPrefixTokensDowngradesTenantHotWhenAffinityEnabled
// guards the regression where a tiny non-chain (legacy prefix_hash)
// request below minimumPrefixTokens could surface as TENANT_HOT under
// affinity Enabled. A StrategyPrefixMatch-only downgrade would let
// StrategyTenantHot pass through to the wire, bypassing the operator's
// "tiny prompts don't surface a positive cache-evidence hint" intent.
// The downgrade
// catches both strategies; StrategyNone then surfaces as AFFINITY_HINT
// via the affinity fallback (or NO_HINT under affinityRouting: Disabled).
func TestLookupRouteMinimumPrefixTokensDowngradesTenantHotWhenAffinityEnabled(t *testing.T) {
	idx := index.New(index.WithTTL(time.Hour), index.WithSweepInterval(time.Hour))
	// Seed a warm replica for the scope so the TENANT_HOT path can fire.
	// Use a prefix the request will MISS (so we don't get StrategyPrefixMatch)
	// but at high enough TokenCount and hit-rate that TENANT_HOT considers
	// the replica warm.
	idx.Ingest(index.Update{
		Tenant: "tenantA", Model: "modelX", ReplicaID: "warm-r", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("warm-prefix"), TokenCount: 256}},
		Stats:    &index.ReplicaStats{ReplicaID: "warm-r", HitRate: 0.9, Pressure: 0.0},
	})
	store := NewPolicyStore()
	// MinimumPrefixTokens=1024 — way above the request's claimed 1 token.
	store.Replace([]ResolvedPolicy{{Namespace: "tenantA", MinimumPrefixTokens: 1024}})
	svc := newInferenceCacheService(idx, newServerMetrics(), store)

	req := &icpb.LookupRouteRequest{
		TenantId:         "tenantA",
		ModelId:          "modelX",
		HashScheme:       "vllm",
		PrefixHash:       []byte("novel-prefix"), // misses the seeded "warm-prefix"
		PrefixTokenCount: 1,                      // 1 << 1024 floor
	}
	resp, err := svc.LookupRoute(context.Background(), req)
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() == "TENANT_HOT" {
		t.Fatalf("reason_code TENANT_HOT leaked through the minimumPrefixTokens downgrade — tiny non-chain prompt must not surface a positive warm-tenant hint")
	}
	if resp.GetReasonCode() != "AFFINITY_HINT" {
		t.Fatalf("reason_code: expected AFFINITY_HINT (downgrade + affinity fallback), got %q", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 1 {
		t.Fatalf("expected 1 replica score, got %d", len(resp.GetReplicaScores()))
	}
}

// TestLookupRouteAffinityHintCountsOnlyChainMalformed pins the symmetric
// chain-malformed guard: a request with block_token_counts set but
// block_hashes empty is just as malformed as the inverse, and the affinity
// fallback must NOT paper over it by falling back to prefix_hash. The
// index treats either chain array being non-empty as chain mode and
// drops the chain ingest when the arrays disagree in length; the
// handler-side affinityEligible guard mirrors that rule.
func TestLookupRouteAffinityHintCountsOnlyChainMalformed(t *testing.T) {
	svc, _, _ := newServiceWithReplicas(t, "tenantA", "modelX", []string{"r-1", "r-2"})

	req := &icpb.LookupRouteRequest{
		TenantId:         "tenantA",
		ModelId:          "modelX",
		HashScheme:       "vllm",
		PrefixHash:       []byte("legacy-prefix"), // would be a valid fallback seed if the guard let it through
		BlockTokenCounts: []int32{16, 16},         // counts-only chain — malformed
	}
	resp, _ := svc.LookupRoute(context.Background(), req)
	if resp.GetReasonCode() == "AFFINITY_HINT" {
		t.Fatalf("counts-only malformed chain leaked through affinityEligible — expected NO_HINT, got AFFINITY_HINT")
	}
}

// TestLookupRouteAffinityHintRequiresAllContractKeys guards the rule
// that affinityEligible must reject empty tenant_id / model_id /
// hash_scheme — not just hash_scheme. Empty contract keys are a contract
// violation, and the index maps them to StrategyNone for the same
// fail-open NO_HINT reason. Without all-three-key guards a buggy
// producer that wrote to an empty-key scope could turn into a positive
// AFFINITY_HINT response.
func TestLookupRouteAffinityHintRequiresAllContractKeys(t *testing.T) {
	// Even seed an empty-tenant scope to make the regression visible —
	// without the guard, AffinityHint would pick "leaked-r" via the
	// empty-tenant servingByScope entry.
	idx := index.New(index.WithTTL(time.Hour), index.WithSweepInterval(time.Hour))
	idx.Ingest(index.Update{
		Tenant: "", Model: "modelX", ReplicaID: "leaked-r", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 32}},
		Stats:    &index.ReplicaStats{ReplicaID: "leaked-r"},
	})
	store := NewPolicyStore()
	svc := newInferenceCacheService(idx, newServerMetrics(), store)

	for _, tc := range []struct {
		name string
		req  *icpb.LookupRouteRequest
	}{
		{name: "empty tenant", req: &icpb.LookupRouteRequest{ModelId: "modelX", HashScheme: "vllm", BlockHashes: [][]byte{[]byte("b")}, BlockTokenCounts: []int32{16}}},
		{name: "empty model", req: &icpb.LookupRouteRequest{TenantId: "tenantA", HashScheme: "vllm", BlockHashes: [][]byte{[]byte("b")}, BlockTokenCounts: []int32{16}}},
		{name: "empty hash_scheme", req: &icpb.LookupRouteRequest{TenantId: "tenantA", ModelId: "modelX", BlockHashes: [][]byte{[]byte("b")}, BlockTokenCounts: []int32{16}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, _ := svc.LookupRoute(context.Background(), tc.req)
			if resp.GetReasonCode() == "AFFINITY_HINT" {
				t.Fatalf("empty contract key leaked through affinityEligible — expected NO_HINT, got AFFINITY_HINT (replicas=%+v)", resp.GetReplicaScores())
			}
		})
	}
}

package index

import (
	"context"
	"testing"
	"time"
)

// Tests covering how the distinguishing-power factor integrates into the
// lookup scoring path. The math leaf is covered in distinguishing_power_test.go;
// this file pins the per-replica score outcomes that the ranker actually
// ships to the gateway for the operator-meaningful cluster shapes
// (every-replica-holds-it, uniquely-held, partial diffusion, single-replica).

// startTestIndex wires the index for the lookup tests in this file: 30m TTL
// so freshness stays ≈ 1 for the duration of the test, with the eviction
// goroutine torn down via the ctx-cancel pattern on cleanup.
func startTestIndex(t *testing.T) *Index {
	t.Helper()
	i := New(WithTTL(30 * time.Minute))
	ctx, cancel := context.WithCancel(context.Background())
	i.Start(ctx)
	t.Cleanup(cancel)
	return i
}

// TestLookupExactZeroDistinguishingWhenAllReplicasHoldPrefix is the headline
// case: three replicas all hold a 16-token chat-template prefix. The
// distinguishing-power factor must collapse to 0, so every Score is 0 —
// the service-layer post-score floor (covered in pkg/server) then downgrades
// the response to NO_HINT. The index itself stays policy-unaware: it still
// returns the matched replicas; the floor decides whether they ship.
func TestLookupExactZeroDistinguishingWhenAllReplicasHoldPrefix(t *testing.T) {
	i := startTestIndex(t)
	for _, rid := range []string{"r0", "r1", "r2"} {
		i.Ingest(Update{
			ReplicaID: rid, Model: "m", Tenant: "t", HashScheme: "vllm",
			Prefixes: []PrefixRef{{PrefixHash: []byte("chat-template"), TokenCount: 16}},
		})
	}
	scores := i.Lookup(LookupRequest{
		Tenant: "t", Model: "m", HashScheme: "vllm",
		PrefixHash: []byte("chat-template"), TokenCount: 16,
	})
	if len(scores) != 3 {
		t.Fatalf("len(scores) = %d, want 3 (index returns all holders pre-floor)", len(scores))
	}
	for _, sc := range scores {
		if sc.Score != 0 {
			t.Errorf("replica %s Score = %v, want 0 (all 3 replicas have the prefix → distinguishing_power = 0)", sc.ReplicaID, sc.Score)
		}
		if sc.MatchedTokens != 16 {
			t.Errorf("replica %s MatchedTokens = %d, want 16 (unchanged by the factor)", sc.ReplicaID, sc.MatchedTokens)
		}
	}
}

// TestLookupExactNonZeroDistinguishingWhenOneOfThreeHoldsPrefix pins the
// unique-prefix case the proposal calls out as "maximally useful for
// routing": only one of three replicas holds a 64-token RAG context. Factor
// is 1 - 1/3 ≈ 0.667; score is matched_tokens × freshness × 0.667 ≈ 42.7.
// The non-holders don't appear in Scores at all (they don't hold this prefix
// — they're only counted toward total_replicas via the engine-domain index).
func TestLookupExactNonZeroDistinguishingWhenOneOfThreeHoldsPrefix(t *testing.T) {
	i := startTestIndex(t)
	// Only r0 holds the queried prefix. r1 and r2 hold a different prefix
	// in the SAME engine domain — they are counted in totalReplicas (the
	// distinguishing-power denominator) but don't appear in the scored
	// result for this lookup.
	i.Ingest(Update{
		ReplicaID: "r0", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: []byte("unique-rag-ctx"), TokenCount: 64}},
	})
	for _, rid := range []string{"r1", "r2"} {
		i.Ingest(Update{
			ReplicaID: rid, Model: "m", Tenant: "t", HashScheme: "vllm",
			Prefixes: []PrefixRef{{PrefixHash: []byte("other-prefix"), TokenCount: 32}},
		})
	}
	scores := i.Lookup(LookupRequest{
		Tenant: "t", Model: "m", HashScheme: "vllm",
		PrefixHash: []byte("unique-rag-ctx"), TokenCount: 64,
	})
	if len(scores) != 1 {
		t.Fatalf("len(scores) = %d, want 1 (only r0 holds the prefix)", len(scores))
	}
	if scores[0].ReplicaID != "r0" {
		t.Fatalf("Scores[0].ReplicaID = %q, want r0", scores[0].ReplicaID)
	}
	// freshness ≈ 1.0 (just-ingested); distinguishing_power = 1 - 1/3 = 2/3.
	// Score = 64 × 1.0 × 2/3 = 42.667. Allow a small float tolerance.
	want := float32(64) * float32(2.0/3.0)
	if delta := scores[0].Score - want; delta < -0.1 || delta > 0.1 {
		t.Fatalf("Scores[0].Score = %v, want ~%v (64 × 1.0 × 2/3)", scores[0].Score, want)
	}
}

// TestLookupExactPartialDiffusionTwoOfThree pins the partial-diffusion
// shape: 2 of 3 replicas hold the prefix. Factor = 1 - 2/3 = 1/3 ≈ 0.333.
// Both holders survive in the scored result with the same per-replica score
// (same prefix, same matched_tokens, same factor).
func TestLookupExactPartialDiffusionTwoOfThree(t *testing.T) {
	i := startTestIndex(t)
	for _, rid := range []string{"r0", "r1"} {
		i.Ingest(Update{
			ReplicaID: rid, Model: "m", Tenant: "t", HashScheme: "vllm",
			Prefixes: []PrefixRef{{PrefixHash: []byte("partial"), TokenCount: 48}},
		})
	}
	// r2 serves the scope but holds a different prefix — counted in
	// totalReplicas, not in matching.
	i.Ingest(Update{
		ReplicaID: "r2", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: []byte("else"), TokenCount: 16}},
	})
	scores := i.Lookup(LookupRequest{
		Tenant: "t", Model: "m", HashScheme: "vllm",
		PrefixHash: []byte("partial"), TokenCount: 48,
	})
	if len(scores) != 2 {
		t.Fatalf("len(scores) = %d, want 2", len(scores))
	}
	want := float32(48) * float32(1.0/3.0)
	for _, sc := range scores {
		if delta := sc.Score - want; delta < -0.1 || delta > 0.1 {
			t.Errorf("Score(%s) = %v, want ~%v (48 × 1.0 × 1/3)", sc.ReplicaID, sc.Score, want)
		}
	}
}

// TestLookupExactSingleReplicaPreservesBaselineScore is the regression
// guard for the simplest cluster shape: ONE replica serving (tenant, model).
// distinguishingPower(1, 1) must degrade to 1.0, so the score equals
// matched_tokens × freshness — exactly the baseline.
func TestLookupExactSingleReplicaPreservesBaselineScore(t *testing.T) {
	i := startTestIndex(t)
	i.Ingest(Update{
		ReplicaID: "solo", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: []byte("p"), TokenCount: 64}},
	})
	scores := i.Lookup(LookupRequest{
		Tenant: "t", Model: "m", HashScheme: "vllm",
		PrefixHash: []byte("p"), TokenCount: 64,
	})
	if len(scores) != 1 {
		t.Fatalf("len(scores) = %d, want 1", len(scores))
	}
	// freshness ≈ 1.0, factor = 1.0 (single replica), score ≈ 64.
	if delta := scores[0].Score - 64; delta < -0.1 || delta > 0.1 {
		t.Fatalf("Scores[0].Score = %v, want ~64 (matched_tokens × freshness × 1.0)", scores[0].Score)
	}
}

// TestLookupChainDepthAwareDistinguishingPower pins the asymmetric chain
// case that motivates per-replica factor in lookupChain (vs lookupExact's
// uniform one). Three replicas, same head block, different depths:
//
//	r0 holds the leading 4 blocks (request matches all 4 → matched=256)
//	r1 holds the leading 2 blocks (matched=128)
//	r2 holds the leading 1 block  (matched=64)
//
// Naive shared-factor scoring would multiply by (1 - 3/3) = 0 and zero
// EVERY score — including r0's, which is the uniquely-deep match. The
// correct depth-aware computation: replicas grouped by matched_tokens
// descending, num_matching_at_R = count of replicas with mt >= R.mt. At
// r0's depth (only r0 reached): factor = 1 - 1/3 = 0.667. At r1's depth
// (r0+r1): factor = 1 - 2/3 = 0.333. At r2's depth (all three): factor = 0.
// r0's score must dominate the response — exactly the routing decision
// the operator wants.
func TestLookupChainDepthAwareDistinguishingPower(t *testing.T) {
	i := startTestIndex(t)
	chain := [][]byte{[]byte("b1"), []byte("b2"), []byte("b3"), []byte("b4")}
	counts := []int32{64, 64, 64, 64}

	i.Ingest(Update{
		ReplicaID: "r0", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: chain, BlockTokenCounts: counts}},
	})
	i.Ingest(Update{
		ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: chain[:2], BlockTokenCounts: counts[:2]}},
	})
	i.Ingest(Update{
		ReplicaID: "r2", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: chain[:1], BlockTokenCounts: counts[:1]}},
	})

	scores := i.Lookup(LookupRequest{
		Tenant: "t", Model: "m", HashScheme: "vllm",
		BlockHashes: chain, BlockTokenCounts: counts,
	})
	if len(scores) != 3 {
		t.Fatalf("len(scores) = %d, want 3 (all three replicas held the head)", len(scores))
	}
	byID := map[string]ReplicaScore{}
	for _, sc := range scores {
		byID[sc.ReplicaID] = sc
	}

	// r0: matched=256, factor=1-1/3=2/3 → score ≈ 256 × 2/3 = 170.67.
	if got := byID["r0"].Score; got < 168 || got > 173 {
		t.Errorf("r0.Score = %v, want ~170.67 (256 × 2/3)", got)
	}
	// r1: matched=128, factor=1-2/3=1/3 → score ≈ 128 × 1/3 = 42.67.
	if got := byID["r1"].Score; got < 41 || got > 45 {
		t.Errorf("r1.Score = %v, want ~42.67 (128 × 1/3)", got)
	}
	// r2: matched=64, factor=1-3/3=0 → score = 0.
	if got := byID["r2"].Score; got != 0 {
		t.Errorf("r2.Score = %v, want 0 (3 of 3 replicas reached this depth)", got)
	}
	// r0 must be ranked first — the deepest unique match.
	if scores[0].ReplicaID != "r0" {
		t.Fatalf("Scores[0] = %q, want r0 (deepest unique match)", scores[0].ReplicaID)
	}
}

// TestLookupChainSharedHeadZeroDistinguishing pins the canonical RAG/chat
// shape: every replica holds the same single-block head (chat template, RAG
// header). With nobody reaching deeper, EVERY score must collapse to 0 —
// the service-layer floor then downgrades to NO_HINT.
func TestLookupChainSharedHeadZeroDistinguishing(t *testing.T) {
	i := startTestIndex(t)
	chain := [][]byte{[]byte("chat-template-head")}
	counts := []int32{16}
	for _, rid := range []string{"r0", "r1", "r2"} {
		i.Ingest(Update{
			ReplicaID: rid, Model: "m", Tenant: "t", HashScheme: "vllm",
			Prefixes: []PrefixRef{{BlockHashes: chain, BlockTokenCounts: counts}},
		})
	}
	scores := i.Lookup(LookupRequest{
		Tenant: "t", Model: "m", HashScheme: "vllm",
		BlockHashes: chain, BlockTokenCounts: counts,
	})
	if len(scores) != 3 {
		t.Fatalf("len(scores) = %d, want 3 holders of the head", len(scores))
	}
	for _, sc := range scores {
		if sc.Score != 0 {
			t.Errorf("replica %s Score = %v, want 0 (all-replicas-hold-it → factor 0)", sc.ReplicaID, sc.Score)
		}
	}
}

// TestLookupChainSingleReplicaPreservesBaselineScore mirrors the exact-match
// regression guard for the chain path: with one replica serving the scope,
// the depth-aware factor must degrade to 1.0 so the chain score is the
// usual matched_tokens × freshness baseline.
func TestLookupChainSingleReplicaPreservesBaselineScore(t *testing.T) {
	i := startTestIndex(t)
	chain := [][]byte{[]byte("b1"), []byte("b2")}
	counts := []int32{64, 64}
	i.Ingest(Update{
		ReplicaID: "solo", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: chain, BlockTokenCounts: counts}},
	})
	scores := i.Lookup(LookupRequest{
		Tenant: "t", Model: "m", HashScheme: "vllm",
		BlockHashes: chain, BlockTokenCounts: counts,
	})
	if len(scores) != 1 {
		t.Fatalf("len(scores) = %d, want 1", len(scores))
	}
	if delta := scores[0].Score - 128; delta < -0.1 || delta > 0.1 {
		t.Fatalf("Scores[0].Score = %v, want ~128 (chain matched_tokens × 1.0 freshness × 1.0 factor)", scores[0].Score)
	}
}

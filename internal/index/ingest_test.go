// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"testing"
	"time"
)

func TestWithClockIgnoresNil(t *testing.T) {
	idx := New(WithClock(nil))
	if idx.now == nil {
		t.Fatal("WithClock(nil) cleared the default clock")
	}
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 1}}})
}

func TestIngestAndLookupRanksByTokensAndFreshness(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	idx := New(withClock(clk.now), WithTTL(time.Hour))

	// replica-a holds the prefix with 100 tokens; replica-b with 50. Same freshness.
	idx.Ingest(Update{ReplicaID: "replica-a", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p1"), TokenCount: 100}}})
	idx.Ingest(Update{ReplicaID: "replica-b", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p1"), TokenCount: 50}}})
	// Decoy: a third replica serves the same engine domain (tenant, model,
	// hash_scheme) but holds a DIFFERENT prefix. It populates the
	// distinguishing-power denominator (total_replicas=3) without showing
	// up in the scored result for hash("p1"). Without it both holders of
	// the queried prefix would have factor (1 - 2/2)=0, zeroing every
	// score and replacing the freshness-vs-tokens story this test
	// asserts with a lexicographic-ID tiebreak.
	idx.Ingest(Update{ReplicaID: "replica-c-decoy", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("decoy"), TokenCount: 1}}})

	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p1")})
	if len(got) != 2 {
		t.Fatalf("expected 2 replica scores, got %d", len(got))
	}
	if got[0].ReplicaID != "replica-a" {
		t.Fatalf("expected replica-a ranked first (more matched tokens), got %q", got[0].ReplicaID)
	}
	if got[0].MatchedTokens != 100 {
		t.Fatalf("matched tokens = %d, want 100", got[0].MatchedTokens)
	}

	// Now make replica-b's entry fresher and replica-a stale-ish: freshness should
	// flip ranking if the token gap is small enough. Re-report b at a later time.
	clk.add(50 * time.Minute) // a is now 50m old (freshness ~0.17), b re-reported fresh
	idx.Ingest(Update{ReplicaID: "replica-b", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p1"), TokenCount: 50}}})

	got = idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p1")})
	// a: 100 * ~0.167 ≈ 16.7 ; b: 50 * 1.0 = 50 → b wins on freshness.
	if got[0].ReplicaID != "replica-b" {
		t.Fatalf("expected replica-b ranked first after freshness decay, got %q (scores: %+v)", got[0].ReplicaID, got)
	}
}

func TestIngestIsIdempotent(t *testing.T) {
	idx := New()
	u := Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}}}
	idx.Ingest(u)
	idx.Ingest(u)
	idx.Ingest(u)

	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p")}); len(got) != 1 {
		t.Fatalf("re-reporting the same prefix should not duplicate: got %d scores", len(got))
	}
	if got := idx.EntryCountsByModel()["m"]; got != 1 {
		t.Fatalf("entry count = %d, want 1", got)
	}
}

func TestPrefixAddedEventDoesNotRefreshAcrossSchemes(t *testing.T) {
	clk := &fakeClock{t: time.Unix(5_000_000, 0)}
	idx := New(withClock(clk.now), WithTTL(10*time.Minute))

	// Same opaque prefix bytes under two engine schemes for the same replica.
	for _, scheme := range []string{"vllm", "sglang"} {
		idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: scheme,
			Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 1}}})
	}

	clk.add(9 * time.Minute) // both entries are 9m old (TTL 10m)
	// A PREFIX_ADDED event (no hash_scheme) must NOT refresh either scheme's entry.
	idx.ApplyEvent(Event{Type: EventPrefixAdded, ReplicaID: "r", Model: "m", Tenant: "t", PrefixHash: hash("p")})

	clk.add(2 * time.Minute) // now 11m old → past TTL since the event did not refresh
	idx.evictExpired()

	for _, scheme := range []string{"vllm", "sglang"} {
		if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: scheme, PrefixHash: hash("p")}); len(got) != 0 {
			t.Fatalf("scheme %q entry should have expired (PREFIX_ADDED must not refresh): got %+v", scheme, got)
		}
	}
}

// TestIngestDefaultsTierT1Exact pins the default-ingest rule on the legacy
// exact-match path: a PrefixRef that leaves Tier unset is stored (and surfaced
// on lookup) as T1 — the engine KV cache. No detection logic runs.
func TestIngestDefaultsTierT1Exact(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	idx.Ingest(Update{ReplicaID: "replica-a", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p1"), TokenCount: 64}}}) // Tier unset

	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p1")})
	if len(got) != 1 || got[0].Tier != TierT1 {
		t.Fatalf("unset tier must default to T1 on lookup; got %+v", got)
	}
}

// TestIngestDefaultsTierT1Chain pins the same default-ingest rule on the
// block-chain path: the run's summarized tier is T1 when every block was
// ingested without an explicit tier (all blocks T1 → coldest is T1).
func TestIngestDefaultsTierT1Chain(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	hashes, counts := chain("b1", "b2", "b3")
	idx.Ingest(Update{ReplicaID: "replica-a", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: hashes, BlockTokenCounts: counts}}}) // Tier unset

	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		BlockHashes: hashes, BlockTokenCounts: counts})
	if len(got) != 1 || got[0].Tier != TierT1 {
		t.Fatalf("unset tier must default to T1 on a chain lookup; got %+v", got)
	}
}

// TestIngestCarriesExplicitTier proves the plumbing round-trips a producer-set
// tier unchanged (the substrate the future detection ticket writes into): an
// entry ingested as T2 surfaces as T2 on lookup — no T1 defaulting overrides it.
func TestIngestCarriesExplicitTier(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	idx.Ingest(Update{ReplicaID: "replica-a", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p1"), TokenCount: 64, Tier: TierT2}}})

	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p1")})
	if len(got) != 1 || got[0].Tier != TierT2 {
		t.Fatalf("explicit T2 tier must round-trip unchanged; got %+v", got)
	}
}

// TestChainLookupAgainstLegacyIngestExactOnly documents the migration window:
// a legacy-style ingest (PrefixHash only) can still be matched exactly by the
// chain path when the request's block 0 equals the stored single blob — but
// it can't drive partial-prefix matching against a single-blob entry.
func TestChainLookupAgainstLegacyIngestExactOnly(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: []byte("p"), TokenCount: 64}}})
	reqHashes, reqCounts := chain("p", "x", "y")
	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		BlockHashes: reqHashes, BlockTokenCounts: reqCounts})
	if len(got) != 1 || got[0].ReplicaID != "r" {
		t.Fatalf("chain lookup against legacy entry should still hit on block 0: got %+v", got)
	}
	if got[0].MatchedTokens != 16 {
		t.Fatalf("matched_tokens for 1-block partial = %d, want 16 (request BlockTokenCounts[0])", got[0].MatchedTokens)
	}
}

// TestChainIngestMismatchedLengthsDropped: parallel arrays must agree in
// length; a malformed PrefixEntry is dropped fail-soft (soft state — a
// stale hint is OK, a wrong one is not).
func TestChainIngestMismatchedLengthsDropped(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	hashes, _ := chain("b1", "b2", "b3")
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: hashes, BlockTokenCounts: []int32{16}}}})
	if n := idx.EntryCountsByModel()["m"]; n != 0 {
		t.Fatalf("mismatched chain lengths should drop the entry; got %d indexed", n)
	}
}

// TestChainIngestEmptyHashSchemeFailsOpen: the engine-opaque guarantee
// extends to chain ingest — no scheme, no indexing.
func TestChainIngestEmptyHashSchemeFailsOpen(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	hashes, counts := chain("b1", "b2", "b3")
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "",
		Prefixes: []PrefixRef{{BlockHashes: hashes, BlockTokenCounts: counts}}})
	if n := idx.EntryCountsByModel()["m"]; n != 0 {
		t.Fatalf("empty hash_scheme should drop chain ingest, got %d entries", n)
	}
}

// TestChainIngestOneSidedHashesOnlyDropped covers the asymmetric malformed
// shape (BlockHashes set but BlockTokenCounts empty). Symmetric to the
// existing mismatched-length test; both paths must drop fail-soft.
func TestChainIngestOneSidedHashesOnlyDropped(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{
			BlockHashes:      [][]byte{[]byte("b1"), []byte("b2")},
			BlockTokenCounts: nil,
		}}})
	if n := idx.EntryCountsByModel()["m"]; n != 0 {
		t.Fatalf("hashes-only chain should drop, got %d entries", n)
	}
}

// TestChainIngestOneSidedCountsOnlyDropped covers the inverse asymmetric
// shape (counts set but hashes empty). Without this guard the entry would
// silently fall through to the legacy single-blob path with an empty
// PrefixHash key — a wrong-hint surface area.
func TestChainIngestOneSidedCountsOnlyDropped(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{
			PrefixHash:       []byte("legacy-p"),
			TokenCount:       64,
			BlockHashes:      nil,
			BlockTokenCounts: []int32{16, 16},
		}}})
	if n := idx.EntryCountsByModel()["m"]; n != 0 {
		t.Fatalf("counts-only chain should drop (must not downgrade to legacy), got %d entries", n)
	}
}

// TestChainIngestWithCoSetLegacyPrefixHashPreservesBoth covers v1alpha1
// backward-compat: a producer that sets BOTH the new chain (block_hashes)
// and the legacy single-blob (PrefixHash) on the same PrefixEntry must
// have BOTH representations indexed. The chain enables longest-prefix
// matching for new clients; the legacy key keeps unmigrated callers
// (legacy LookupRoute on prefix_hash) hitting.
func TestChainIngestWithCoSetLegacyPrefixHashPreservesBoth(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	hashes, counts := chain("b1", "b2", "b3")
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{
			PrefixHash:       []byte("legacy-full"),
			TokenCount:       128,
			BlockHashes:      hashes,
			BlockTokenCounts: counts,
		}}})

	// Chain lookup hits the per-block entries.
	gotChain := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		BlockHashes: hashes, BlockTokenCounts: counts})
	if len(gotChain) != 1 || gotChain[0].ReplicaID != "r" || gotChain[0].MatchedTokens != 48 {
		t.Fatalf("chain lookup against co-set entry should hit all 3 blocks: got %+v", gotChain)
	}

	// Legacy lookup on the co-set PrefixHash MUST still hit (backward-compat).
	gotLegacy := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: []byte("legacy-full")})
	if len(gotLegacy) != 1 || gotLegacy[0].ReplicaID != "r" || gotLegacy[0].MatchedTokens != 128 {
		t.Fatalf("legacy lookup against co-set entry must still hit prefix_hash with TokenCount=128: got %+v", gotLegacy)
	}
}

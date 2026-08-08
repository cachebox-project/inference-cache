// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"testing"
	"time"
)

func TestLookupUnknownPrefixIsEmpty(t *testing.T) {
	idx := New()
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("known"), TokenCount: 10}}})

	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("missing")}); len(got) != 0 {
		t.Fatalf("unknown prefix should yield no scores, got %d", len(got))
	}
}

func TestHashSchemeIsolatesMatches(t *testing.T) {
	idx := New()
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}}})

	// Same bytes, different scheme → must not match (engine hashes stay disjoint).
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "sglang", PrefixHash: hash("p")}); len(got) != 0 {
		t.Fatalf("cross-scheme match leaked: got %d scores", len(got))
	}
}

// TestNoCrossEngineFalseHitVLLMvsSGLang is the second-engine no-cross-engine-
// false-hit guarantee: with a
// vLLM replica and a SGLang replica BOTH holding a bytewise-identical prefix
// (same tenant, model, and prefix_hash bytes — exactly the collision the
// hash_scheme tag exists to keep disjoint), a lookup under one scheme must
// return ONLY that engine's replica and never the other's. This is the
// stronger form of TestHashSchemeIsolatesMatches (which only checks the
// empty-other-scheme miss): here both schemes are populated, so it proves the
// tag — not the absence of the other entry — is what isolates them.
func TestNoCrossEngineFalseHitVLLMvsSGLang(t *testing.T) {
	idx := New()
	const (
		tenant = "t"
		model  = "shared-model"
	)
	// Identical prefix bytes recorded by each engine under its own scheme.
	prefix := hash("the quick brown fox")
	idx.Ingest(Update{ReplicaID: "vllm-replica-0", Model: model, Tenant: tenant, HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: prefix, TokenCount: 32}}})
	idx.Ingest(Update{ReplicaID: "sglang-replica-0", Model: model, Tenant: tenant, HashScheme: "sglang",
		Prefixes: []PrefixRef{{PrefixHash: prefix, TokenCount: 32}}})

	// A request hashed under the SGLang scheme matches ONLY the SGLang replica.
	sglangScores := idx.Lookup(LookupRequest{Model: model, Tenant: tenant, HashScheme: "sglang", PrefixHash: prefix})
	if len(sglangScores) != 1 || sglangScores[0].ReplicaID != "sglang-replica-0" {
		t.Fatalf("sglang lookup = %+v, want exactly [sglang-replica-0] (no cross-engine false hit on the vLLM entry)", sglangScores)
	}

	// And the symmetric direction: a vLLM-scheme request matches ONLY the vLLM replica.
	vllmScores := idx.Lookup(LookupRequest{Model: model, Tenant: tenant, HashScheme: "vllm", PrefixHash: prefix})
	if len(vllmScores) != 1 || vllmScores[0].ReplicaID != "vllm-replica-0" {
		t.Fatalf("vllm lookup = %+v, want exactly [vllm-replica-0] (no cross-engine false hit on the SGLang entry)", vllmScores)
	}
}

func TestEmptyHashSchemeFailsOpen(t *testing.T) {
	idx := New()

	// An update without a hash_scheme must not be indexed (can't be matched safely).
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}}})
	if n := idx.EntryCountsByModel()["m"]; n != 0 {
		t.Fatalf("entries indexed without a hash_scheme = %d, want 0", n)
	}

	// A lookup without a hash_scheme returns no hint, even if a real entry exists.
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}}})
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "", PrefixHash: hash("p")}); len(got) != 0 {
		t.Fatalf("lookup without a hash_scheme should fail open, got %+v", got)
	}
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p")}); len(got) != 1 {
		t.Fatalf("sanity: scoped lookup should still match, got %d", len(got))
	}
}

func TestTenantIsolation(t *testing.T) {
	idx := New()
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "tenant-a", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}}})

	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "tenant-b", HashScheme: "vllm", PrefixHash: hash("p")}); len(got) != 0 {
		t.Fatalf("tenant-b saw tenant-a's entry: %d scores", len(got))
	}
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "tenant-a", HashScheme: "vllm", PrefixHash: hash("p")}); len(got) != 1 {
		t.Fatalf("tenant-a should see its own entry, got %d", len(got))
	}
}

// TestLookupRouteOrchestratorStrategies is the table-driven proof that the
// LookupRoute orchestrator picks the right strategy for each scenario:
// prefix-match, tenant-hot fallback, full miss. Adding a future strategy
// (e.g. longest-prefix block matching) plugs in here as one more row.
func TestLookupRouteOrchestratorStrategies(t *testing.T) {
	const (
		tenant = "t"
		model  = "m"
		scheme = "vllm"
	)
	hashFor := func(s string) []byte { return hash(s) }

	tests := []struct {
		name       string
		ingest     []Update // state to populate before lookup
		req        LookupRequest
		wantStrat  Strategy
		wantFirst  string // expected top-ranked replica id, "" if no scores
		wantScores int    // expected number of scores
	}{
		{
			name: "exact prefix match wins over a warm tenant",
			ingest: []Update{
				{ReplicaID: "prefix-holder", Model: model, Tenant: tenant, HashScheme: scheme,
					Prefixes: []PrefixRef{{PrefixHash: hashFor("p"), TokenCount: 32}},
					Stats:    &ReplicaStats{HitRate: 0.9}},
				{ReplicaID: "warm-only", Model: model, Tenant: tenant, HashScheme: scheme,
					Stats: &ReplicaStats{HitRate: 0.9}},
			},
			req:        LookupRequest{Model: model, Tenant: tenant, HashScheme: scheme, PrefixHash: hashFor("p")},
			wantStrat:  StrategyPrefixMatch,
			wantFirst:  "prefix-holder",
			wantScores: 1,
		},
		{
			name: "tenant-hot fallback on prefix miss with warm replica",
			ingest: []Update{
				{ReplicaID: "warm", Model: model, Tenant: tenant, HashScheme: scheme,
					Prefixes: []PrefixRef{{PrefixHash: hashFor("other"), TokenCount: 1}},
					Stats:    &ReplicaStats{HitRate: 0.7, Pressure: 0.1}},
				{ReplicaID: "cold", Model: model, Tenant: tenant, HashScheme: scheme,
					Prefixes: []PrefixRef{{PrefixHash: hashFor("other"), TokenCount: 1}},
					Stats:    &ReplicaStats{HitRate: 0.0, Pressure: 0.5}},
			},
			req:        LookupRequest{Model: model, Tenant: tenant, HashScheme: scheme, PrefixHash: hashFor("novel")},
			wantStrat:  StrategyTenantHot,
			wantFirst:  "warm",
			wantScores: 1, // cold replica filtered by hit_rate threshold
		},
		{
			// Stats-only ingest registers no prefix entries → the prefix map is
			// globally empty → the cold-start carve-out keeps this on the
			// fail-open NO_HINT path. The no-replica-leak intent is preserved
			// by the wantScores==0 assertion.
			name: "stats-only ingest, novel prefix → StrategyNone (globally empty prefix map)",
			ingest: []Update{
				{ReplicaID: "cold", Model: model, Tenant: tenant, HashScheme: scheme,
					Stats: &ReplicaStats{HitRate: 0.0}},
			},
			req:        LookupRequest{Model: model, Tenant: tenant, HashScheme: scheme, PrefixHash: hashFor("novel")},
			wantStrat:  StrategyNone,
			wantScores: 0,
		},
		{
			// Empty index = cold start. The cold-start carve-out short-circuits
			// classifyMiss to NO_HINT so a freshly-started server does not flood
			// every gateway with UNKNOWN_TENANT until the first ReportCacheState
			// lands. The diagnostic resumes the moment any prefix is reported.
			name:       "empty index → StrategyNone (cold-start carve-out)",
			ingest:     nil,
			req:        LookupRequest{Model: model, Tenant: tenant, HashScheme: scheme, PrefixHash: hashFor("novel")},
			wantStrat:  StrategyNone,
			wantScores: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx := New(WithRanker(DefaultRankerConfig()))
			for _, u := range tc.ingest {
				idx.Ingest(u)
			}
			res := idx.LookupRoute(tc.req)
			if res.Strategy != tc.wantStrat {
				t.Fatalf("strategy = %v, want %v (scores %+v)", res.Strategy, tc.wantStrat, res.Scores)
			}
			if len(res.Scores) != tc.wantScores {
				t.Fatalf("got %d scores, want %d (%+v)", len(res.Scores), tc.wantScores, res.Scores)
			}
			if tc.wantFirst != "" && res.Scores[0].ReplicaID != tc.wantFirst {
				t.Errorf("top rank = %q, want %q", res.Scores[0].ReplicaID, tc.wantFirst)
			}
		})
	}
}

// TestTenantHotRecencyClampedAgainstClockSkew guards that a future
// statsReported timestamp (e.g. from clock skew between the replica and the
// server) is clamped to recency=1 rather than producing recency>1, which
// would otherwise amplify both the score and the SLO bias factor and let a
// stale-but-future-stamped replica outrank everyone else. Mirrors
// freshnessAt's `age <= 0 → 1` clamp on the prefix-match path.
func TestTenantHotRecencyClampedAgainstClockSkew(t *testing.T) {
	clk := &fakeClock{t: time.Unix(13_500_000, 0)}
	cfg := DefaultRankerConfig()
	idx := New(withClock(clk.now), WithTTL(time.Hour), WithRanker(cfg))

	// Ingest serving prefix + stats normally so the replica qualifies.
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("other"), TokenCount: 1}},
		Stats:    &ReplicaStats{HitRate: 0.5, Pressure: 0}})

	// Now move the clock BACKWARDS so the previously-stored statsReported
	// is in the "future" relative to now — i.e. simulate a server-side clock
	// step backwards while a replica's report is in flight.
	clk.t = clk.t.Add(-2 * time.Minute)

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyTenantHot || len(res.Scores) != 1 {
		t.Fatalf("expected TENANT_HOT candidate, got %v (%+v)", res.Strategy, res.Scores)
	}
	// With recency clamped to 1, and PressureWeight default 1 × pressure 0
	// → pressureFactor 1, and no SLO budget set → sloBias 1:
	//   score = hit_rate × 1 × 1 × 1 = 0.5.
	// Without the clamp, recency could exceed 1 and amplify the score.
	if got := res.Scores[0].Score; got > 0.5 {
		t.Fatalf("recency not clamped against clock skew: score = %v, want <= 0.5", got)
	}
}

// TestTenantHotMatchedTokensIsZero pins a contract detail: a TENANT_HOT
// candidate carries MatchedTokens=0 because there is no prefix overlap. A
// gateway client that filters or weights by MatchedTokens must therefore
// treat 0 as "softer hint" rather than "no overlap → ignore"; the reason_code
// is the load-bearing signal. Ingests an unrelated prefix entry under the
// requested hash_scheme so the replica clears the engine-domain guard.
func TestTenantHotMatchedTokensIsZero(t *testing.T) {
	idx := New(WithRanker(DefaultRankerConfig()))
	idx.Ingest(Update{ReplicaID: "warm", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("other"), TokenCount: 1}},
		Stats:    &ReplicaStats{HitRate: 0.8}})

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyTenantHot || len(res.Scores) != 1 {
		t.Fatalf("expected single TENANT_HOT candidate, got strat=%v scores=%+v", res.Strategy, res.Scores)
	}
	if res.Scores[0].MatchedTokens != 0 {
		t.Fatalf("TENANT_HOT MatchedTokens must be 0 (no prefix overlap), got %d", res.Scores[0].MatchedTokens)
	}
}

// TestTenantHotHonorsHitRateThreshold pins the warmth threshold: a replica
// with hit_rate below TenantHotMinHitRate is "not warm enough" to be a
// useful hint, even if it was reported recently AND serves the engine
// domain.
func TestTenantHotHonorsHitRateThreshold(t *testing.T) {
	cfg := DefaultRankerConfig()
	cfg.TenantHotMinHitRate = 0.5
	idx := New(WithRanker(cfg))

	idx.Ingest(Update{ReplicaID: "tepid", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("other"), TokenCount: 1}},
		Stats:    &ReplicaStats{HitRate: 0.2}}) // below the 0.5 threshold

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone {
		t.Fatalf("below-threshold hit_rate should NOT trigger TENANT_HOT, got %v (%+v)", res.Strategy, res.Scores)
	}
}

// TestTenantHotDisabledByZeroMaxAge proves the kill-switch: a RankerConfig
// with TenantHotMaxAge=0 disables the soft locality fallback, so a
// same-key prefix miss (the case set up below — (t, m, vllm) populated,
// only this prefix novel) lands at StrategyNone (NO_HINT) instead of
// TENANT_HOT. The miss-classifier still runs for mismatched contract keys
// — see the dedicated diagnostics tests; this test pins only the
// kill-switch behavior on the same-key path.
func TestTenantHotDisabledByZeroMaxAge(t *testing.T) {
	cfg := DefaultRankerConfig()
	cfg.TenantHotMaxAge = 0 // explicit disable
	idx := New(WithRanker(cfg))

	idx.Ingest(Update{ReplicaID: "warm", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("other"), TokenCount: 1}},
		Stats:    &ReplicaStats{HitRate: 0.9}})

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone {
		t.Fatalf("TenantHotMaxAge=0 must disable fallback; got %v (%+v)", res.Strategy, res.Scores)
	}
}

// TestTenantHotRequiresReplicaServingRequestedScheme guards the
// engine-domain check: a replica with high hit_rate stats but NO prefix
// entries in the requested hash_scheme cannot become a TENANT_HOT hint —
// it isn't proven to serve this engine. Otherwise stats-only updates (or
// updates under a different scheme) could leak into hints for the wrong
// domain. The replica below holds a prefix only under "sglang"; a lookup
// under "vllm" must NOT promote it via TENANT_HOT.
//
// Post-diagnostics: this is also the canonical UNKNOWN_HASH_SCHEME diagnostic
// shape — (t, m) populated under sglang, the lookup asks under vllm — so
// the classifier now surfaces the more specific code. The leak guarantee is
// unchanged: no replica from another scheme ever appears in Scores.
func TestTenantHotRequiresReplicaServingRequestedScheme(t *testing.T) {
	idx := New(WithRanker(DefaultRankerConfig()))
	idx.Ingest(Update{ReplicaID: "wrong-engine", Model: "m", Tenant: "t", HashScheme: "sglang",
		Prefixes: []PrefixRef{{PrefixHash: hash("other"), TokenCount: 1}},
		Stats:    &ReplicaStats{HitRate: 0.9}})

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyUnknownHashScheme {
		t.Fatalf("(t, m) populated under sglang but the lookup asks under vllm: must surface UNKNOWN_HASH_SCHEME; got %v (%+v)",
			res.Strategy, res.Scores)
	}
	if len(res.Scores) != 0 {
		t.Fatalf("UNKNOWN_HASH_SCHEME must carry no scores (no cross-scheme leak); got %+v", res.Scores)
	}
}

// TestTenantHotDropsReplicaAfterPrefixSweep guards that the TENANT_HOT
// fallback stops promoting a replica once the sweeper has evicted its last
// serving prefix entry. The secondary servingByScope index gives TENANT_HOT
// an O(1) "does R serve scope S?" check; removeReplicaLocked must keep it
// consistent with i.prefixes, so a stale entry that's been swept no longer
// counts as proof of serving. (Before the sweep runs, soft-state semantics
// allow a stale entry to keep the replica "serving" — at worst a suboptimal
// hint, not a wrong answer; the sweep then cleans it.)
func TestTenantHotDropsReplicaAfterPrefixSweep(t *testing.T) {
	clk := &fakeClock{t: time.Unix(11_500_000, 0)}
	cfg := DefaultRankerConfig()
	cfg.TenantHotMaxAge = time.Hour // warm window much wider than the TTL
	idx := New(withClock(clk.now), WithTTL(10*time.Minute), WithRanker(cfg))

	// Ingest a serving prefix; with warm stats this replica qualifies.
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("other"), TokenCount: 1}},
		Stats:    &ReplicaStats{HitRate: 0.9}})

	// Sanity: pre-sweep the replica is a TENANT_HOT candidate.
	if res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t",
		HashScheme: "vllm", PrefixHash: hash("novel")}); res.Strategy != StrategyTenantHot {
		t.Fatalf("pre-sweep should be TENANT_HOT, got %v", res.Strategy)
	}

	// Advance past the prefix TTL but NOT past TenantHotMaxAge, then refresh
	// stats so the stats entry stays warm/recent. The prefix entry is now
	// stale but not yet swept — soft-state semantics tolerate one more hint.
	clk.add(15 * time.Minute)
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Stats: &ReplicaStats{HitRate: 0.9}})

	// Run the sweep: the stale prefix is removed from i.prefixes AND from
	// the servingByScope secondary index (via removeReplicaLocked). The
	// stats are still fresh under TenantHotMaxAge, but the replica is no
	// longer serving the requested scheme → TENANT_HOT must NOT fire.
	idx.evictExpired()

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	// Sweep drops the only prefix → the index is now globally empty for the
	// prefix map → cold-start carve-out short-circuits classifyMiss to NO_HINT.
	// The original "TENANT_HOT must NOT fire" intent is preserved (Scores
	// empty); reverting to NO_HINT instead of a diagnostic is correct here
	// because there is no other-tenant data to compare against.
	if res.Strategy != StrategyNone {
		t.Fatalf("after sweep, replica with no live serving prefix must NOT enable TENANT_HOT; got %v (%+v)",
			res.Strategy, res.Scores)
	}
	if len(res.Scores) != 0 {
		t.Fatalf("post-sweep response must carry no scores; got %+v", res.Scores)
	}
}

// TestTenantHotIsolatedByTenant guards that a warm replica in tenant-a's
// index can never leak into tenant-b's TENANT_HOT fallback — per-tenant
// isolation is a hard constraint of the index regardless of strategy.
//
// Setup is stats-only ingest, so the prefix map is globally empty → the
// cold-start carve-out keeps the response on NO_HINT. The no-leak property
// the test guards (no tenant-a replica appears in tenant-b's Scores) is
// preserved by the wantScores==0 assertion. The asymmetric UNKNOWN_TENANT
// case (tenant-a populated with REAL prefixes, lookup for tenant-b) is
// covered by TestLookupRouteUnknownTenantOnlyWhenIndexHasData in
// diagnostics_test.go.
func TestTenantHotIsolatedByTenant(t *testing.T) {
	idx := New(WithRanker(DefaultRankerConfig()))
	idx.Ingest(Update{ReplicaID: "warm-a", Model: "m", Tenant: "tenant-a", HashScheme: "vllm",
		Stats: &ReplicaStats{HitRate: 0.9}})

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "tenant-b", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone {
		t.Fatalf("tenant-b lookup leaked tenant-a's warm replica: %+v", res)
	}
	if len(res.Scores) != 0 {
		t.Fatalf("response must carry no scores (no cross-tenant leak); got %+v", res.Scores)
	}
}

// TestLookupRouteEmptyHashSchemeFailsOpenAcrossStrategies guards that an
// unspecified hash_scheme produces NO_HINT through BOTH ranking paths, not
// just the prefix-match one. The TENANT_HOT fallback keys only on
// (tenant, model) and would otherwise still emit a hint based on stats
// alone — but a request whose engine domain we can't identify must fail
// open, per the soft-state / fail-open contract (PROJECT_CONTEXT §5).
func TestLookupRouteEmptyHashSchemeFailsOpenAcrossStrategies(t *testing.T) {
	idx := New(WithRanker(DefaultRankerConfig()))
	// Warm replica with high hit_rate would normally qualify for TENANT_HOT.
	idx.Ingest(Update{ReplicaID: "warm", Model: "m", Tenant: "t", HashScheme: "vllm",
		Stats: &ReplicaStats{HitRate: 0.9}})

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t",
		HashScheme: "", PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone || len(res.Scores) != 0 {
		t.Fatalf("empty hash_scheme must fail open; got strategy=%v scores=%+v",
			res.Strategy, res.Scores)
	}
}

// TestTenantHotIsolatedByModel guards the analogous model isolation: a warm
// replica for model A in tenant t doesn't surface for model B in tenant t.
// Different models have disjoint cache state; mixing them would mis-hint.
//
// Stats-only ingest registers no prefix entries → prefix map globally empty
// → cold-start carve-out keeps the response on NO_HINT. The no-leak
// property is preserved by wantScores==0. The asymmetric UNKNOWN_MODEL case
// (model-a populated with REAL prefixes, lookup for model-b) is covered by
// TestLookupRouteClassifiesUnknownModel in diagnostics_test.go.
func TestTenantHotIsolatedByModel(t *testing.T) {
	idx := New(WithRanker(DefaultRankerConfig()))
	idx.Ingest(Update{ReplicaID: "warm", Model: "model-a", Tenant: "t", HashScheme: "vllm",
		Stats: &ReplicaStats{HitRate: 0.9}})

	res := idx.LookupRoute(LookupRequest{Model: "model-b", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone {
		t.Fatalf("model-b lookup leaked model-a's warm replica: %+v", res)
	}
	if len(res.Scores) != 0 {
		t.Fatalf("response must carry no scores (no cross-model leak); got %+v", res.Scores)
	}
}

// TestChainLookupReturnsLongestCommonPrefix is the core longest-prefix behavior:
// two replicas hold different 5-block chains; the one sharing more leading
// blocks with the request wins, and matched_tokens reflects the partial run
// (3 × 16 = 48), not the full request chain (80).
func TestChainLookupReturnsLongestCommonPrefix(t *testing.T) {
	idx := New(WithTTL(time.Hour))

	reqHashes, reqCounts := chain("b1", "b2", "b3", "b4", "b5")
	hashesA, countsA := chain("b1", "b2", "b3", "x4", "x5")
	idx.Ingest(Update{ReplicaID: "replica-a", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: hashesA, BlockTokenCounts: countsA}}})
	hashesB, countsB := chain("b1", "b2", "y3", "y4", "y5")
	idx.Ingest(Update{ReplicaID: "replica-b", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: hashesB, BlockTokenCounts: countsB}}})

	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		BlockHashes: reqHashes, BlockTokenCounts: reqCounts})
	if len(got) != 2 {
		t.Fatalf("expected 2 replica scores (both share at least block 0), got %d: %+v", len(got), got)
	}
	if got[0].ReplicaID != "replica-a" || got[0].MatchedTokens != 48 {
		t.Fatalf("replica-a should win with matched_tokens=48 (3 blocks × 16); got %+v", got[0])
	}
	if got[1].ReplicaID != "replica-b" || got[1].MatchedTokens != 32 {
		t.Fatalf("replica-b should follow with matched_tokens=32 (2 blocks × 16); got %+v", got[1])
	}
}

// TestChainLookupFullChainMatch confirms a replica that holds the entire
// request chain reports matched_tokens equal to the full chain's token count.
func TestChainLookupFullChainMatch(t *testing.T) {
	idx := New(WithTTL(time.Hour))

	hashes, counts := chain("b1", "b2", "b3", "b4")
	idx.Ingest(Update{ReplicaID: "replica-a", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: hashes, BlockTokenCounts: counts}}})

	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		BlockHashes: hashes, BlockTokenCounts: counts})
	if len(got) != 1 || got[0].ReplicaID != "replica-a" || got[0].MatchedTokens != 64 {
		t.Fatalf("expected single full-chain hit for replica-a with matched_tokens=64, got %+v", got)
	}
}

// TestChainLookupReportsColdestTierAcrossRun exercises worstTier folding: a
// run that spans a T2 head block and a T1 tail block summarizes to the tier
// the replica can serve the ENTIRE run from — T2, the constraining block's
// tier. Claiming T1 would overstate the hint: serving the full matched prefix
// means touching the T2-only block.
func TestChainLookupReportsColdestTierAcrossRun(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	hashes, counts := chain("b1", "b2")
	// Two entries for the same replica under different tiers: block b1 held in
	// T2, block b2 in T1. A single Update expands one tier across its blocks,
	// so report the two blocks as separate prefixes to mix tiers within the run.
	idx.Ingest(Update{ReplicaID: "replica-a", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{
			{BlockHashes: hashes[:1], BlockTokenCounts: counts[:1], Tier: TierT2},
			{BlockHashes: hashes[1:], BlockTokenCounts: counts[1:], Tier: TierT1},
		}})

	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		BlockHashes: hashes, BlockTokenCounts: counts})
	if len(got) != 1 || got[0].MatchedTokens != 32 {
		t.Fatalf("expected a full 2-block run (matched_tokens=32); got %+v", got)
	}
	if got[0].Tier != TierT2 {
		t.Fatalf("mixed-tier run must summarize to the coldest tier (T2 — the constraining block); got %v", got[0].Tier)
	}
}

// TestChainLookupNoOverlapReturnsEmpty: zero shared blocks → no hint. Guards
// against the longest-prefix walk silently returning matched_tokens=0 scores.
func TestChainLookupNoOverlapReturnsEmpty(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	hashesHeld, countsHeld := chain("h1", "h2", "h3")
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: hashesHeld, BlockTokenCounts: countsHeld}}})
	reqHashes, reqCounts := chain("q1", "q2", "q3")
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		BlockHashes: reqHashes, BlockTokenCounts: reqCounts}); len(got) != 0 {
		t.Fatalf("no overlap should yield no-hint, got %+v", got)
	}
}

// TestChainLookupHashSchemeIsolation guards cross-engine isolation: a chain
// stored under vllm must not match the same byte chain looked up under sglang.
func TestChainLookupHashSchemeIsolation(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	hashes, counts := chain("b1", "b2", "b3")
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: hashes, BlockTokenCounts: counts}}})
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "sglang",
		BlockHashes: hashes, BlockTokenCounts: counts}); len(got) != 0 {
		t.Fatalf("cross-scheme chain lookup leaked: %+v", got)
	}
}

// TestChainLookupRunFreshnessIsWeakestLink shows the oldest matched block
// caps the run's freshness — a stale block in the middle of the chain
// pulls the whole run's score down rather than letting a fresh tail
// disguise an aging hold.
func TestChainLookupRunFreshnessIsWeakestLink(t *testing.T) {
	clk := &fakeClock{t: time.Unix(7_000_000, 0)}
	idx := New(withClock(clk.now), WithTTL(10*time.Minute))

	hashes0, counts0 := chain("b1")
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: hashes0, BlockTokenCounts: counts0}}})
	clk.add(8 * time.Minute) // b1 now 8m old → freshness ~0.2 at TTL=10m
	hashesRest, countsRest := chain("b2", "b3")
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: hashesRest, BlockTokenCounts: countsRest}}})

	reqHashes, reqCounts := chain("b1", "b2", "b3")
	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		BlockHashes: reqHashes, BlockTokenCounts: reqCounts})
	if len(got) != 1 {
		t.Fatalf("expected one replica with the full chain, got %+v", got)
	}
	if got[0].EstimatedCacheHitProb >= 0.5 {
		t.Fatalf("freshness should reflect the oldest block (~0.2), got %v", got[0].EstimatedCacheHitProb)
	}
}

// TestChainLookupMismatchedLengthsFailOpen mirrors the Ingest-side guarantee
// (TestChainIngestMismatchedLengthsDropped): when a request carries a chain
// whose parallel arrays disagree in length, the lookup MUST return no hint
// rather than silently downgrade to legacy exact-match on PrefixHash —
// otherwise a chain-aware client with a producer bug could surface an
// unrelated legacy entry as a partial-prefix match.
func TestChainLookupMismatchedLengthsFailOpen(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	// Seed a legacy single-blob entry under PrefixHash="legacy-p" so the bug
	// would manifest as a wrong hit if the lookup fell through.
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("legacy-p"), TokenCount: 128}}})
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash:       hash("legacy-p"),
		BlockHashes:      [][]byte{[]byte("b1"), []byte("b2")},
		BlockTokenCounts: []int32{16}, // length 1 vs 2 — malformed
	}); len(got) != 0 {
		t.Fatalf("malformed chain must fail open (NO_HINT), got %+v — would have leaked legacy hit", got)
	}
}

// TestChainLookupOneSidedCountsOnlyFailsOpen guards the lookup side of the
// same shape: a request with BlockTokenCounts set but no BlockHashes is
// malformed and must return NO_HINT, not fall back to legacy exact-match.
func TestChainLookupOneSidedCountsOnlyFailsOpen(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("legacy-p"), TokenCount: 128}}})
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash:       hash("legacy-p"),
		BlockTokenCounts: []int32{16, 16},
	}); len(got) != 0 {
		t.Fatalf("counts-only chain lookup must fail open, got %+v", got)
	}
}

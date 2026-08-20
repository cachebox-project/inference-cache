// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"testing"
	"time"
)

func TestDefaultRankerConfigMatchesStableProductionTuple(t *testing.T) {
	got := DefaultRankerConfig()
	want := RankerConfig{
		PressureWeight:      1,
		SLOTightTTFTMs:      200,
		SLOTightBias:        1,
		TenantHotMinHitRate: 0.1,
		TenantHotMaxAge:     5 * time.Minute,
	}
	if got != want {
		t.Fatalf("DefaultRankerConfig() = %+v, want stable production tuple %+v", got, want)
	}
}

// TestLookupPressureAndSLOFactorsCollapseToUnityWhenSignalsAbsent locks in the
// contract that the pressure and SLO score factors collapse to 1 when (a) no
// replica stats are reported (pressure=0) and (b) the request carries no SLO
// hint (TTFT=0). The distinguishing-power factor still applies (it depends on
// cluster cardinality, not on these signals), so the expected scores below
// fold it in (1 - 2/3 = 1/3 with the decoy replica below) — the test is about
// the pressure/SLO contribution being 1, not about the score being equal to
// matched_tokens × freshness alone.
func TestLookupPressureAndSLOFactorsCollapseToUnityWhenSignalsAbsent(t *testing.T) {
	clk := &fakeClock{t: time.Unix(6_000_000, 0)}
	idx := New(withClock(clk.now), WithTTL(time.Hour))

	idx.Ingest(Update{ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 80}}})
	idx.Ingest(Update{ReplicaID: "r2", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 40}}})
	// Decoy replica holds a different prefix in the same engine domain so
	// the distinguishing-power factor is not zero (matching=2 < total=3).
	// Factor = 1 - 2/3 = 1/3, so expected scores are 80/3 and 40/3.
	idx.Ingest(Update{ReplicaID: "r3-decoy", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("decoy"), TokenCount: 1}}})

	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p")})
	if len(got) != 2 {
		t.Fatalf("expected 2 scores, got %d", len(got))
	}
	// freshness == 1, no pressure, no SLO, distinguishing_power = 1/3.
	approxEq := func(got, want float32) bool { return got-want > -1e-3 && got-want < 1e-3 }
	if got[0].ReplicaID != "r1" || !approxEq(got[0].Score, 80.0/3.0) {
		t.Fatalf("r1 baseline score = %v (id %q), want 80/3 (~26.67) — matched_tokens × freshness × distinguishing_power(2,3)", got[0].Score, got[0].ReplicaID)
	}
	if got[1].ReplicaID != "r2" || !approxEq(got[1].Score, 40.0/3.0) {
		t.Fatalf("r2 baseline score = %v (id %q), want 40/3 (~13.33)", got[1].Score, got[1].ReplicaID)
	}
}

// TestLookupPressureAwareRanking walks a table of pressure profiles and
// asserts how the ordering changes vs. the baseline. The point is to show
// the ranker balances locality against load: a replica that holds the prefix
// but is saturated should yield to a fresher, less-loaded peer.
func TestLookupPressureAwareRanking(t *testing.T) {
	type replica struct {
		id         string
		tokenCount int32
		pressure   float32
	}
	tests := []struct {
		name      string
		pressureW float32
		replicas  []replica
		wantOrder []string // expected ReplicaID order, best first
	}{
		{
			// Both replicas hold the prefix with identical token count and
			// freshness. The only differentiator is pressure → low-pressure wins.
			name:      "equal tokens, pressure breaks the tie",
			pressureW: 1.0,
			replicas: []replica{
				{id: "saturated", tokenCount: 100, pressure: 0.9},
				{id: "idle", tokenCount: 100, pressure: 0.0},
			},
			wantOrder: []string{"idle", "saturated"},
		},
		{
			// The token-rich replica is also saturated (pressure=0.9, weight=1
			// → factor 0.1); a smaller-tokencount peer at low pressure can
			// overtake it.
			name:      "pressure flips locality vs. load",
			pressureW: 1.0,
			replicas: []replica{
				{id: "big-but-hot", tokenCount: 100, pressure: 0.9}, // 100 × 0.1 = 10
				{id: "small-cool", tokenCount: 50, pressure: 0.0},   // 50 × 1.0 = 50
			},
			wantOrder: []string{"small-cool", "big-but-hot"},
		},
		{
			// PressureWeight=0 → pressure factor collapses to 1 → ordering
			// matches the baseline (token count wins). This is the toggle a
			// future calibration could use to disable the penalty without
			// touching code paths.
			name:      "PressureWeight=0 disables the penalty",
			pressureW: 0.0,
			replicas: []replica{
				{id: "big-hot", tokenCount: 100, pressure: 0.9},
				{id: "small-cool", tokenCount: 50, pressure: 0.0},
			},
			wantOrder: []string{"big-hot", "small-cool"},
		},
		{
			// pressure > 1/weight clamps to 0: a replica with pressure 1.5
			// under weight 1 would otherwise produce a negative score and
			// silently outrank a 0-score peer due to sort stability.
			name:      "pressure factor clamps to zero",
			pressureW: 1.0,
			replicas: []replica{
				{id: "broken", tokenCount: 100, pressure: 1.5}, // factor → 0
				{id: "alive", tokenCount: 1, pressure: 0.0},    // factor → 1
			},
			wantOrder: []string{"alive", "broken"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(7_000_000, 0)}
			cfg := DefaultRankerConfig()
			cfg.PressureWeight = tc.pressureW
			idx := New(withClock(clk.now), WithTTL(time.Hour), WithRanker(cfg))

			for _, r := range tc.replicas {
				idx.Ingest(Update{ReplicaID: r.id, Model: "m", Tenant: "t", HashScheme: "vllm",
					Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: r.tokenCount}},
					Stats:    &ReplicaStats{Pressure: r.pressure}})
			}
			// Decoy replica with a different prefix in the same engine
			// domain keeps the distinguishing-power factor strictly
			// positive (matching < total). Without it, when EVERY
			// replica in the test holds hash("p"), the factor zeroes
			// every score and the ordering this test asserts collapses
			// to a meaningless lexicographic tiebreak.
			idx.Ingest(Update{ReplicaID: "zzz-decoy", Model: "m", Tenant: "t", HashScheme: "vllm",
				Prefixes: []PrefixRef{{PrefixHash: hash("decoy"), TokenCount: 1}}})

			got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p")})
			if len(got) != len(tc.wantOrder) {
				t.Fatalf("got %d scores, want %d (%+v)", len(got), len(tc.wantOrder), got)
			}
			for i, want := range tc.wantOrder {
				if got[i].ReplicaID != want {
					t.Errorf("rank %d = %q, want %q (full: %+v)", i, got[i].ReplicaID, want, got)
				}
			}
		})
	}
}

// TestLookupSLOAwareRankingBiasesFreshness exercises the tight-TTFT bias.
// Two replicas hold the prefix; one has many tokens but is older, the other
// fewer tokens and fresh. Without SLO pressure the token-rich older one wins
// (B6 baseline). Under tight SLO (ttft_ms below threshold) the freshness bias
// kicks in and the fresh one overtakes; under loose SLO the baseline ordering
// is restored. Table-shaped so adding bands (e.g. P95 vs P99 budgets) is easy.
func TestLookupSLOAwareRankingBiasesFreshness(t *testing.T) {
	clk := &fakeClock{t: time.Unix(8_000_000, 0)}

	cfg := DefaultRankerConfig()
	cfg.SLOTightTTFTMs = 100
	cfg.SLOTightBias = 5.0 // strong bias so the flip is unambiguous
	idx := New(withClock(clk.now), WithTTL(time.Hour), WithRanker(cfg))

	// big-old: 100 tokens, 20m old (freshness ≈ 2/3).
	// small-fresh: 50 tokens, just reported (freshness = 1).
	// Baseline:  big-old ≈ 66.7 ; small-fresh = 50 → big-old wins.
	// Tight SLO: small-fresh's freshness bonus (1 + 1×5 = 6) dominates
	// big-old's bonus (1 + 0.667×5 ≈ 4.33) → small-fresh wins.
	idx.Ingest(Update{ReplicaID: "big-old", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 100}}, Timestamp: clk.t})
	clk.add(20 * time.Minute)
	idx.Ingest(Update{ReplicaID: "small-fresh", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 50}}, Timestamp: clk.t})
	// Decoy: holds a different prefix in the same engine domain so the
	// distinguishing-power factor stays > 0 (matching=2 < total=3). The
	// factor multiplies both replicas' scores by the same constant, so
	// the SLO-bias ordering this test asserts is preserved.
	idx.Ingest(Update{ReplicaID: "zzz-decoy", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("decoy"), TokenCount: 1}}, Timestamp: clk.t})

	tests := []struct {
		name      string
		ttftMs    int32
		wantFirst string
	}{
		{"no SLO hint (baseline) → token-rich wins", 0, "big-old"},
		{"loose SLO (>= threshold) → no bias, baseline wins", 500, "big-old"},
		{"tight SLO (< threshold) → freshness bias flips ranking", 50, "small-fresh"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := idx.Lookup(LookupRequest{
				Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p"),
				TTFTBudgetMs: tc.ttftMs,
			})
			if len(got) != 2 {
				t.Fatalf("expected 2 scores, got %d (%+v)", len(got), got)
			}
			if got[0].ReplicaID != tc.wantFirst {
				t.Errorf("top rank = %q, want %q (full: %+v)", got[0].ReplicaID, tc.wantFirst, got)
			}
		})
	}
}

// TestLookupSLOBiasDisabledWhenKnobsZero pins the kill-switch: SLOTightBias
// = 0 collapses the bias coefficient to zero, so a tight SLO no longer
// changes ordering. Useful when a calibration regresses and we want the
// strict baseline back without code changes.
func TestLookupSLOBiasDisabledWhenKnobsZero(t *testing.T) {
	clk := &fakeClock{t: time.Unix(8_500_000, 0)}
	cfg := DefaultRankerConfig()
	cfg.SLOTightTTFTMs = 100
	cfg.SLOTightBias = 0 // disabled
	idx := New(withClock(clk.now), WithTTL(time.Hour), WithRanker(cfg))

	idx.Ingest(Update{ReplicaID: "big-old", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 100}}, Timestamp: clk.t})
	clk.add(20 * time.Minute)
	idx.Ingest(Update{ReplicaID: "small-fresh", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 50}}, Timestamp: clk.t})
	// Decoy in the same engine domain (different prefix) keeps the
	// distinguishing-power factor > 0 so the disabled-SLO ordering this
	// test asserts isn't masked by a lexicographic tiebreak.
	idx.Ingest(Update{ReplicaID: "zzz-decoy", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("decoy"), TokenCount: 1}}, Timestamp: clk.t})

	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("p"), TTFTBudgetMs: 50})
	if got[0].ReplicaID != "big-old" {
		t.Fatalf("with SLOTightBias=0, tight SLO must not change ordering; got %+v", got)
	}
}

// TestWorstTierPrefersLeastLocal is a unit check on the across-run fold helper:
// the colder tier (higher enum value) wins, and TierUnspecified poisons the
// fold — an unknown block tier makes the run's tier unknown rather than a
// false claim.
func TestWorstTierPrefersLeastLocal(t *testing.T) {
	cases := []struct{ a, b, want CacheTier }{
		{TierT1, TierT2, TierT2},
		{TierT3, TierT2, TierT3},
		{TierT1, TierT1, TierT1},
		{TierUnspecified, TierT3, TierUnspecified},
		{TierT2, TierUnspecified, TierUnspecified},
		{TierUnspecified, TierUnspecified, TierUnspecified},
	}
	for _, c := range cases {
		if got := worstTier(c.a, c.b); got != c.want {
			t.Errorf("worstTier(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

// TestChainLookupSharesPressureAndSLOFactorsWithExact verifies the chain
// scoring path composes the same pressure and SLO factors as lookupExact —
// the chain walk changes how matched_tokens is computed but the score
// formula is unchanged. Without this, a saturated replica that happens to
// have a chain hit would outrank a fresher idle peer the chain-aware
// formula was supposed to demote.
func TestChainLookupSharesPressureAndSLOFactorsWithExact(t *testing.T) {
	cfg := DefaultRankerConfig()
	cfg.PressureWeight = 1
	idx := New(WithTTL(time.Hour), WithRanker(cfg))
	hashes, counts := chain("b1", "b2", "b3")

	idx.Ingest(Update{ReplicaID: "big-but-hot", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: hashes, BlockTokenCounts: counts}},
		Stats:    &ReplicaStats{Pressure: 0.9}})
	idx.Ingest(Update{ReplicaID: "small-cool", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: hashes[:2], BlockTokenCounts: counts[:2]}},
		Stats:    &ReplicaStats{Pressure: 0.0}})
	// Decoy replica in the same engine domain (different prefix) keeps the
	// distinguishing-power denominator above the per-depth matching count,
	// so the chain factor for small-cool is 1 - 2/3 = 1/3 (not 0). Without
	// it small-cool's score would collapse to 0 (matching at its depth ==
	// total) and big-but-hot's pressure-discounted score would dominate by
	// default, masking the pressure-flips-ranking property this test
	// asserts.
	idx.Ingest(Update{ReplicaID: "zzz-decoy", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("decoy"), TokenCount: 1}}})

	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		BlockHashes: hashes, BlockTokenCounts: counts})
	if len(got) != 2 {
		t.Fatalf("expected 2 chain scores, got %+v", got)
	}
	// With totalReplicas = 3 (big-but-hot + small-cool + decoy):
	// big-but-hot: matched=48, pressureFactor=0.1, dp=1-1/3=2/3 → 48 × 0.1 × 2/3 = 3.2
	// small-cool:  matched=32, pressureFactor=1.0, dp=1-2/3=1/3 → 32 × 1.0 × 1/3 ≈ 10.67
	// Without pressure folding (pressureFactor=1.0 for both),
	// big-but-hot's 48×2/3=32 would tie or beat small-cool's 32×1/3≈10.67.
	if got[0].ReplicaID != "small-cool" {
		t.Fatalf("pressure factor missing from chain score: ranked %+v first (want small-cool)", got[0])
	}
}

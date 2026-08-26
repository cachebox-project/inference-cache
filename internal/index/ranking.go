// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"sort"
	"time"
)

// Defaults for the ranking-v2 knobs (pressure / SLO / tenant-hot fallback).
// Calibrated so the formula reduces to the baseline matchedTokens × freshness
// when no stats are present and no SLO hint is set — see DefaultRankerConfig.
const (
	// Pressure penalty: pressureFactor = 1 - PressureWeight × pressure.
	// 1.0 → a fully-saturated replica (pressure=1.0) drops to score 0, so a
	// fresher lower-pressure peer can win. Lower values are gentler.
	DefaultPressureWeight = 1.0
	// TTFT below this (ms) is treated as "tight" — the SLO bias kicks in.
	// 200 ms is a conservative threshold; tune per workload.
	DefaultSLOTightTTFTMs = 200
	// Tight-SLO bias: sloBias = 1 + freshness × SLOTightBias, applied
	// multiplicatively. Higher → freshness gets weighted more aggressively
	// against matched-token count when latency is critical.
	DefaultSLOTightBias = 1.0
	// TENANT_HOT fallback: replicas with hit_rate >= this count as "warm".
	DefaultTenantHotMinHitRate = 0.1
	// TENANT_HOT fallback: stats lastSeen within this window count as
	// "recent" — anything older is treated as cold for the fallback.
	DefaultTenantHotMaxAge = 5 * time.Minute

	// Admission and the PolicyStore trust boundary enforce these upper bounds
	// so operator input cannot create negative or unbounded score multipliers.
	MaxPressureWeight = 4.0
	MaxSLOTightBias   = 8.0
)

// rankerFor overlays the tenant's presence-aware overrides onto the Index's
// server-wide baseline. Resolving once per lookup keeps a concurrent policy
// replacement from mixing two configs inside one routing decision.
func (i *Index) rankerFor(tenant string) RankerConfig {
	base := i.ranker
	if i.rankerResolver == nil {
		return base
	}
	overrides, ok := i.rankerResolver.Ranker(tenant)
	if !ok {
		return base
	}
	if overrides.PressureWeight != nil {
		base.PressureWeight = *overrides.PressureWeight
	}
	if overrides.SLOTightTTFTMs != nil {
		base.SLOTightTTFTMs = *overrides.SLOTightTTFTMs
	}
	if overrides.SLOTightBias != nil {
		base.SLOTightBias = *overrides.SLOTightBias
	}
	if overrides.TenantHotMinHitRate != nil {
		base.TenantHotMinHitRate = *overrides.TenantHotMinHitRate
	}
	if overrides.TenantHotMaxAge != nil {
		base.TenantHotMaxAge = *overrides.TenantHotMaxAge
	}
	return base
}

// applyChainDistinguishingPower folds the depth-aware distinguishing-power
// factor into a chain-lookup's per-replica scores in place. Unlike the
// exact-match path — where every scored replica shares the same prefix
// hash and the factor is uniform — a chain match can have replicas at
// different depths (some reached more blocks than others). For each
// replica R the factor is computed from
//
//	matching_at_R = count of replicas with matched_tokens >= R.matched_tokens
//
// so a uniquely-deepest replica sees the strongest factor (1 - 1/N) and a
// shallow-only sibling sees a much smaller one (or 0 when every replica
// reached the same depth). Without this, naïve shared-factor scoring would
// zero a uniquely-deep replica's score whenever a sibling held the head
// — the very routing decision the cache plane wants to surface.
//
// Sort-then-group walk is O(N log N) in the number of scored replicas;
// pure arithmetic, no locking needed (caller releases the read lock first).
// No-op when totalReplicas <= 1 (single-replica deployment) or len(scores)
// == 0; the inner distinguishingPower call also degrades gracefully on
// those branches but the guard saves an unnecessary sort.
//
// Grouping is by `MatchedTokens`, not by raw block depth: two replicas at
// different block-counts that happen to sum to the same matched-tokens
// total are treated as the same "depth" for cardinality. This is
// intentional and consistent with the ranking score (which is also based
// on matched_tokens, not block count): a 0-token block contributes 0 to
// the score AND 0 to the depth tie-break, so two replicas separated only
// by 0-token blocks get the same factor. If two replicas have the same
// matched_tokens but different per-block compositions, they are
// indistinguishable from the gateway's perspective anyway — the score is
// the only routing input.
func applyChainDistinguishingPower(scores []ReplicaScore, totalReplicas int) {
	if totalReplicas <= 1 || len(scores) == 0 {
		return
	}
	// Sort by matched_tokens descending, ID ascending for deterministic
	// grouping when several replicas reach the same depth. The caller's
	// sortScoresDescByScoreThenID will re-sort by the final Score
	// afterwards, so this intermediate order is internal.
	sort.Slice(scores, func(a, b int) bool {
		if scores[a].MatchedTokens != scores[b].MatchedTokens {
			return scores[a].MatchedTokens > scores[b].MatchedTokens
		}
		return scores[a].ReplicaID < scores[b].ReplicaID
	})
	// Walk in groups of equal matched_tokens. Every replica in a group
	// shares the same num_matching_at_depth = right (the count of
	// replicas at this depth or deeper), so the factor is the same for
	// the group. Tied replicas keep their relative score because they
	// land in the same group.
	for left := 0; left < len(scores); {
		right := left
		for right < len(scores) && scores[right].MatchedTokens == scores[left].MatchedTokens {
			right++
		}
		dp := distinguishingPower(right, totalReplicas)
		if dp != 1.0 {
			f := float32(dp)
			for k := left; k < right; k++ {
				scores[k].Score *= f
			}
		}
		left = right
	}
}

// sortScoresDescByScoreThenID gives both lookup paths the same deterministic
// ordering: higher score first, then lexicographic replica ID for tie-break.
func sortScoresDescByScoreThenID(scores []ReplicaScore) {
	sort.Slice(scores, func(a, b int) bool {
		if scores[a].Score != scores[b].Score {
			return scores[a].Score > scores[b].Score
		}
		return scores[a].ReplicaID < scores[b].ReplicaID
	})
}

// sloTightBiasCoefficient returns the coefficient applied to the freshness
// term inside (1 + freshness × coefficient). 0 → no bias (baseline). The
// bias only fires when (a) the ranker has SLOTightTTFTMs and SLOTightBias
// configured AND (b) the request carries a TTFT budget below the threshold.
func sloTightBiasCoefficient(ttftMs int32, ranker RankerConfig) float32 {
	if ranker.SLOTightTTFTMs <= 0 || ranker.SLOTightBias <= 0 {
		return 0
	}
	if ttftMs <= 0 || ttftMs >= ranker.SLOTightTTFTMs {
		return 0
	}
	return ranker.SLOTightBias
}

// pressureFactorAt computes 1 - weight × pressure, clamped to [0, 1]. Kept
// pure so the prefix-match and tenant-hot scorers compute it identically.
func pressureFactorAt(pressure, weight float32) float32 {
	f := 1 - weight*pressure
	if f < 0 {
		return 0
	}
	if f > 1 {
		return 1
	}
	return f
}

// distinguishingPower returns the multiplier the LookupRoute ranker uses to
// discount prefix matches that don't distinguish between replicas. Defined as
//
//	distinguishingPower = 1 - matching/total   (for total >= 2)
//
// so every-replica-holds-it overlaps (chat-template framing, RAG corpus
// headers, custom system prompts shared across the deployment) collapse to
// zero — the per-namespace post-score floor (CachePolicy.spec.routingFloorScore)
// then downgrades the response off the PREFIX_MATCH path to
// StrategyNone, which surfaces as AFFINITY_HINT under default-enabled
// affinity (with a usable seed + serving replica) so repeat prompts
// pin to a stable replica, or as NO_HINT under affinityRouting:
// Disabled so the gateway round-robins honestly instead of being
// credited with a trivial routing decision. A
// uniquely-held match (matching=1, total=N) sees the strongest factor
// (1 - 1/N), proportional to how diluted the prefix is in the cluster.
//
// total <= 1 degrades to 1.0: a single-replica deployment has nothing to
// distinguish among, so a naïve factor of 0 would zero EVERY score and
// downgrade every hint. Returning 1 preserves the baseline ranking on that
// shape (matched_tokens × freshness × pressure × slo_bias), which is the
// only useful answer.
//
// Negative `matching` (only possible from a buggy caller — production paths
// derive it from len(...)) clamps to 1.0 — same shape as total <= 1 — so a
// bug never amplifies a score above its baseline. matching > total clamps
// to 0 — same conservative interpretation as "every replica has it" — so a
// transient over-count (e.g. a stale total from a concurrent ingest) fails
// safe rather than inverting ranking with a negative factor.
//
// Pure function: no allocation, no locking. Cheap enough that the lookup
// path multiplies it in per replica without flinching.
func distinguishingPower(matching, total int) float64 {
	if total <= 1 || matching < 0 {
		return 1.0
	}
	if matching >= total {
		return 0.0
	}
	return 1.0 - float64(matching)/float64(total)
}

// freshnessAt decays linearly from 1 (just seen) to 0 (>= ttl old). Pure
// function so the index can compute it under per-tenant TTL without taking
// the resolver lock inside the per-entry loop.
func freshnessAt(now, lastSeen time.Time, ttl time.Duration) float32 {
	if ttl <= 0 {
		return 0
	}
	age := now.Sub(lastSeen)
	if age <= 0 {
		return 1
	}
	if age >= ttl {
		return 0
	}
	return float32(1 - float64(age)/float64(ttl))
}

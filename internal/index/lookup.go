// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"time"
)

// Lookup returns replicas holding the requested prefix, ranked by the
// ranking-v2 score:
//
//	score = matchedTokens × freshness × pressureFactor × sloBias × distinguishingPower
//
// distinguishingPower is `1 - num_matching_at_depth / total_replicas` for
// multi-replica deployments (per-replica depth-aware for chain matches —
// see lookuproute-ranking.md §2.7), `1.0` for single-replica.
// pressureFactor folds in ReplicaStats.Pressure when the replica has stats
// reported in this (tenant, model) (otherwise 1 — a replica with no stats
// is treated as unloaded). sloBias kicks in when the request's TTFT budget
// is below RankerConfig.SLOTightTTFTMs, biasing toward fresher candidates.
// With pressure=0 and no SLO hint, score reduces to matchedTokens × freshness
// (the B6 baseline). Empty result means "no hint" — the caller fails open.
//
// When the request carries a non-empty block-hash chain (BlockHashes with a
// matching-length BlockTokenCounts), the lookup walks the chain block-by-block
// and computes each replica's longest common leading run; MatchedTokens
// reflects the sum of the request's BlockTokenCounts for that run. When
// BlockHashes is set but BlockTokenCounts has a different length, the chain
// is malformed; the lookup returns no hint rather than silently downgrading
// to legacy exact-match on PrefixHash (symmetric with chain Ingest, which
// drops the entry — "a wrong hint is worse than a stale one"). When neither
// chain field is set, the legacy exact-match path on PrefixHash is used.
func (i *Index) Lookup(req LookupRequest) []ReplicaScore {
	scores, _ := i.lookupWithHits(req)
	return scores
}

// lookupWithHits runs the lookup and ALSO returns the entries that contributed
// matched tokens to the result, keyed by replica ID (LFU namespaces only; nil
// otherwise). The lookup itself is side-effect-free — it never bumps the LFU
// counter. The public Lookup discards the hits; LookupRoute carries them on
// its LookupResult so the gRPC handler can credit them via
// LookupResult.CreditHits once — and only if — it actually delivers the
// response (not on a TIMEOUT/early-deadline path). The per-replica keying
// lets a post-lookup filter (the service-layer matched-tokens floor) drop
// sub-floor replicas' entries from the credit list in lockstep with
// dropping their Scores.
func (i *Index) lookupWithHits(req LookupRequest) ([]ReplicaScore, map[string][]*replicaEntry) {
	// Without a known hash_scheme, opaque hash bytes cannot be matched
	// safely (they would span engines), so fail open with no hint.
	if req.HashScheme == "" {
		return nil, nil
	}
	if len(req.BlockHashes) > 0 || len(req.BlockTokenCounts) > 0 {
		if len(req.BlockHashes) != len(req.BlockTokenCounts) {
			return nil, nil
		}
		return i.lookupChain(req)
	}
	return i.lookupExact(req)
}

// lookupExact is the legacy single-blob exact-match path. The wire shape
// is unchanged for existing callers (no block-hash chain), but the
// per-replica score now folds in the replica-distinguishing-power factor on
// top of the matched_tokens × freshness × pressure × slo_bias baseline. The
// service layer can also downgrade an exact-match response off the
// PREFIX_MATCH path when the top score falls below the per-namespace
// routingFloorScore OR when every replica's matched_tokens falls below
// the per-namespace minimumMatchedTokens floor — see
// internal/server/inferencecache_service.go buildLookupResponse. The downgrade
// lands on StrategyNone, which surfaces as AFFINITY_HINT under
// default-enabled affinity with a usable seed + serving replica or as
// NO_HINT under affinityRouting: Disabled. Old gateway clients that only
// inspect reason_code
// continue to fail open on a downgrade.
func (i *Index) lookupExact(req LookupRequest) ([]ReplicaScore, map[string][]*replicaEntry) {
	key := prefixKey{req.Tenant, req.Model, req.HashScheme, req.Adapter, string(req.PrefixHash)}
	now := i.now()
	sloBiasFactor := i.sloTightBiasCoefficient(req.TTFTBudgetMs)

	ttl := i.ttlFor(req.Tenant)
	// Resolve the algorithm once, outside the lock (the resolver owns its own
	// concurrency, same as ttlFor): only LFU namespaces collect hit entries, so
	// LRU lookups allocate nothing and never touch the counter.
	lfu := i.evictionFor(req.Tenant) == EvictionLFU

	i.mu.RLock()
	replicas := i.prefixes[key]
	// totalReplicas counts every replica known to be serving this engine
	// domain (tenant, model, hash_scheme), not just the holders of THIS
	// prefix. That's the denominator the distinguishing-power factor wants:
	// "out of the replicas that could hold the content, how many do?"
	// Captured under the read lock so it's consistent with the prefixes
	// view this lookup observes.
	//
	// Definition caveat — "total replicas" means "replicas with at least
	// one prefix entry in the requested scope," not "replicas serving the
	// scope." servingByScope is incremented by upsertReplicaLocked when a
	// replica's first prefix entry lands in the scope; it tracks the set
	// of replicas the index has observed reporting state in the scope.
	// A replica that's part of the cluster but has not reported a prefix
	// in this (tenant, model, hash_scheme) — e.g. just started, just
	// cleared its cache, or is serving a different scope — is invisible
	// to the index and so absent from the denominator. Two consequences:
	//   - 2-of-3 partial-diffusion case (replicas r0, r1 hold the prefix;
	//     r2 has reported no prefix in scope) is scored 2-of-2 → factor
	//     0 → downgrade. This is the right answer from the cache plane's
	//     limited view: r2 is invisible, so the cache plane has no
	//     evidence r2 is a peer. Treating r2 as a peer (factor 0.33)
	//     would require speculating about replicas the cache plane has
	//     not observed.
	//   - "Total replicas in scope" semantics are eventually consistent
	//     with the engine reporting state. The C1 kvevent subscriber
	//     reports prefix and stats together on ReportCacheState, so a
	//     production engine that's been running for one TTL cycle will
	//     appear in servingByScope reliably. The visible-only edge case
	//     is benign for the steady-state regime and explicitly tested
	//     by TestLookupExactNonZeroDistinguishingWhenOneOfThreeHoldsPrefix
	//     (the "decoy replica holds OTHER prefix in scope" shape).
	//
	// Soft-state caveat — KNOWN bounded limitation. servingByScope is
	// decremented at TTL-sweep time (removeReplicaLocked /
	// scopeDecLocked), not at every freshness check. A replica that has
	// gone stale (no recent report) stays in the denominator until the
	// next sweep — bounded above by DefaultSweepInterval (1 min by
	// default). During that window the denominator (servingByScope)
	// counts the stale entry; the numerator (post-freshness `scores`
	// below) does not. A trivial overlap held by every CURRENTLY-FRESH
	// replica can briefly surface a small but non-zero distinguishing-
	// power factor instead of the intended 0 — so a sub-trivial
	// PREFIX_MATCH can ship from this lookup before the next sweep
	// collapses the denominator. Net: at worst one DefaultSweepInterval
	// of inflated PREFIX_MATCH rate on a workload that's already trivial,
	// never a wrong routing answer. The resulting hint is "route to one
	// of the replicas that all hold the same trivial overlap," which the
	// gateway serves equivalently regardless of which it picks; the next
	// sweep tick removes the stale denominator entry and the routing-
	// floor catches the next-tick lookup. A correct-by-construction fix
	// would maintain a separate per-scope fresh-replica counter updated
	// lazily on the sweep; the bookkeeping is out of scope for this PR,
	// the soft-state behavior matches the rest of the index, and the
	// window is short enough that operators see the steady-state
	// behavior.
	totalReplicas := len(i.servingByScope[scopeKey{req.Tenant, req.Model, req.HashScheme}])
	scores := make([]ReplicaScore, 0, len(replicas))
	var hitsByReplica map[string][]*replicaEntry
	for id, e := range replicas {
		fresh := freshnessAt(now, e.lastSeen, ttl)
		if fresh <= 0 {
			continue // stale; will be swept
		}
		// LFU hit capture: this entry is about to contribute matched tokens to
		// the result, so record it as a candidate "use" under THIS replica's
		// key. Only entries that contribute a non-zero MatchedTokens are
		// captured — counting merely-considered entries would inflate the
		// counter with cold data. The counter is bumped later
		// (LookupResult.CreditHits) and only if the handler actually delivers
		// this result, never under the read lock. Keying by replica ID is what
		// lets a post-lookup filter (the matched-tokens floor) drop a
		// sub-floor replica's entries from the credit list in lockstep with
		// dropping its Score.
		if lfu && e.tokenCount > 0 {
			if hitsByReplica == nil {
				hitsByReplica = make(map[string][]*replicaEntry, len(replicas))
			}
			hitsByReplica[id] = append(hitsByReplica[id], e)
		}
		// Only fold in pressure when the replica's stats *payload* is still
		// fresh under the global TTL. statsReported reflects when the stat
		// values themselves were ingested — distinct from lastSeen, which a
		// REPLICA_UPDATED liveness event refreshes without supplying new
		// stat values. Without that distinction a stale high-pressure
		// reading kept "alive" by liveness events could keep demoting a
		// perfectly fresh prefix score indefinitely.
		pressure := float32(0)
		if s, ok := i.stats[statsKey{req.Tenant, req.Model, id}]; ok &&
			freshnessAt(now, s.statsReported, ttl) > 0 {
			pressure = s.stats.Pressure
		}
		pressureFactor := pressureFactorAt(pressure, i.ranker.PressureWeight)
		sloBias := 1 + fresh*sloBiasFactor
		scores = append(scores, ReplicaScore{
			ReplicaID:             id,
			Score:                 float32(e.tokenCount) * fresh * pressureFactor * sloBias,
			MatchedTokens:         e.tokenCount,
			EstimatedCacheHitProb: fresh,
			Tier:                  e.tier,
		})
	}
	i.mu.RUnlock()

	// Replica-distinguishing-power factor: every score in an exact-match
	// response shares the same prefix-hash, so num_matching = len(scores)
	// (after the staleness filter) and the factor is uniform. A factor of
	// 0 (every replica holds the prefix — the trivial-overlap case) zeroes
	// every Score so the service-layer post-score floor can downgrade the
	// response to NO_HINT. totalReplicas <= 1 degrades to factor 1.0 so
	// single-replica deployments preserve their baseline ranking.
	//
	if dp := distinguishingPower(len(scores), totalReplicas); dp != 1.0 {
		f := float32(dp)
		for k := range scores {
			scores[k].Score *= f
		}
	}

	sortScoresDescByScoreThenID(scores)
	return scores, hitsByReplica
}

// lookupChain implements longest-common-prefix matching against the
// per-block-hash index. For each replica we find the longest leading run
// [block_hashes[0]..block_hashes[k]] it holds; MatchedTokens is the sum of
// the request's BlockTokenCounts up to k (the request's view of how many
// tokens the matched prefix covers). The freshness signal is the OLDEST
// lastSeen across the matched blocks (the run's weakest link), so a single
// stale block can't make the whole run look fresher than it is.
//
// The pressure and SLO factors from lookupExact compose unchanged: the chain
// walk only changes how matched_tokens is derived; the score formula
// (matched_tokens × freshness × pressureFactor × sloBias) is the same.
func (i *Index) lookupChain(req LookupRequest) ([]ReplicaScore, map[string][]*replicaEntry) {
	type running struct {
		matchedTokens  int32
		oldestLastSeen time.Time
		// runTier is the least-local (coldest) tier across the blocks matched
		// so far (folded via worstTier) — the tier the replica can serve the
		// ENTIRE run from. Carried to the run's ReplicaScore.Tier. Per-block
		// entry tiers stay most-local (what the producer reported for that
		// block); only the across-run summary takes the constraining view.
		runTier CacheTier
		// entries are the block entries forming this replica's matched run,
		// collected ONLY when the namespace runs LFU (nil otherwise, so the LRU
		// hot path allocates nothing). Captured into the returned hits once the
		// run is confirmed to contribute to the result; credited later by the
		// handler, never under the read lock.
		entries []*replicaEntry
	}
	now := i.now()
	ttl := i.ttlFor(req.Tenant)
	sloBiasFactor := i.sloTightBiasCoefficient(req.TTFTBudgetMs)
	// Resolve the algorithm once, outside the lock (see lookupExact): LFU
	// tracks the per-block entry pointers so each contributing block's counter
	// can be bumped; LRU skips both the tracking and the bump.
	lfu := i.evictionFor(req.Tenant) == EvictionLFU

	i.mu.RLock()
	current := map[string]running{}
	finalized := map[string]running{}
	for blockIdx, h := range req.BlockHashes {
		key := prefixKey{req.Tenant, req.Model, req.HashScheme, req.Adapter, string(h)}
		holders := i.prefixes[key]
		blockTokens := req.BlockTokenCounts[blockIdx]
		if blockIdx == 0 {
			for id, e := range holders {
				if freshnessAt(now, e.lastSeen, ttl) <= 0 {
					continue // stale; will be swept
				}
				r := running{matchedTokens: blockTokens, oldestLastSeen: e.lastSeen, runTier: e.tier}
				if lfu {
					r.entries = []*replicaEntry{e}
				}
				current[id] = r
			}
		} else {
			next := make(map[string]running, len(current))
			for id, st := range current {
				e, ok := holders[id]
				if !ok || freshnessAt(now, e.lastSeen, ttl) <= 0 {
					finalized[id] = st
					continue
				}
				oldest := st.oldestLastSeen
				if e.lastSeen.Before(oldest) {
					oldest = e.lastSeen
				}
				nr := running{matchedTokens: st.matchedTokens + blockTokens, oldestLastSeen: oldest, runTier: worstTier(st.runTier, e.tier), entries: st.entries}
				if lfu {
					nr.entries = append(nr.entries, e)
				}
				next[id] = nr
			}
			current = next
		}
		if len(current) == 0 {
			break
		}
	}
	// Replicas still running at the end matched the full chain.
	for id, st := range current {
		finalized[id] = st
	}

	scores := make([]ReplicaScore, 0, len(finalized))
	var hitsByReplica map[string][]*replicaEntry
	for id, st := range finalized {
		if st.matchedTokens <= 0 {
			continue
		}
		fresh := freshnessAt(now, st.oldestLastSeen, ttl)
		if fresh <= 0 {
			continue
		}
		// LFU hit capture: this run contributes a non-zero MatchedTokens to the
		// result, so every block entry in the matched run is a candidate "use".
		// Captured here under THIS replica's ID so a post-lookup filter (the
		// matched-tokens floor) can drop sub-floor replicas' entries from the
		// credit list in lockstep with dropping their Scores. Credited later
		// only on a delivered response. (st.entries is non-nil only under LFU.)
		if lfu && len(st.entries) > 0 {
			if hitsByReplica == nil {
				hitsByReplica = make(map[string][]*replicaEntry, len(finalized))
			}
			hitsByReplica[id] = st.entries
		}
		// Pressure / SLO compose exactly as in lookupExact: same source of
		// truth (statsReported within TTL), same factor formulas. The chain
		// walk only changes matched_tokens and the freshness anchor; the
		// rest of the score formula is unchanged so a chain request lands in
		// the same calibration the legacy path is tuned against.
		pressure := float32(0)
		if s, ok := i.stats[statsKey{req.Tenant, req.Model, id}]; ok &&
			freshnessAt(now, s.statsReported, ttl) > 0 {
			pressure = s.stats.Pressure
		}
		pressureFactor := pressureFactorAt(pressure, i.ranker.PressureWeight)
		sloBias := 1 + fresh*sloBiasFactor
		scores = append(scores, ReplicaScore{
			ReplicaID:             id,
			Score:                 float32(st.matchedTokens) * fresh * pressureFactor * sloBias,
			MatchedTokens:         st.matchedTokens,
			EstimatedCacheHitProb: fresh,
			Tier:                  st.runTier,
		})
	}
	// Cardinality denominator for the distinguishing-power factor: every
	// replica with at least one prefix entry in this engine domain
	// (tenant, model, hash_scheme) — see the definition + soft-state
	// caveats on lookupExact's totalReplicas declaration above for the
	// full discussion of (a) replicas that are in the cluster but have
	// not reported any prefix in scope being invisible to this counter,
	// and (b) the TTL-sweep-window soft-state behavior.
	// Captured BEFORE releasing the read lock so it stays consistent with
	// the prefix view the chain walk just observed.
	totalReplicas := len(i.servingByScope[scopeKey{req.Tenant, req.Model, req.HashScheme}])
	i.mu.RUnlock()

	// Per-replica depth-aware distinguishing-power: a replica that reached
	// deeper into the chain than its siblings holds something unique to it.
	// For each scored replica R, num_matching_at_R's_depth = count of
	// replicas whose matched_tokens >= R.matched_tokens (R plus every
	// replica that went at least as deep). Sort-and-group walk is
	// O(N log N) — pure arithmetic, no locking needed.
	applyChainDistinguishingPower(scores, totalReplicas)

	sortScoresDescByScoreThenID(scores)
	return scores, hitsByReplica
}

// LookupRoute is the orchestrated ranking entrypoint used by the gRPC
// LookupRoute handler. It runs the prefix-match path first; on a miss it
// falls back to TENANT_HOT (replicas warm for this tenant+model); on a
// miss of that too it runs the diagnostic miss classifier to surface a
// specific contract-key mismatch (UNKNOWN_TENANT / UNKNOWN_MODEL /
// UNKNOWN_HASH_SCHEME) when one of (tenant, model, hash_scheme) does not
// match any data held. The returned Strategy tells the handler which
// contract reason_code to emit (PREFIX_MATCH | TENANT_HOT | NO_HINT |
// UNKNOWN_TENANT | UNKNOWN_MODEL | UNKNOWN_HASH_SCHEME) — keeping that
// decision in the index keeps the ranker pluggable and the handler
// stateless. See docs/design/lookuproute-diagnostics.md.
//
// TENANT_HOT is intentionally a SOFTER hint than PREFIX_MATCH: there is no
// prefix overlap, so MatchedTokens is 0 and the gateway is free to ignore.
// The UNKNOWN_* strategies return empty Scores — fail-open semantics are
// unchanged from NO_HINT; the diagnostic only narrows the reason code.
func (i *Index) LookupRoute(req LookupRequest) LookupResult {
	// Empty/unspecified contract keys fail open before any prefix-match,
	// TENANT_HOT, or diagnostic logic runs. The contract requires
	// tenant_id, model_id, and hash_scheme to be supplied; a request that
	// omits any of them is a contract violation, not a key mismatch. The
	// hash_scheme short-circuit also protected against the TENANT_HOT
	// fallback emitting a hint for an unidentified engine domain; the
	// tenant/model short-circuits additionally protect against
	// equally-broken producer state — entries indexed under Tenant: ""
	// or Model: "" (e.g. the DefaultTenantSentinel bucket the cluster
	// aggregate counts) would otherwise produce a real PREFIX_MATCH or
	// TENANT_HOT hint for an empty-key caller. The classifyMiss empty-key
	// guard alone is not enough: it only runs on a miss path, so a
	// matching empty-key prefix lookup would bypass it entirely.
	if req.Tenant == "" || req.Model == "" || req.HashScheme == "" {
		return LookupResult{Strategy: StrategyNone}
	}
	// Chain-bearing requests short-circuit on ANY chain failure (malformed
	// parallel arrays OR a well-formed chain with zero overlap) — never
	// fall through to TENANT_HOT. The chain caller is asking specifically
	// for longest-prefix matching; surfacing an unrelated tenant-warm
	// replica as a soft locality nudge would be a wrong hint against what
	// they explicitly requested. Same-key chain misses surface as NO_HINT
	// (the genuine novel-prefix case); chain misses with a mismatched
	// contract key surface as the matching UNKNOWN_* code via the
	// miss-classifier below — same diagnostic surface as the exact path.
	chainBearing := len(req.BlockHashes) > 0 || len(req.BlockTokenCounts) > 0
	if chainBearing {
		if len(req.BlockHashes) != len(req.BlockTokenCounts) {
			return LookupResult{Strategy: StrategyNone}
		}
		if scores, hits := i.lookupWithHits(req); len(scores) > 0 {
			return LookupResult{Scores: scores, Strategy: StrategyPrefixMatch, hitsByReplica: hits}
		}
		// Chain misses never fall through to TENANT_HOT (by design — see
		// contract doc), so run the miss classifier directly.
		return LookupResult{Strategy: i.classifyMiss(req)}
	}
	if scores, hits := i.lookupWithHits(req); len(scores) > 0 {
		return LookupResult{Scores: scores, Strategy: StrategyPrefixMatch, hitsByReplica: hits}
	}
	if hot := i.tenantHotCandidates(req); len(hot) > 0 {
		// TENANT_HOT carries MatchedTokens=0, so no hits to credit — it is a
		// softer locality nudge, not a prefix HIT.
		return LookupResult{Scores: hot, Strategy: StrategyTenantHot}
	}
	// Prefix miss + TENANT_HOT miss → diagnose which contract key (if any) is
	// the mismatched one. A request whose keys are all populated but whose
	// prefix is novel still lands at StrategyNone (the fail-open NO_HINT).
	return LookupResult{Strategy: i.classifyMiss(req)}
}

// tenantHotCandidates returns replicas warm for (tenant, model) within the
// requested hash_scheme — used when the exact-prefix path returns nothing.
// "Warm" requires three things:
//
//  1. The replica has reported at least one prefix entry for
//     (tenant, model, req.HashScheme). Stats in the index are deliberately
//     scheme-independent (the (tenant, model, replicaID) statsKey carries no
//     hash_scheme), so without this check a stats-only update — or an update
//     with an empty/unrelated hash_scheme — could leak into
//     a TENANT_HOT hint for the wrong engine domain. Proving the replica
//     has SOME prefix in the requested scheme is the cheapest way to assert
//     "this replica actually serves this engine".
//  2. The replica's stats were reported within RankerConfig.TenantHotMaxAge
//     (the recency cutoff). Older stats are stale hints.
//  3. The replica's hit_rate is at least RankerConfig.TenantHotMinHitRate.
//     Below that, it's "not warm enough" to be a useful hint.
//
// The fallback is gated on TenantHotMaxAge > 0 so RankerConfig{} disables
// only the soft locality nudge. The miss-classifier still runs after, so
// a same-key novel-prefix miss lands at NO_HINT while a mismatched contract
// key still surfaces as the matching UNKNOWN_* code. The score uses
// hit_rate as the locality proxy (in place of matched_tokens, which is zero
// by definition here) and reuses the same pressure/SLO factors as the
// prefix-match path so a tight-SLO caller still gets a freshness-biased
// ranking.
func (i *Index) tenantHotCandidates(req LookupRequest) []ReplicaScore {
	if i.ranker.TenantHotMaxAge <= 0 {
		return nil
	}
	// LookupRoute already short-circuits an empty hash_scheme to NO_HINT,
	// but enforce the same guard here so the helper stays safe to call
	// independently: an empty scheme can't be matched against any stored
	// prefix entry, so no candidate could ever qualify.
	if req.HashScheme == "" {
		return nil
	}
	now := i.now()
	maxAge := i.ranker.TenantHotMaxAge
	minHitRate := i.ranker.TenantHotMinHitRate
	sloBiasFactor := i.sloTightBiasCoefficient(req.TTFTBudgetMs)

	i.mu.RLock()
	defer i.mu.RUnlock()

	// Pass 1 (cheap): collect the warm replicas for (tenant, model). Bounded
	// by the size of the (tenant, model) scope — typically tens of replicas —
	// thanks to the replicasByModel secondary index. Without it this would
	// scan the whole i.stats map on every prefix miss, an O(total stats)
	// hot-path cost. Short-circuit BEFORE the prefixes-scope check if no
	// replica qualifies: the common-case prefix miss for a tenant with no
	// recent activity has zero warm replicas.
	type warmReplica struct {
		hitRate, pressure float32
		lastSeen          time.Time
	}
	scoped := i.replicasByModel[modelKey{req.Tenant, req.Model}]
	if len(scoped) == 0 {
		return nil
	}
	warm := make(map[string]warmReplica, len(scoped))
	for replicaID := range scoped {
		s, ok := i.stats[statsKey{req.Tenant, req.Model, replicaID}]
		if !ok {
			continue // defensive: scoped membership and i.stats should be in lockstep
		}
		// Use statsReported, not lastSeen: TENANT_HOT must hint based on
		// recently reported stat payloads, not on liveness events that
		// only refresh lastSeen without supplying new values.
		if now.Sub(s.statsReported) >= maxAge {
			continue
		}
		if s.stats.HitRate < minHitRate {
			continue
		}
		warm[replicaID] = warmReplica{
			hitRate:  s.stats.HitRate,
			pressure: s.stats.Pressure,
			lastSeen: s.statsReported,
		}
	}
	if len(warm) == 0 {
		return nil
	}

	// Pass 2: confirm each warm replica actually serves the requested
	// (tenant, model, hash_scheme) engine domain. The secondary
	// servingByScope index gives this in O(1) per replica — no full scan
	// of i.prefixes — so the hot path stays bounded by the number of warm
	// replicas (typically tens). Stale prefix entries don't leak in:
	// removeReplicaLocked decrements the count when a prefix is evicted
	// (either by sweep or by event), so the count tracks live entries.
	serving := i.servingByScope[scopeKey{req.Tenant, req.Model, req.HashScheme}]
	if len(serving) == 0 {
		return nil
	}

	scores := make([]ReplicaScore, 0, len(warm))
	for id, w := range warm {
		if serving[id] == 0 {
			continue
		}
		// Recency decays from 1 (just seen) to 0 (>= maxAge old). Same
		// shape as freshness in the prefix-match path so the same SLO and
		// pressure factors compose cleanly. Clamp to [0, 1] to defend
		// against clock skew (a future statsReported timestamp would
		// otherwise produce recency > 1 and amplify both score and SLO
		// bias). Mirrors freshnessAt's `age <= 0 → 1` clamp.
		age := now.Sub(w.lastSeen)
		var recency float32
		switch {
		case age <= 0:
			recency = 1
		case age >= maxAge:
			recency = 0
		default:
			recency = 1 - float32(age)/float32(maxAge)
		}
		pressureFactor := pressureFactorAt(w.pressure, i.ranker.PressureWeight)
		sloBias := 1 + recency*sloBiasFactor
		scores = append(scores, ReplicaScore{
			ReplicaID: id,
			Score:     w.hitRate * recency * pressureFactor * sloBias,
			// No prefix matched in this strategy — leave MatchedTokens at 0
			// so a downstream "best prefix hit" guard never mistakes a hot
			// tenant signal for a prefix overlap.
			MatchedTokens:         0,
			EstimatedCacheHitProb: w.hitRate,
		})
	}

	sort.Slice(scores, func(a, b int) bool {
		if scores[a].Score != scores[b].Score {
			return scores[a].Score > scores[b].Score
		}
		return scores[a].ReplicaID < scores[b].ReplicaID
	})
	return scores
}

// AffinityHint returns a stable replica assignment for the given (tenant,
// model, hashScheme) using consistent hashing of seed against the
// currently-known replica set that actually SERVES the requested engine
// domain. Returns ok=false if no replicas serve the scope or seed is
// empty.
//
// The replica set is read under RLock from servingByScope — the same
// per-(tenant, model, hash_scheme) serving-membership accelerator
// TENANT_HOT's Pass 2 check uses — and sorted by replicaID for
// determinism across restarts and across stats-event arrival order. The
// scheme-aware set is what makes affinity routing correct in a mixed-
// engine-domain deployment: hashing over the scheme-blind replicasByModel
// would let a vLLM request pin to an SGLang replica that has never
// reported a vLLM prefix entry, contradicting the engine-disjoint hash
// contract documented in grpc-contract.md (and matching the same fix
// pattern as TENANT_HOT, which also moved from per-(tenant, model) to
// per-(tenant, model, hash_scheme) for this reason).
//
// The seed is the raw canonical request fingerprint (length-prefixed
// concatenation of block_hashes, or the legacy prefix_hash bytes); the
// SHA-256 is computed HERE, exactly once, so the documented contract
// "SHA-256 over the canonical sequence" matches what the implementation
// does — no second hash anywhere on the path. An operator who logs the
// seed bytes can independently compute the same SHA-256 and reproduce
// the routing decision, which is the debuggability story.
//
// The assignment shifts when the replica set changes (the standard
// modulo trade-off); a Phase-2 follow-up may replace the modulo with
// Rendezvous / HRW without altering this method's signature.
//
// Wired by internal/server/inferencecache_service.buildLookupResponse on the
// StrategyNone branch when CachePolicy.spec.affinityRouting is Enabled
// (the kubebuilder default). The index is policy-unaware; the toggle
// lives entirely in the server.
func (i *Index) AffinityHint(tenant, model, hashScheme string, seed []byte) (replicaID string, ok bool) {
	if len(seed) == 0 {
		return "", false
	}
	replicas := i.replicasForRouting(tenant, model, hashScheme)
	if len(replicas) == 0 {
		return "", false
	}
	sum := sha256.Sum256(seed)
	idx := binary.BigEndian.Uint64(sum[:8]) % uint64(len(replicas))
	return replicas[idx], true
}

// replicasForRouting returns the sorted list of replica IDs the index
// currently tracks AS SERVING the (tenant, model, hashScheme) engine
// domain. Derived from servingByScope, the same per-scope serving-
// membership accelerator the TENANT_HOT path uses for its scheme-aware
// Pass 2 check. Sorted by replicaID for determinism across restarts and
// stats-event arrival order. Empty hashScheme returns nil — an
// engine-domain-less request has no scheme-disjoint replica set to
// route against, so affinity falls through to NO_HINT in the caller.
func (i *Index) replicasForRouting(tenant, model, hashScheme string) []string {
	if hashScheme == "" {
		return nil
	}
	i.mu.RLock()
	defer i.mu.RUnlock()
	set, ok := i.servingByScope[scopeKey{tenant: tenant, model: model, hashScheme: hashScheme}]
	if !ok || len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for rid := range set {
		out = append(out, rid)
	}
	sort.Strings(out)
	return out
}

// HasAnyForTenant reports whether the index holds any prefix entries for the
// tenant across every model and hash_scheme. O(1): backed by prefixesByTenant.
// Used by LookupRoute's miss-path classifier (and exposed publicly so other
// debugging / status surfaces can reuse it) to distinguish a wrong tenant_id
// from a genuinely novel prefix — see docs/design/lookuproute-diagnostics.md.
func (i *Index) HasAnyForTenant(tenant string) bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.hasAnyForTenantLocked(tenant)
}

// HasAnyForTenantModel reports whether (tenant, model) has any prefix entries
// across every hash_scheme. O(1): backed by prefixesByTenantModel. Used by
// the miss classifier to surface UNKNOWN_MODEL — must stay O(1) so a
// sustained misconfigured client (a gateway pinned to the wrong model_id)
// can't put a global scan on the LookupRoute miss path.
func (i *Index) HasAnyForTenantModel(tenant, model string) bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.hasAnyForTenantModelLocked(tenant, model)
}

// HasAnyForTenantModelScheme reports whether (tenant, model, hash_scheme) has
// any prefix entries. O(1): backed by servingByScope. Used by the miss
// classifier to surface UNKNOWN_HASH_SCHEME — the scheme-mismatch case
// (ingest under "vllm", lookup under "vllm-v1").
func (i *Index) HasAnyForTenantModelScheme(tenant, model, hashScheme string) bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.hasAnyForTenantModelSchemeLocked(tenant, model, hashScheme)
}

// hasAnyForTenantLocked is the lock-free variant. Caller holds at least the
// read lock. The Locked split mirrors aggregateLocked/Aggregate so the miss
// classifier can run all three checks under a single read-lock acquisition.
func (i *Index) hasAnyForTenantLocked(tenant string) bool {
	return i.prefixesByTenant[tenant] > 0
}

// hasAnyForTenantModelLocked is the O(1) (tenant, model) presence check.
// Caller holds at least the read lock. Backed by prefixesByTenantModel,
// which upsert/removeReplicaLocked maintain in lockstep with the prefix map
// at distinct-prefix-key granularity (same unit as prefixesByTenant).
func (i *Index) hasAnyForTenantModelLocked(tenant, model string) bool {
	return i.prefixesByTenantModel[modelKey{tenant, model}] > 0
}

// hasAnyForTenantModelSchemeLocked is the per-scope check. Caller holds at
// least the read lock.
func (i *Index) hasAnyForTenantModelSchemeLocked(tenant, model, hashScheme string) bool {
	return len(i.servingByScope[scopeKey{tenant, model, hashScheme}]) > 0
}

// classifyMiss returns the diagnostic Strategy for a LookupRoute call whose
// prefix lookup found nothing AND whose TENANT_HOT fallback (when applicable)
// also did. It walks the contract keys outer-to-inner (widest scope first)
// — tenant, then (tenant, model), then (tenant, model, hash_scheme) — and
// returns the first level at which the index has no data. If every level
// is populated the miss is a genuinely novel prefix → StrategyNone (the
// existing fail-open NO_HINT).
//
// The whole walk runs under one RLock acquisition so concurrent ingests can't
// produce a contradictory classification (e.g. tenant unknown → tenant known
// in a single call). The caller (LookupRoute) takes no other locks across
// this call, so there is no lock-ordering concern.
//
// Preconditions enforced by LookupRoute (the only caller): req.Tenant,
// req.Model, and req.HashScheme are all non-empty. Missing-key requests are
// short-circuited at the top of LookupRoute and never reach this function,
// so no empty-key guard is needed here.
//
// Cold-start carve-out: a globally-empty index stays on NO_HINT (every
// tenant query would otherwise classify as UNKNOWN_TENANT before any
// ReportCacheState lands). The UNKNOWN_* codes are meant to signal "you
// queried with a key that does not match what I hold" — but during cold
// start the server holds NOTHING, so the honest answer is "no hint",
// not "your tenant_id is wrong." The diagnostic resumes the moment any
// replica has reported state, which is the asymmetric case the SDK
// guidance is targeted at (one tenant populated, the gateway pointing at
// another).
func (i *Index) classifyMiss(req LookupRequest) Strategy {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if len(i.prefixes) == 0 {
		return StrategyNone
	}
	if !i.hasAnyForTenantLocked(req.Tenant) {
		return StrategyUnknownTenant
	}
	if !i.hasAnyForTenantModelLocked(req.Tenant, req.Model) {
		return StrategyUnknownModel
	}
	if !i.hasAnyForTenantModelSchemeLocked(req.Tenant, req.Model, req.HashScheme) {
		return StrategyUnknownHashScheme
	}
	return StrategyNone
}

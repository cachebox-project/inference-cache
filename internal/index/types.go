// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"time"
)

// Metrics is the optional sink the index reports to. It is satisfied by the
// server's Prometheus wiring; kept as a tiny interface so the index has no
// dependency on the metrics/registry implementation.
type Metrics interface {
	SetIndexEntries(model string, entries int)
	// AddTenantEvictions records n quota-driven entry evictions for a tenant.
	// reason is the budget that was exceeded (currently only "over_entries").
	AddTenantEvictions(tenantID, reason string, n int)
	// AddIndexEvictions records n entries evicted by the cap or TTL sweep.
	// algorithm is the namespace's resolved algorithm ("lru"/"lfu"); reason is
	// "cap" (entry cap exceeded) or "ttl" (freshness sweep).
	AddIndexEvictions(algorithm, reason string, n int)
}

// TenantQuotaResolver returns the per-tenant index-entry budget the index
// enforces at ingest time. ok=false (a nil resolver, or no matching CacheTenant)
// means "no quota" — the tenant is unbounded (fail open / soft state), matching
// today's behavior before any CacheTenant exists. A configured budget of 0 is a
// valid enforceable cap (admit nothing), distinct from "no quota". The index
// does not import any CRD/policy types; the resolver is satisfied by the
// server's policy store. Mirrors TTLResolver.
type TenantQuotaResolver interface {
	TenantQuota(tenant string) (maxEntries int64, ok bool)
}

// TTLResolver returns the per-tenant eviction TTL applied by the index. A
// return of <=0 (or a nil resolver) means "use the global default TTL". The
// index does not import any CRD/policy types; the resolver is satisfied by the
// server's policy store. Kept tiny on purpose, matching the Metrics interface.
type TTLResolver interface {
	TTL(tenant string) time.Duration
}

// RankerOverrides is a presence-aware set of per-tenant ranking-v2 knobs.
// Nil fields inherit the Index baseline configured through WithRanker; a
// non-nil zero preserves the knob's documented kill-switch behavior.
type RankerOverrides struct {
	PressureWeight      *float32
	SLOTightTTFTMs      *int32
	SLOTightBias        *float32
	TenantHotMinHitRate *float32
	TenantHotMaxAge     *time.Duration
}

// RankerResolver returns per-tenant ranker overrides. ok=false (including a
// nil resolver) means the tenant uses the Index's WithRanker baseline. The
// resolver implementation owns its own concurrency.
type RankerResolver interface {
	Ranker(tenant string) (overrides RankerOverrides, ok bool)
}

// Eviction algorithm identifiers. The wire form is lower-case ("lru"/"lfu") to
// match the casing of ResolvedPolicy.Eviction and reason_code; the CRD enum is
// upper-case per K8s convention and the controller lower-cases when flattening.
// These are also the values carried by the index_evictions_total `algorithm`
// label.
const (
	EvictionLRU = "lru"
	EvictionLFU = "lfu"
)

// EvictionResolver returns the per-tenant (namespace) eviction algorithm. An
// empty string, an unrecognized value, or a nil resolver all mean LRU — the
// default and the pre-LFU behavior. The index consults it in two places: the
// cap-based sweep (to order victims) and, in LFU namespaces, the lookup path
// (to decide whether to capture which entries a delivered hint credits). The TTL
// sweep runs identically regardless. The index imports no CRD/policy types; the
// server's policy store satisfies it. Mirrors TTLResolver.
type EvictionResolver interface {
	Eviction(tenant string) string
}

// ReplicaStats is the per-replica cache health reported alongside an update.
type ReplicaStats struct {
	ReplicaID        string
	CacheMemoryBytes int64
	HitRate          float32
	Pressure         float32
	// T2HitTokens / T2QueryTokens are cumulative tier-2 (external offload, e.g.
	// LMCache) reload token counters as last reported by the replica. The
	// poller derives a presence-aware per-CacheBackend hit-rate from the
	// query-weighted ratio (a 0% rate is only meaningful once T2QueryTokens>0).
	T2HitTokens   int64
	T2QueryTokens int64
}

// CacheTier ranks WHERE a replica holds a prefix, from the most local / fastest
// tier (T1 — engine KV cache) down to colder tiers (T2 — external offload such
// as LMCache; T3 — remote / disaggregated store). Lower value = more local =
// preferred. The zero value TierUnspecified means "no tier known": the current
// ingest path normalizes it to TierT1 (see Ingest), and a non-prefix hint
// (TENANT_HOT / AFFINITY_HINT) leaves it unspecified. Mirrors the proto
// CacheTier enum values one-for-one. Tier *detection* is done upstream by the
// kvevent-subscriber from the engine block lifecycle (stored → T1,
// evicted-but-L2-retained → T2); the tier arrives set on the ingested
// PrefixRef. T3 is not yet emitted.
type CacheTier int32

const (
	TierUnspecified CacheTier = iota // 0 — mirrors CACHE_TIER_UNSPECIFIED
	TierT1                           // 1 — engine KV cache (most local)
	TierT2                           // 2 — external offload (e.g. LMCache)
	TierT3                           // 3 — remote / disaggregated
)

// worstTier returns the less-local (colder) of two tiers: T3 dominates T2
// dominates T1. Used to summarize a multi-block chain match down to the tier
// the replica can serve the ENTIRE matched run from — a single block only
// present in a colder tier constrains the whole run (serving it means touching
// that tier, so claiming the warmer tier would overstate the hint).
// TierUnspecified poisons the fold: if any block's tier is unknown, the run's
// tier is unknown — an honest "no claim" beats a fabricated one. (Unspecified
// should not occur post-ingest normalization; this is defense in depth.)
func worstTier(a, b CacheTier) CacheTier {
	if a == TierUnspecified || b == TierUnspecified {
		return TierUnspecified
	}
	if a > b { // higher enum value = colder (T1 < T2 < T3)
		return a
	}
	return b
}

// PrefixRef is one prefix a replica reports holding: engine-opaque hash bytes
// plus how many tokens that prefix covers.
//
// Engines that hash per KV block (vLLM, SGLang) may report the prefix as an
// ordered chain of block hashes via BlockHashes + a parallel BlockTokenCounts
// (same length, per-block). The index then stores one per-block entry per
// hash so longest-prefix lookups can compute the longest common leading run.
// When the chain fields are set the legacy PrefixHash / TokenCount are
// ignored; entries that only set PrefixHash + TokenCount remain valid for
// legacy exact-match indexing.
type PrefixRef struct {
	PrefixHash       []byte
	TokenCount       int32
	BlockHashes      [][]byte
	BlockTokenCounts []int32
	// Tier is which cache tier this replica holds the prefix in. Unset
	// (TierUnspecified) is normalized to TierT1 at ingest. The kvevent-subscriber
	// sets it from the engine block lifecycle: a stored block reports T1, a block
	// the engine evicts while a paired L2 tier (LMCache) retains it is re-reported
	// at T2. A chain entry's Tier applies to every block it expands to.
	Tier CacheTier
	// Adapter is the stable adapter (e.g. LoRA) identity whose KV this entry
	// describes — the index partition the entry lands in. Per-entry because one
	// replica can hold KV for several adapters at once. Empty falls back to
	// Update.Adapter, and an empty result is the default partition: identical to
	// the behavior before adapters existed. A chain entry's Adapter applies to
	// every per-block entry it expands to.
	Adapter string
}

// Update is the authoritative state a replica reports (from ReportCacheState).
type Update struct {
	ReplicaID  string
	Model      string
	Tenant     string
	HashScheme string
	// Adapter is the default adapter partition for Prefixes that set no Adapter
	// of their own — the convenient form for a producer whose replica serves a
	// single adapter. Empty = the default partition (pre-adapter behavior).
	// Stats are adapter-independent and are never partitioned by it.
	Adapter   string
	Timestamp time.Time // zero → treated as "now"
	Prefixes  []PrefixRef
	Stats     *ReplicaStats
}

// EventType mirrors the proto CacheEvent.Type deltas.
type EventType int

const (
	EventPrefixAdded EventType = iota + 1
	EventPrefixEvicted
	EventReplicaUpdated
	EventAllCleared
)

// Event is a lightweight delta (from PublishEvent). Events carry no hash_scheme
// or token_count, so they refine already-known state; ReportCacheState is the
// authoritative population path.
type Event struct {
	Type       EventType
	ReplicaID  string
	Model      string
	Tenant     string
	PrefixHash []byte
	// Adapter narrows a PREFIX_EVICTED removal to one adapter partition. Empty
	// removes the prefix from EVERY adapter partition — the conservative legacy
	// behavior, and identical to the pre-adapter code for producers that never
	// set it (all of whose entries live in the "" partition anyway). Ignored by
	// ALL_CLEARED (a flush clears the replica across adapters) and by
	// REPLICA_UPDATED (liveness is adapter-independent).
	Adapter   string
	Timestamp time.Time
}

// LookupRequest asks which replicas hold a given prefix, within a hash scheme.
//
// When BlockHashes is non-empty (and BlockTokenCounts has the same length),
// the index walks the chain block-by-block and returns each replica's longest
// common leading run; MatchedTokens reflects the sum of the request's
// BlockTokenCounts up to the last matched block. Otherwise it falls back to
// exact-match on PrefixHash (legacy path).
//
// TTFTBudgetMs / TBTBudgetMs carry the caller's SLO targets (proto SLO message);
// 0 means "no SLO hint" and the ranker treats the request as baseline-latency.
type LookupRequest struct {
	Model      string
	Tenant     string
	HashScheme string
	// Adapter selects the adapter partition to match in. Because the content
	// fingerprint is token-only, a lookup MUST supply the same adapter identity
	// the producer ingested under or it will not match — the same producer/
	// consumer agreement HashScheme already requires. Empty is the default
	// partition, which is where every non-LoRA deployment both ingests and looks
	// up, so non-LoRA behavior is unchanged.
	Adapter          string
	PrefixHash       []byte
	TokenCount       int32
	BlockHashes      [][]byte
	BlockTokenCounts []int32

	TTFTBudgetMs int32
	TBTBudgetMs  int32
}

// ReplicaScore is one ranked hint returned to the gateway. Higher score = better.
type ReplicaScore struct {
	ReplicaID             string
	Score                 float32
	MatchedTokens         int32
	EstimatedCacheHitProb float32
	// Tier is the tier the replica can serve the ENTIRE matched prefix from:
	// the entry's tier for an exact match; for a chain match, the least-local
	// (coldest) tier across the matched run — one block only present in a
	// colder tier constrains the whole run (see worstTier). TierUnspecified on
	// a non-prefix hint (TENANT_HOT / AFFINITY_HINT), which carries no
	// per-prefix tier evidence. Carried through to the response's
	// ReplicaScore.tier; scoring itself does not read it (tier-weighted
	// blending is a later ticket).
	Tier CacheTier
}

// Strategy names which ranking-or-classification path produced a LookupResult,
// so the gRPC handler can map it to the contract's reason_code vocabulary
// (PREFIX_MATCH | TENANT_HOT | NO_HINT | UNKNOWN_TENANT | UNKNOWN_MODEL |
// UNKNOWN_HASH_SCHEME) without re-running the index logic.
type Strategy int

const (
	// StrategyNone — no candidates from any strategy. The handler emits
	// AFFINITY_HINT (with a stable single-replica pick from
	// servingByScope) when CachePolicy.spec.affinityRouting is Enabled
	// — the kubebuilder default — and the request has a usable seed
	// plus a serving replica; otherwise it emits NO_HINT.
	StrategyNone Strategy = iota
	// StrategyPrefixMatch — at least one replica holds the requested prefix
	// in this hash_scheme. Handler emits PREFIX_MATCH, BUT the service-layer
	// matched-tokens floor (CachePolicy.spec.minimumMatchedTokens, default 64
	// — see docs/design/lookuproute-ranking.md §2.6) can still downgrade the
	// response off the prefix-match path before it ships to the wire:
	// replicas whose matched_tokens falls below the floor are filtered,
	// and if none survive the strategy is replaced with StrategyNone in
	// buildLookupResponse (which then surfaces as AFFINITY_HINT under
	// default-enabled affinity or NO_HINT when disabled). The index
	// itself stays policy-unaware — this Strategy is the *pre-policy*
	// prefix-match outcome.
	StrategyPrefixMatch
	// StrategyTenantHot — no exact prefix match, but the tenant has recently
	// warm replicas (hit_rate-based). A coarser locality signal than prefix
	// match. Handler emits TENANT_HOT.
	StrategyTenantHot
	// StrategyUnknownTenant — the request supplied a non-empty tenant_id and
	// the index holds zero prefix entries for that tenant across every model
	// and hash_scheme. Handler emits UNKNOWN_TENANT. See
	// docs/design/lookuproute-diagnostics.md.
	StrategyUnknownTenant
	// StrategyUnknownModel — the tenant is known but the (tenant, model_id)
	// pair has zero entries. Handler emits UNKNOWN_MODEL.
	StrategyUnknownModel
	// StrategyUnknownHashScheme — the (tenant, model_id) pair has entries,
	// but none under the request's hash_scheme. Handler emits
	// UNKNOWN_HASH_SCHEME — the scheme-mismatch case (e.g. ingest under
	// "vllm", lookup under "vllm-v1").
	StrategyUnknownHashScheme
)

// LookupResult is the orchestrated outcome of LookupRoute — the ranked
// scores plus which strategy produced them.
type LookupResult struct {
	Scores   []ReplicaScore
	Strategy Strategy
	// hitsByReplica are the entries that contributed matched tokens to Scores,
	// captured (LFU namespaces only) during the lookup but NOT yet counted.
	// Keyed by replica ID so callers that prune Scores (e.g. the service-layer
	// matched-tokens floor that drops sub-floor replicas) can drop the
	// corresponding entries in lockstep via RetainReplicas — preserving the
	// no-credit-on-non-delivery invariant even when the response is partially
	// filtered. The caller credits the surviving entries via CreditHits ONLY
	// when it actually delivers this result — so a lookup the gRPC handler
	// discards as TIMEOUT never bumps an LFU counter. Unexported:
	// *replicaEntry is an index-internal type.
	hitsByReplica map[string][]*replicaEntry
}

// CreditHits records one LFU access for each entry that contributed matched
// tokens to a DELIVERED LookupRoute response. The gRPC handler calls it from
// buildLookupResponse, which runs only on the paths that return real scores —
// never on the TIMEOUT/early-deadline paths — so the counter reflects hints the
// caller actually received. Lock-free (each accessCount is an atomic), so it is
// safe to call after the index read lock has been released; a concurrently
// evicted entry's bump is harmless (soft state). A no-op for LRU namespaces and
// for NO_HINT/TENANT_HOT results (hitsByReplica is empty).
func (r LookupResult) CreditHits() {
	for _, entries := range r.hitsByReplica {
		for _, e := range entries {
			e.accessCount.Add(1)
		}
	}
}

// RetainReplicas prunes Scores AND hitsByReplica down to the replica IDs whose
// boolean is true in keep. Callers that filter the scored result post-lookup
// (the service-layer matched-tokens floor, which drops sub-floor replicas)
// must call this rather than mutating Scores directly, so the hits map stays
// in lockstep — otherwise the dropped replica's entries would still be
// credited at CreditHits time, violating the no-credit-on-non-delivery
// invariant and skewing LFU cap eviction toward replicas whose hints never
// reached the gateway. The Scores slice is rebuilt without the original
// backing array so the dropped scores are eligible for GC. A no-op when
// every score is already kept; an all-empty keep map collapses Scores to
// nothing (the caller should normally swap in StrategyNone in that case).
func (r *LookupResult) RetainReplicas(keep map[string]bool) {
	if len(r.Scores) == 0 {
		return
	}
	kept := make([]ReplicaScore, 0, len(r.Scores))
	for _, sc := range r.Scores {
		if keep[sc.ReplicaID] {
			kept = append(kept, sc)
		}
	}
	r.Scores = kept
	for id := range r.hitsByReplica {
		if !keep[id] {
			delete(r.hitsByReplica, id)
		}
	}
}

// RankerConfig tunes the pressure / SLO / tenant-hot strategies layered on
// the baseline matchedTokens × freshness score. Zero-valued knobs collapse
// those layers back to the baseline — so they're safe to leave enabled
// even when stats are absent or SLO is unspecified. The cardinality-aware
// distinguishingPower factor (PREFIX_MATCH path only) is always on for
// multi-replica deployments and degrades to 1.0 for single-replica
// deployments; no per-knob disable. See lookuproute-ranking.md §2.7.
//
// Concretely (PREFIX_MATCH path):
//
//	score              = matchedTokens × freshness × pressureFactor × sloBias × distinguishingPower
//	pressureFactor     = max(0, 1 - PressureWeight × pressure)             // 1 when no stats
//	sloBias            = 1 + freshness × SLOTightBias                      // when TTFT tight
//	                   = 1                                                  // otherwise
//	distinguishingPower = 1 - num_matching_at_depth / total_replicas        // when total_replicas ≥ 2
//	                   = 1                                                  // single-replica deployment
//
// PressureWeight = 0 disables the penalty (pressureFactor=1). SLOTightBias
// = 0 disables the SLO bias (sloBias=1). TenantHotMaxAge ≤ 0 disables only
// the TENANT_HOT fallback (a prefix miss whose keys all populate the index
// goes straight to NO_HINT instead of trying for a tenant-warm hint); the
// miss-classifier still runs, so a prefix miss with a mismatched contract
// key still surfaces as the matching UNKNOWN_* code.
type RankerConfig struct {
	PressureWeight      float32
	SLOTightTTFTMs      int32
	SLOTightBias        float32
	TenantHotMinHitRate float32
	TenantHotMaxAge     time.Duration
}

// DefaultRankerConfig returns the calibrated default knobs — ranking v2 is
// on out of the box, but reduces to the baseline whenever the supporting
// inputs (replica stats, SLO hint) aren't there.
func DefaultRankerConfig() RankerConfig {
	return RankerConfig{
		PressureWeight:      DefaultPressureWeight,
		SLOTightTTFTMs:      DefaultSLOTightTTFTMs,
		SLOTightBias:        DefaultSLOTightBias,
		TenantHotMinHitRate: DefaultTenantHotMinHitRate,
		TenantHotMaxAge:     DefaultTenantHotMaxAge,
	}
}

// Package index is part of inferencecache-server: the cluster cache-state aggregator
// (the CacheIndex), populated from engine KV events and queried by LookupRoute.
// Observability and routing input only — not a routing-decision substrate.
//
// The index is intentionally decoupled from the gRPC/proto layer: callers
// translate proto messages into the domain types below. It is soft state —
// losing it causes cache misses, never wrong answers (tech spec §"soft state").
package index

import (
	"context"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Defaults for the soft-state index. TTL matches the CachePolicy default in the
// tech spec; the cap bounds memory (entries beyond it are evicted oldest-first).
const (
	DefaultTTL           = 30 * time.Minute
	DefaultSweepInterval = time.Minute
	DefaultMaxEntries    = 1_000_000
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

// tenantEvictionReasonOverEntries is the only quota dimension enforced today:
// the index entry table is the one cache resource the server authoritatively
// owns. (Per-tenant memory is not enforceable on a shared, tenant-unaware
// engine — see the CacheTenant CRD docs.)
const tenantEvictionReasonOverEntries = "over_entries"

// Reason labels for inferencecache_index_evictions_total. "cap" = the global
// entry cap (maxEntries) was exceeded; "ttl" = the freshness sweep removed a
// stale entry. Distinct from the quota path's tenant_evictions_total.
const (
	indexEvictionReasonCap = "cap"
	indexEvictionReasonTTL = "ttl"
)

// TTLResolver returns the per-tenant eviction TTL applied by the index. A
// return of <=0 (or a nil resolver) means "use the global default TTL". The
// index does not import any CRD/policy types; the resolver is satisfied by the
// server's policy store. Kept tiny on purpose, matching the Metrics interface.
type TTLResolver interface {
	TTL(tenant string) time.Duration
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
}

// Update is the authoritative state a replica reports (from ReportCacheState).
type Update struct {
	ReplicaID  string
	Model      string
	Tenant     string
	HashScheme string
	Timestamp  time.Time // zero → treated as "now"
	Prefixes   []PrefixRef
	Stats      *ReplicaStats
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
	Timestamp  time.Time
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
	Model            string
	Tenant           string
	HashScheme       string
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
}

// Strategy names which ranking-or-classification path produced a LookupResult,
// so the gRPC handler can map it to the contract's reason_code vocabulary
// (PREFIX_MATCH | TENANT_HOT | NO_HINT | UNKNOWN_TENANT | UNKNOWN_MODEL |
// UNKNOWN_HASH_SCHEME) without re-running the index logic.
type Strategy int

const (
	// StrategyNone — no candidates from any strategy. Handler emits NO_HINT.
	StrategyNone Strategy = iota
	// StrategyPrefixMatch — at least one replica holds the requested prefix
	// in this hash_scheme. Handler emits PREFIX_MATCH, BUT the service-layer
	// matched-tokens floor (CachePolicy.spec.minimumMatchedTokens, default 64
	// — see docs/design/lookuproute-ranking.md §2.6) can still downgrade the
	// response to NO_HINT before it ships to the wire: replicas whose
	// matched_tokens falls below the floor are filtered, and if none survive
	// the strategy is replaced with StrategyNone in buildLookupResponse. The
	// index itself stays policy-unaware — this Strategy is the *pre-policy*
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

type prefixKey struct {
	tenant     string
	model      string
	hashScheme string
	prefixHash string // raw engine bytes, used as an opaque map key
}

type statsKey struct {
	tenant    string
	model     string
	replicaID string
}

// modelKey identifies a (tenant, model) — the granularity at which stats are
// keyed in the index (stats are scheme-independent: one ReplicaStats applies
// across engine domains). Used by the TENANT_HOT fallback to look up the
// (tenant, model) stats subset in O(replicas-in-this-(tenant, model)) rather
// than O(total stats in the index).
type modelKey struct {
	tenant string
	model  string
}

// scopeKey identifies a (tenant, model, hash_scheme) — the engine domain
// granularity TENANT_HOT needs for its serving-membership check.
type scopeKey struct {
	tenant     string
	model      string
	hashScheme string
}

type replicaEntry struct {
	tokenCount int32
	lastSeen   time.Time
	// accessCount is the LFU access counter. The lookup path CAPTURES the
	// entries that contribute matched tokens (LFU namespaces only) but does not
	// bump — the gRPC handler credits them lock-free via LookupResult.CreditHits
	// only when it actually delivers the response, so a TIMEOUT'd lookup never
	// counts. It never ages: the TTL sweep handles staleness regardless of
	// algorithm, so the counter only governs cap-based eviction. Entries are
	// held by pointer (map[string]*replicaEntry) so this atomic is never copied;
	// CreditHits runs lock-free (outside i.mu) and the cap sweep reads the count
	// under the write lock.
	accessCount atomic.Int64
}

type statEntry struct {
	stats ReplicaStats
	// lastSeen tracks replica LIVENESS — refreshed by Ingest AND by
	// REPLICA_UPDATED events. Used for eviction and observability.
	lastSeen time.Time
	// statsReported tracks when these stat values themselves were last
	// reported (Ingest only). The ranker uses this for the pressure /
	// TENANT_HOT freshness check so a stale stats payload kept artificially
	// alive by liveness events does not keep demoting or hinting.
	statsReported time.Time
}

// Index is the in-memory, concurrent-safe, soft-state cache-state aggregator.
type Index struct {
	ttl              time.Duration
	sweepInterval    time.Duration
	maxEntries       int
	now              func() time.Time
	metrics          Metrics
	ranker           RankerConfig
	ttlResolver      TTLResolver
	quotaResolver    TenantQuotaResolver
	evictionResolver EvictionResolver
	// reservedTenants identifies tenant ids whose prefix entries are EXCLUDED
	// from the global maxEntries cap accounting AND the cap-sweep victim
	// candidate set. The index doesn't know what these tenants are for —
	// callers (the server) declare them via WithReservedTenants. The intent
	// is to host ephemeral synthetic state (e.g. the server's functional
	// self-test probe) that a concurrent real-workload Ingest must never see
	// as either a cap pressure source OR a candidate to evict. TTL sweep and
	// per-tenant quota enforcement still apply unchanged. Nil/empty means no
	// exemptions and the cap behaves identically to its historical shape.
	reservedTenants map[string]struct{}

	ready atomic.Bool

	mu sync.RWMutex
	// prefixes holds entries by POINTER (not value) because replicaEntry carries
	// an atomic.Int64 (the LFU counter) that must never be copied — a value map
	// would copy the atomic on every read/write (vet copylocks) and its values
	// aren't addressable for the counter bump. i.stats stays a value map: its
	// statEntry has no atomic and the same migration there is out of scope.
	prefixes     map[prefixKey]map[string]*replicaEntry // prefix → replicaID → entry
	stats        map[statsKey]statEntry
	totalEntries int // sum of replicaEntries across all prefixes (memory bound)
	// reservedEntries counts the subset of totalEntries whose tenant is in
	// reservedTenants. The cap math is `totalEntries - reservedEntries` so
	// reserved-tenant entries contribute to memory accounting but neither
	// fill the cap nor get picked as victims. Maintained in lockstep with
	// totalEntries by upsert/removeReplicaLocked.
	reservedEntries int

	// prefixesByTenant counts DISTINCT prefix keys per tenant (one per
	// (tenant, model, hash_scheme, prefix_hash), regardless of how many replicas
	// hold it), so the per-tenant quota check at ingest is O(1) instead of
	// scanning i.prefixes. Maintained by upsert/removeReplicaLocked: bumped when a
	// key is first created for the tenant, dropped when the key's last replica
	// leaves. This is the unit maxIndexEntries bounds and the unit reported as
	// tenants[].indexEntries — equal, per tenant, to that tenant's slice of
	// prefixes.summary.total.
	prefixesByTenant map[string]int

	// prefixesByTenantModel mirrors prefixesByTenant at (tenant, model)
	// granularity, so HasAnyForTenantModel (the LookupRoute miss-classifier
	// UNKNOWN_MODEL check) is O(1) instead of iterating servingByScope. Same
	// counted unit (distinct prefix key) and same maintenance invariants:
	// bumped on first-sight of a new prefix key for the (tenant, model);
	// dropped when the key's last replica leaves. Without this secondary
	// index a sustained misconfigured client (e.g. a gateway pinned to the
	// wrong model_id) would put a global servingByScope scan on the miss
	// path, scaling with the cluster's scope count instead of staying O(1).
	prefixesByTenantModel map[modelKey]int

	// servingByScope counts, for each (tenant, model, hash_scheme), how many
	// distinct prefix entries each replica currently holds. It exists purely
	// to give the TENANT_HOT fallback an O(1) "does replica R serve scope S?"
	// check instead of scanning the whole prefixes map on every prefix miss.
	// The count goes up on Ingest of a new (scope, replica, prefix), down on
	// removeReplicaLocked, and the entry is dropped when the count hits 0.
	servingByScope map[scopeKey]map[string]int

	// replicasByModel is the (tenant, model) → set of replicas with stats
	// reported in that scope. It exists purely so TENANT_HOT's warmth scan
	// touches only the stats for the requested (tenant, model) instead of
	// iterating the full i.stats map. Updated in lockstep with i.stats on
	// ingest, replica-clear events, and stats eviction.
	replicasByModel map[modelKey]map[string]struct{}

	// reportMu guards reportedModels, the set of models last pushed to the
	// metrics sink — used to zero a model's gauge when it drains to empty.
	reportMu       sync.Mutex
	reportedModels map[string]struct{}
}

// Option configures an Index.
type Option func(*Index)

// WithTTL sets how long an entry survives without a refresh.
func WithTTL(d time.Duration) Option { return func(i *Index) { i.ttl = d } }

// WithSweepInterval sets how often the eviction loop runs.
func WithSweepInterval(d time.Duration) Option { return func(i *Index) { i.sweepInterval = d } }

// WithMaxEntries caps total replica×prefix entries (0 = unbounded).
func WithMaxEntries(n int) Option { return func(i *Index) { i.maxEntries = n } }

// WithMetrics wires the metrics sink the index reports to: the per-model entry
// gauge (inferencecache_index_entries) plus the eviction counters
// (inferencecache_tenant_evictions_total, inferencecache_index_evictions_total).
func WithMetrics(m Metrics) Option { return func(i *Index) { i.metrics = m } }

// WithRanker overrides the ranking-v2 knobs. The default (set in New) is
// DefaultRankerConfig() — sensible production values that collapse to the
// matchedTokens × freshness baseline when stats and SLO are absent. Pass
// RankerConfig{} to disable every v2 strategy and run pure baseline.
func WithRanker(cfg RankerConfig) Option { return func(i *Index) { i.ranker = cfg } }

// WithTTLResolver wires a per-tenant TTL resolver. A nil resolver, or one that
// returns <=0 for a tenant, falls back to the global TTL set via WithTTL (or
// DefaultTTL). The index reads it on every freshness/eviction decision; the
// resolver implementation owns its own concurrency.
func WithTTLResolver(r TTLResolver) Option { return func(i *Index) { i.ttlResolver = r } }

// WithTenantQuotaResolver wires a per-tenant index-entry quota resolver. A nil
// resolver, or one that reports no quota for a tenant, disables enforcement for
// that tenant (unbounded — identical to today's behavior). The index reads it
// once per Ingest, before taking the write lock; the resolver implementation
// owns its own concurrency.
func WithTenantQuotaResolver(r TenantQuotaResolver) Option {
	return func(i *Index) { i.quotaResolver = r }
}

// WithEvictionResolver wires a per-tenant cap-eviction-algorithm resolver. A nil
// resolver, or one returning "" / an unrecognized value, leaves the tenant on
// LRU (the default). The index reads it at sort time during a cap sweep and on
// each lookup HIT (to decide whether to bump the LFU counter); the resolver
// implementation owns its own concurrency. Mirrors WithTTLResolver.
func WithEvictionResolver(r EvictionResolver) Option {
	return func(i *Index) { i.evictionResolver = r }
}

// WithReservedTenants declares a set of tenant ids whose prefix entries are
// EXCLUDED from the global maxEntries cap accounting AND the cap-sweep victim
// candidate set. Intended for ephemeral server-internal state (e.g. the
// functional self-test probe) so that a concurrent real-workload Ingest
// neither sees probe entries as cap pressure nor picks one of its own
// real-workload entries as a victim to make room for a transient probe entry.
// TTL sweep and per-tenant quota enforcement still apply to reserved tenants
// unchanged; only the global cap is bypassed. The set is read-only after
// construction; callers thread the set through this Option once. Empty/nil
// means no exemptions (historical behavior).
func WithReservedTenants(tenants ...string) Option {
	return func(i *Index) {
		if len(tenants) == 0 {
			return
		}
		if i.reservedTenants == nil {
			i.reservedTenants = make(map[string]struct{}, len(tenants))
		}
		for _, t := range tenants {
			if t == "" {
				continue
			}
			i.reservedTenants[t] = struct{}{}
		}
	}
}

// withClock overrides the time source (tests only).
func withClock(now func() time.Time) Option { return func(i *Index) { i.now = now } }

// New builds an index with the given options.
func New(opts ...Option) *Index {
	i := &Index{
		ttl:                   DefaultTTL,
		sweepInterval:         DefaultSweepInterval,
		maxEntries:            DefaultMaxEntries,
		now:                   time.Now,
		ranker:                DefaultRankerConfig(),
		prefixes:              make(map[prefixKey]map[string]*replicaEntry),
		stats:                 make(map[statsKey]statEntry),
		prefixesByTenant:      make(map[string]int),
		prefixesByTenantModel: make(map[modelKey]int),
		servingByScope:        make(map[scopeKey]map[string]int),
		replicasByModel:       make(map[modelKey]map[string]struct{}),
		reportedModels:        make(map[string]struct{}),
	}
	for _, opt := range opts {
		opt(i)
	}
	// Clamp non-positive durations to defaults so misconfigured options can't
	// produce a divide-by-zero freshness or panic time.NewTicker(0) in Start.
	if i.ttl <= 0 {
		i.ttl = DefaultTTL
	}
	if i.sweepInterval <= 0 {
		i.sweepInterval = DefaultSweepInterval
	}
	return i
}

// Start marks the index ready and runs the eviction loop until ctx is done.
// It returns immediately; the loop runs in a goroutine.
func (i *Index) Start(ctx context.Context) {
	i.ready.Store(true)
	go func() {
		t := time.NewTicker(i.sweepInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				i.ready.Store(false)
				return
			case <-t.C:
				i.evictExpired()
			}
		}
	}()
}

// Ready reports whether the index is started and accepting/serving state.
// Engine-warm gating (waiting for initial sync) arrives with the C1 hook.
func (i *Index) Ready() bool { return i.ready.Load() }

// Ingest applies a replica update (from ReportCacheState). Updates are
// additive deltas, NOT snapshots: each call adds or refreshes the reported
// prefixes (idempotent on (replica_id, hash_scheme, prefix_hash)). A prefix's
// absence from a later update does NOT remove it — removals arrive as explicit
// CacheEvents (PREFIX_EVICTED / ALL_CLEARED) or expire via TTL. This matches the
// engine KV-event model (e.g. vLLM BlockStored / BlockRemoved) and the soft-state
// guarantee: a stale hint causes a cache miss, never a wrong answer.
func (i *Index) Ingest(u Update) {
	ts := u.Timestamp
	if ts.IsZero() {
		ts = i.now()
	}

	// Resolve the tenant's entry budget before locking: the resolver owns its
	// own lock, and calling it under i.mu would nest the index lock with the
	// policy-store lock on the hot ingest path. ok=false → no quota → unbounded.
	maxEntries, hasQuota := i.tenantQuotaFor(u.Tenant)

	i.mu.Lock()
	// prefix_hash is engine-opaque and only safe within a known hash_scheme; an
	// empty/unspecified scheme would collapse all engines into one domain, so we
	// do not index prefixes without one (fail open). Stats are scheme-independent.
	if u.HashScheme != "" {
		for _, p := range u.Prefixes {
			// Chain form: expand into one per-block entry per hash, keyed by
			// the block hash with cumulative tokenCount so a legacy exact-match
			// against any single block hash still works. The parallel arrays
			// must agree in length AND both be non-empty for the chain path
			// to engage; a chain whose two arrays disagree (including the
			// one-sided cases — hashes set with no counts, or counts set with
			// no hashes) is dropped silently (fail-soft — a stale hint is OK,
			// a wrong one isn't) and is NOT downgraded to the legacy single-
			// blob path. Only an entry that sets neither chain field falls
			// through to the legacy PrefixHash path.
			if len(p.BlockHashes) > 0 || len(p.BlockTokenCounts) > 0 {
				if len(p.BlockHashes) != len(p.BlockTokenCounts) {
					continue
				}
				var cumulative int32
				for j, h := range p.BlockHashes {
					cumulative += p.BlockTokenCounts[j]
					i.upsertReplicaLocked(prefixKey{u.Tenant, u.Model, u.HashScheme, string(h)}, u.ReplicaID, cumulative, ts)
				}
				// Preserve the legacy single-blob key too when the producer
				// set both representations on the same entry — so legacy
				// LookupRoute callers (no chain in the request) still hit
				// via exact-match on PrefixHash. The chain path otherwise
				// silently breaks backward-compat for unmigrated clients.
				if len(p.PrefixHash) > 0 {
					i.upsertReplicaLocked(prefixKey{u.Tenant, u.Model, u.HashScheme, string(p.PrefixHash)}, u.ReplicaID, p.TokenCount, ts)
				}
				continue
			}
			// Legacy single-blob exact-match entry. The helper does the
			// totalEntries + scope bookkeeping that main's inline form did,
			// so the chain and legacy paths agree on accounting.
			i.upsertReplicaLocked(prefixKey{u.Tenant, u.Model, u.HashScheme, string(p.PrefixHash)}, u.ReplicaID, p.TokenCount, ts)
		}
	}
	if u.Stats != nil {
		st := *u.Stats
		st.ReplicaID = u.ReplicaID // top-level replica id is authoritative — it is the index key
		// Clamp non-finite rates to 0 so a bad engine stat can't poison /snapshot:
		// encoding/json rejects NaN/±Inf and would 500 the endpoint until the
		// stat expires (TTL), stalling the CacheIndex poller.
		st.HitRate = sanitizeRate(st.HitRate)
		st.Pressure = sanitizeRate(st.Pressure)
		i.stats[statsKey{u.Tenant, u.Model, u.ReplicaID}] = statEntry{
			stats:         st,
			lastSeen:      ts,
			statsReported: ts,
		}
		i.statsScopeAddLocked(modelKey{u.Tenant, u.Model}, u.ReplicaID)
	}
	// Enforce the tenant's maxIndexEntries budget on the freshly-ingested state.
	// Fairness mode: evict only THIS tenant's own oldest distinct prefixes down to
	// budget; other tenants are untouched. Memory budgets are not enforced here
	// (the engine owns KV memory) — distinct-prefix count is the only dimension we
	// control.
	evictedPrefixes := 0
	if hasQuota {
		evictedPrefixes = i.evictOldestForTenantLocked(u.Tenant, maxEntries)
	}
	// enforceCapLocked is a no-op for reserved-tenant writes that don't push
	// the effective total (totalEntries - reservedEntries) over maxEntries —
	// reserved tenants do not fill the cap, so a probe ingest never triggers
	// eviction here, and a concurrent real-workload ingest sees the probe
	// entry as cap-invisible too. See WithReservedTenants.
	capEvicted := i.enforceCapLocked()
	i.mu.Unlock()

	if i.metrics != nil {
		if evictedPrefixes > 0 {
			i.metrics.AddTenantEvictions(u.Tenant, tenantEvictionReasonOverEntries, evictedPrefixes)
		}
		// Cap evictions are tallied per resolved algorithm so dashboards can tell
		// LRU from LFU pressure. Emitted after the lock, mirroring AddTenantEvictions.
		for algo, n := range capEvicted {
			i.metrics.AddIndexEvictions(algo, indexEvictionReasonCap, n)
		}
	}
	i.reportEntries()
}

// ApplyEvent applies a delta from PublishEvent. CacheEvent carries no
// hash_scheme, and prefix_hash is only meaningful within a scheme, so events
// never refresh scheme-specific prefix freshness — that is owned by
// ReportCacheState (authoritative). Events only do scheme-safe work: removals
// (conservative — at worst a cache miss, soft state) and replica liveness.
func (i *Index) ApplyEvent(ev Event) {
	ts := ev.Timestamp
	if ts.IsZero() {
		ts = i.now()
	}
	hash := string(ev.PrefixHash)

	i.mu.Lock()
	switch ev.Type {
	case EventReplicaUpdated:
		// Replica liveness only: keep its stats entry from expiring. Prefix
		// freshness is not touched here (no hash_scheme to target it safely).
		if s, ok := i.stats[statsKey{ev.Tenant, ev.Model, ev.ReplicaID}]; ok {
			s.lastSeen = ts
			i.stats[statsKey{ev.Tenant, ev.Model, ev.ReplicaID}] = s
		}
	case EventPrefixEvicted:
		// Remove the replica from the prefix across schemes — removal is
		// conservative, so matching opaque bytes without a scheme is safe.
		for key, replicas := range i.prefixes {
			if key.tenant != ev.Tenant || key.model != ev.Model || key.prefixHash != hash {
				continue
			}
			i.removeReplicaLocked(key, replicas, ev.ReplicaID)
		}
	case EventAllCleared:
		for key, replicas := range i.prefixes {
			if key.tenant != ev.Tenant || key.model != ev.Model {
				continue
			}
			i.removeReplicaLocked(key, replicas, ev.ReplicaID)
		}
		delete(i.stats, statsKey{ev.Tenant, ev.Model, ev.ReplicaID})
		i.statsScopeRemoveLocked(modelKey{ev.Tenant, ev.Model}, ev.ReplicaID)
	}
	// EventPrefixAdded is intentionally a no-op: ReportCacheState is the
	// authoritative add/refresh path, and the event lacks hash_scheme +
	// token_count to create or refresh a scheme-specific entry without risking
	// a cross-scheme false match.
	i.mu.Unlock()

	i.reportEntries()
}

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
// service layer can also downgrade an exact-match response to NO_HINT when
// the top score falls below the per-namespace routingFloorScore OR when
// every replica's matched_tokens falls below the per-namespace
// minimumMatchedTokens floor — see pkg/server/inferencecache_service.go
// buildLookupResponse. Old gateway clients that only inspect reason_code
// continue to fail open on a downgrade.
func (i *Index) lookupExact(req LookupRequest) ([]ReplicaScore, map[string][]*replicaEntry) {
	key := prefixKey{req.Tenant, req.Model, req.HashScheme, string(req.PrefixHash)}
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
		key := prefixKey{req.Tenant, req.Model, req.HashScheme, string(h)}
		holders := i.prefixes[key]
		blockTokens := req.BlockTokenCounts[blockIdx]
		if blockIdx == 0 {
			for id, e := range holders {
				if freshnessAt(now, e.lastSeen, ttl) <= 0 {
					continue // stale; will be swept
				}
				r := running{matchedTokens: blockTokens, oldestLastSeen: e.lastSeen}
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
				nr := running{matchedTokens: st.matchedTokens + blockTokens, oldestLastSeen: oldest, entries: st.entries}
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

// sloTightBiasCoefficient returns the coefficient applied to the freshness
// term inside (1 + freshness × coefficient). 0 → no bias (baseline). The
// bias only fires when (a) the ranker has SLOTightTTFTMs and SLOTightBias
// configured AND (b) the request carries a TTFT budget below the threshold.
func (i *Index) sloTightBiasCoefficient(ttftMs int32) float32 {
	if i.ranker.SLOTightTTFTMs <= 0 || i.ranker.SLOTightBias <= 0 {
		return 0
	}
	if ttftMs <= 0 || ttftMs >= i.ranker.SLOTightTTFTMs {
		return 0
	}
	return i.ranker.SLOTightBias
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

// CacheState returns the per-replica stats and the distinct-prefix count for a
// (tenant, model) namespace, for GetCacheState / observability. Replicas are
// sorted by ID for deterministic output.
func (i *Index) CacheState(tenant, model string) (replicas []ReplicaStats, totalPrefixes int) {
	i.mu.RLock()
	for key := range i.prefixes {
		if key.tenant == tenant && key.model == model {
			totalPrefixes++
		}
	}
	for sk, s := range i.stats {
		if sk.tenant == tenant && sk.model == model {
			replicas = append(replicas, s.stats)
		}
	}
	i.mu.RUnlock()

	sort.Slice(replicas, func(a, b int) bool { return replicas[a].ReplicaID < replicas[b].ReplicaID })
	return replicas, totalPrefixes
}

// DefaultTenantSentinel is the bucket distinct prefixes with no tenant ID are
// attributed to in cluster-wide aggregates: the empty string itself. Untenanted
// prefixes count toward the grand total, so they must also appear as a tenants[]
// bucket or Σ tenants[].indexEntries would silently fall short of the total —
// they're kept under "" rather than dropped. The empty string is deliberately
// collision-free: a real CacheTenant.spec.tenantID is MinLength=1, so this bucket
// can NEVER be claimed by a real tenant's per-CacheTenant status (which keys on
// the tenant ID). That is why no reserved non-empty name is needed.
const DefaultTenantSentinel = ""

// Aggregate is the index's prefix-count aggregate: the per-tenant distinct-prefix
// counts and the grand total, both produced by a SINGLE walk of the prefix map
// so they cannot disagree. Total == Σ PerTenant by construction — this is the
// hard invariant the CacheIndex/CacheTenant status surfaces rely on (a tenant's
// reported indexEntries always sum to the cluster prefix total). The counted
// unit is a distinct prefix key — (tenant, model, hash_scheme, prefix_hash),
// regardless of how many replicas hold it — which is exactly the unit
// prefixes.summary.total reports and the per-tenant maxIndexEntries quota bounds.
// (Tenant is part of the key, so the per-tenant partition is exact.)
type Aggregate struct {
	PerTenant map[string]int64
	Total     int64
}

// Aggregate returns the prefix-count aggregate under a single read-lock + single
// walk. Exposed so callers/tests can assert the invariant directly.
func (i *Index) Aggregate() Aggregate {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.aggregateLocked()
}

// aggregateLocked walks the prefix map exactly once, attributing every distinct
// prefix key to its tenant bucket and the running total in the same step. Caller
// holds at least the read lock. Because both numbers come from the one
// iteration, Total == Σ PerTenant always holds — no second pass, no separate
// counter that could drift. Reserved-tenant entries (see WithReservedTenants)
// are excluded from BOTH PerTenant and Total so the cluster aggregate the
// operator sees never temporarily surfaces synthetic probe state.
//
// Unit note: Aggregate counts DISTINCT PREFIX KEYS — one (tenant, model,
// hash_scheme, prefix_hash) tuple, regardless of how many replicas hold it.
// The cap accounting (totalEntries / reservedEntries / effectiveTotal)
// counts REPLICA×PREFIX entries — a prefix held by N replicas contributes N
// to totalEntries. The two are different units; they aren't expected to
// match numerically, only to track the same RESERVED-TENANT EXCLUSION
// principle (the operator-visible aggregate and the cap-enforcement view
// both treat reserved tenants as if they weren't there).
func (i *Index) aggregateLocked() Aggregate {
	agg := Aggregate{PerTenant: make(map[string]int64)}
	for key := range i.prefixes {
		if i.isReservedTenant(key.tenant) {
			continue
		}
		// Untenanted prefixes (key.tenant == "") bucket under "" — collision-free,
		// since no real CacheTenant tenantID is empty.
		agg.PerTenant[key.tenant]++
		agg.Total++
	}
	return agg
}

// Snapshot is a point-in-time, cluster-wide view of the index for the
// CacheIndex status surface (consumed by the controller). Metadata only.
//
// TotalPrefixes is the number of distinct prefix keys (a prefix held by
// multiple replicas counts once), and it equals the sum of
// tenants[].indexEntries — see Aggregate.
type Snapshot struct {
	Replicas      []ReplicaSnapshot `json:"replicas"`
	Tenants       []TenantSnapshot  `json:"tenants"`
	TotalPrefixes int               `json:"totalPrefixes"`
	HotPrefixes   int               `json:"hotPrefixes"` // always 0: intentionally unwired. The per-entry LFU access counter exists but governs cap eviction only; it is not aggregated into a cluster-wide "hot prefix" count.
}

// ReplicaSnapshot is the latest reported state for one replica, cluster-wide.
//
// PrefixCount and LastEventAt are derived from the prefix map and are the
// per-replica view consumed by the CacheBackend status projection (see
// internal/controller/cacheindex_controller.go). LastEventAt is the zero
// time when the replica holds no prefix entries — interpret a zero value as
// "no KV event observed yet" rather than "epoch."
//
// Tenant is the tenant_id the subscriber reported with the replica. The
// subscriber sidecar derives it from POD_NAMESPACE, so for the in-cluster
// path it equals the engine pod's namespace and lets a controller-side
// consumer scope a pod lookup. Empty when the replica is only known through
// older code paths that did not carry tenant context.
type ReplicaSnapshot struct {
	ReplicaID        string    `json:"replicaId"`
	Tenant           string    `json:"tenant,omitempty"`
	CacheMemoryBytes int64     `json:"cacheMemoryBytes"`
	HitRate          float32   `json:"hitRate"`
	Pressure         float32   `json:"pressure"`
	LastUpdate       time.Time `json:"lastUpdate"`
	PrefixCount      int       `json:"prefixCount"`
	LastEventAt      time.Time `json:"lastEventAt,omitempty"`
}

// TenantSnapshot is the aggregate footprint for one tenant.
//
// IndexEntries is the tenant's live distinct-prefix count, the quantity
// CacheTenant.spec.quota.maxIndexEntries bounds; across all tenants these sum
// to Snapshot.TotalPrefixes by construction (see Aggregate). Per-tenant memory
// is deliberately absent: cache_memory_bytes is the engine total across all
// tenants on a replica, so summing it per tenant double-counts on shared
// engines (see PROJECT_CONTEXT §Enforcement boundary).
type TenantSnapshot struct {
	TenantID     string  `json:"tenantId"`
	IndexEntries int64   `json:"indexEntries"`
	HitRate      float32 `json:"hitRate"`
}

// Snapshot returns the current cluster-wide aggregate. Replicas use the latest
// stats reported for each replica id; tenant hit-rate dedups replicas within a
// tenant (it is an approximation — a replica serving multiple models for a
// tenant is counted once). Results are sorted for deterministic output.
//
// Reserved tenants (see WithReservedTenants) are excluded from the snapshot
// entirely — replicas, tenants, and totals — so a probe in flight cannot
// temporarily publish its synthetic `__probe-<backend>` replica or the
// reserved tenant id into the CacheIndex CR status the controller polls.
// Same rationale as their exclusion from the cap-sweep victim set:
// server-internal state must not leak to operator-visible surfaces.
func (i *Index) Snapshot() Snapshot {
	i.mu.RLock()

	type tenantReplica struct{ tenant, replica string }
	// Cluster-wide replica snapshots key on (tenant, replicaID): two pods in
	// different namespaces can legitimately share a metadata.name (e.g.
	// "vllm-0"), and merging them into one row would mis-attribute prefixes
	// across tenancy. We then aggregate ONLY across models / hash_schemes
	// within the same (tenant, replicaID).
	latestByReplica := make(map[tenantReplica]statEntry)
	latestByTenantReplica := make(map[tenantReplica]statEntry)
	for sk, s := range i.stats {
		if i.isReservedTenant(sk.tenant) {
			continue
		}
		tr := tenantReplica{sk.tenant, sk.replicaID}
		if cur, ok := latestByReplica[tr]; !ok || s.lastSeen.After(cur.lastSeen) {
			latestByReplica[tr] = s
		}
		if cur, ok := latestByTenantReplica[tr]; !ok || s.lastSeen.After(cur.lastSeen) {
			latestByTenantReplica[tr] = s
		}
	}

	// Per-replica prefix counts + last KV-event timestamps. Keyed on
	// (tenant, replicaID) for the same reason as latestByReplica — so two
	// pods in different namespaces with the same name do not merge into a
	// single row. Derived from the prefix map (not the stats map) so the
	// projection reflects what the replica actually holds, not just whether
	// its stats are alive.
	type replicaPrefixAgg struct {
		count       int
		lastEventAt time.Time
	}
	prefixByReplica := make(map[tenantReplica]*replicaPrefixAgg)
	for key, replicas := range i.prefixes {
		if i.isReservedTenant(key.tenant) {
			continue
		}
		for id, e := range replicas {
			tr := tenantReplica{key.tenant, id}
			a := prefixByReplica[tr]
			if a == nil {
				a = &replicaPrefixAgg{}
				prefixByReplica[tr] = a
			}
			a.count++
			if e.lastSeen.After(a.lastEventAt) {
				a.lastEventAt = e.lastSeen
			}
		}
	}

	// Per-tenant distinct-prefix counts + the grand total come from ONE
	// authoritative walk (aggregateLocked) so the reported numbers can't drift:
	// TotalPrefixes == Σ tenants[].indexEntries by construction. The walk
	// already excludes reserved tenants, so the snapshot total matches what
	// operator dashboards expect to see.
	agg := i.aggregateLocked()
	snap := Snapshot{TotalPrefixes: int(agg.Total)}

	// Union of (tenant, replicaID) seen in stats AND in prefixes — a replica
	// may have reported prefixes via Ingest but had its stats entry evicted,
	// or vice versa; the snapshot surfaces both so per-backend projection is
	// robust. Each row is a unique (tenant, replicaID).
	seen := make(map[tenantReplica]struct{}, len(latestByReplica)+len(prefixByReplica))
	for tr := range latestByReplica {
		seen[tr] = struct{}{}
	}
	for tr := range prefixByReplica {
		seen[tr] = struct{}{}
	}
	for tr := range seen {
		r := ReplicaSnapshot{ReplicaID: tr.replica, Tenant: tr.tenant}
		if s, ok := latestByReplica[tr]; ok {
			r.CacheMemoryBytes = s.stats.CacheMemoryBytes
			r.HitRate = s.stats.HitRate
			r.Pressure = s.stats.Pressure
			r.LastUpdate = s.lastSeen
		}
		if a, ok := prefixByReplica[tr]; ok {
			r.PrefixCount = a.count
			r.LastEventAt = a.lastEventAt
		}
		snap.Replicas = append(snap.Replicas, r)
	}

	type tenantAgg struct {
		sumHit float64
		n      int
	}
	byTenant := make(map[string]*tenantAgg)
	for tr, s := range latestByTenantReplica {
		// Untenanted stats bucket under "" — the same key the entry walk uses, so a
		// tenant's stats and its indexEntries land together.
		bucket := tr.tenant
		a := byTenant[bucket]
		if a == nil {
			a = &tenantAgg{}
			byTenant[bucket] = a
		}
		a.sumHit += float64(s.stats.HitRate)
		a.n++
	}
	// Union the stats-bearing tenants with the entry-bearing tenants: a tenant
	// can have index entries but no stats reported yet (prefixes reported without
	// a stats payload), or stats but no live entries. A tenant in only one map
	// gets zeroes for the other dimension. Emitting every entry-bearing tenant
	// (agg.PerTenant) is what makes Σ tenants[].indexEntries == TotalPrefixes:
	// no entry bucket is ever dropped from tenants[].
	tenantSeen := make(map[string]struct{}, len(byTenant)+len(agg.PerTenant))
	emit := func(t string) {
		if _, ok := tenantSeen[t]; ok {
			return
		}
		tenantSeen[t] = struct{}{}
		var hit float32
		if a := byTenant[t]; a != nil {
			if a.n > 0 {
				hit = float32(a.sumHit / float64(a.n))
			}
		}
		snap.Tenants = append(snap.Tenants, TenantSnapshot{
			TenantID:     t,
			IndexEntries: agg.PerTenant[t],
			HitRate:      hit,
		})
	}
	for t := range byTenant {
		emit(t)
	}
	for t := range agg.PerTenant {
		emit(t)
	}
	i.mu.RUnlock()

	sort.Slice(snap.Replicas, func(a, b int) bool {
		if snap.Replicas[a].Tenant != snap.Replicas[b].Tenant {
			return snap.Replicas[a].Tenant < snap.Replicas[b].Tenant
		}
		return snap.Replicas[a].ReplicaID < snap.Replicas[b].ReplicaID
	})
	sort.Slice(snap.Tenants, func(a, b int) bool { return snap.Tenants[a].TenantID < snap.Tenants[b].TenantID })
	return snap
}

// EntryCountsByModel returns the number of distinct prefixes per model.
// Reserved-tenant entries (see WithReservedTenants) are excluded so the
// inferencecache_index_entries gauge reportEntries publishes never
// transiently surfaces synthetic probe state during a Run. Mirrors the
// snapshot/aggregate exclusion of the reserved scope.
func (i *Index) EntryCountsByModel() map[string]int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	counts := make(map[string]int)
	for key := range i.prefixes {
		if i.isReservedTenant(key.tenant) {
			continue
		}
		counts[key.model]++
	}
	return counts
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

// ttlFor returns the effective TTL for a tenant. A nil resolver, or one that
// returns <=0, falls back to the index's global TTL (which is itself clamped
// to DefaultTTL in New). Per-tenant TTL lets a namespace's CachePolicy widen
// or shrink the freshness window independently of every other namespace.
func (i *Index) ttlFor(tenant string) time.Duration {
	if i.ttlResolver != nil {
		if d := i.ttlResolver.TTL(tenant); d > 0 {
			return d
		}
	}
	return i.ttl
}

// evictionFor returns the normalized cap-eviction algorithm for a tenant:
// EvictionLFU only when the resolver explicitly says so, otherwise EvictionLRU
// (a nil resolver, an empty string, or any unrecognized value all fall back to
// LRU — the default and the pre-LFU behavior). Mirrors ttlFor.
func (i *Index) evictionFor(tenant string) string {
	if i.evictionResolver != nil {
		if strings.ToLower(i.evictionResolver.Eviction(tenant)) == EvictionLFU {
			return EvictionLFU
		}
	}
	return EvictionLRU
}

// distinguishingPower returns the multiplier the LookupRoute ranker uses to
// discount prefix matches that don't distinguish between replicas. Defined as
//
//	distinguishingPower = 1 - matching/total   (for total >= 2)
//
// so every-replica-holds-it overlaps (chat-template framing, RAG corpus
// headers, custom system prompts shared across the deployment) collapse to
// zero — the per-namespace post-score floor (CachePolicy.spec.routingFloorScore)
// then downgrades the response to NO_HINT and the gateway round-robins
// honestly instead of being credited with a trivial routing decision. A
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

// upsertReplicaLocked refreshes (or inserts) a replica's hold on one prefix
// key. Caller holds the write lock. Bumps totalEntries on first insert so the
// memory cap stays accurate when chains expand into N per-block entries, and
// bumps the (tenant, model, hash_scheme) → replica serving count in lockstep
// so tenantHotCandidates' O(1) scope lookup stays consistent with i.prefixes.
func (i *Index) upsertReplicaLocked(key prefixKey, replicaID string, tokenCount int32, ts time.Time) {
	replicas := i.prefixes[key]
	if replicas == nil {
		replicas = make(map[string]*replicaEntry)
		i.prefixes[key] = replicas
		// First replica of a brand-new prefix key → one more distinct prefix
		// for this tenant (the maxIndexEntries unit), and one more for the
		// (tenant, model) bucket the miss-classifier's UNKNOWN_MODEL check
		// reads.
		i.prefixesByTenant[key.tenant]++
		i.prefixesByTenantModel[modelKey{key.tenant, key.model}]++
	}
	e, existed := replicas[replicaID]
	if !existed {
		// First sight of this (prefix, replica): allocate a fresh entry whose
		// accessCount starts at zero. A refresh below mutates this SAME pointer
		// in place, so re-ingesting an existing entry never resets its LFU
		// counter (the counter tracks lookup usefulness, not ingest recency).
		e = &replicaEntry{}
		replicas[replicaID] = e
		i.totalEntries++
		if i.isReservedTenant(key.tenant) {
			i.reservedEntries++
		}
		i.scopeIncLocked(scopeKey{key.tenant, key.model, key.hashScheme}, replicaID)
	}
	e.tokenCount = tokenCount
	e.lastSeen = ts
}

// removeReplicaLocked drops a replica from a prefix, deleting the prefix if it
// becomes empty. Caller holds the write lock.
func (i *Index) removeReplicaLocked(key prefixKey, replicas map[string]*replicaEntry, replicaID string) {
	if _, ok := replicas[replicaID]; !ok {
		return
	}
	delete(replicas, replicaID)
	i.totalEntries--
	if i.isReservedTenant(key.tenant) {
		i.reservedEntries--
	}
	if len(replicas) == 0 {
		delete(i.prefixes, key)
		// Last replica gone → the prefix key is removed → one fewer distinct
		// prefix for this tenant AND for the (tenant, model) bucket.
		if n := i.prefixesByTenant[key.tenant] - 1; n <= 0 {
			delete(i.prefixesByTenant, key.tenant)
		} else {
			i.prefixesByTenant[key.tenant] = n
		}
		mk := modelKey{key.tenant, key.model}
		if n := i.prefixesByTenantModel[mk] - 1; n <= 0 {
			delete(i.prefixesByTenantModel, mk)
		} else {
			i.prefixesByTenantModel[mk] = n
		}
	}
	// Drop the replica from the (tenant, model, hash_scheme) serving count
	// in lockstep with the prefix removal so TENANT_HOT's O(1) check stays
	// consistent with what's actually in i.prefixes.
	i.scopeDecLocked(scopeKey{key.tenant, key.model, key.hashScheme}, replicaID)
}

// scopeIncLocked increments the (scope, replica) serving count, creating
// the inner map on first sight. Caller holds the write lock.
func (i *Index) scopeIncLocked(scope scopeKey, replicaID string) {
	m := i.servingByScope[scope]
	if m == nil {
		m = make(map[string]int)
		i.servingByScope[scope] = m
	}
	m[replicaID]++
}

// scopeDecLocked decrements the (scope, replica) serving count and removes
// the entry once it reaches zero (and the outer scope once it's empty), so
// the map doesn't accumulate dead keys. Caller holds the write lock.
func (i *Index) scopeDecLocked(scope scopeKey, replicaID string) {
	m := i.servingByScope[scope]
	if m == nil {
		return
	}
	n := m[replicaID] - 1
	if n <= 0 {
		delete(m, replicaID)
		if len(m) == 0 {
			delete(i.servingByScope, scope)
		}
		return
	}
	m[replicaID] = n
}

// statsScopeAddLocked records replicaID as having stats reported in (tenant,
// model) so tenantHotCandidates can scan only the relevant subset rather
// than the whole i.stats map. Caller holds the write lock.
func (i *Index) statsScopeAddLocked(mk modelKey, replicaID string) {
	m := i.replicasByModel[mk]
	if m == nil {
		m = make(map[string]struct{})
		i.replicasByModel[mk] = m
	}
	m[replicaID] = struct{}{}
}

// statsScopeRemoveLocked drops replicaID from the (tenant, model) set when
// its stats entry has been deleted (event-driven clear or TTL sweep).
// Caller holds the write lock.
func (i *Index) statsScopeRemoveLocked(mk modelKey, replicaID string) {
	m := i.replicasByModel[mk]
	if m == nil {
		return
	}
	delete(m, replicaID)
	if len(m) == 0 {
		delete(i.replicasByModel, mk)
	}
}

// evictExpired removes entries older than each tenant's TTL. Runs on the
// sweep loop. Per-tenant TTLs let two namespaces with very different
// CachePolicy TTLs evict on independent schedules (the sweep itself
// remains shared).
func (i *Index) evictExpired() {
	now := i.now()

	// Cache per-tenant TTL across one sweep so a slow resolver isn't called
	// once per entry. The cache lives only for this sweep — built lazily on
	// first sight of a tenant. Lookups still go through i.ttlFor (which may
	// call the resolver), but at most once per tenant per sweep.
	ttlCache := make(map[string]time.Duration)
	ttlOf := func(tenant string) time.Duration {
		if d, ok := ttlCache[tenant]; ok {
			return d
		}
		d := i.ttlFor(tenant)
		ttlCache[tenant] = d
		return d
	}
	// Resolve each tenant's algorithm once per sweep, only for the eviction
	// metric label — TTL eviction itself is algorithm-independent.
	algoCache := make(map[string]string)
	algoOf := func(tenant string) string {
		if a, ok := algoCache[tenant]; ok {
			return a
		}
		a := i.evictionFor(tenant)
		algoCache[tenant] = a
		return a
	}
	var ttlEvicted map[string]int

	i.mu.Lock()
	for key, replicas := range i.prefixes {
		ttl := ttlOf(key.tenant)
		for id, e := range replicas {
			if ttl > 0 && now.Sub(e.lastSeen) >= ttl {
				i.removeReplicaLocked(key, replicas, id)
				if ttlEvicted == nil {
					ttlEvicted = make(map[string]int, 2)
				}
				ttlEvicted[algoOf(key.tenant)]++
			}
		}
	}
	for sk, s := range i.stats {
		ttl := ttlOf(sk.tenant)
		if ttl > 0 && now.Sub(s.lastSeen) >= ttl {
			delete(i.stats, sk)
			i.statsScopeRemoveLocked(modelKey{sk.tenant, sk.model}, sk.replicaID)
		}
	}
	i.mu.Unlock()

	if i.metrics != nil {
		for algo, n := range ttlEvicted {
			i.metrics.AddIndexEvictions(algo, indexEvictionReasonTTL, n)
		}
	}
	i.reportEntries()
}

// enforceCapLocked evicts entries until totalEntries is within maxEntries,
// choosing victims by each entry's per-namespace algorithm under a single global
// cap. Victims are ordered by a unified key — effectiveCount ASC, then lastSeen
// ASC — where effectiveCount is the entry's LFU access count in an LFU namespace
// and 0 in an LRU namespace. So an all-LRU cap degenerates to pure
// oldest-by-lastSeen (the historical behavior), an all-LFU cap to
// lowest-count-first with an oldest-lastSeen tie-break, and mixed namespaces
// interleave LRU and low-count LFU entries by recency. The algorithm is resolved
// at sort time and never stored on the entry, so a policy switch takes effect on
// the next sweep with no counter migration. A final stable tie-break on the
// entry's identity keeps the victim set deterministic on full (count, lastSeen)
// ties regardless of map iteration order.
//
// Returns the count of entries evicted per algorithm ("lru"/"lfu") so the caller
// can emit the eviction metric AFTER releasing the lock (nil when nothing was
// over cap). Caller holds the write lock. maxEntries == 0 means unbounded. The
// sort is O(n log n); it only runs while over the cap.
func (i *Index) enforceCapLocked() map[string]int {
	// Reserved-tenant entries (the probe's synthetic state, etc.) are excluded
	// from the cap accounting AND the victim candidate set — so a concurrent
	// real-workload Ingest that fires while a probe is in flight cannot evict
	// a real-workload entry to make room for a transient probe entry that
	// cleanup is about to remove. effectiveTotal is the over-cap measurement.
	effectiveTotal := i.totalEntries - i.reservedEntries
	if i.maxEntries <= 0 || effectiveTotal <= i.maxEntries {
		return nil
	}
	// Resolve each tenant's algorithm once per sweep (the resolver takes the
	// policy-store lock; evictExpired already nests it under i.mu the same way).
	algoCache := make(map[string]string)
	algoOf := func(tenant string) string {
		if a, ok := algoCache[tenant]; ok {
			return a
		}
		a := i.evictionFor(tenant)
		algoCache[tenant] = a
		return a
	}
	type ref struct {
		key            prefixKey
		replica        string
		algo           string
		effectiveCount int64
		lastSeen       time.Time
	}
	all := make([]ref, 0, effectiveTotal)
	for key, replicas := range i.prefixes {
		// Skip reserved-tenant entries from the victim candidate set entirely.
		if i.isReservedTenant(key.tenant) {
			continue
		}
		algo := algoOf(key.tenant)
		for id, e := range replicas {
			var eff int64
			if algo == EvictionLFU {
				eff = e.accessCount.Load()
			}
			all = append(all, ref{key, id, algo, eff, e.lastSeen})
		}
	}
	sort.Slice(all, func(a, b int) bool {
		x, y := all[a], all[b]
		if x.effectiveCount != y.effectiveCount {
			return x.effectiveCount < y.effectiveCount
		}
		if !x.lastSeen.Equal(y.lastSeen) {
			return x.lastSeen.Before(y.lastSeen)
		}
		// Deterministic full-tie order (locked decision): break on the entry's
		// stable identity so the victim set never depends on map iteration order.
		if x.key != y.key {
			if x.key.tenant != y.key.tenant {
				return x.key.tenant < y.key.tenant
			}
			if x.key.model != y.key.model {
				return x.key.model < y.key.model
			}
			if x.key.hashScheme != y.key.hashScheme {
				return x.key.hashScheme < y.key.hashScheme
			}
			return x.key.prefixHash < y.key.prefixHash
		}
		return x.replica < y.replica
	})
	var evicted map[string]int
	for _, r := range all {
		if i.totalEntries-i.reservedEntries <= i.maxEntries {
			break
		}
		i.removeReplicaLocked(r.key, i.prefixes[r.key], r.replica)
		if evicted == nil {
			evicted = make(map[string]int, 2)
		}
		evicted[r.algo]++
	}
	return evicted
}

// isReservedTenant reports whether the given tenant id was declared as
// reserved via WithReservedTenants. Tight inlining matters because this is
// checked on every prefix-entry insert/remove. Returns false on nil sets so
// the default index (no reservations) pays exactly one extra map-nil check
// per call.
func (i *Index) isReservedTenant(tenant string) bool {
	if len(i.reservedTenants) == 0 {
		return false
	}
	_, ok := i.reservedTenants[tenant]
	return ok
}

// tenantQuotaFor returns the tenant's index-entry budget and whether one is
// configured. A nil resolver (or no matching CacheTenant) reports ok=false →
// the index leaves the tenant unbounded (fail open), identical to the behavior
// before any CacheTenant exists. Mirrors ttlFor.
func (i *Index) tenantQuotaFor(tenant string) (maxEntries int64, ok bool) {
	if i.quotaResolver == nil {
		return 0, false
	}
	return i.quotaResolver.TenantQuota(tenant)
}

// evictOldestForTenantLocked evicts the tenant's oldest distinct prefixes until
// its prefix count is within maxPrefixes, returning how many prefixes it
// removed. Caller holds the write lock.
//
// This is the Fairness-mode primitive: it touches ONLY the named tenant's
// prefixes, never another tenant's, so one tenant overrunning its budget can't
// evict a well-behaved tenant's hints. A prefix's age is its freshest replica's
// lastSeen (the most recent time any replica refreshed it); the oldest such
// prefixes go first. Removing a prefix drops ALL its replicas — the quota unit
// is the distinct prefix key, so a prefix counts once no matter how many
// replicas hold it. Ties on age break by prefix hash for deterministic order.
func (i *Index) evictOldestForTenantLocked(tenant string, maxPrefixes int64) int {
	if maxPrefixes < 0 {
		return 0
	}
	if int64(i.prefixesByTenant[tenant]) <= maxPrefixes {
		return 0
	}
	type ref struct {
		key prefixKey
		age time.Time // freshest replica lastSeen across the prefix's holders
	}
	all := make([]ref, 0, i.prefixesByTenant[tenant])
	for key, replicas := range i.prefixes {
		if key.tenant != tenant {
			continue
		}
		var newest time.Time
		for _, e := range replicas {
			if e.lastSeen.After(newest) {
				newest = e.lastSeen
			}
		}
		all = append(all, ref{key, newest})
	}
	sort.Slice(all, func(a, b int) bool {
		if !all[a].age.Equal(all[b].age) {
			return all[a].age.Before(all[b].age)
		}
		return all[a].key.prefixHash < all[b].key.prefixHash
	})
	removed := 0
	for _, r := range all {
		if int64(i.prefixesByTenant[tenant]) <= maxPrefixes {
			break
		}
		// Drop the whole prefix: collect replica ids first (removeReplicaLocked
		// mutates the inner map and deletes the key on the last removal).
		replicas := i.prefixes[r.key]
		ids := make([]string, 0, len(replicas))
		for id := range replicas {
			ids = append(ids, id)
		}
		for _, id := range ids {
			i.removeReplicaLocked(r.key, i.prefixes[r.key], id)
		}
		removed++
	}
	return removed
}

// reportEntries pushes live per-model counts to the metrics sink, if wired.
// Models that have drained to zero since the last report are explicitly set to
// 0 so their gauge series doesn't go stale.
//
// The snapshot is taken while holding reportMu so concurrent reporters can't
// publish out of order: reportMu serializes them, and because each snapshot
// reads live index state at publish time, whichever reporter runs last writes
// the current count (mutations complete under i.mu before reportEntries is
// called). Lock order is always reportMu → i.mu, never the reverse.
func (i *Index) reportEntries() {
	if i.metrics == nil {
		return
	}

	i.reportMu.Lock()
	defer i.reportMu.Unlock()
	counts := i.EntryCountsByModel()
	for model := range i.reportedModels {
		if _, ok := counts[model]; !ok {
			i.metrics.SetIndexEntries(model, 0)
			delete(i.reportedModels, model)
		}
	}
	for model, n := range counts {
		i.metrics.SetIndexEntries(model, n)
		i.reportedModels[model] = struct{}{}
	}
}

// sanitizeRate clamps non-finite values (NaN, ±Inf) to 0. Engine adapters can
// produce these (e.g. hit_rate = hits/(hits+misses) with 0 total). encoding/json
// rejects them, so letting them into the index would later break /snapshot.
func sanitizeRate(f float32) float32 {
	x := float64(f)
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return 0
	}
	return f
}

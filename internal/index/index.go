// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"context"
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

// prefixKey is the index partition key. adapter joins (tenant, model,
// hashScheme) ahead of the content hash because the content fingerprint is
// derived from token IDs ALONE: two requests with identical tokens under
// different LoRA adapters produce the SAME prefixHash, and without the partition
// they would collide on one map entry — a lookup for adapter A could be handed a
// replica that only holds adapter B's KV for those tokens. Partitioning (rather
// than mixing the adapter into the hash) keeps the fingerprint construction and
// its golden vectors untouched, and keeps the empty-adapter case byte-identical
// to the pre-adapter key.
//
// scopeKey deliberately does NOT gain adapter — see its doc comment.
type prefixKey struct {
	tenant     string
	model      string
	hashScheme string
	adapter    string // stable adapter identity ("" = default / no adapter)
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
//
// Adapter is intentionally NOT part of this key, even though it IS part of
// prefixKey. scopeKey answers a REPLICA-membership question ("which replicas
// serve this engine domain?"), and the surfaces that read it — the
// distinguishing-power denominator, the TENANT_HOT serving check, the
// AFFINITY_HINT candidate set, and the UNKNOWN_HASH_SCHEME classifier — carry no
// per-adapter cache-content claim (TENANT_HOT and AFFINITY_HINT ship
// matched_tokens=0 by contract). Adapters are also loaded and unloaded under a
// live engine, so adapter residency is a property of individual KV entries, not
// of a replica's membership in an engine domain. The aliasing hazard is
// content-level, so the CONTENT key is what gets partitioned; keeping scopeKey
// adapter-free additionally leaves the diagnostic reason codes meaning exactly
// what they meant before (UNKNOWN_HASH_SCHEME still means "wrong hash_scheme",
// not "unseen adapter").
type scopeKey struct {
	tenant     string
	model      string
	hashScheme string
}

type replicaEntry struct {
	tokenCount int32
	lastSeen   time.Time
	// tier is the cache tier this replica holds the prefix in, set by
	// upsertReplicaLocked (normalized to T1 when the producer left it
	// unspecified). Carried into the lookup's ReplicaScore.Tier; not read by
	// the ranker. A re-ingest can move an entry between tiers (last write wins).
	tier CacheTier
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
	// (tenant, model, hash_scheme, adapter, prefix_hash), regardless of how many
	// replicas hold it), so the per-tenant quota check at ingest is O(1) instead of
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

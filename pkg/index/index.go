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

// Metrics is the optional sink the index reports live entry counts to. It is
// satisfied by the server's Prometheus wiring; kept as a tiny interface so the
// index has no dependency on the metrics/registry implementation.
type Metrics interface {
	SetIndexEntries(model string, entries int)
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
type LookupRequest struct {
	Model            string
	Tenant           string
	HashScheme       string
	PrefixHash       []byte
	TokenCount       int32
	BlockHashes      [][]byte
	BlockTokenCounts []int32
}

// ReplicaScore is one ranked hint returned to the gateway. Higher score = better.
type ReplicaScore struct {
	ReplicaID             string
	Score                 float32
	MatchedTokens         int32
	EstimatedCacheHitProb float32
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

type replicaEntry struct {
	tokenCount int32
	lastSeen   time.Time
}

type statEntry struct {
	stats    ReplicaStats
	lastSeen time.Time
}

// Index is the in-memory, concurrent-safe, soft-state cache-state aggregator.
type Index struct {
	ttl           time.Duration
	sweepInterval time.Duration
	maxEntries    int
	now           func() time.Time
	metrics       Metrics

	ready atomic.Bool

	mu           sync.RWMutex
	prefixes     map[prefixKey]map[string]replicaEntry // prefix → replicaID → entry
	stats        map[statsKey]statEntry
	totalEntries int // sum of replicaEntries across all prefixes (memory bound)

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

// WithMetrics wires a metrics sink for inferencecache_index_entries.
func WithMetrics(m Metrics) Option { return func(i *Index) { i.metrics = m } }

// withClock overrides the time source (tests only).
func withClock(now func() time.Time) Option { return func(i *Index) { i.now = now } }

// New builds an index with the given options.
func New(opts ...Option) *Index {
	i := &Index{
		ttl:            DefaultTTL,
		sweepInterval:  DefaultSweepInterval,
		maxEntries:     DefaultMaxEntries,
		now:            time.Now,
		prefixes:       make(map[prefixKey]map[string]replicaEntry),
		stats:          make(map[statsKey]statEntry),
		reportedModels: make(map[string]struct{}),
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

	i.mu.Lock()
	// prefix_hash is engine-opaque and only safe within a known hash_scheme; an
	// empty/unspecified scheme would collapse all engines into one domain, so we
	// do not index prefixes without one (fail open). Stats are scheme-independent.
	if u.HashScheme != "" {
		for _, p := range u.Prefixes {
			// Chain form: expand into one per-block entry per hash, keyed by
			// the block hash with cumulative tokenCount so a legacy exact-match
			// against any single block hash still works. The parallel arrays
			// must agree in length; a chain with mismatched lengths is dropped
			// silently (fail-soft — a stale hint is OK, a wrong one isn't) and
			// is NOT downgraded to the legacy single-blob path.
			if len(p.BlockHashes) > 0 {
				if len(p.BlockHashes) != len(p.BlockTokenCounts) {
					continue
				}
				var cumulative int32
				for j, h := range p.BlockHashes {
					cumulative += p.BlockTokenCounts[j]
					i.upsertReplicaLocked(prefixKey{u.Tenant, u.Model, u.HashScheme, string(h)}, u.ReplicaID, cumulative, ts)
				}
				continue
			}
			// Legacy single-blob exact-match entry.
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
		i.stats[statsKey{u.Tenant, u.Model, u.ReplicaID}] = statEntry{stats: st, lastSeen: ts}
	}
	i.enforceCapLocked()
	i.mu.Unlock()

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
	}
	// EventPrefixAdded is intentionally a no-op: ReportCacheState is the
	// authoritative add/refresh path, and the event lacks hash_scheme +
	// token_count to create or refresh a scheme-specific entry without risking
	// a cross-scheme false match.
	i.mu.Unlock()

	i.reportEntries()
}

// Lookup returns replicas holding the requested prefix, ranked by matched
// tokens × freshness, best first. Empty result means "no hint" — the caller
// fails open.
//
// When the request carries a non-empty block-hash chain (BlockHashes with a
// matching-length BlockTokenCounts), the lookup walks the chain block-by-block
// and computes each replica's longest common leading run; MatchedTokens
// reflects the sum of the request's BlockTokenCounts for that run. When
// BlockHashes is set but BlockTokenCounts has a different length, the chain
// is malformed; the lookup returns no hint rather than silently downgrading
// to legacy exact-match on PrefixHash (symmetric with chain Ingest, which
// drops the entry — "a wrong hint is worse than a stale one"). When
// BlockHashes is empty the legacy exact-match path on PrefixHash is used.
func (i *Index) Lookup(req LookupRequest) []ReplicaScore {
	// Without a known hash_scheme, opaque hash bytes cannot be matched
	// safely (they would span engines), so fail open with no hint.
	if req.HashScheme == "" {
		return nil
	}
	if len(req.BlockHashes) > 0 {
		if len(req.BlockHashes) != len(req.BlockTokenCounts) {
			return nil
		}
		return i.lookupChain(req)
	}
	return i.lookupExact(req)
}

// lookupExact is the legacy single-blob exact-match path. Preserved
// unchanged so existing callers (no block-hash chain) keep their behavior.
func (i *Index) lookupExact(req LookupRequest) []ReplicaScore {
	key := prefixKey{req.Tenant, req.Model, req.HashScheme, string(req.PrefixHash)}
	now := i.now()

	i.mu.RLock()
	replicas := i.prefixes[key]
	scores := make([]ReplicaScore, 0, len(replicas))
	for id, e := range replicas {
		fresh := i.freshness(now, e.lastSeen)
		if fresh <= 0 {
			continue // stale; will be swept
		}
		scores = append(scores, ReplicaScore{
			ReplicaID:             id,
			Score:                 float32(e.tokenCount) * fresh,
			MatchedTokens:         e.tokenCount,
			EstimatedCacheHitProb: fresh,
		})
	}
	i.mu.RUnlock()

	sortScoresDescByScoreThenID(scores)
	return scores
}

// lookupChain implements longest-common-prefix matching against the
// per-block-hash index. For each replica we find the longest leading run
// [block_hashes[0]..block_hashes[k]] it holds; MatchedTokens is the sum of
// the request's BlockTokenCounts up to k (the request's view of how many
// tokens the matched prefix covers). The freshness signal is the OLDEST
// lastSeen across the matched blocks (the run's weakest link), so a single
// stale block can't make the whole run look fresher than it is.
func (i *Index) lookupChain(req LookupRequest) []ReplicaScore {
	type running struct {
		matchedTokens  int32
		oldestLastSeen time.Time
	}
	now := i.now()

	i.mu.RLock()
	current := map[string]running{}
	finalized := map[string]running{}
	for blockIdx, h := range req.BlockHashes {
		key := prefixKey{req.Tenant, req.Model, req.HashScheme, string(h)}
		holders := i.prefixes[key]
		blockTokens := req.BlockTokenCounts[blockIdx]
		if blockIdx == 0 {
			for id, e := range holders {
				if i.freshness(now, e.lastSeen) <= 0 {
					continue // stale; will be swept
				}
				current[id] = running{matchedTokens: blockTokens, oldestLastSeen: e.lastSeen}
			}
		} else {
			next := make(map[string]running, len(current))
			for id, st := range current {
				e, ok := holders[id]
				if !ok || i.freshness(now, e.lastSeen) <= 0 {
					finalized[id] = st
					continue
				}
				oldest := st.oldestLastSeen
				if e.lastSeen.Before(oldest) {
					oldest = e.lastSeen
				}
				next[id] = running{matchedTokens: st.matchedTokens + blockTokens, oldestLastSeen: oldest}
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
	i.mu.RUnlock()

	scores := make([]ReplicaScore, 0, len(finalized))
	for id, st := range finalized {
		if st.matchedTokens <= 0 {
			continue
		}
		fresh := i.freshness(now, st.oldestLastSeen)
		if fresh <= 0 {
			continue
		}
		scores = append(scores, ReplicaScore{
			ReplicaID:             id,
			Score:                 float32(st.matchedTokens) * fresh,
			MatchedTokens:         st.matchedTokens,
			EstimatedCacheHitProb: fresh,
		})
	}

	sortScoresDescByScoreThenID(scores)
	return scores
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

// Snapshot is a point-in-time, cluster-wide view of the index for the
// CacheIndex status surface (consumed by the controller). Metadata only.
type Snapshot struct {
	Replicas      []ReplicaSnapshot `json:"replicas"`
	Tenants       []TenantSnapshot  `json:"tenants"`
	TotalPrefixes int               `json:"totalPrefixes"`
	HotPrefixes   int               `json:"hotPrefixes"` // 0 until access-counting exists
}

// ReplicaSnapshot is the latest reported state for one replica, cluster-wide.
type ReplicaSnapshot struct {
	ReplicaID        string    `json:"replicaId"`
	CacheMemoryBytes int64     `json:"cacheMemoryBytes"`
	HitRate          float32   `json:"hitRate"`
	Pressure         float32   `json:"pressure"`
	LastUpdate       time.Time `json:"lastUpdate"`
}

// TenantSnapshot is the aggregate footprint for one tenant.
type TenantSnapshot struct {
	TenantID   string  `json:"tenantId"`
	MemoryUsed int64   `json:"memoryUsed"`
	HitRate    float32 `json:"hitRate"`
}

// Snapshot returns the current cluster-wide aggregate. Replicas use the latest
// stats reported for each replica id; tenant memory/hit-rate dedup replicas
// within a tenant (it is an approximation — a replica serving multiple models
// for a tenant is counted once). Results are sorted for deterministic output.
func (i *Index) Snapshot() Snapshot {
	i.mu.RLock()

	type tenantReplica struct{ tenant, replica string }
	latestByReplica := make(map[string]statEntry)
	latestByTenantReplica := make(map[tenantReplica]statEntry)
	for sk, s := range i.stats {
		if cur, ok := latestByReplica[sk.replicaID]; !ok || s.lastSeen.After(cur.lastSeen) {
			latestByReplica[sk.replicaID] = s
		}
		tr := tenantReplica{sk.tenant, sk.replicaID}
		if cur, ok := latestByTenantReplica[tr]; !ok || s.lastSeen.After(cur.lastSeen) {
			latestByTenantReplica[tr] = s
		}
	}

	snap := Snapshot{TotalPrefixes: len(i.prefixes)}

	for id, s := range latestByReplica {
		snap.Replicas = append(snap.Replicas, ReplicaSnapshot{
			ReplicaID:        id,
			CacheMemoryBytes: s.stats.CacheMemoryBytes,
			HitRate:          s.stats.HitRate,
			Pressure:         s.stats.Pressure,
			LastUpdate:       s.lastSeen,
		})
	}

	type tenantAgg struct {
		mem    int64
		sumHit float64
		n      int
	}
	byTenant := make(map[string]*tenantAgg)
	for tr, s := range latestByTenantReplica {
		a := byTenant[tr.tenant]
		if a == nil {
			a = &tenantAgg{}
			byTenant[tr.tenant] = a
		}
		a.mem += s.stats.CacheMemoryBytes
		a.sumHit += float64(s.stats.HitRate)
		a.n++
	}
	for t, a := range byTenant {
		var hit float32
		if a.n > 0 {
			hit = float32(a.sumHit / float64(a.n))
		}
		snap.Tenants = append(snap.Tenants, TenantSnapshot{TenantID: t, MemoryUsed: a.mem, HitRate: hit})
	}
	i.mu.RUnlock()

	sort.Slice(snap.Replicas, func(a, b int) bool { return snap.Replicas[a].ReplicaID < snap.Replicas[b].ReplicaID })
	sort.Slice(snap.Tenants, func(a, b int) bool { return snap.Tenants[a].TenantID < snap.Tenants[b].TenantID })
	return snap
}

// EntryCountsByModel returns the number of distinct prefixes per model.
func (i *Index) EntryCountsByModel() map[string]int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	counts := make(map[string]int)
	for key := range i.prefixes {
		counts[key.model]++
	}
	return counts
}

// freshness decays linearly from 1 (just seen) to 0 (>= ttl old).
func (i *Index) freshness(now, lastSeen time.Time) float32 {
	age := now.Sub(lastSeen)
	if age <= 0 {
		return 1
	}
	if age >= i.ttl {
		return 0
	}
	return float32(1 - float64(age)/float64(i.ttl))
}

// upsertReplicaLocked refreshes (or inserts) a replica's hold on one prefix
// key. Caller holds the write lock. Bumps totalEntries on first insert so the
// memory cap stays accurate when chains expand into N per-block entries.
func (i *Index) upsertReplicaLocked(key prefixKey, replicaID string, tokenCount int32, ts time.Time) {
	replicas := i.prefixes[key]
	if replicas == nil {
		replicas = make(map[string]replicaEntry)
		i.prefixes[key] = replicas
	}
	if _, existed := replicas[replicaID]; !existed {
		i.totalEntries++
	}
	replicas[replicaID] = replicaEntry{tokenCount: tokenCount, lastSeen: ts}
}

// removeReplicaLocked drops a replica from a prefix, deleting the prefix if it
// becomes empty. Caller holds the write lock.
func (i *Index) removeReplicaLocked(key prefixKey, replicas map[string]replicaEntry, replicaID string) {
	if _, ok := replicas[replicaID]; !ok {
		return
	}
	delete(replicas, replicaID)
	i.totalEntries--
	if len(replicas) == 0 {
		delete(i.prefixes, key)
	}
}

// evictExpired removes entries older than the TTL. Runs on the sweep loop.
func (i *Index) evictExpired() {
	now := i.now()
	i.mu.Lock()
	for key, replicas := range i.prefixes {
		for id, e := range replicas {
			if now.Sub(e.lastSeen) >= i.ttl {
				i.removeReplicaLocked(key, replicas, id)
			}
		}
	}
	for sk, s := range i.stats {
		if now.Sub(s.lastSeen) >= i.ttl {
			delete(i.stats, sk)
		}
	}
	i.mu.Unlock()

	i.reportEntries()
}

// enforceCapLocked evicts oldest entries until within maxEntries. Caller holds
// the write lock. maxEntries == 0 means unbounded. The sort is O(n log n); it
// only runs while over the cap, which for soft state is an acceptable
// backstop — a smarter incremental scheme can replace it if profiling demands.
func (i *Index) enforceCapLocked() {
	if i.maxEntries <= 0 || i.totalEntries <= i.maxEntries {
		return
	}
	type ref struct {
		key      prefixKey
		replica  string
		lastSeen time.Time
	}
	all := make([]ref, 0, i.totalEntries)
	for key, replicas := range i.prefixes {
		for id, e := range replicas {
			all = append(all, ref{key, id, e.lastSeen})
		}
	}
	sort.Slice(all, func(a, b int) bool { return all[a].lastSeen.Before(all[b].lastSeen) })
	for _, r := range all {
		if i.totalEntries <= i.maxEntries {
			break
		}
		i.removeReplicaLocked(r.key, i.prefixes[r.key], r.replica)
	}
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

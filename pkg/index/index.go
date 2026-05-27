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
type PrefixRef struct {
	PrefixHash []byte
	TokenCount int32
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
type LookupRequest struct {
	Model      string
	Tenant     string
	HashScheme string
	PrefixHash []byte
	TokenCount int32
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

// Ingest applies an authoritative replica update (from ReportCacheState).
// Idempotent on (replica_id, hash_scheme, prefix_hash): re-reporting a prefix
// refreshes its freshness rather than duplicating it.
func (i *Index) Ingest(u Update) {
	ts := u.Timestamp
	if ts.IsZero() {
		ts = i.now()
	}

	i.mu.Lock()
	for _, p := range u.Prefixes {
		key := prefixKey{u.Tenant, u.Model, u.HashScheme, string(p.PrefixHash)}
		replicas := i.prefixes[key]
		if replicas == nil {
			replicas = make(map[string]replicaEntry)
			i.prefixes[key] = replicas
		}
		if _, existed := replicas[u.ReplicaID]; !existed {
			i.totalEntries++
		}
		replicas[u.ReplicaID] = replicaEntry{tokenCount: p.TokenCount, lastSeen: ts}
	}
	if u.Stats != nil {
		i.stats[statsKey{u.Tenant, u.Model, u.ReplicaID}] = statEntry{stats: *u.Stats, lastSeen: ts}
	}
	i.enforceCapLocked()
	i.mu.Unlock()

	i.reportEntries()
}

// ApplyEvent applies a delta from PublishEvent. PREFIX_ADDED refreshes an
// already-known entry (events lack hash_scheme/token_count to synthesize a
// new, matchable one — ReportCacheState is authoritative for additions).
func (i *Index) ApplyEvent(ev Event) {
	ts := ev.Timestamp
	if ts.IsZero() {
		ts = i.now()
	}
	hash := string(ev.PrefixHash)

	i.mu.Lock()
	switch ev.Type {
	case EventPrefixAdded, EventReplicaUpdated:
		// Refresh freshness for matching entries already known for this replica.
		for key, replicas := range i.prefixes {
			if key.tenant != ev.Tenant || key.model != ev.Model {
				continue
			}
			if ev.Type == EventPrefixAdded && key.prefixHash != hash {
				continue
			}
			if e, ok := replicas[ev.ReplicaID]; ok {
				e.lastSeen = ts
				replicas[ev.ReplicaID] = e
			}
		}
		if s, ok := i.stats[statsKey{ev.Tenant, ev.Model, ev.ReplicaID}]; ok {
			s.lastSeen = ts
			i.stats[statsKey{ev.Tenant, ev.Model, ev.ReplicaID}] = s
		}
	case EventPrefixEvicted:
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
	i.mu.Unlock()

	i.reportEntries()
}

// Lookup returns replicas holding the requested prefix (exact hash match within
// the same hash_scheme), ranked by matched tokens × freshness, best first.
// Empty result means "no hint" — the caller fails open.
func (i *Index) Lookup(req LookupRequest) []ReplicaScore {
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

	sort.Slice(scores, func(a, b int) bool {
		if scores[a].Score != scores[b].Score {
			return scores[a].Score > scores[b].Score
		}
		return scores[a].ReplicaID < scores[b].ReplicaID // stable, deterministic
	})
	return scores
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
func (i *Index) reportEntries() {
	if i.metrics == nil {
		return
	}
	counts := i.EntryCountsByModel()

	i.reportMu.Lock()
	defer i.reportMu.Unlock()
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

package index

import (
	"sync"
	"testing"
	"time"
)

// staticEviction is an EvictionResolver returning fixed per-tenant algorithms
// for tests. An absent tenant returns "" → the index defaults it to LRU.
type staticEviction map[string]string

func (s staticEviction) Eviction(tenant string) string { return s[tenant] }

// lookupExactN performs n exact-match lookups of a prefix. Each lookup that
// returns a hit bumps the LFU access counter once (in an LFU namespace).
func lookupExactN(idx *Index, tenant, model, scheme, prefix string, n int) {
	for i := 0; i < n; i++ {
		idx.Lookup(LookupRequest{Tenant: tenant, Model: model, HashScheme: scheme, PrefixHash: hash(prefix)})
	}
}

// accessCountOf reads an entry's LFU counter (same-package internal access).
func accessCountOf(t *testing.T, idx *Index, tenant, model, scheme, prefix, replica string) int64 {
	t.Helper()
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	e := idx.prefixes[prefixKey{tenant, model, scheme, prefix}][replica]
	if e == nil {
		t.Fatalf("no entry for prefix %q replica %q", prefix, replica)
	}
	return e.accessCount.Load()
}

// setAccessCount forces an entry's LFU counter, so a test can construct an
// identical index state for two algorithms without relying on lookup bumps.
func setAccessCount(t *testing.T, idx *Index, tenant, model, scheme, prefix, replica string, n int64) {
	t.Helper()
	idx.mu.Lock()
	defer idx.mu.Unlock()
	e := idx.prefixes[prefixKey{tenant, model, scheme, prefix}][replica]
	if e == nil {
		t.Fatalf("no entry for prefix %q replica %q", prefix, replica)
	}
	e.accessCount.Store(n)
}

func present(idx *Index, prefix string) bool {
	return len(idx.Lookup(LookupRequest{Tenant: "t", Model: "m", HashScheme: "vllm", PrefixHash: hash(prefix)})) > 0
}

// TestEvictByLFUEvictsLowestCountFirst proves the cap sweep under LFU evicts the
// lowest-access-count entry — and that a high-count entry survives even when it
// is the OLDEST (which LRU would evict first), so the dispatch is genuinely LFU.
func TestEvictByLFUEvictsLowestCountFirst(t *testing.T) {
	clk := &fakeClock{t: time.Unix(4_000_000, 0)}
	idx := New(withClock(clk.now), WithTTL(time.Hour), WithMaxEntries(3),
		WithEvictionResolver(staticEviction{"t": EvictionLFU}))

	// Ingest hot, warm, cold oldest-first; "hot" is the oldest entry.
	for _, p := range []string{"hot", "warm", "cold"} {
		idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
			Prefixes: []PrefixRef{{PrefixHash: hash(p), TokenCount: 10}}})
		clk.add(time.Minute)
	}

	// Counts: hot=5 (oldest but hottest), warm=2, cold=0. Lookups bump under LFU.
	lookupExactN(idx, "t", "m", "vllm", "hot", 5)
	lookupExactN(idx, "t", "m", "vllm", "warm", 2)

	// A fresh prefix (count 0, newest) pushes total to 4, over the cap of 3.
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("new"), TokenCount: 10}}})

	if total := idx.EntryCountsByModel()["m"]; total != 3 {
		t.Fatalf("cap should hold total at 3, got %d", total)
	}
	// Lowest count is a tie between cold(0) and new(0); tie-break is oldest →
	// cold (ingested before new) is the victim.
	if present(idx, "cold") {
		t.Fatalf("cold (lowest count, oldest of the zero-count entries) should be evicted")
	}
	// hot is the OLDEST entry but has the highest count → it must survive. Under
	// LRU it would have been evicted first; surviving proves the LFU dispatch.
	if !present(idx, "hot") {
		t.Fatalf("hot has the highest access count and must survive LFU cap eviction")
	}
	for _, p := range []string{"warm", "new"} {
		if !present(idx, p) {
			t.Fatalf("%q should survive LFU cap eviction", p)
		}
	}
}

// TestEvictByLFUTieBreaksOnLastSeenAscending proves that on equal access counts
// the cap sweep evicts the oldest-by-lastSeen entry first (deterministic).
func TestEvictByLFUTieBreaksOnLastSeenAscending(t *testing.T) {
	clk := &fakeClock{t: time.Unix(4_100_000, 0)}
	idx := New(withClock(clk.now), WithTTL(time.Hour), WithMaxEntries(2),
		WithEvictionResolver(staticEviction{"t": EvictionLFU}))

	// Three entries, all access count 0, ingested oldest-first.
	for _, p := range []string{"older", "newer", "trigger"} {
		idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
			Prefixes: []PrefixRef{{PrefixHash: hash(p), TokenCount: 10}}})
		clk.add(time.Minute)
	}

	if total := idx.EntryCountsByModel()["m"]; total != 2 {
		t.Fatalf("cap should hold total at 2, got %d", total)
	}
	// All counts equal → pure oldest-lastSeen order → "older" evicted.
	if present(idx, "older") {
		t.Fatalf("on a full access-count tie the oldest entry must be evicted first")
	}
	for _, p := range []string{"newer", "trigger"} {
		if !present(idx, p) {
			t.Fatalf("%q should survive the tie-break eviction", p)
		}
	}
}

// TestEvictionPolicySwitchSelectsDifferentVictims builds two indexes with an
// IDENTICAL entry+counter state and only the algorithm differing, then proves
// the cap sweep picks different victims — the dispatch reads the policy.
func TestEvictionPolicySwitchSelectsDifferentVictims(t *testing.T) {
	build := func(algo string) *Index {
		clk := &fakeClock{t: time.Unix(4_200_000, 0)}
		idx := New(withClock(clk.now), WithTTL(time.Hour), WithMaxEntries(3),
			WithEvictionResolver(staticEviction{"t": algo}))
		for _, p := range []string{"A", "B", "C"} { // A oldest … C newest
			idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
				Prefixes: []PrefixRef{{PrefixHash: hash(p), TokenCount: 10}}})
			clk.add(time.Minute)
		}
		// Identical counter state on both indexes: A hottest, all pre-existing
		// entries non-zero so the about-to-arrive D(0) is the unique LFU min.
		setAccessCount(t, idx, "t", "m", "vllm", "A", "r", 9)
		setAccessCount(t, idx, "t", "m", "vllm", "B", "r", 3)
		setAccessCount(t, idx, "t", "m", "vllm", "C", "r", 5)
		idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
			Prefixes: []PrefixRef{{PrefixHash: hash("D"), TokenCount: 10}}}) // count 0, newest, triggers cap
		return idx
	}

	lru := build(EvictionLRU)
	lfu := build(EvictionLFU)

	// LRU evicts the oldest entry (A) regardless of counts; D (newest) survives.
	if present(lru, "A") {
		t.Fatalf("LRU should evict the oldest entry A")
	}
	if !present(lru, "D") {
		t.Fatalf("LRU should keep the newest entry D")
	}
	// LFU evicts the lowest-count entry (D=0); the old-but-hot A survives.
	if present(lfu, "D") {
		t.Fatalf("LFU should evict the lowest-count entry D")
	}
	if !present(lfu, "A") {
		t.Fatalf("LFU should keep the high-count entry A")
	}
}

// TestLFUCounterAtomicityUnderConcurrentLookups hammers one entry with many
// concurrent lookups and asserts no increments are lost (run with -race).
func TestLFUCounterAtomicityUnderConcurrentLookups(t *testing.T) {
	idx := New(WithTTL(time.Hour), WithEvictionResolver(staticEviction{"t": EvictionLFU}))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}}})

	const goroutines, perGoroutine = 50, 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lookupExactN(idx, "t", "m", "vllm", "p", perGoroutine)
		}()
	}
	wg.Wait()

	want := int64(goroutines * perGoroutine)
	if got := accessCountOf(t, idx, "t", "m", "vllm", "p", "r"); got != want {
		t.Fatalf("accessCount = %d, want %d (lost increments under concurrency)", got, want)
	}
}

// TestLFUCounterOnlyBumpsUnderLFU proves the lookup-hit increment is gated on
// the namespace algorithm: LRU namespaces never pay the atomic Add.
func TestLFUCounterOnlyBumpsUnderLFU(t *testing.T) {
	lru := New(WithTTL(time.Hour), WithEvictionResolver(staticEviction{"t": EvictionLRU}))
	lru.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}}})
	lookupExactN(lru, "t", "m", "vllm", "p", 5)
	if got := accessCountOf(t, lru, "t", "m", "vllm", "p", "r"); got != 0 {
		t.Fatalf("LRU namespace bumped the counter: got %d, want 0", got)
	}

	lfu := New(WithTTL(time.Hour), WithEvictionResolver(staticEviction{"t": EvictionLFU}))
	lfu.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}}})
	lookupExactN(lfu, "t", "m", "vllm", "p", 5)
	if got := accessCountOf(t, lfu, "t", "m", "vllm", "p", "r"); got != 5 {
		t.Fatalf("LFU namespace counter = %d, want 5", got)
	}
}

// TestLFUCounterSurvivesReingest proves a re-ingest (freshness refresh) of an
// existing entry preserves its access counter — the counter tracks lookup
// usefulness, not ingest recency.
func TestLFUCounterSurvivesReingest(t *testing.T) {
	idx := New(WithTTL(time.Hour), WithEvictionResolver(staticEviction{"t": EvictionLFU}))
	u := Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}}}
	idx.Ingest(u)
	lookupExactN(idx, "t", "m", "vllm", "p", 3)
	idx.Ingest(u) // refresh
	if got := accessCountOf(t, idx, "t", "m", "vllm", "p", "r"); got != 3 {
		t.Fatalf("re-ingest reset the LFU counter: got %d, want 3", got)
	}
}

// TestLFUCounterBumpsEveryBlockInAChainHit proves the chain (longest-prefix)
// lookup path bumps every block entry in a replica's matched run, and only the
// matched run — a block beyond the overlap is not counted.
func TestLFUCounterBumpsEveryBlockInAChainHit(t *testing.T) {
	idx := New(WithTTL(time.Hour), WithEvictionResolver(staticEviction{"t": EvictionLFU}))

	// replica-a holds blocks b1,b2,b3,x4; the request shares the leading b1,b2,b3.
	hashesA, countsA := chain("b1", "b2", "b3", "x4")
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: hashesA, BlockTokenCounts: countsA}}})

	reqHashes, reqCounts := chain("b1", "b2", "b3")
	for i := 0; i < 2; i++ {
		if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
			BlockHashes: reqHashes, BlockTokenCounts: reqCounts}); len(got) != 1 {
			t.Fatalf("chain lookup should hit replica r, got %d scores", len(got))
		}
	}

	// Each of the 3 matched blocks was bumped once per lookup → 2.
	for _, b := range []string{"b1", "b2", "b3"} {
		if got := accessCountOf(t, idx, "t", "m", "vllm", b, "r"); got != 2 {
			t.Fatalf("block %q accessCount = %d, want 2", b, got)
		}
	}
	// The non-matched block x4 was never part of the returned run → not bumped.
	if got := accessCountOf(t, idx, "t", "m", "vllm", "x4", "r"); got != 0 {
		t.Fatalf("non-matched block x4 accessCount = %d, want 0", got)
	}
}

// TestIndexEvictionMetricLabels asserts the cap and TTL sweeps emit
// inferencecache_index_evictions_total with the right {algorithm, reason}.
func TestIndexEvictionMetricLabels(t *testing.T) {
	// Cap path, LFU namespace → {lfu, cap}.
	mCapLFU := &countingMetrics{}
	idx := New(WithTTL(time.Hour), WithMaxEntries(1), WithMetrics(mCapLFU),
		WithEvictionResolver(staticEviction{"t": EvictionLFU}))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p1"), TokenCount: 10}}})
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p2"), TokenCount: 10}}}) // over cap → evict 1
	if got := mCapLFU.indexEvictionCount(EvictionLFU, indexEvictionReasonCap); got != 1 {
		t.Fatalf("cap eviction metric {lfu,cap} = %d, want 1", got)
	}

	// Cap path, default (no policy) namespace → {lru, cap}.
	mCapLRU := &countingMetrics{}
	idx2 := New(WithTTL(time.Hour), WithMaxEntries(1), WithMetrics(mCapLRU))
	idx2.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p1"), TokenCount: 10}}})
	idx2.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p2"), TokenCount: 10}}})
	if got := mCapLRU.indexEvictionCount(EvictionLRU, indexEvictionReasonCap); got != 1 {
		t.Fatalf("cap eviction metric {lru,cap} = %d, want 1", got)
	}

	// TTL path → {lfu, ttl}.
	mTTL := &countingMetrics{}
	clk := &fakeClock{t: time.Unix(4_300_000, 0)}
	idx3 := New(withClock(clk.now), WithTTL(10*time.Minute), WithMetrics(mTTL),
		WithEvictionResolver(staticEviction{"t": EvictionLFU}))
	idx3.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}}})
	clk.add(11 * time.Minute)
	idx3.evictExpired()
	if got := mTTL.indexEvictionCount(EvictionLFU, indexEvictionReasonTTL); got != 1 {
		t.Fatalf("ttl eviction metric {lfu,ttl} = %d, want 1", got)
	}
}

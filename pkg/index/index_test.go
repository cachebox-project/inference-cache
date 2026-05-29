package index

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually-advanced time source for deterministic freshness/TTL tests.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time      { return c.t }
func (c *fakeClock) add(d time.Duration) { c.t = c.t.Add(d) }

func hash(s string) []byte { return []byte(s) }

func TestIngestAndLookupRanksByTokensAndFreshness(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	idx := New(withClock(clk.now), WithTTL(time.Hour))

	// replica-a holds the prefix with 100 tokens; replica-b with 50. Same freshness.
	idx.Ingest(Update{ReplicaID: "replica-a", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p1"), TokenCount: 100}}})
	idx.Ingest(Update{ReplicaID: "replica-b", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p1"), TokenCount: 50}}})

	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p1")})
	if len(got) != 2 {
		t.Fatalf("expected 2 replica scores, got %d", len(got))
	}
	if got[0].ReplicaID != "replica-a" {
		t.Fatalf("expected replica-a ranked first (more matched tokens), got %q", got[0].ReplicaID)
	}
	if got[0].MatchedTokens != 100 {
		t.Fatalf("matched tokens = %d, want 100", got[0].MatchedTokens)
	}

	// Now make replica-b's entry fresher and replica-a stale-ish: freshness should
	// flip ranking if the token gap is small enough. Re-report b at a later time.
	clk.add(50 * time.Minute) // a is now 50m old (freshness ~0.17), b re-reported fresh
	idx.Ingest(Update{ReplicaID: "replica-b", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p1"), TokenCount: 50}}})

	got = idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p1")})
	// a: 100 * ~0.167 ≈ 16.7 ; b: 50 * 1.0 = 50 → b wins on freshness.
	if got[0].ReplicaID != "replica-b" {
		t.Fatalf("expected replica-b ranked first after freshness decay, got %q (scores: %+v)", got[0].ReplicaID, got)
	}
}

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

func TestIngestIsIdempotent(t *testing.T) {
	idx := New()
	u := Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}}}
	idx.Ingest(u)
	idx.Ingest(u)
	idx.Ingest(u)

	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p")}); len(got) != 1 {
		t.Fatalf("re-reporting the same prefix should not duplicate: got %d scores", len(got))
	}
	if got := idx.EntryCountsByModel()["m"]; got != 1 {
		t.Fatalf("entry count = %d, want 1", got)
	}
}

func TestEvictExpiredRemovesStaleEntries(t *testing.T) {
	clk := &fakeClock{t: time.Unix(2_000_000, 0)}
	idx := New(withClock(clk.now), WithTTL(10*time.Minute))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}}})

	clk.add(11 * time.Minute) // past TTL
	idx.evictExpired()

	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p")}); len(got) != 0 {
		t.Fatalf("stale entry should be evicted, got %d scores", len(got))
	}
	if n := idx.EntryCountsByModel()["m"]; n != 0 {
		t.Fatalf("entry count after eviction = %d, want 0", n)
	}
}

func TestMaxEntriesCapEvictsOldest(t *testing.T) {
	clk := &fakeClock{t: time.Unix(3_000_000, 0)}
	idx := New(withClock(clk.now), WithTTL(time.Hour), WithMaxEntries(2))

	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("old"), TokenCount: 1}}})
	clk.add(time.Minute)
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("mid"), TokenCount: 1}}})
	clk.add(time.Minute)
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("new"), TokenCount: 1}}}) // exceeds cap of 2

	if total := idx.EntryCountsByModel()["m"]; total != 2 {
		t.Fatalf("expected cap to hold total at 2, got %d", total)
	}
	// Oldest ("old") should be gone; "new" present.
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("old")}); len(got) != 0 {
		t.Fatalf("oldest entry should have been evicted by the cap")
	}
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("new")}); len(got) != 1 {
		t.Fatalf("newest entry should be retained under the cap")
	}
}

func TestApplyEventEvictAndClear(t *testing.T) {
	idx := New()
	ingest := func(replica, h string) {
		idx.Ingest(Update{ReplicaID: replica, Model: "m", Tenant: "t", HashScheme: "vllm",
			Prefixes: []PrefixRef{{PrefixHash: hash(h), TokenCount: 10}}})
	}
	ingest("r1", "p1")
	ingest("r1", "p2")
	ingest("r2", "p1")

	// PREFIX_EVICTED for r1/p1 removes only that replica from that prefix.
	idx.ApplyEvent(Event{Type: EventPrefixEvicted, ReplicaID: "r1", Model: "m", Tenant: "t", PrefixHash: hash("p1")})
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p1")}); len(got) != 1 || got[0].ReplicaID != "r2" {
		t.Fatalf("after evict, p1 should only have r2; got %+v", got)
	}

	// ALL_CLEARED for r1 drops the remainder of r1's entries.
	idx.ApplyEvent(Event{Type: EventAllCleared, ReplicaID: "r1", Model: "m", Tenant: "t"})
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p2")}); len(got) != 0 {
		t.Fatalf("ALL_CLEARED should remove r1/p2; got %+v", got)
	}
}

func TestPrefixAddedEventDoesNotRefreshAcrossSchemes(t *testing.T) {
	clk := &fakeClock{t: time.Unix(5_000_000, 0)}
	idx := New(withClock(clk.now), WithTTL(10*time.Minute))

	// Same opaque prefix bytes under two engine schemes for the same replica.
	for _, scheme := range []string{"vllm", "sglang"} {
		idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: scheme,
			Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 1}}})
	}

	clk.add(9 * time.Minute) // both entries are 9m old (TTL 10m)
	// A PREFIX_ADDED event (no hash_scheme) must NOT refresh either scheme's entry.
	idx.ApplyEvent(Event{Type: EventPrefixAdded, ReplicaID: "r", Model: "m", Tenant: "t", PrefixHash: hash("p")})

	clk.add(2 * time.Minute) // now 11m old → past TTL since the event did not refresh
	idx.evictExpired()

	for _, scheme := range []string{"vllm", "sglang"} {
		if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: scheme, PrefixHash: hash("p")}); len(got) != 0 {
			t.Fatalf("scheme %q entry should have expired (PREFIX_ADDED must not refresh): got %+v", scheme, got)
		}
	}
}

func TestSnapshotAggregates(t *testing.T) {
	idx := New()
	idx.Ingest(Update{ReplicaID: "replica-a", Model: "m1", Tenant: "tenant-a", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p1"), TokenCount: 1}},
		Stats:    &ReplicaStats{CacheMemoryBytes: 100, HitRate: 0.8, Pressure: 0.5}})
	// Same replica reports again under a different model for the same tenant: the
	// tenant footprint must count the replica once (dedup), not double its memory.
	idx.Ingest(Update{ReplicaID: "replica-a", Model: "m2", Tenant: "tenant-a", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p2"), TokenCount: 1}},
		Stats:    &ReplicaStats{CacheMemoryBytes: 100, HitRate: 0.8, Pressure: 0.5}})
	idx.Ingest(Update{ReplicaID: "replica-b", Model: "m1", Tenant: "tenant-b", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p3"), TokenCount: 1}},
		Stats:    &ReplicaStats{CacheMemoryBytes: 200, HitRate: 0.6, Pressure: 0.3}})

	snap := idx.Snapshot()

	if snap.TotalPrefixes != 3 {
		t.Fatalf("total prefixes = %d, want 3", snap.TotalPrefixes)
	}
	if snap.HotPrefixes != 0 {
		t.Fatalf("hot prefixes = %d, want 0 (not tracked yet)", snap.HotPrefixes)
	}
	// Replicas sorted by id, deduped cluster-wide.
	if len(snap.Replicas) != 2 || snap.Replicas[0].ReplicaID != "replica-a" || snap.Replicas[1].ReplicaID != "replica-b" {
		t.Fatalf("replicas = %+v, want [replica-a replica-b]", snap.Replicas)
	}
	if snap.Replicas[0].CacheMemoryBytes != 100 || snap.Replicas[0].HitRate != 0.8 {
		t.Fatalf("replica-a stats = %+v", snap.Replicas[0])
	}
	// Tenants sorted by id; tenant-a counts replica-a once despite two reports.
	if len(snap.Tenants) != 2 {
		t.Fatalf("tenants = %+v, want 2", snap.Tenants)
	}
	if snap.Tenants[0].TenantID != "tenant-a" || snap.Tenants[0].MemoryUsed != 100 {
		t.Fatalf("tenant-a = %+v, want memoryUsed 100 (deduped)", snap.Tenants[0])
	}
	if snap.Tenants[1].TenantID != "tenant-b" || snap.Tenants[1].MemoryUsed != 200 {
		t.Fatalf("tenant-b = %+v, want memoryUsed 200", snap.Tenants[1])
	}
}

func TestReadyReflectsStartAndStop(t *testing.T) {
	idx := New(WithSweepInterval(10 * time.Millisecond))
	if idx.Ready() {
		t.Fatal("index should not be ready before Start")
	}
	ctx, cancel := context.WithCancel(context.Background())
	idx.Start(ctx)
	if !idx.Ready() {
		t.Fatal("index should be ready after Start")
	}
	cancel()
	// Ready flips to false once the loop observes cancellation.
	deadline := time.After(time.Second)
	for idx.Ready() {
		select {
		case <-deadline:
			t.Fatal("index still ready well after context cancel")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// countingMetrics records the latest reported entry count per model.
type countingMetrics struct{ last map[string]int }

func (c *countingMetrics) SetIndexEntries(model string, n int) {
	if c.last == nil {
		c.last = map[string]int{}
	}
	c.last[model] = n
}

func TestMetricsSinkReceivesCounts(t *testing.T) {
	m := &countingMetrics{}
	idx := New(WithMetrics(m))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("a"), TokenCount: 1}, {PrefixHash: hash("b"), TokenCount: 1}}})
	if m.last["m"] != 2 {
		t.Fatalf("metrics sink got %d entries for model m, want 2", m.last["m"])
	}
}

func TestNonPositiveDurationsClampToDefaults(t *testing.T) {
	// WithSweepInterval(0) must not panic time.NewTicker(0); both clamp to defaults.
	idx := New(WithTTL(0), WithSweepInterval(0))
	if idx.ttl != DefaultTTL {
		t.Fatalf("ttl = %v, want default %v", idx.ttl, DefaultTTL)
	}
	if idx.sweepInterval != DefaultSweepInterval {
		t.Fatalf("sweepInterval = %v, want default %v", idx.sweepInterval, DefaultSweepInterval)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	idx.Start(ctx) // would panic if sweepInterval were 0
}

func TestStatsKeyedByTopLevelReplicaID(t *testing.T) {
	idx := New()
	// The nested stats.ReplicaID disagrees with the authoritative top-level one;
	// CacheState must report the top-level id (the key), not the nested value.
	idx.Ingest(Update{ReplicaID: "real-replica", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 1}},
		Stats:    &ReplicaStats{ReplicaID: "mismatched", CacheMemoryBytes: 42}})

	replicas, total := idx.CacheState("t", "m")
	if total != 1 {
		t.Fatalf("total prefixes = %d, want 1", total)
	}
	if len(replicas) != 1 || replicas[0].ReplicaID != "real-replica" {
		t.Fatalf("stats should carry the top-level replica id, got %+v", replicas)
	}
	if replicas[0].CacheMemoryBytes != 42 {
		t.Fatalf("stats payload lost: cacheMemoryBytes = %d, want 42", replicas[0].CacheMemoryBytes)
	}
}

func TestConcurrentIngestReportsFinalCount(t *testing.T) {
	m := &countingMetrics{}
	idx := New(WithMetrics(m))

	const n = 50
	var wg sync.WaitGroup
	for k := 0; k < n; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
				Prefixes: []PrefixRef{{PrefixHash: []byte(fmt.Sprintf("p%d", k)), TokenCount: 1}}})
		}(k)
	}
	wg.Wait()

	if got := idx.EntryCountsByModel()["m"]; got != n {
		t.Fatalf("index has %d entries, want %d", got, n)
	}
	// After all reporters have run (serialized by reportMu), the gauge must equal
	// the live count — never a stale earlier snapshot.
	if m.last["m"] != n {
		t.Fatalf("reported gauge = %d, want %d (stale report ordering)", m.last["m"], n)
	}
}

func TestMetricsZeroedWhenModelDrains(t *testing.T) {
	clk := &fakeClock{t: time.Unix(4_000_000, 0)}
	m := &countingMetrics{}
	idx := New(withClock(clk.now), WithTTL(10*time.Minute), WithMetrics(m))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("a"), TokenCount: 1}}})
	if m.last["m"] != 1 {
		t.Fatalf("expected 1 entry reported, got %d", m.last["m"])
	}

	clk.add(11 * time.Minute) // expire everything
	idx.evictExpired()

	// The drained model's gauge must be reset to 0, not left stale at 1.
	if m.last["m"] != 0 {
		t.Fatalf("drained model gauge = %d, want 0", m.last["m"])
	}
}

func TestIngestSanitizesNonFiniteStats(t *testing.T) {
	idx := New()
	// NaN / +Inf / -Inf would later make /snapshot's JSON encode fail
	// (and 500 the endpoint) — Ingest must clamp them to 0 at the boundary.
	idx.Ingest(Update{
		ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 1}},
		Stats: &ReplicaStats{
			HitRate:  float32(math.NaN()),
			Pressure: float32(math.Inf(1)),
		},
	})
	replicas, _ := idx.CacheState("t", "m")
	if len(replicas) != 1 {
		t.Fatalf("expected 1 replica, got %d", len(replicas))
	}
	r := replicas[0]
	if x := float64(r.HitRate); math.IsNaN(x) || math.IsInf(x, 0) {
		t.Fatalf("HitRate not sanitized: %v", r.HitRate)
	}
	if x := float64(r.Pressure); math.IsNaN(x) || math.IsInf(x, 0) {
		t.Fatalf("Pressure not sanitized: %v", r.Pressure)
	}
	// The whole snapshot must JSON-encode cleanly — that's the failure mode
	// this guards: encoding/json rejects non-finite floats.
	if _, err := json.Marshal(idx.Snapshot()); err != nil {
		t.Fatalf("snapshot encode after sanitization: %v", err)
	}
}

func TestIngestSanitizesNegativeInfinity(t *testing.T) {
	idx := New()
	idx.Ingest(Update{
		ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Stats: &ReplicaStats{HitRate: float32(math.Inf(-1))},
	})
	replicas, _ := idx.CacheState("t", "m")
	if len(replicas) != 1 || replicas[0].HitRate != 0 {
		t.Fatalf("-Inf HitRate should be clamped to 0, got %+v", replicas)
	}
}

// ---------------------------------------------------------------------------
// Block-hash chain ingest + longest-prefix lookup
// ---------------------------------------------------------------------------

// chain assembles a parallel (hash, tokenCount) chain for the table-driven
// tests below. Block hashes are opaque bytes so we use short strings.
func chain(blocks ...string) (hashes [][]byte, counts []int32) {
	hashes = make([][]byte, len(blocks))
	counts = make([]int32, len(blocks))
	for i, b := range blocks {
		hashes[i] = []byte(b)
		counts[i] = 16 // uniform per-block token count for the test
	}
	return hashes, counts
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

// TestLegacyExactMatchPathUnchanged locks in the migration-window guarantee:
// legacy single-blob ingest + lookup behavior is unchanged from the B6 path.
func TestLegacyExactMatchPathUnchanged(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 128}}})
	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p")})
	if len(got) != 1 || got[0].ReplicaID != "r" || got[0].MatchedTokens != 128 {
		t.Fatalf("legacy exact-match path changed: got %+v", got)
	}
}

// TestChainLookupAgainstLegacyIngestExactOnly documents the migration window:
// a legacy-style ingest (PrefixHash only) can still be matched exactly by the
// chain path when the request's block 0 equals the stored single blob — but
// it can't drive partial-prefix matching against a single-blob entry.
func TestChainLookupAgainstLegacyIngestExactOnly(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: []byte("p"), TokenCount: 64}}})
	reqHashes, reqCounts := chain("p", "x", "y")
	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		BlockHashes: reqHashes, BlockTokenCounts: reqCounts})
	if len(got) != 1 || got[0].ReplicaID != "r" {
		t.Fatalf("chain lookup against legacy entry should still hit on block 0: got %+v", got)
	}
	if got[0].MatchedTokens != 16 {
		t.Fatalf("matched_tokens for 1-block partial = %d, want 16 (request BlockTokenCounts[0])", got[0].MatchedTokens)
	}
}

// TestChainIngestMismatchedLengthsDropped: parallel arrays must agree in
// length; a malformed PrefixEntry is dropped fail-soft (soft state — a
// stale hint is OK, a wrong one is not).
func TestChainIngestMismatchedLengthsDropped(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	hashes, _ := chain("b1", "b2", "b3")
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: hashes, BlockTokenCounts: []int32{16}}}})
	if n := idx.EntryCountsByModel()["m"]; n != 0 {
		t.Fatalf("mismatched chain lengths should drop the entry; got %d indexed", n)
	}
}

// TestChainIngestEmptyHashSchemeFailsOpen: the engine-opaque guarantee
// extends to chain ingest — no scheme, no indexing.
func TestChainIngestEmptyHashSchemeFailsOpen(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	hashes, counts := chain("b1", "b2", "b3")
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "",
		Prefixes: []PrefixRef{{BlockHashes: hashes, BlockTokenCounts: counts}}})
	if n := idx.EntryCountsByModel()["m"]; n != 0 {
		t.Fatalf("empty hash_scheme should drop chain ingest, got %d entries", n)
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

// TestChainIngestOneSidedHashesOnlyDropped covers the asymmetric malformed
// shape (BlockHashes set but BlockTokenCounts empty). Symmetric to the
// existing mismatched-length test; both paths must drop fail-soft.
func TestChainIngestOneSidedHashesOnlyDropped(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{
			BlockHashes:      [][]byte{[]byte("b1"), []byte("b2")},
			BlockTokenCounts: nil,
		}}})
	if n := idx.EntryCountsByModel()["m"]; n != 0 {
		t.Fatalf("hashes-only chain should drop, got %d entries", n)
	}
}

// TestChainIngestOneSidedCountsOnlyDropped covers the inverse asymmetric
// shape (counts set but hashes empty). Without this guard the entry would
// silently fall through to the legacy single-blob path with an empty
// PrefixHash key — a wrong-hint surface area.
func TestChainIngestOneSidedCountsOnlyDropped(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{
			PrefixHash:       []byte("legacy-p"),
			TokenCount:       64,
			BlockHashes:      nil,
			BlockTokenCounts: []int32{16, 16},
		}}})
	if n := idx.EntryCountsByModel()["m"]; n != 0 {
		t.Fatalf("counts-only chain should drop (must not downgrade to legacy), got %d entries", n)
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

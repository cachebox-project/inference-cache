package index

import (
	"context"
	"fmt"
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

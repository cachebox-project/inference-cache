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
// Ranking v2 — pressure / SLO / TENANT_HOT fallback
// ---------------------------------------------------------------------------

// TestLookupBaselinePreservedWhenSignalsAbsent locks in the contract that the
// new ranker reduces to the matched_tokens × freshness baseline (B6)
// when (a) no replica stats are reported (pressure=0) and (b) the request
// carries no SLO hint (TTFT=0). The whole reason the new score factors compose
// multiplicatively: this property has to hold so existing ranking tests stay
// authoritative for the baseline.
func TestLookupBaselinePreservedWhenSignalsAbsent(t *testing.T) {
	clk := &fakeClock{t: time.Unix(6_000_000, 0)}
	idx := New(withClock(clk.now), WithTTL(time.Hour))

	idx.Ingest(Update{ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 80}}})
	idx.Ingest(Update{ReplicaID: "r2", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 40}}})

	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p")})
	if len(got) != 2 {
		t.Fatalf("expected 2 scores, got %d", len(got))
	}
	// At ingest time freshness == 1 for both, no pressure, no SLO → score
	// must equal tokenCount exactly. Floats are exact here (whole ints × 1).
	if got[0].ReplicaID != "r1" || got[0].Score != 80 {
		t.Fatalf("r1 baseline score = %v (id %q), want 80", got[0].Score, got[0].ReplicaID)
	}
	if got[1].ReplicaID != "r2" || got[1].Score != 40 {
		t.Fatalf("r2 baseline score = %v (id %q), want 40", got[1].Score, got[1].ReplicaID)
	}
}

// TestLookupPressureAwareRanking walks a table of pressure profiles and
// asserts how the ordering changes vs. the baseline. The point is to show
// the ranker balances locality against load: a replica that holds the prefix
// but is saturated should yield to a fresher, less-loaded peer.
func TestLookupPressureAwareRanking(t *testing.T) {
	type replica struct {
		id         string
		tokenCount int32
		pressure   float32
	}
	tests := []struct {
		name      string
		pressureW float32
		replicas  []replica
		wantOrder []string // expected ReplicaID order, best first
	}{
		{
			// Both replicas hold the prefix with identical token count and
			// freshness. The only differentiator is pressure → low-pressure wins.
			name:      "equal tokens, pressure breaks the tie",
			pressureW: 1.0,
			replicas: []replica{
				{id: "saturated", tokenCount: 100, pressure: 0.9},
				{id: "idle", tokenCount: 100, pressure: 0.0},
			},
			wantOrder: []string{"idle", "saturated"},
		},
		{
			// The token-rich replica is also saturated (pressure=0.9, weight=1
			// → factor 0.1); a smaller-tokencount peer at low pressure can
			// overtake it.
			name:      "pressure flips locality vs. load",
			pressureW: 1.0,
			replicas: []replica{
				{id: "big-but-hot", tokenCount: 100, pressure: 0.9}, // 100 × 0.1 = 10
				{id: "small-cool", tokenCount: 50, pressure: 0.0},   // 50 × 1.0 = 50
			},
			wantOrder: []string{"small-cool", "big-but-hot"},
		},
		{
			// PressureWeight=0 → pressure factor collapses to 1 → ordering
			// matches the baseline (token count wins). This is the toggle a
			// future calibration could use to disable the penalty without
			// touching code paths.
			name:      "PressureWeight=0 disables the penalty",
			pressureW: 0.0,
			replicas: []replica{
				{id: "big-hot", tokenCount: 100, pressure: 0.9},
				{id: "small-cool", tokenCount: 50, pressure: 0.0},
			},
			wantOrder: []string{"big-hot", "small-cool"},
		},
		{
			// pressure > 1/weight clamps to 0: a replica with pressure 1.5
			// under weight 1 would otherwise produce a negative score and
			// silently outrank a 0-score peer due to sort stability.
			name:      "pressure factor clamps to zero",
			pressureW: 1.0,
			replicas: []replica{
				{id: "broken", tokenCount: 100, pressure: 1.5}, // factor → 0
				{id: "alive", tokenCount: 1, pressure: 0.0},    // factor → 1
			},
			wantOrder: []string{"alive", "broken"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clk := &fakeClock{t: time.Unix(7_000_000, 0)}
			cfg := DefaultRankerConfig()
			cfg.PressureWeight = tc.pressureW
			idx := New(withClock(clk.now), WithTTL(time.Hour), WithRanker(cfg))

			for _, r := range tc.replicas {
				idx.Ingest(Update{ReplicaID: r.id, Model: "m", Tenant: "t", HashScheme: "vllm",
					Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: r.tokenCount}},
					Stats:    &ReplicaStats{Pressure: r.pressure}})
			}

			got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p")})
			if len(got) != len(tc.wantOrder) {
				t.Fatalf("got %d scores, want %d (%+v)", len(got), len(tc.wantOrder), got)
			}
			for i, want := range tc.wantOrder {
				if got[i].ReplicaID != want {
					t.Errorf("rank %d = %q, want %q (full: %+v)", i, got[i].ReplicaID, want, got)
				}
			}
		})
	}
}

// TestLookupSLOAwareRankingBiasesFreshness exercises the tight-TTFT bias.
// Two replicas hold the prefix; one has many tokens but is older, the other
// fewer tokens and fresh. Without SLO pressure the token-rich older one wins
// (B6 baseline). Under tight SLO (ttft_ms below threshold) the freshness bias
// kicks in and the fresh one overtakes; under loose SLO the baseline ordering
// is restored. Table-shaped so adding bands (e.g. P95 vs P99 budgets) is easy.
func TestLookupSLOAwareRankingBiasesFreshness(t *testing.T) {
	clk := &fakeClock{t: time.Unix(8_000_000, 0)}

	cfg := DefaultRankerConfig()
	cfg.SLOTightTTFTMs = 100
	cfg.SLOTightBias = 5.0 // strong bias so the flip is unambiguous
	idx := New(withClock(clk.now), WithTTL(time.Hour), WithRanker(cfg))

	// big-old: 100 tokens, 20m old (freshness ≈ 2/3).
	// small-fresh: 50 tokens, just reported (freshness = 1).
	// Baseline:  big-old ≈ 66.7 ; small-fresh = 50 → big-old wins.
	// Tight SLO: small-fresh's freshness bonus (1 + 1×5 = 6) dominates
	// big-old's bonus (1 + 0.667×5 ≈ 4.33) → small-fresh wins.
	idx.Ingest(Update{ReplicaID: "big-old", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 100}}, Timestamp: clk.t})
	clk.add(20 * time.Minute)
	idx.Ingest(Update{ReplicaID: "small-fresh", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 50}}, Timestamp: clk.t})

	tests := []struct {
		name      string
		ttftMs    int32
		wantFirst string
	}{
		{"no SLO hint (baseline) → token-rich wins", 0, "big-old"},
		{"loose SLO (>= threshold) → no bias, baseline wins", 500, "big-old"},
		{"tight SLO (< threshold) → freshness bias flips ranking", 50, "small-fresh"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := idx.Lookup(LookupRequest{
				Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p"),
				TTFTBudgetMs: tc.ttftMs,
			})
			if len(got) != 2 {
				t.Fatalf("expected 2 scores, got %d (%+v)", len(got), got)
			}
			if got[0].ReplicaID != tc.wantFirst {
				t.Errorf("top rank = %q, want %q (full: %+v)", got[0].ReplicaID, tc.wantFirst, got)
			}
		})
	}
}

// TestLookupSLOBiasDisabledWhenKnobsZero pins the kill-switch: SLOTightBias
// = 0 collapses the bias coefficient to zero, so a tight SLO no longer
// changes ordering. Useful when a calibration regresses and we want the
// strict baseline back without code changes.
func TestLookupSLOBiasDisabledWhenKnobsZero(t *testing.T) {
	clk := &fakeClock{t: time.Unix(8_500_000, 0)}
	cfg := DefaultRankerConfig()
	cfg.SLOTightTTFTMs = 100
	cfg.SLOTightBias = 0 // disabled
	idx := New(withClock(clk.now), WithTTL(time.Hour), WithRanker(cfg))

	idx.Ingest(Update{ReplicaID: "big-old", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 100}}, Timestamp: clk.t})
	clk.add(20 * time.Minute)
	idx.Ingest(Update{ReplicaID: "small-fresh", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 50}}, Timestamp: clk.t})

	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("p"), TTFTBudgetMs: 50})
	if got[0].ReplicaID != "big-old" {
		t.Fatalf("with SLOTightBias=0, tight SLO must not change ordering; got %+v", got)
	}
}

// TestLookupRouteOrchestratorStrategies is the table-driven proof that the
// LookupRoute orchestrator picks the right strategy for each scenario:
// prefix-match, tenant-hot fallback, full miss. Adding a future strategy
// (e.g. longest-prefix block matching) plugs in here as one more row.
func TestLookupRouteOrchestratorStrategies(t *testing.T) {
	const (
		tenant = "t"
		model  = "m"
		scheme = "vllm"
	)
	hashFor := func(s string) []byte { return hash(s) }

	tests := []struct {
		name       string
		ingest     []Update // state to populate before lookup
		req        LookupRequest
		wantStrat  Strategy
		wantFirst  string // expected top-ranked replica id, "" if no scores
		wantScores int    // expected number of scores
	}{
		{
			name: "exact prefix match wins over a warm tenant",
			ingest: []Update{
				{ReplicaID: "prefix-holder", Model: model, Tenant: tenant, HashScheme: scheme,
					Prefixes: []PrefixRef{{PrefixHash: hashFor("p"), TokenCount: 32}},
					Stats:    &ReplicaStats{HitRate: 0.9}},
				{ReplicaID: "warm-only", Model: model, Tenant: tenant, HashScheme: scheme,
					Stats: &ReplicaStats{HitRate: 0.9}},
			},
			req:        LookupRequest{Model: model, Tenant: tenant, HashScheme: scheme, PrefixHash: hashFor("p")},
			wantStrat:  StrategyPrefixMatch,
			wantFirst:  "prefix-holder",
			wantScores: 1,
		},
		{
			name: "tenant-hot fallback on prefix miss with warm replica",
			ingest: []Update{
				{ReplicaID: "warm", Model: model, Tenant: tenant, HashScheme: scheme,
					Prefixes: []PrefixRef{{PrefixHash: hashFor("other"), TokenCount: 1}},
					Stats:    &ReplicaStats{HitRate: 0.7, Pressure: 0.1}},
				{ReplicaID: "cold", Model: model, Tenant: tenant, HashScheme: scheme,
					Prefixes: []PrefixRef{{PrefixHash: hashFor("other"), TokenCount: 1}},
					Stats:    &ReplicaStats{HitRate: 0.0, Pressure: 0.5}},
			},
			req:        LookupRequest{Model: model, Tenant: tenant, HashScheme: scheme, PrefixHash: hashFor("novel")},
			wantStrat:  StrategyTenantHot,
			wantFirst:  "warm",
			wantScores: 1, // cold replica filtered by hit_rate threshold
		},
		{
			name: "no prefix match AND no warm replicas → StrategyNone",
			ingest: []Update{
				{ReplicaID: "cold", Model: model, Tenant: tenant, HashScheme: scheme,
					Stats: &ReplicaStats{HitRate: 0.0}},
			},
			req:        LookupRequest{Model: model, Tenant: tenant, HashScheme: scheme, PrefixHash: hashFor("novel")},
			wantStrat:  StrategyNone,
			wantScores: 0,
		},
		{
			name:       "empty index, no signals → StrategyNone (fail open)",
			ingest:     nil,
			req:        LookupRequest{Model: model, Tenant: tenant, HashScheme: scheme, PrefixHash: hashFor("novel")},
			wantStrat:  StrategyNone,
			wantScores: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx := New(WithRanker(DefaultRankerConfig()))
			for _, u := range tc.ingest {
				idx.Ingest(u)
			}
			res := idx.LookupRoute(tc.req)
			if res.Strategy != tc.wantStrat {
				t.Fatalf("strategy = %v, want %v (scores %+v)", res.Strategy, tc.wantStrat, res.Scores)
			}
			if len(res.Scores) != tc.wantScores {
				t.Fatalf("got %d scores, want %d (%+v)", len(res.Scores), tc.wantScores, res.Scores)
			}
			if tc.wantFirst != "" && res.Scores[0].ReplicaID != tc.wantFirst {
				t.Errorf("top rank = %q, want %q", res.Scores[0].ReplicaID, tc.wantFirst)
			}
		})
	}
}

// TestTenantHotMatchedTokensIsZero pins a contract detail: a TENANT_HOT
// candidate carries MatchedTokens=0 because there is no prefix overlap. A
// gateway client that filters or weights by MatchedTokens must therefore
// treat 0 as "softer hint" rather than "no overlap → ignore"; the reason_code
// is the load-bearing signal. Ingests an unrelated prefix entry under the
// requested hash_scheme so the replica clears the engine-domain guard.
func TestTenantHotMatchedTokensIsZero(t *testing.T) {
	idx := New(WithRanker(DefaultRankerConfig()))
	idx.Ingest(Update{ReplicaID: "warm", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("other"), TokenCount: 1}},
		Stats:    &ReplicaStats{HitRate: 0.8}})

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyTenantHot || len(res.Scores) != 1 {
		t.Fatalf("expected single TENANT_HOT candidate, got strat=%v scores=%+v", res.Strategy, res.Scores)
	}
	if res.Scores[0].MatchedTokens != 0 {
		t.Fatalf("TENANT_HOT MatchedTokens must be 0 (no prefix overlap), got %d", res.Scores[0].MatchedTokens)
	}
}

// TestTenantHotRequiresRecentStats pins the recency cutoff: a warm replica
// whose stats are older than TenantHotMaxAge does NOT trigger the fallback —
// the index would otherwise hint based on stale information.
func TestTenantHotRequiresRecentStats(t *testing.T) {
	clk := &fakeClock{t: time.Unix(10_000_000, 0)}
	cfg := DefaultRankerConfig()
	cfg.TenantHotMaxAge = 5 * time.Minute
	idx := New(withClock(clk.now), WithTTL(time.Hour), WithRanker(cfg))

	// Ingest a prefix entry so the engine-domain guard is satisfied; the
	// test is about stats recency, not the domain check.
	idx.Ingest(Update{ReplicaID: "stale-warm", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("other"), TokenCount: 1}},
		Stats:    &ReplicaStats{HitRate: 0.9}})

	// Advance past TenantHotMaxAge — the stats are now "old" for fallback
	// purposes (even though they're still inside the global TTL).
	clk.add(10 * time.Minute)

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone {
		t.Fatalf("stale stats should NOT trigger TENANT_HOT, got %v (%+v)", res.Strategy, res.Scores)
	}
}

// TestTenantHotHonorsHitRateThreshold pins the warmth threshold: a replica
// with hit_rate below TenantHotMinHitRate is "not warm enough" to be a
// useful hint, even if it was reported recently AND serves the engine
// domain.
func TestTenantHotHonorsHitRateThreshold(t *testing.T) {
	cfg := DefaultRankerConfig()
	cfg.TenantHotMinHitRate = 0.5
	idx := New(WithRanker(cfg))

	idx.Ingest(Update{ReplicaID: "tepid", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("other"), TokenCount: 1}},
		Stats:    &ReplicaStats{HitRate: 0.2}}) // below the 0.5 threshold

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone {
		t.Fatalf("below-threshold hit_rate should NOT trigger TENANT_HOT, got %v (%+v)", res.Strategy, res.Scores)
	}
}

// TestTenantHotDisabledByZeroMaxAge proves the kill-switch: a RankerConfig
// with TenantHotMaxAge=0 disables the fallback entirely, so a prefix miss
// always lands at StrategyNone (NO_HINT). Useful when an operator wants
// strict baseline behavior back.
func TestTenantHotDisabledByZeroMaxAge(t *testing.T) {
	cfg := DefaultRankerConfig()
	cfg.TenantHotMaxAge = 0 // explicit disable
	idx := New(WithRanker(cfg))

	idx.Ingest(Update{ReplicaID: "warm", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("other"), TokenCount: 1}},
		Stats:    &ReplicaStats{HitRate: 0.9}})

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone {
		t.Fatalf("TenantHotMaxAge=0 must disable fallback; got %v (%+v)", res.Strategy, res.Scores)
	}
}

// TestTenantHotRequiresReplicaServingRequestedScheme guards the
// engine-domain check: a replica with high hit_rate stats but NO prefix
// entries in the requested hash_scheme cannot become a TENANT_HOT hint —
// it isn't proven to serve this engine. Otherwise stats-only updates (or
// updates under a different scheme) could leak into hints for the wrong
// domain. The replica below holds a prefix only under "sglang"; a lookup
// under "vllm" must NOT promote it via TENANT_HOT.
func TestTenantHotRequiresReplicaServingRequestedScheme(t *testing.T) {
	idx := New(WithRanker(DefaultRankerConfig()))
	idx.Ingest(Update{ReplicaID: "wrong-engine", Model: "m", Tenant: "t", HashScheme: "sglang",
		Prefixes: []PrefixRef{{PrefixHash: hash("other"), TokenCount: 1}},
		Stats:    &ReplicaStats{HitRate: 0.9}})

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone {
		t.Fatalf("replica serving sglang must NOT surface for a vllm lookup; got %v (%+v)",
			res.Strategy, res.Scores)
	}
}

// TestTenantHotDropsReplicaAfterPrefixSweep guards that the TENANT_HOT
// fallback stops promoting a replica once the sweeper has evicted its last
// serving prefix entry. The secondary servingByScope index gives TENANT_HOT
// an O(1) "does R serve scope S?" check; removeReplicaLocked must keep it
// consistent with i.prefixes, so a stale entry that's been swept no longer
// counts as proof of serving. (Before the sweep runs, soft-state semantics
// allow a stale entry to keep the replica "serving" — at worst a suboptimal
// hint, not a wrong answer; the sweep then cleans it.)
func TestTenantHotDropsReplicaAfterPrefixSweep(t *testing.T) {
	clk := &fakeClock{t: time.Unix(11_500_000, 0)}
	cfg := DefaultRankerConfig()
	cfg.TenantHotMaxAge = time.Hour // warm window much wider than the TTL
	idx := New(withClock(clk.now), WithTTL(10*time.Minute), WithRanker(cfg))

	// Ingest a serving prefix; with warm stats this replica qualifies.
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("other"), TokenCount: 1}},
		Stats:    &ReplicaStats{HitRate: 0.9}})

	// Sanity: pre-sweep the replica is a TENANT_HOT candidate.
	if res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t",
		HashScheme: "vllm", PrefixHash: hash("novel")}); res.Strategy != StrategyTenantHot {
		t.Fatalf("pre-sweep should be TENANT_HOT, got %v", res.Strategy)
	}

	// Advance past the prefix TTL but NOT past TenantHotMaxAge, then refresh
	// stats so the stats entry stays warm/recent. The prefix entry is now
	// stale but not yet swept — soft-state semantics tolerate one more hint.
	clk.add(15 * time.Minute)
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Stats: &ReplicaStats{HitRate: 0.9}})

	// Run the sweep: the stale prefix is removed from i.prefixes AND from
	// the servingByScope secondary index (via removeReplicaLocked). The
	// stats are still fresh under TenantHotMaxAge, but the replica is no
	// longer serving the requested scheme → TENANT_HOT must NOT fire.
	idx.evictExpired()

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone {
		t.Fatalf("after sweep, replica with no live serving prefix must NOT enable TENANT_HOT; got %v (%+v)",
			res.Strategy, res.Scores)
	}
}

// TestLookupIgnoresStaleStatsPressurePenalty guards a symmetric freshness
// rule for the prefix-match path: a stats entry that has aged past the
// index TTL (but not yet swept) must NOT demote a freshly refreshed prefix
// score. Otherwise a high-pressure reading from minutes ago could zero a
// replica that's actually idle right now, just because the sweeper hasn't
// run yet. The fresh-prefix replica below has stale high-pressure stats;
// its score must equal the unpenalized baseline (matched_tokens × freshness).
func TestLookupIgnoresStaleStatsPressurePenalty(t *testing.T) {
	clk := &fakeClock{t: time.Unix(12_000_000, 0)}
	idx := New(withClock(clk.now), WithTTL(10*time.Minute), WithRanker(DefaultRankerConfig()))

	// Ingest stats first: high pressure, will be stale by the time the
	// prefix is refreshed and looked up.
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Stats: &ReplicaStats{Pressure: 0.9}})

	// Advance past the stats freshness window.
	clk.add(15 * time.Minute)

	// Now ingest a fresh prefix entry. The stats are stale at this point.
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 50}}})

	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("p")})
	if len(got) != 1 {
		t.Fatalf("expected 1 score, got %d (%+v)", len(got), got)
	}
	// Stale pressure must NOT be applied. Score should equal the baseline
	// (50 tokens × 1.0 freshness × 1.0 pressure factor × 1.0 SLO bias) = 50.
	if got[0].Score != 50 {
		t.Fatalf("stale pressure leaked into score: got %v, want 50 (no penalty)", got[0].Score)
	}
}

// TestTenantHotIgnoresStatsOnlyReplicas guards the same engine-domain check
// for a more subtle case: an update that carries stats but NO prefix entry
// (regardless of HashScheme) cannot become a TENANT_HOT candidate, because
// the index has no evidence the replica serves any prefix at all.
func TestTenantHotIgnoresStatsOnlyReplicas(t *testing.T) {
	idx := New(WithRanker(DefaultRankerConfig()))
	idx.Ingest(Update{ReplicaID: "stats-only", Model: "m", Tenant: "t", HashScheme: "vllm",
		Stats: &ReplicaStats{HitRate: 0.95}})

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone {
		t.Fatalf("stats-only update must NOT surface in TENANT_HOT; got %v (%+v)",
			res.Strategy, res.Scores)
	}
}

// TestTenantHotIsolatedByTenant guards that a warm replica in tenant-a's
// index can never leak into tenant-b's TENANT_HOT fallback — per-tenant
// isolation is a hard constraint of the index regardless of strategy.
func TestTenantHotIsolatedByTenant(t *testing.T) {
	idx := New(WithRanker(DefaultRankerConfig()))
	idx.Ingest(Update{ReplicaID: "warm-a", Model: "m", Tenant: "tenant-a", HashScheme: "vllm",
		Stats: &ReplicaStats{HitRate: 0.9}})

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "tenant-b", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone {
		t.Fatalf("tenant-b lookup leaked tenant-a's warm replica: %+v", res)
	}
}

// TestLookupRouteEmptyHashSchemeFailsOpenAcrossStrategies guards that an
// unspecified hash_scheme produces NO_HINT through BOTH ranking paths, not
// just the prefix-match one. The TENANT_HOT fallback keys only on
// (tenant, model) and would otherwise still emit a hint based on stats
// alone — but a request whose engine domain we can't identify must fail
// open, per the soft-state / fail-open contract (PROJECT_CONTEXT §5).
func TestLookupRouteEmptyHashSchemeFailsOpenAcrossStrategies(t *testing.T) {
	idx := New(WithRanker(DefaultRankerConfig()))
	// Warm replica with high hit_rate would normally qualify for TENANT_HOT.
	idx.Ingest(Update{ReplicaID: "warm", Model: "m", Tenant: "t", HashScheme: "vllm",
		Stats: &ReplicaStats{HitRate: 0.9}})

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t",
		HashScheme: "", PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone || len(res.Scores) != 0 {
		t.Fatalf("empty hash_scheme must fail open; got strategy=%v scores=%+v",
			res.Strategy, res.Scores)
	}
}

// TestTenantHotIsolatedByModel guards the analogous model isolation: a warm
// replica for model A in tenant t doesn't surface for model B in tenant t.
// Different models have disjoint cache state; mixing them would mis-hint.
func TestTenantHotIsolatedByModel(t *testing.T) {
	idx := New(WithRanker(DefaultRankerConfig()))
	idx.Ingest(Update{ReplicaID: "warm", Model: "model-a", Tenant: "t", HashScheme: "vllm",
		Stats: &ReplicaStats{HitRate: 0.9}})

	res := idx.LookupRoute(LookupRequest{Model: "model-b", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone {
		t.Fatalf("model-b lookup leaked model-a's warm replica: %+v", res)
	}
}

// TestReplicaUpdatedEventDoesNotKeepStaleStatsFresh guards a subtle
// interaction between liveness events and the ranker's stats freshness
// check: REPLICA_UPDATED refreshes the index's liveness timestamp without
// supplying new stat values. The ranker uses a separate statsReported
// timestamp so a stale high-pressure / high-hit_rate payload kept "alive"
// by liveness events can't keep demoting prefix scores or qualifying for
// TENANT_HOT indefinitely.
//
// Two assertions in one test, with the same setup, so the bug is easy to
// recognise if either path regresses.
func TestReplicaUpdatedEventDoesNotKeepStaleStatsFresh(t *testing.T) {
	clk := &fakeClock{t: time.Unix(13_000_000, 0)}
	cfg := DefaultRankerConfig()
	cfg.TenantHotMaxAge = 5 * time.Minute
	idx := New(withClock(clk.now), WithTTL(10*time.Minute), WithRanker(cfg))

	// Replica reports an initial state with high pressure and high hit_rate.
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 50}},
		Stats:    &ReplicaStats{Pressure: 0.9, HitRate: 0.9}})

	// Advance past both freshness windows so the stats payload is stale.
	clk.add(20 * time.Minute)

	// Now a stream of REPLICA_UPDATED liveness events keeps refreshing
	// the in-index lastSeen — but NOT the stats payload.
	for k := 0; k < 5; k++ {
		idx.ApplyEvent(Event{Type: EventReplicaUpdated,
			ReplicaID: "r", Model: "m", Tenant: "t"})
		clk.add(time.Minute)
	}

	// Refresh the prefix so the prefix-match path has a candidate to score.
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 50}}})

	// Prefix-match path: stale Pressure must NOT be applied. The score
	// should equal the unpenalized baseline (50 × 1.0 × 1.0 × 1.0 = 50).
	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("p")})
	if len(got) != 1 || got[0].Score != 50 {
		t.Fatalf("liveness-refreshed stale pressure leaked into score: got %+v, want score 50", got)
	}

	// TENANT_HOT path: the stale HitRate must NOT qualify the replica for
	// the fallback either. Look up a novel prefix to force the miss.
	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone {
		t.Fatalf("liveness-refreshed stale hit_rate leaked into TENANT_HOT: got %v (%+v)",
			res.Strategy, res.Scores)
	}
}

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

// TestReservedTenantHiddenFromCapAndAggregate pins the WithReservedTenants
// contract: reserved-tenant entries are present in the index (so the probe's
// Stage A lookup still finds them) but invisible to the cap accounting,
// aggregate, snapshot, and per-model entry-count gauge — so a probe in flight
// cannot displace real workload state via the cap sweep AND cannot leak
// into observability surfaces. Mirrors TestProberRun* in pkg/server, but
// from the index's perspective.
func TestReservedTenantHiddenFromCapAndAggregate(t *testing.T) {
	const reserved = "inferencecache.io/probe"
	idx := New(WithMaxEntries(1), WithReservedTenants(reserved))

	idx.Ingest(Update{
		ReplicaID: "real", Model: "m", Tenant: "real-tenant", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("rp"), TokenCount: 64}},
	})
	idx.Ingest(Update{
		ReplicaID: "__probe-cb", Model: "m", Tenant: reserved, HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("pp"), TokenCount: 16}},
		Stats:    &ReplicaStats{ReplicaID: "__probe-cb", CacheMemoryBytes: 1234, HitRate: 1.0},
	})

	// Cap math sees only the real entry; the probe entry didn't trigger
	// eviction (which would have removed the real entry under cap=1).
	if scores := idx.Lookup(LookupRequest{
		Tenant: "real-tenant", Model: "m", HashScheme: "vllm", PrefixHash: hash("rp"),
	}); len(scores) != 1 || scores[0].ReplicaID != "real" {
		t.Fatalf("real workload entry evicted under cap=1; got scores = %+v", scores)
	}

	// Aggregate excludes the reserved tenant — Total == real-tenant entry count.
	agg := idx.Aggregate()
	if agg.Total != 1 {
		t.Errorf("Aggregate.Total = %d, want 1 — reserved tenant must not contribute", agg.Total)
	}
	if _, present := agg.PerTenant[reserved]; present {
		t.Errorf("Aggregate.PerTenant includes reserved tenant: %+v", agg.PerTenant)
	}

	// EntryCountsByModel feeds inferencecache_index_entries — must not surface
	// the synthetic model count from a reserved-tenant entry.
	if got := idx.EntryCountsByModel()["m"]; got != 1 {
		t.Errorf("EntryCountsByModel[m] = %d, want 1 — reserved tenant must not bump the per-model gauge", got)
	}

	// Snapshot: no reserved tenant, no reserved replica.
	snap := idx.Snapshot()
	if snap.TotalPrefixes != 1 {
		t.Errorf("Snapshot.TotalPrefixes = %d, want 1", snap.TotalPrefixes)
	}
	for _, r := range snap.Replicas {
		if r.Tenant == reserved || r.ReplicaID == "__probe-cb" {
			t.Errorf("Snapshot exposed reserved replica: %+v", r)
		}
	}
	for _, tn := range snap.Tenants {
		if tn.TenantID == reserved {
			t.Errorf("Snapshot exposed reserved tenant: %+v", tn)
		}
	}

	// But the probe's own Stage A lookup STILL finds its entry — the
	// exemption applies only to external surfaces, not to internal callers.
	if scores := idx.Lookup(LookupRequest{
		Tenant: reserved, Model: "m", HashScheme: "vllm", PrefixHash: hash("pp"),
	}); len(scores) != 1 || scores[0].ReplicaID != "__probe-cb" {
		t.Fatalf("reserved-tenant lookup must still work for the probe's own Stage A check; got scores = %+v", scores)
	}
}

func TestIngestAndLookupRanksByTokensAndFreshness(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	idx := New(withClock(clk.now), WithTTL(time.Hour))

	// replica-a holds the prefix with 100 tokens; replica-b with 50. Same freshness.
	idx.Ingest(Update{ReplicaID: "replica-a", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p1"), TokenCount: 100}}})
	idx.Ingest(Update{ReplicaID: "replica-b", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p1"), TokenCount: 50}}})
	// Decoy: a third replica serves the same engine domain (tenant, model,
	// hash_scheme) but holds a DIFFERENT prefix. It populates the
	// distinguishing-power denominator (total_replicas=3) without showing
	// up in the scored result for hash("p1"). Without it both holders of
	// the queried prefix would have factor (1 - 2/2)=0, zeroing every
	// score and replacing the freshness-vs-tokens story this test
	// asserts with a lexicographic-ID tiebreak.
	idx.Ingest(Update{ReplicaID: "replica-c-decoy", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("decoy"), TokenCount: 1}}})

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

// TestNoCrossEngineFalseHitVLLMvsSGLang is the second-engine no-cross-engine-
// false-hit guarantee: with a
// vLLM replica and a SGLang replica BOTH holding a bytewise-identical prefix
// (same tenant, model, and prefix_hash bytes — exactly the collision the
// hash_scheme tag exists to keep disjoint), a lookup under one scheme must
// return ONLY that engine's replica and never the other's. This is the
// stronger form of TestHashSchemeIsolatesMatches (which only checks the
// empty-other-scheme miss): here both schemes are populated, so it proves the
// tag — not the absence of the other entry — is what isolates them.
func TestNoCrossEngineFalseHitVLLMvsSGLang(t *testing.T) {
	idx := New()
	const (
		tenant = "t"
		model  = "shared-model"
	)
	// Identical prefix bytes recorded by each engine under its own scheme.
	prefix := hash("the quick brown fox")
	idx.Ingest(Update{ReplicaID: "vllm-replica-0", Model: model, Tenant: tenant, HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: prefix, TokenCount: 32}}})
	idx.Ingest(Update{ReplicaID: "sglang-replica-0", Model: model, Tenant: tenant, HashScheme: "sglang",
		Prefixes: []PrefixRef{{PrefixHash: prefix, TokenCount: 32}}})

	// A request hashed under the SGLang scheme matches ONLY the SGLang replica.
	sglangScores := idx.Lookup(LookupRequest{Model: model, Tenant: tenant, HashScheme: "sglang", PrefixHash: prefix})
	if len(sglangScores) != 1 || sglangScores[0].ReplicaID != "sglang-replica-0" {
		t.Fatalf("sglang lookup = %+v, want exactly [sglang-replica-0] (no cross-engine false hit on the vLLM entry)", sglangScores)
	}

	// And the symmetric direction: a vLLM-scheme request matches ONLY the vLLM replica.
	vllmScores := idx.Lookup(LookupRequest{Model: model, Tenant: tenant, HashScheme: "vllm", PrefixHash: prefix})
	if len(vllmScores) != 1 || vllmScores[0].ReplicaID != "vllm-replica-0" {
		t.Fatalf("vllm lookup = %+v, want exactly [vllm-replica-0] (no cross-engine false hit on the SGLang entry)", vllmScores)
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

func TestSnapshotCarriesT2Counters(t *testing.T) {
	idx := New()
	idx.Ingest(Update{ReplicaID: "replica-a", Model: "m1", Tenant: "tenant-a", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p1"), TokenCount: 1}},
		Stats:    &ReplicaStats{CacheMemoryBytes: 100, T2HitTokens: 600, T2QueryTokens: 1000}})

	snap := idx.Snapshot()
	if len(snap.Replicas) != 1 {
		t.Fatalf("replicas = %d, want 1", len(snap.Replicas))
	}
	if r := snap.Replicas[0]; r.T2HitTokens != 600 || r.T2QueryTokens != 1000 {
		t.Fatalf("t2 counters = (%d, %d), want (600, 1000)", r.T2HitTokens, r.T2QueryTokens)
	}
}

func TestSnapshotAggregates(t *testing.T) {
	idx := New()
	idx.Ingest(Update{ReplicaID: "replica-a", Model: "m1", Tenant: "tenant-a", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p1"), TokenCount: 1}},
		Stats:    &ReplicaStats{CacheMemoryBytes: 100, HitRate: 0.8, Pressure: 0.5}})
	// Same replica reports again under a different model for the same tenant.
	// Tenant HitRate dedups replicas (counts replica-a once). Tenant
	// IndexEntries counts distinct (tenant, model, hash_scheme, prefix_hash)
	// keys — replica is not in the aggregate key, so the same prefix from two
	// replicas would still count once, but a second MODEL on the same replica
	// is a distinct key and adds a row to the tenant's entry count.
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
	// Both replicas reported stats, so StatsReported is the "hit rate is real,
	// not a fabricated 0" presence bit for the CacheIndex status projection.
	if !snap.Replicas[0].StatsReported || !snap.Replicas[1].StatsReported {
		t.Fatalf("StatsReported should be true for stats-bearing replicas: a=%v b=%v",
			snap.Replicas[0].StatsReported, snap.Replicas[1].StatsReported)
	}
	// Per-replica prefix counts are aggregated cluster-wide across models /
	// hash_schemes: replica-a holds two distinct prefixes (one per model),
	// replica-b holds one.
	if snap.Replicas[0].PrefixCount != 2 {
		t.Fatalf("replica-a prefixCount = %d, want 2", snap.Replicas[0].PrefixCount)
	}
	if snap.Replicas[1].PrefixCount != 1 {
		t.Fatalf("replica-b prefixCount = %d, want 1", snap.Replicas[1].PrefixCount)
	}
	// LastEventAt is the max replica-entry lastSeen across the replica's
	// prefixes; here both Ingest calls happened in the same test, so the
	// field must at least be non-zero.
	if snap.Replicas[0].LastEventAt.IsZero() || snap.Replicas[1].LastEventAt.IsZero() {
		t.Fatalf("lastEventAt should be set after Ingest: %+v / %+v",
			snap.Replicas[0].LastEventAt, snap.Replicas[1].LastEventAt)
	}
	// Tenant is the namespace the subscriber sidecar reports; the controller
	// uses it to scope engine-pod lookups when attributing replicas to
	// CacheBackends. Must reflect the Ingest's Tenant field.
	if snap.Replicas[0].Tenant != "tenant-a" || snap.Replicas[1].Tenant != "tenant-b" {
		t.Fatalf("tenants on replicas = %q / %q, want tenant-a / tenant-b",
			snap.Replicas[0].Tenant, snap.Replicas[1].Tenant)
	}
	// Tenants sorted by id. tenant-a's IndexEntries == 2: two distinct
	// (tenant, model, hash_scheme, prefix_hash) keys ((tenant-a, m1, vllm, p1)
	// and (tenant-a, m2, vllm, p2)) — the second Ingest added a new key
	// because the model differed. HitRate is deduped per replica (replica-a
	// counted once), so tenant-a's HitRate is 0.8 from that single replica.
	if len(snap.Tenants) != 2 {
		t.Fatalf("tenants = %+v, want 2", snap.Tenants)
	}
	if snap.Tenants[0].TenantID != "tenant-a" || snap.Tenants[0].IndexEntries != 2 || snap.Tenants[0].HitRate != 0.8 {
		t.Fatalf("tenant-a = %+v, want indexEntries 2 hitRate 0.8 (deduped)", snap.Tenants[0])
	}
	if snap.Tenants[1].TenantID != "tenant-b" || snap.Tenants[1].IndexEntries != 1 || snap.Tenants[1].HitRate != 0.6 {
		t.Fatalf("tenant-b = %+v, want indexEntries 1 hitRate 0.6", snap.Tenants[1])
	}
	// Both tenants have at least one stats-reporting replica, so HitRateReported
	// is the "mean hit rate is real, not a fabricated 0" presence bit.
	if !snap.Tenants[0].HitRateReported || !snap.Tenants[1].HitRateReported {
		t.Fatalf("HitRateReported should be true for tenants with reported stats: a=%v b=%v",
			snap.Tenants[0].HitRateReported, snap.Tenants[1].HitRateReported)
	}
	// MemoryUsed is deprecated and never accumulated: it stays 0 even though
	// both tenants' replicas reported non-zero CacheMemoryBytes.
	if snap.Tenants[0].MemoryUsed != 0 || snap.Tenants[1].MemoryUsed != 0 {
		t.Fatalf("tenant MemoryUsed must be 0 (deprecated, not accumulated): a=%d b=%d",
			snap.Tenants[0].MemoryUsed, snap.Tenants[1].MemoryUsed)
	}
}

// TestSnapshotPresenceBitsDistinguishAbsentFromZero pins the absent-vs-zero
// presence bits the CacheIndex status projection relies on. A prefix-only
// Ingest (no Stats) records index entries but no stats row: the replica reports
// StatsReported=false and the tenant HitRateReported=false, while IndexEntries
// still reflects the real count. Downstream, the controller keeps the
// cluster-aggregate tenant hitRate nil for such a tenant; note a stats-less
// replica is dropped from CacheIndex.status.replicas[] entirely (the
// LastUpdate.IsZero() filter), so the nil-hitRate case is only observable on
// the tenant surface — this test asserts the snapshot-level bits that feed it.
func TestSnapshotPresenceBitsDistinguishAbsentFromZero(t *testing.T) {
	idx := New()
	// Prefix-only report: two distinct prefixes, no Stats payload.
	idx.Ingest(Update{
		ReplicaID: "vllm-0", Model: "m", Tenant: "tenant-a", HashScheme: "vllm",
		Prefixes: []PrefixRef{
			{PrefixHash: hash("p1"), TokenCount: 1},
			{PrefixHash: hash("p2"), TokenCount: 1},
		},
	})

	snap := idx.Snapshot()

	if len(snap.Replicas) != 1 {
		t.Fatalf("replicas = %+v, want one prefix-only row", snap.Replicas)
	}
	r := snap.Replicas[0]
	if r.StatsReported {
		t.Fatalf("StatsReported = true for a prefix-only replica, want false (no stats reported yet)")
	}
	// A prefix-only replica has zero-valued stats — the exact "observed 0 vs
	// not reported" ambiguity the presence bit resolves.
	if r.HitRate != 0 || r.CacheMemoryBytes != 0 || !r.LastUpdate.IsZero() {
		t.Fatalf("prefix-only replica should carry zero-valued stats: %+v", r)
	}
	if r.PrefixCount != 2 {
		t.Fatalf("prefixCount = %d, want 2 (entries are still counted)", r.PrefixCount)
	}

	if len(snap.Tenants) != 1 {
		t.Fatalf("tenants = %+v, want one row", snap.Tenants)
	}
	tn := snap.Tenants[0]
	if tn.HitRateReported {
		t.Fatalf("HitRateReported = true for a tenant with no reported stats, want false")
	}
	if tn.HitRate != 0 {
		t.Fatalf("tenant HitRate = %v, want 0 (no stats), distinguishable via HitRateReported=false", tn.HitRate)
	}
	if tn.IndexEntries != 2 {
		t.Fatalf("tenant IndexEntries = %d, want 2 (a real observed count, always present)", tn.IndexEntries)
	}
}

// TestSnapshotJSONRoundtripPreservesTenantAndPrefixFields guards the wire
// shape of /snapshot. The controller decodes the JSON into the same
// Snapshot type, so a silent rename of one of the JSON tags (e.g. someone
// dropping `Tenant` to `omitempty` and writing a tenant-less replica)
// would still compile but break per-backend attribution downstream. This
// test JSON-encodes a snapshot with all the new fields set and asserts
// they survive the round-trip.
func TestSnapshotJSONRoundtripPreservesTenantAndPrefixFields(t *testing.T) {
	idx := New()
	idx.Ingest(Update{
		ReplicaID: "vllm-0", Model: "m", Tenant: "ns-a", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 1}},
		Stats:    &ReplicaStats{CacheMemoryBytes: 100, HitRate: 0.5, Pressure: 0.2},
	})

	raw, err := json.Marshal(idx.Snapshot())
	if err != nil {
		t.Fatalf("encode snapshot: %v", err)
	}
	var decoded Snapshot
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if len(decoded.Replicas) != 1 {
		t.Fatalf("replicas = %d, want 1", len(decoded.Replicas))
	}
	r := decoded.Replicas[0]
	if r.ReplicaID != "vllm-0" || r.Tenant != "ns-a" {
		t.Fatalf("identity round-trip lost: replicaId=%q tenant=%q", r.ReplicaID, r.Tenant)
	}
	if r.PrefixCount != 1 {
		t.Fatalf("prefixCount round-trip = %d, want 1", r.PrefixCount)
	}
	if r.LastEventAt.IsZero() {
		t.Fatal("lastEventAt round-trip lost (zero)")
	}
	if r.CacheMemoryBytes != 100 || r.HitRate != 0.5 || r.Pressure != 0.2 {
		t.Fatalf("stats round-trip lost: %+v", r)
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

// countingMetrics records the latest reported entry count per model and the
// running total of tenant evictions per (tenant, reason) and index evictions
// per (algorithm, reason).
type countingMetrics struct {
	mu             sync.Mutex
	last           map[string]int
	evictions      map[string]int // key: tenantID + "|" + reason
	indexEvictions map[string]int // key: algorithm + "|" + reason
}

func (c *countingMetrics) SetIndexEntries(model string, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.last == nil {
		c.last = map[string]int{}
	}
	c.last[model] = n
}

func (c *countingMetrics) AddTenantEvictions(tenantID, reason string, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.evictions == nil {
		c.evictions = map[string]int{}
	}
	c.evictions[tenantID+"|"+reason] += n
}

func (c *countingMetrics) AddIndexEvictions(algorithm, reason string, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.indexEvictions == nil {
		c.indexEvictions = map[string]int{}
	}
	c.indexEvictions[algorithm+"|"+reason] += n
}

// indexEvictionCount returns the recorded index-eviction total for an
// (algorithm, reason) pair.
func (c *countingMetrics) indexEvictionCount(algorithm, reason string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.indexEvictions[algorithm+"|"+reason]
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

// TestLookupPressureAndSLOFactorsCollapseToUnityWhenSignalsAbsent locks in the
// contract that the pressure and SLO score factors collapse to 1 when (a) no
// replica stats are reported (pressure=0) and (b) the request carries no SLO
// hint (TTFT=0). The distinguishing-power factor still applies (it depends on
// cluster cardinality, not on these signals), so the expected scores below
// fold it in (1 - 2/3 = 1/3 with the decoy replica below) — the test is about
// the pressure/SLO contribution being 1, not about the score being equal to
// matched_tokens × freshness alone.
func TestLookupPressureAndSLOFactorsCollapseToUnityWhenSignalsAbsent(t *testing.T) {
	clk := &fakeClock{t: time.Unix(6_000_000, 0)}
	idx := New(withClock(clk.now), WithTTL(time.Hour))

	idx.Ingest(Update{ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 80}}})
	idx.Ingest(Update{ReplicaID: "r2", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 40}}})
	// Decoy replica holds a different prefix in the same engine domain so
	// the distinguishing-power factor is not zero (matching=2 < total=3).
	// Factor = 1 - 2/3 = 1/3, so expected scores are 80/3 and 40/3.
	idx.Ingest(Update{ReplicaID: "r3-decoy", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("decoy"), TokenCount: 1}}})

	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p")})
	if len(got) != 2 {
		t.Fatalf("expected 2 scores, got %d", len(got))
	}
	// freshness == 1, no pressure, no SLO, distinguishing_power = 1/3.
	approxEq := func(got, want float32) bool { return got-want > -1e-3 && got-want < 1e-3 }
	if got[0].ReplicaID != "r1" || !approxEq(got[0].Score, 80.0/3.0) {
		t.Fatalf("r1 baseline score = %v (id %q), want 80/3 (~26.67) — matched_tokens × freshness × distinguishing_power(2,3)", got[0].Score, got[0].ReplicaID)
	}
	if got[1].ReplicaID != "r2" || !approxEq(got[1].Score, 40.0/3.0) {
		t.Fatalf("r2 baseline score = %v (id %q), want 40/3 (~13.33)", got[1].Score, got[1].ReplicaID)
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
			// Decoy replica with a different prefix in the same engine
			// domain keeps the distinguishing-power factor strictly
			// positive (matching < total). Without it, when EVERY
			// replica in the test holds hash("p"), the factor zeroes
			// every score and the ordering this test asserts collapses
			// to a meaningless lexicographic tiebreak.
			idx.Ingest(Update{ReplicaID: "zzz-decoy", Model: "m", Tenant: "t", HashScheme: "vllm",
				Prefixes: []PrefixRef{{PrefixHash: hash("decoy"), TokenCount: 1}}})

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
	// Decoy: holds a different prefix in the same engine domain so the
	// distinguishing-power factor stays > 0 (matching=2 < total=3). The
	// factor multiplies both replicas' scores by the same constant, so
	// the SLO-bias ordering this test asserts is preserved.
	idx.Ingest(Update{ReplicaID: "zzz-decoy", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("decoy"), TokenCount: 1}}, Timestamp: clk.t})

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
	// Decoy in the same engine domain (different prefix) keeps the
	// distinguishing-power factor > 0 so the disabled-SLO ordering this
	// test asserts isn't masked by a lexicographic tiebreak.
	idx.Ingest(Update{ReplicaID: "zzz-decoy", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("decoy"), TokenCount: 1}}, Timestamp: clk.t})

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
			// Stats-only ingest registers no prefix entries → the prefix map is
			// globally empty → the cold-start carve-out keeps this on the
			// fail-open NO_HINT path. The no-replica-leak intent is preserved
			// by the wantScores==0 assertion.
			name: "stats-only ingest, novel prefix → StrategyNone (globally empty prefix map)",
			ingest: []Update{
				{ReplicaID: "cold", Model: model, Tenant: tenant, HashScheme: scheme,
					Stats: &ReplicaStats{HitRate: 0.0}},
			},
			req:        LookupRequest{Model: model, Tenant: tenant, HashScheme: scheme, PrefixHash: hashFor("novel")},
			wantStrat:  StrategyNone,
			wantScores: 0,
		},
		{
			// Empty index = cold start. The cold-start carve-out short-circuits
			// classifyMiss to NO_HINT so a freshly-started server does not flood
			// every gateway with UNKNOWN_TENANT until the first ReportCacheState
			// lands. The diagnostic resumes the moment any prefix is reported.
			name:       "empty index → StrategyNone (cold-start carve-out)",
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

// TestTenantHotRecencyClampedAgainstClockSkew guards that a future
// statsReported timestamp (e.g. from clock skew between the replica and the
// server) is clamped to recency=1 rather than producing recency>1, which
// would otherwise amplify both the score and the SLO bias factor and let a
// stale-but-future-stamped replica outrank everyone else. Mirrors
// freshnessAt's `age <= 0 → 1` clamp on the prefix-match path.
func TestTenantHotRecencyClampedAgainstClockSkew(t *testing.T) {
	clk := &fakeClock{t: time.Unix(13_500_000, 0)}
	cfg := DefaultRankerConfig()
	idx := New(withClock(clk.now), WithTTL(time.Hour), WithRanker(cfg))

	// Ingest serving prefix + stats normally so the replica qualifies.
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("other"), TokenCount: 1}},
		Stats:    &ReplicaStats{HitRate: 0.5, Pressure: 0}})

	// Now move the clock BACKWARDS so the previously-stored statsReported
	// is in the "future" relative to now — i.e. simulate a server-side clock
	// step backwards while a replica's report is in flight.
	clk.t = clk.t.Add(-2 * time.Minute)

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyTenantHot || len(res.Scores) != 1 {
		t.Fatalf("expected TENANT_HOT candidate, got %v (%+v)", res.Strategy, res.Scores)
	}
	// With recency clamped to 1, and PressureWeight default 1 × pressure 0
	// → pressureFactor 1, and no SLO budget set → sloBias 1:
	//   score = hit_rate × 1 × 1 × 1 = 0.5.
	// Without the clamp, recency could exceed 1 and amplify the score.
	if got := res.Scores[0].Score; got > 0.5 {
		t.Fatalf("recency not clamped against clock skew: score = %v, want <= 0.5", got)
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
// with TenantHotMaxAge=0 disables the soft locality fallback, so a
// same-key prefix miss (the case set up below — (t, m, vllm) populated,
// only this prefix novel) lands at StrategyNone (NO_HINT) instead of
// TENANT_HOT. The miss-classifier still runs for mismatched contract keys
// — see the dedicated diagnostics tests; this test pins only the
// kill-switch behavior on the same-key path.
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
//
// Post-diagnostics: this is also the canonical UNKNOWN_HASH_SCHEME diagnostic
// shape — (t, m) populated under sglang, the lookup asks under vllm — so
// the classifier now surfaces the more specific code. The leak guarantee is
// unchanged: no replica from another scheme ever appears in Scores.
func TestTenantHotRequiresReplicaServingRequestedScheme(t *testing.T) {
	idx := New(WithRanker(DefaultRankerConfig()))
	idx.Ingest(Update{ReplicaID: "wrong-engine", Model: "m", Tenant: "t", HashScheme: "sglang",
		Prefixes: []PrefixRef{{PrefixHash: hash("other"), TokenCount: 1}},
		Stats:    &ReplicaStats{HitRate: 0.9}})

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyUnknownHashScheme {
		t.Fatalf("(t, m) populated under sglang but the lookup asks under vllm: must surface UNKNOWN_HASH_SCHEME; got %v (%+v)",
			res.Strategy, res.Scores)
	}
	if len(res.Scores) != 0 {
		t.Fatalf("UNKNOWN_HASH_SCHEME must carry no scores (no cross-scheme leak); got %+v", res.Scores)
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
	// Sweep drops the only prefix → the index is now globally empty for the
	// prefix map → cold-start carve-out short-circuits classifyMiss to NO_HINT.
	// The original "TENANT_HOT must NOT fire" intent is preserved (Scores
	// empty); reverting to NO_HINT instead of a diagnostic is correct here
	// because there is no other-tenant data to compare against.
	if res.Strategy != StrategyNone {
		t.Fatalf("after sweep, replica with no live serving prefix must NOT enable TENANT_HOT; got %v (%+v)",
			res.Strategy, res.Scores)
	}
	if len(res.Scores) != 0 {
		t.Fatalf("post-sweep response must carry no scores; got %+v", res.Scores)
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
//
// Stats-only ingest registers no prefix entries → prefix map globally empty
// → cold-start carve-out keeps the response on NO_HINT. The original
// guarantee (stats-only replica never appears in Scores) is preserved.
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
	if len(res.Scores) != 0 {
		t.Fatalf("stats-only response must carry no scores; got %+v", res.Scores)
	}
}

// TestTenantHotIsolatedByTenant guards that a warm replica in tenant-a's
// index can never leak into tenant-b's TENANT_HOT fallback — per-tenant
// isolation is a hard constraint of the index regardless of strategy.
//
// Setup is stats-only ingest, so the prefix map is globally empty → the
// cold-start carve-out keeps the response on NO_HINT. The no-leak property
// the test guards (no tenant-a replica appears in tenant-b's Scores) is
// preserved by the wantScores==0 assertion. The asymmetric UNKNOWN_TENANT
// case (tenant-a populated with REAL prefixes, lookup for tenant-b) is
// covered by TestLookupRouteUnknownTenantOnlyWhenIndexHasData in
// diagnostics_test.go.
func TestTenantHotIsolatedByTenant(t *testing.T) {
	idx := New(WithRanker(DefaultRankerConfig()))
	idx.Ingest(Update{ReplicaID: "warm-a", Model: "m", Tenant: "tenant-a", HashScheme: "vllm",
		Stats: &ReplicaStats{HitRate: 0.9}})

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "tenant-b", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone {
		t.Fatalf("tenant-b lookup leaked tenant-a's warm replica: %+v", res)
	}
	if len(res.Scores) != 0 {
		t.Fatalf("response must carry no scores (no cross-tenant leak); got %+v", res.Scores)
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
//
// Stats-only ingest registers no prefix entries → prefix map globally empty
// → cold-start carve-out keeps the response on NO_HINT. The no-leak
// property is preserved by wantScores==0. The asymmetric UNKNOWN_MODEL case
// (model-a populated with REAL prefixes, lookup for model-b) is covered by
// TestLookupRouteClassifiesUnknownModel in diagnostics_test.go.
func TestTenantHotIsolatedByModel(t *testing.T) {
	idx := New(WithRanker(DefaultRankerConfig()))
	idx.Ingest(Update{ReplicaID: "warm", Model: "model-a", Tenant: "t", HashScheme: "vllm",
		Stats: &ReplicaStats{HitRate: 0.9}})

	res := idx.LookupRoute(LookupRequest{Model: "model-b", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone {
		t.Fatalf("model-b lookup leaked model-a's warm replica: %+v", res)
	}
	if len(res.Scores) != 0 {
		t.Fatalf("response must carry no scores (no cross-model leak); got %+v", res.Scores)
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

// ---------------------------------------------------------------------------
// Per-tenant TTL (CachePolicy propagation)
// ---------------------------------------------------------------------------

// staticTTL is a TTLResolver returning fixed per-tenant TTLs for tests.
type staticTTL map[string]time.Duration

func (s staticTTL) TTL(tenant string) time.Duration { return s[tenant] }

func TestPerTenantTTLDrivesFreshnessAndEviction(t *testing.T) {
	clk := &fakeClock{t: time.Unix(4_000_000, 0)}
	// Global TTL is long; tenant-short overrides to 5m, tenant-long uses default.
	resolver := staticTTL{"tenant-short": 5 * time.Minute}
	idx := New(
		withClock(clk.now),
		WithTTL(time.Hour),
		WithTTLResolver(resolver),
	)

	for _, tenant := range []string{"tenant-short", "tenant-long"} {
		idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: tenant, HashScheme: "vllm",
			Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}}})
	}

	// Advance 10m: tenant-short's TTL (5m) has elapsed; tenant-long's (1h) has not.
	clk.add(10 * time.Minute)

	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "tenant-short", HashScheme: "vllm", PrefixHash: hash("p")}); len(got) != 0 {
		t.Fatalf("tenant-short entry should be stale under 5m TTL, got %+v", got)
	}
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "tenant-long", HashScheme: "vllm", PrefixHash: hash("p")}); len(got) != 1 {
		t.Fatalf("tenant-long entry should still be fresh under 1h TTL, got %+v", got)
	}

	// Eviction sweep removes only tenant-short; tenant-long survives.
	idx.evictExpired()
	if n := idx.EntryCountsByModel()["m"]; n != 1 {
		t.Fatalf("after sweep, only tenant-long should remain (count = %d, want 1)", n)
	}
}

func TestNilTTLResolverFallsBackToGlobalTTL(t *testing.T) {
	clk := &fakeClock{t: time.Unix(4_500_000, 0)}
	idx := New(withClock(clk.now), WithTTL(time.Hour), WithTTLResolver(staticTTL{}))

	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "anything", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}}})

	clk.add(30 * time.Minute) // half the global TTL → still fresh
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "anything", HashScheme: "vllm", PrefixHash: hash("p")}); len(got) != 1 {
		t.Fatalf("resolver returning 0 should fall back to global TTL (entry should be fresh), got %+v", got)
	}
}

// dynamicTTL exposes a setter so the test can mutate while the index reads.
type dynamicTTL struct {
	mu sync.RWMutex
	v  time.Duration
}

func (d *dynamicTTL) set(v time.Duration) {
	d.mu.Lock()
	d.v = v
	d.mu.Unlock()
}
func (d *dynamicTTL) TTL(string) time.Duration {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.v
}

// TestConcurrentTTLResolverMutation hammers Lookup while a writer flips the
// per-tenant TTL — the race detector catches a missing lock in the resolver
// path.
func TestConcurrentTTLResolverMutation(t *testing.T) {
	r := &dynamicTTL{v: time.Hour}
	idx := New(WithTTL(time.Hour), WithTTLResolver(r))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 1}}})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p")})
				}
			}
		}()
	}

	for i := 0; i < 200; i++ {
		if i%2 == 0 {
			r.set(time.Minute)
		} else {
			r.set(time.Hour)
		}
	}
	close(stop)
	wg.Wait()
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

// TestChainIngestWithCoSetLegacyPrefixHashPreservesBoth covers v1alpha1
// backward-compat: a producer that sets BOTH the new chain (block_hashes)
// and the legacy single-blob (PrefixHash) on the same PrefixEntry must
// have BOTH representations indexed. The chain enables longest-prefix
// matching for new clients; the legacy key keeps unmigrated callers
// (legacy LookupRoute on prefix_hash) hitting.
func TestChainIngestWithCoSetLegacyPrefixHashPreservesBoth(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	hashes, counts := chain("b1", "b2", "b3")
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{
			PrefixHash:       []byte("legacy-full"),
			TokenCount:       128,
			BlockHashes:      hashes,
			BlockTokenCounts: counts,
		}}})

	// Chain lookup hits the per-block entries.
	gotChain := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		BlockHashes: hashes, BlockTokenCounts: counts})
	if len(gotChain) != 1 || gotChain[0].ReplicaID != "r" || gotChain[0].MatchedTokens != 48 {
		t.Fatalf("chain lookup against co-set entry should hit all 3 blocks: got %+v", gotChain)
	}

	// Legacy lookup on the co-set PrefixHash MUST still hit (backward-compat).
	gotLegacy := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: []byte("legacy-full")})
	if len(gotLegacy) != 1 || gotLegacy[0].ReplicaID != "r" || gotLegacy[0].MatchedTokens != 128 {
		t.Fatalf("legacy lookup against co-set entry must still hit prefix_hash with TokenCount=128: got %+v", gotLegacy)
	}
}

// TestChainLookupSharesPressureAndSLOFactorsWithExact verifies the chain
// scoring path composes the same pressure and SLO factors as lookupExact —
// the chain walk changes how matched_tokens is computed but the score
// formula is unchanged. Without this, a saturated replica that happens to
// have a chain hit would outrank a fresher idle peer the chain-aware
// formula was supposed to demote.
func TestChainLookupSharesPressureAndSLOFactorsWithExact(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	hashes, counts := chain("b1", "b2", "b3")

	idx.Ingest(Update{ReplicaID: "big-but-hot", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: hashes, BlockTokenCounts: counts}},
		Stats:    &ReplicaStats{Pressure: 0.9}})
	idx.Ingest(Update{ReplicaID: "small-cool", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{BlockHashes: hashes[:2], BlockTokenCounts: counts[:2]}},
		Stats:    &ReplicaStats{Pressure: 0.0}})
	// Decoy replica in the same engine domain (different prefix) keeps the
	// distinguishing-power denominator above the per-depth matching count,
	// so the chain factor for small-cool is 1 - 2/3 = 1/3 (not 0). Without
	// it small-cool's score would collapse to 0 (matching at its depth ==
	// total) and big-but-hot's pressure-discounted score would dominate by
	// default, masking the pressure-flips-ranking property this test
	// asserts.
	idx.Ingest(Update{ReplicaID: "zzz-decoy", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("decoy"), TokenCount: 1}}})

	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		BlockHashes: hashes, BlockTokenCounts: counts})
	if len(got) != 2 {
		t.Fatalf("expected 2 chain scores, got %+v", got)
	}
	// With totalReplicas = 3 (big-but-hot + small-cool + decoy):
	// big-but-hot: matched=48, pressureFactor=0.1, dp=1-1/3=2/3 → 48 × 0.1 × 2/3 = 3.2
	// small-cool:  matched=32, pressureFactor=1.0, dp=1-2/3=1/3 → 32 × 1.0 × 1/3 ≈ 10.67
	// Without pressure folding (pressureFactor=1.0 for both),
	// big-but-hot's 48×2/3=32 would tie or beat small-cool's 32×1/3≈10.67.
	if got[0].ReplicaID != "small-cool" {
		t.Fatalf("pressure factor missing from chain score: ranked %+v first (want small-cool)", got[0])
	}
}

package index

import (
	"testing"
	"time"
)

// fakeQuota is a static TenantQuotaResolver: a tenant present in the map has a
// configured budget; one absent reports "no quota" (fail open).
type fakeQuota map[string]int64

func (f fakeQuota) TenantQuota(tenant string) (int64, bool) {
	v, ok := f[tenant]
	return v, ok
}

// tenantEntries reads a tenant's live index-entry count from the snapshot.
func tenantEntries(t *testing.T, idx *Index, tenant string) int64 {
	t.Helper()
	for _, ts := range idx.Snapshot().Tenants {
		if ts.TenantID == tenant {
			return ts.IndexEntries
		}
	}
	return 0
}

// ingestPrefix ingests a single prefix for a tenant at a fixed timestamp, so
// "oldest" is well-defined across calls.
func ingestPrefix(idx *Index, tenant, prefix string, ts time.Time) {
	idx.Ingest(Update{
		ReplicaID:  "r1",
		Model:      "m",
		Tenant:     tenant,
		HashScheme: "vllm",
		Timestamp:  ts,
		Prefixes:   []PrefixRef{{PrefixHash: hash(prefix), TokenCount: 1}},
	})
}

func TestTenantQuotaEvictsOldestOverEntryBudget(t *testing.T) {
	m := &countingMetrics{}
	base := time.Unix(5_000_000, 0)
	// Fix the clock just after the newest ingest so Lookup's freshness check
	// (now - lastSeen, against the 30m TTL) sees the surviving entries as fresh.
	clk := &fakeClock{t: base.Add(5 * time.Minute)}
	idx := New(withClock(clk.now), WithMetrics(m), WithTenantQuotaResolver(fakeQuota{"team": 3}))

	// Five distinct prefixes, strictly increasing lastSeen. Enforcement runs
	// after each ingest, so once over budget the oldest is evicted each time.
	for i, p := range []string{"a", "b", "c", "d", "e"} {
		ingestPrefix(idx, "team", p, base.Add(time.Duration(i)*time.Minute))
	}

	if got := tenantEntries(t, idx, "team"); got != 3 {
		t.Fatalf("tenant entries = %d, want 3 (capped at budget)", got)
	}
	// The two oldest (a, b) must be gone; the three newest (c, d, e) survive.
	for _, gone := range []string{"a", "b"} {
		if scores := idx.Lookup(LookupRequest{Model: "m", Tenant: "team", HashScheme: "vllm", PrefixHash: hash(gone)}); len(scores) != 0 {
			t.Fatalf("prefix %q should have been evicted, got %d hits", gone, len(scores))
		}
	}
	for _, kept := range []string{"c", "d", "e"} {
		if scores := idx.Lookup(LookupRequest{Model: "m", Tenant: "team", HashScheme: "vllm", PrefixHash: hash(kept)}); len(scores) != 1 {
			t.Fatalf("prefix %q should be present, got %d hits", kept, len(scores))
		}
	}
	// Metric: two evictions total (a, then b), labelled over_entries.
	if n := m.evictions["team|"+tenantEvictionReasonOverEntries]; n != 2 {
		t.Fatalf("tenant evictions = %d, want 2", n)
	}
}

func TestTenantQuotaNoOpWhenUnderBudget(t *testing.T) {
	m := &countingMetrics{}
	idx := New(WithMetrics(m), WithTenantQuotaResolver(fakeQuota{"team": 10}))
	base := time.Unix(6_000_000, 0)
	for i, p := range []string{"a", "b", "c"} {
		ingestPrefix(idx, "team", p, base.Add(time.Duration(i)*time.Minute))
	}
	if got := tenantEntries(t, idx, "team"); got != 3 {
		t.Fatalf("tenant entries = %d, want 3 (all retained)", got)
	}
	if n := m.evictions["team|"+tenantEvictionReasonOverEntries]; n != 0 {
		t.Fatalf("tenant evictions = %d, want 0 (under budget)", n)
	}
}

func TestTenantQuotaFailsOpenWithoutResolverEntry(t *testing.T) {
	// "team" has no entry in the resolver → unbounded, identical to no quota.
	idx := New(WithTenantQuotaResolver(fakeQuota{"other": 1}))
	base := time.Unix(7_000_000, 0)
	for i, p := range []string{"a", "b", "c", "d", "e"} {
		ingestPrefix(idx, "team", p, base.Add(time.Duration(i)*time.Minute))
	}
	if got := tenantEntries(t, idx, "team"); got != 5 {
		t.Fatalf("tenant entries = %d, want 5 (fail open, no enforcement)", got)
	}
}

func TestTenantQuotaNilResolverNeverEnforces(t *testing.T) {
	// No resolver wired at all → behavior byte-identical to pre-enforcement.
	idx := New()
	base := time.Unix(8_000_000, 0)
	for i, p := range []string{"a", "b", "c", "d"} {
		ingestPrefix(idx, "team", p, base.Add(time.Duration(i)*time.Minute))
	}
	if got := tenantEntries(t, idx, "team"); got != 4 {
		t.Fatalf("tenant entries = %d, want 4 (no resolver)", got)
	}
}

func TestTenantQuotaIsPerTenantScoped(t *testing.T) {
	// team-a is capped at 2; team-b has no quota. team-a overrunning must not
	// touch team-b's entries (Fairness: a tenant evicts only its own).
	idx := New(WithTenantQuotaResolver(fakeQuota{"team-a": 2}))
	base := time.Unix(9_000_000, 0)
	for i, p := range []string{"a", "b", "c", "d"} {
		ingestPrefix(idx, "team-a", p, base.Add(time.Duration(i)*time.Minute))
		ingestPrefix(idx, "team-b", p, base.Add(time.Duration(i)*time.Minute))
	}
	if got := tenantEntries(t, idx, "team-a"); got != 2 {
		t.Fatalf("team-a entries = %d, want 2 (capped)", got)
	}
	if got := tenantEntries(t, idx, "team-b"); got != 4 {
		t.Fatalf("team-b entries = %d, want 4 (untouched by team-a's eviction)", got)
	}
}

func TestTenantQuotaZeroBudgetEvictsAll(t *testing.T) {
	// A configured budget of 0 is a valid enforceable cap (admit nothing),
	// distinct from "no quota". Every ingested entry is evicted immediately.
	m := &countingMetrics{}
	idx := New(WithMetrics(m), WithTenantQuotaResolver(fakeQuota{"team": 0}))
	base := time.Unix(10_000_000, 0)
	for i, p := range []string{"a", "b"} {
		ingestPrefix(idx, "team", p, base.Add(time.Duration(i)*time.Minute))
	}
	if got := tenantEntries(t, idx, "team"); got != 0 {
		t.Fatalf("tenant entries = %d, want 0 (zero budget)", got)
	}
}

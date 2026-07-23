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

// ingestPrefixReplica ingests one prefix for a tenant under a specific replica,
// so a single prefix key can be placed on multiple replicas (each call adds one
// holder) to exercise the distinct-prefix quota unit.
func ingestPrefixReplica(idx *Index, tenant, replica, prefix string, ts time.Time) {
	idx.Ingest(Update{
		ReplicaID:  replica,
		Model:      "m",
		Tenant:     tenant,
		HashScheme: "vllm",
		Timestamp:  ts,
		Prefixes:   []PrefixRef{{PrefixHash: hash(prefix), TokenCount: 1}},
	})
}

func TestTenantQuotaCountsMultiReplicaPrefixOnce(t *testing.T) {
	// The quota unit is the distinct prefix key, NOT the replica×prefix entry: a
	// prefix held by several replicas counts ONCE toward maxIndexEntries, and when
	// it's evicted it is dropped from ALL its replicas as a single unit. (A naive
	// per-replica count would over-charge the tenant and evict prematurely.)
	m := &countingMetrics{}
	base := time.Unix(11_000_000, 0)
	clk := &fakeClock{t: base.Add(10 * time.Minute)}
	idx := New(withClock(clk.now), WithMetrics(m), WithTenantQuotaResolver(fakeQuota{"team": 2}))

	// Prefix "a" on two replicas → still ONE distinct prefix, within budget 2.
	ingestPrefixReplica(idx, "team", "r1", "a", base.Add(0*time.Minute))
	ingestPrefixReplica(idx, "team", "r2", "a", base.Add(1*time.Minute))
	if got := tenantEntries(t, idx, "team"); got != 1 {
		t.Fatalf("two replicas of one prefix → entries = %d, want 1 (counted once)", got)
	}
	// Add "b" → 2 distinct prefixes, exactly at budget, no eviction.
	ingestPrefixReplica(idx, "team", "r1", "b", base.Add(2*time.Minute))
	if got := tenantEntries(t, idx, "team"); got != 2 {
		t.Fatalf("entries = %d, want 2 (at budget, no eviction yet)", got)
	}
	// Add "c" → 3 distinct, over budget. "a" is the oldest distinct prefix
	// (freshest holder at +1m, vs b@+2m, c@+3m) → evicted as one unit.
	ingestPrefixReplica(idx, "team", "r1", "c", base.Add(3*time.Minute))

	if got := tenantEntries(t, idx, "team"); got != 2 {
		t.Fatalf("entries = %d, want 2 (capped at budget)", got)
	}
	// "a" must be gone from BOTH r1 and r2 (whole-prefix eviction), so Lookup
	// finds no holder at all.
	if scores := idx.Lookup(LookupRequest{Model: "m", Tenant: "team", HashScheme: "vllm", PrefixHash: hash("a")}); len(scores) != 0 {
		t.Fatalf("prefix a should be evicted from all replicas, got %d hits", len(scores))
	}
	for _, kept := range []string{"b", "c"} {
		if scores := idx.Lookup(LookupRequest{Model: "m", Tenant: "team", HashScheme: "vllm", PrefixHash: hash(kept)}); len(scores) != 1 {
			t.Fatalf("prefix %q should be present, got %d hits", kept, len(scores))
		}
	}
	// Exactly one prefix evicted (a), counted once — not once per replica.
	if n := m.evictions["team|"+tenantEvictionReasonOverEntries]; n != 1 {
		t.Fatalf("tenant evictions = %d, want 1 (a evicted as a single prefix)", n)
	}
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

// The quota-eviction victim is chosen by (age, then the full remaining prefix
// key). When two of a tenant's prefixes share an age, the tie MUST break on
// every remaining key field — model, hashScheme, adapter, prefixHash — or the
// victim would depend on Go's randomized map iteration order. This pins that
// each field participates: two same-age keys differing in exactly ONE field are
// ingested with a budget of 1, so precisely the analytically-smaller key is
// evicted, and the scenario is repeated enough times that an incomplete
// comparator (which would treat the pair as equal and pick a map-order victim)
// would flake here rather than in production.
func TestTenantQuotaTieBreakIsDeterministicAcrossFullKey(t *testing.T) {
	// key varies one field of the prefix identity; lo sorts before hi, so lo is
	// the deterministic eviction victim and hi must survive.
	type keyid struct{ model, scheme, adapter, prefix string }
	cases := map[string]struct{ lo, hi keyid }{
		"model": {
			lo: keyid{"m1", "vllm", "", "p"},
			hi: keyid{"m2", "vllm", "", "p"},
		},
		"hashScheme": {
			lo: keyid{"m", "vllm-a", "", "p"},
			hi: keyid{"m", "vllm-b", "", "p"},
		},
		"adapter": {
			lo: keyid{"m", "vllm", "lora-a", "p"},
			hi: keyid{"m", "vllm", "lora-b", "p"},
		},
		"prefixHash": {
			lo: keyid{"m", "vllm", "lora", "p1"},
			hi: keyid{"m", "vllm", "lora", "p2"},
		},
	}

	ingest := func(idx *Index, k keyid, ts time.Time) {
		idx.Ingest(Update{
			ReplicaID:  "r1",
			Model:      k.model,
			Tenant:     "team",
			HashScheme: k.scheme,
			Adapter:    k.adapter,
			Timestamp:  ts,
			Prefixes:   []PrefixRef{{PrefixHash: hash(k.prefix), TokenCount: 1}},
		})
	}
	held := func(idx *Index, k keyid) bool {
		return len(idx.Lookup(LookupRequest{
			Model: k.model, Tenant: "team", HashScheme: k.scheme,
			Adapter: k.adapter, PrefixHash: hash(k.prefix),
		})) > 0
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// Many independent index instances → many independent map seeds, so a
			// tie-break that ignored this field would evict hi on some runs.
			for iter := 0; iter < 64; iter++ {
				base := time.Unix(20_000_000, 0)
				clk := &fakeClock{t: base.Add(time.Minute)} // both entries fresh, equal age
				idx := New(withClock(clk.now), WithTenantQuotaResolver(fakeQuota{"team": 1}))

				// Same timestamp for both → identical age → the tie-break decides.
				ingest(idx, tc.hi, base) // ingest hi first so insertion order can't be what saves it
				ingest(idx, tc.lo, base) // second ingest pushes over budget 1 → exactly one eviction

				if held(idx, tc.lo) {
					t.Fatalf("iter %d: lo key %+v survived — it is the smaller of the tie and must be the victim", iter, tc.lo)
				}
				if !held(idx, tc.hi) {
					t.Fatalf("iter %d: hi key %+v was evicted — victim selection is not deterministic on the %s field (map-iteration-order dependent)", iter, tc.hi, name)
				}
			}
		})
	}
}

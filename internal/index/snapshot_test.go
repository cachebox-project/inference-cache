// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"testing"
)

// TestReservedTenantHiddenFromCapAndAggregate pins the WithReservedTenants
// contract: reserved-tenant entries are present in the index (so the probe's
// Stage A lookup still finds them) but invisible to the cap accounting,
// aggregate, snapshot, and per-model entry-count gauge — so a probe in flight
// cannot displace real workload state via the cap sweep AND cannot leak
// into observability surfaces. Mirrors TestProberRun* in internal/server, but
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

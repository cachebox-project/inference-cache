package index

import (
	"testing"
	"time"
)

// TestSnapshotEntryInvariant is the hard guard on the aggregate's core
// invariant: a tenant's reported indexEntries always sum to the cluster prefix
// total, with no entry unattributed. It checks the invariant on BOTH the raw
// Aggregate() (Total == Σ PerTenant) and the public Snapshot surface
// (TotalPrefixes == Σ tenants[].indexEntries) across a spread of populations.
func TestSnapshotEntryInvariant(t *testing.T) {
	base := time.Unix(30_000_000, 0)
	addPrefix := func(idx *Index, tenant, replica, prefix string, ts time.Time) {
		idx.Ingest(Update{
			ReplicaID: replica, Model: "m", Tenant: tenant, HashScheme: "vllm",
			Timestamp: ts,
			Prefixes:  []PrefixRef{{PrefixHash: hash(prefix), TokenCount: 1}},
		})
	}

	cases := []struct {
		name          string
		build         func() *Index
		wantPerTenant map[string]int64
		wantTotal     int64
	}{
		{
			name:          "empty",
			build:         func() *Index { return New() },
			wantPerTenant: map[string]int64{},
			wantTotal:     0,
		},
		{
			name: "single tenant",
			build: func() *Index {
				idx := New()
				addPrefix(idx, "team", "r1", "a", base)
				addPrefix(idx, "team", "r1", "b", base)
				return idx
			},
			wantPerTenant: map[string]int64{"team": 2},
			wantTotal:     2,
		},
		{
			name: "multi-tenant with a multi-replica prefix",
			build: func() *Index {
				idx := New()
				// prefix "a" held by two replicas → two entries for team-a, so the
				// total (3) exceeds the distinct-prefix count (2) — the case where
				// an entry-count vs prefix-count confusion would break the invariant.
				addPrefix(idx, "team-a", "r1", "a", base)
				addPrefix(idx, "team-a", "r2", "a", base)
				addPrefix(idx, "team-b", "r3", "c", base)
				return idx
			},
			wantPerTenant: map[string]int64{"team-a": 2, "team-b": 1},
			wantTotal:     3,
		},
		{
			name: "default (untenanted) entries present",
			build: func() *Index {
				idx := New()
				addPrefix(idx, "", "r1", "a", base) // empty tenant → _default bucket
				addPrefix(idx, "team", "r2", "b", base)
				return idx
			},
			wantPerTenant: map[string]int64{DefaultTenantSentinel: 1, "team": 1},
			wantTotal:     2,
		},
		{
			name: "post-eviction",
			build: func() *Index {
				idx := New(WithTenantQuotaResolver(fakeQuota{"team": 2}))
				for i, p := range []string{"a", "b", "c", "d"} {
					addPrefix(idx, "team", "r1", p, base.Add(time.Duration(i)*time.Minute))
				}
				return idx
			},
			wantPerTenant: map[string]int64{"team": 2},
			wantTotal:     2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			idx := tc.build()

			// Aggregate() is internally consistent: Total == Σ PerTenant.
			agg := idx.Aggregate()
			var aggSum int64
			for _, n := range agg.PerTenant {
				aggSum += n
			}
			if aggSum != agg.Total {
				t.Fatalf("Aggregate: Σ PerTenant = %d, Total = %d", aggSum, agg.Total)
			}
			if agg.Total != tc.wantTotal {
				t.Fatalf("Aggregate.Total = %d, want %d", agg.Total, tc.wantTotal)
			}

			// The public snapshot carries the same invariant:
			// Σ tenants[].indexEntries == TotalPrefixes.
			snap := idx.Snapshot()
			var snapSum int64
			got := map[string]int64{}
			for _, ts := range snap.Tenants {
				snapSum += ts.IndexEntries
				got[ts.TenantID] = ts.IndexEntries
			}
			if snapSum != int64(snap.TotalPrefixes) {
				t.Fatalf("Σ tenants[].indexEntries = %d, TotalPrefixes = %d (tenants=%+v)", snapSum, snap.TotalPrefixes, snap.Tenants)
			}
			if int64(snap.TotalPrefixes) != tc.wantTotal {
				t.Fatalf("snap.TotalPrefixes = %d, want %d", snap.TotalPrefixes, tc.wantTotal)
			}
			for tenant, want := range tc.wantPerTenant {
				if got[tenant] != want {
					t.Fatalf("tenant %q indexEntries = %d, want %d (tenants=%+v)", tenant, got[tenant], want, snap.Tenants)
				}
			}
			if len(got) != len(tc.wantPerTenant) {
				t.Fatalf("tenant buckets = %v, want %v", got, tc.wantPerTenant)
			}
		})
	}
}

// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"sort"
	"time"
)

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

// DefaultTenantSentinel is the bucket distinct prefixes with no tenant ID are
// attributed to in cluster-wide aggregates: the empty string itself. Untenanted
// prefixes count toward the grand total, so they must also appear as a tenants[]
// bucket or Σ tenants[].indexEntries would silently fall short of the total —
// they're kept under "" rather than dropped. The empty string is deliberately
// collision-free: a real CacheTenant.spec.tenantID is MinLength=1, so this bucket
// can NEVER be claimed by a real tenant's per-CacheTenant status (which keys on
// the tenant ID). That is why no reserved non-empty name is needed.
const DefaultTenantSentinel = ""

// Aggregate is the index's prefix-count aggregate: the per-tenant distinct-prefix
// counts and the grand total, both produced by a SINGLE walk of the prefix map
// so they cannot disagree. Total == Σ PerTenant by construction — this is the
// hard invariant the CacheIndex/CacheTenant status surfaces rely on (a tenant's
// reported indexEntries always sum to the cluster prefix total). The counted
// unit is a distinct prefix key — (tenant, model, hash_scheme, adapter, prefix_hash),
// regardless of how many replicas hold it — which is exactly the unit
// prefixes.summary.total reports and the per-tenant maxIndexEntries quota bounds.
// (Tenant is part of the key, so the per-tenant partition is exact.)
type Aggregate struct {
	PerTenant map[string]int64
	Total     int64
}

// Aggregate returns the prefix-count aggregate under a single read-lock + single
// walk. Exposed so callers/tests can assert the invariant directly.
func (i *Index) Aggregate() Aggregate {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.aggregateLocked()
}

// aggregateLocked walks the prefix map exactly once, attributing every distinct
// prefix key to its tenant bucket and the running total in the same step. Caller
// holds at least the read lock. Because both numbers come from the one
// iteration, Total == Σ PerTenant always holds — no second pass, no separate
// counter that could drift. Reserved-tenant entries (see WithReservedTenants)
// are excluded from BOTH PerTenant and Total so the cluster aggregate the
// operator sees never temporarily surfaces synthetic probe state.
//
// Unit note: Aggregate counts DISTINCT PREFIX KEYS — one (tenant, model,
// hash_scheme, prefix_hash) tuple, regardless of how many replicas hold it.
// The cap accounting (totalEntries / reservedEntries / effectiveTotal)
// counts REPLICA×PREFIX entries — a prefix held by N replicas contributes N
// to totalEntries. The two are different units; they aren't expected to
// match numerically, only to track the same RESERVED-TENANT EXCLUSION
// principle (the operator-visible aggregate and the cap-enforcement view
// both treat reserved tenants as if they weren't there).
func (i *Index) aggregateLocked() Aggregate {
	agg := Aggregate{PerTenant: make(map[string]int64)}
	for key := range i.prefixes {
		if i.isReservedTenant(key.tenant) {
			continue
		}
		// Untenanted prefixes (key.tenant == "") bucket under "" — collision-free,
		// since no real CacheTenant tenantID is empty.
		agg.PerTenant[key.tenant]++
		agg.Total++
	}
	return agg
}

// Snapshot is a point-in-time, cluster-wide domain view of the index. The
// server maps it to the controller-facing /snapshot DTO. Metadata only.
//
// TotalPrefixes is the number of distinct prefix keys (a prefix held by
// multiple replicas counts once), and it equals the sum of
// tenants[].indexEntries — see Aggregate.
type Snapshot struct {
	Replicas      []ReplicaSnapshot
	Tenants       []TenantSnapshot
	TotalPrefixes int
	HotPrefixes   int // always 0: intentionally unwired. The per-entry LFU access counter exists but governs cap eviction only; it is not aggregated into a cluster-wide "hot prefix" count.
}

// ReplicaSnapshot is the latest reported state for one replica, cluster-wide.
//
// PrefixCount and LastEventAt are derived from the prefix map and are the
// per-replica view consumed by the CacheBackend status projection (see
// internal/controller/cacheindex_controller.go). LastEventAt is the zero
// time when the replica holds no prefix entries — interpret a zero value as
// "no KV event observed yet" rather than "epoch."
//
// Tenant is the tenant_id the subscriber reported with the replica. The
// subscriber sidecar derives it from POD_NAMESPACE, so for the in-cluster
// path it equals the engine pod's namespace and lets a controller-side
// consumer scope a pod lookup. Empty when the replica is only known through
// older code paths that did not carry tenant context.
type ReplicaSnapshot struct {
	ReplicaID        string
	Tenant           string
	CacheMemoryBytes int64
	HitRate          float32
	Pressure         float32
	LastUpdate       time.Time
	PrefixCount      int
	LastEventAt      time.Time
	// StatsReported is true once the replica's stats reporter has emitted at
	// least one stats payload (the replica appears in the stats map). It is the
	// presence bit that lets a consumer distinguish an observed 0 hit rate /
	// pressure / memory from "not yet reported": a replica known only through
	// reported prefixes (Ingest) but no stats payload has StatsReported=false and
	// zero-valued HitRate/Pressure/CacheMemoryBytes/LastUpdate. The CacheIndex
	// status projection uses it to leave the cluster-aggregate replica hitRate
	// nil rather than fabricating "0" (see internal/controller).
	StatsReported bool
	// T2HitTokens / T2QueryTokens carry the replica's cumulative tier-2
	// (external offload) reload token counters across the /snapshot wire.
	T2HitTokens   int64
	T2QueryTokens int64
}

// TenantSnapshot is the aggregate footprint for one tenant.
//
// IndexEntries is the tenant's live distinct-prefix count, the quantity
// CacheTenant.spec.quota.maxIndexEntries bounds; across all tenants these sum
// to Snapshot.TotalPrefixes by construction (see Aggregate).
//
// MemoryUsed is deprecated and never populated (always 0): cache_memory_bytes
// is the engine total across all tenants on a replica, so summing it per tenant
// double-counts on shared engines. The field is retained in the wire shape for
// controller/server skew-compatibility (an older controller still finds the
// key) and scheduled for removal at v1beta1. For memory, read the per-replica
// ReplicaSnapshot.CacheMemoryBytes (engine total per replica). See
// docs/design/crd-contract.md and docs/concepts/cachetenant-identity-and-quota.md
// for the enforcement-boundary rationale.
type TenantSnapshot struct {
	TenantID     string
	IndexEntries int64
	HitRate      float32
	// HitRateReported is true once at least one replica of this tenant has
	// reported stats (the hit-rate average had n > 0 samples). It is the
	// presence bit that distinguishes an observed mean hit rate of 0 from "no
	// replica has reported stats yet": a tenant known only through reported
	// prefixes (index entries) but no replica stats has HitRateReported=false
	// and a zero-valued HitRate. The CacheIndex status projection uses it to
	// leave the cluster-aggregate tenant hitRate nil rather than fabricating
	// "0" (see internal/controller).
	HitRateReported bool
	// Deprecated: always 0; read ReplicaSnapshot.CacheMemoryBytes instead.
	MemoryUsed int64
}

// Snapshot returns the current cluster-wide aggregate. Replicas use the latest
// stats reported for each replica id; tenant hit-rate dedups replicas within a
// tenant (it is an approximation — a replica serving multiple models for a
// tenant is counted once). Results are sorted for deterministic output.
//
// Reserved tenants (see WithReservedTenants) are excluded from the snapshot
// entirely — replicas, tenants, and totals — so a probe in flight cannot
// temporarily publish its synthetic `__probe-<backend>` replica or the
// reserved tenant id into the CacheIndex CR status the controller polls.
// Same rationale as their exclusion from the cap-sweep victim set:
// server-internal state must not leak to operator-visible surfaces.
func (i *Index) Snapshot() Snapshot {
	i.mu.RLock()

	type tenantReplica struct{ tenant, replica string }
	// Cluster-wide replica snapshots key on (tenant, replicaID): two pods in
	// different namespaces can legitimately share a metadata.name (e.g.
	// "vllm-0"), and merging them into one row would mis-attribute prefixes
	// across tenancy. We then aggregate ONLY across models / hash_schemes
	// within the same (tenant, replicaID).
	latestByReplica := make(map[tenantReplica]statEntry)
	for sk, s := range i.stats {
		if i.isReservedTenant(sk.tenant) {
			continue
		}
		tr := tenantReplica{sk.tenant, sk.replicaID}
		if cur, ok := latestByReplica[tr]; !ok || s.lastSeen.After(cur.lastSeen) {
			latestByReplica[tr] = s
		}
	}

	// Per-replica prefix counts + last KV-event timestamps. Keyed on
	// (tenant, replicaID) for the same reason as latestByReplica — so two
	// pods in different namespaces with the same name do not merge into a
	// single row. Derived from the prefix map (not the stats map) so the
	// projection reflects what the replica actually holds, not just whether
	// its stats are alive.
	type replicaPrefixAgg struct {
		count       int
		lastEventAt time.Time
	}
	prefixByReplica := make(map[tenantReplica]*replicaPrefixAgg)
	for key, replicas := range i.prefixes {
		if i.isReservedTenant(key.tenant) {
			continue
		}
		for id, e := range replicas {
			tr := tenantReplica{key.tenant, id}
			a := prefixByReplica[tr]
			if a == nil {
				a = &replicaPrefixAgg{}
				prefixByReplica[tr] = a
			}
			a.count++
			if e.lastSeen.After(a.lastEventAt) {
				a.lastEventAt = e.lastSeen
			}
		}
	}

	// Per-tenant distinct-prefix counts + the grand total come from ONE
	// authoritative walk (aggregateLocked) so the reported numbers can't drift:
	// TotalPrefixes == Σ tenants[].indexEntries by construction. The walk
	// already excludes reserved tenants, so the snapshot total matches what
	// operator dashboards expect to see.
	agg := i.aggregateLocked()
	snap := Snapshot{TotalPrefixes: int(agg.Total)}

	// Union of (tenant, replicaID) seen in stats AND in prefixes — a replica
	// may have reported prefixes via Ingest but had its stats entry evicted,
	// or vice versa; the snapshot surfaces both so per-backend projection is
	// robust. Each row is a unique (tenant, replicaID).
	seen := make(map[tenantReplica]struct{}, len(latestByReplica)+len(prefixByReplica))
	for tr := range latestByReplica {
		seen[tr] = struct{}{}
	}
	for tr := range prefixByReplica {
		seen[tr] = struct{}{}
	}
	for tr := range seen {
		r := ReplicaSnapshot{ReplicaID: tr.replica, Tenant: tr.tenant}
		if s, ok := latestByReplica[tr]; ok {
			r.CacheMemoryBytes = s.stats.CacheMemoryBytes
			r.HitRate = s.stats.HitRate
			r.Pressure = s.stats.Pressure
			r.T2HitTokens = s.stats.T2HitTokens
			r.T2QueryTokens = s.stats.T2QueryTokens
			r.LastUpdate = s.lastSeen
			// Presence of a stats entry is the "stats reporter emitted" signal;
			// a prefix-only replica (in prefixByReplica but not here) stays
			// StatsReported=false so consumers can tell an observed 0 from
			// "not yet reported".
			r.StatsReported = true
		}
		if a, ok := prefixByReplica[tr]; ok {
			r.PrefixCount = a.count
			r.LastEventAt = a.lastEventAt
		}
		snap.Replicas = append(snap.Replicas, r)
	}

	type tenantAgg struct {
		sumHit float64
		n      int
	}
	byTenant := make(map[string]*tenantAgg)
	for tr, s := range latestByReplica {
		// Untenanted stats bucket under "" — the same key the entry walk uses, so a
		// tenant's stats and its indexEntries land together.
		bucket := tr.tenant
		a := byTenant[bucket]
		if a == nil {
			a = &tenantAgg{}
			byTenant[bucket] = a
		}
		a.sumHit += float64(s.stats.HitRate)
		a.n++
	}
	// Union the stats-bearing tenants with the entry-bearing tenants: a tenant
	// can have index entries but no stats reported yet (prefixes reported without
	// a stats payload), or stats but no live entries. A tenant in only one map
	// gets zeroes for the other dimension. Emitting every entry-bearing tenant
	// (agg.PerTenant) is what makes Σ tenants[].indexEntries == TotalPrefixes:
	// no entry bucket is ever dropped from tenants[].
	tenantSeen := make(map[string]struct{}, len(byTenant)+len(agg.PerTenant))
	emit := func(t string) {
		if _, ok := tenantSeen[t]; ok {
			return
		}
		tenantSeen[t] = struct{}{}
		var hit float32
		var hitReported bool
		if a := byTenant[t]; a != nil {
			if a.n > 0 {
				hit = float32(a.sumHit / float64(a.n))
				// n > 0 means at least one replica reported stats for the
				// tenant — the "stats reporter emitted" signal for the tenant's
				// mean hit rate. A tenant with index entries but no stats stays
				// HitRateReported=false so an observed 0 is distinguishable.
				hitReported = true
			}
		}
		snap.Tenants = append(snap.Tenants, TenantSnapshot{
			TenantID:        t,
			IndexEntries:    agg.PerTenant[t],
			HitRate:         hit,
			HitRateReported: hitReported,
		})
	}
	for t := range byTenant {
		emit(t)
	}
	for t := range agg.PerTenant {
		emit(t)
	}
	i.mu.RUnlock()

	sort.Slice(snap.Replicas, func(a, b int) bool {
		if snap.Replicas[a].Tenant != snap.Replicas[b].Tenant {
			return snap.Replicas[a].Tenant < snap.Replicas[b].Tenant
		}
		return snap.Replicas[a].ReplicaID < snap.Replicas[b].ReplicaID
	})
	sort.Slice(snap.Tenants, func(a, b int) bool { return snap.Tenants[a].TenantID < snap.Tenants[b].TenantID })
	return snap
}

// EntryCountsByModel returns the number of distinct prefixes per model.
// Reserved-tenant entries (see WithReservedTenants) are excluded so the
// inferencecache_index_entries gauge reportEntries publishes never
// transiently surfaces synthetic probe state during a Run. Mirrors the
// snapshot/aggregate exclusion of the reserved scope.
func (i *Index) EntryCountsByModel() map[string]int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	counts := make(map[string]int)
	for key := range i.prefixes {
		if i.isReservedTenant(key.tenant) {
			continue
		}
		counts[key.model]++
	}
	return counts
}

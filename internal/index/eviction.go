// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"sort"
	"strings"
	"time"
)

// tenantEvictionReasonOverEntries is the only quota dimension enforced today:
// the index entry table is the one cache resource the server authoritatively
// owns. (Per-tenant memory is not enforceable on a shared, tenant-unaware
// engine — see the CacheTenant CRD docs.)
const tenantEvictionReasonOverEntries = "over_entries"

// Reason labels for inferencecache_index_evictions_total. "cap" = the global
// entry cap (maxEntries) was exceeded; "ttl" = the freshness sweep removed a
// stale entry. Distinct from the quota path's tenant_evictions_total.
const (
	indexEvictionReasonCap = "cap"
	indexEvictionReasonTTL = "ttl"
)

// ttlFor returns the effective TTL for a tenant. A nil resolver, or one that
// returns <=0, falls back to the index's global TTL (which is itself clamped
// to DefaultTTL in New). Per-tenant TTL lets a namespace's CachePolicy widen
// or shrink the freshness window independently of every other namespace.
func (i *Index) ttlFor(tenant string) time.Duration {
	if i.ttlResolver != nil {
		if d := i.ttlResolver.TTL(tenant); d > 0 {
			return d
		}
	}
	return i.ttl
}

// evictionFor returns the normalized cap-eviction algorithm for a tenant:
// EvictionLFU only when the resolver explicitly says so, otherwise EvictionLRU
// (a nil resolver, an empty string, or any unrecognized value all fall back to
// LRU — the default and the pre-LFU behavior). Mirrors ttlFor.
func (i *Index) evictionFor(tenant string) string {
	if i.evictionResolver != nil {
		if strings.ToLower(i.evictionResolver.Eviction(tenant)) == EvictionLFU {
			return EvictionLFU
		}
	}
	return EvictionLRU
}

// evictExpired removes entries older than each tenant's TTL. Runs on the
// sweep loop. Per-tenant TTLs let two namespaces with very different
// CachePolicy TTLs evict on independent schedules (the sweep itself
// remains shared).
func (i *Index) evictExpired() {
	now := i.now()

	// Cache per-tenant TTL across one sweep so a slow resolver isn't called
	// once per entry. The cache lives only for this sweep — built lazily on
	// first sight of a tenant. Lookups still go through i.ttlFor (which may
	// call the resolver), but at most once per tenant per sweep.
	ttlCache := make(map[string]time.Duration)
	ttlOf := func(tenant string) time.Duration {
		if d, ok := ttlCache[tenant]; ok {
			return d
		}
		d := i.ttlFor(tenant)
		ttlCache[tenant] = d
		return d
	}
	// Resolve each tenant's algorithm once per sweep, only for the eviction
	// metric label — TTL eviction itself is algorithm-independent.
	algoCache := make(map[string]string)
	algoOf := func(tenant string) string {
		if a, ok := algoCache[tenant]; ok {
			return a
		}
		a := i.evictionFor(tenant)
		algoCache[tenant] = a
		return a
	}
	var ttlEvicted map[string]int

	i.mu.Lock()
	for key, replicas := range i.prefixes {
		ttl := ttlOf(key.tenant)
		for id, e := range replicas {
			if ttl > 0 && now.Sub(e.lastSeen) >= ttl {
				i.removeReplicaLocked(key, replicas, id)
				if ttlEvicted == nil {
					ttlEvicted = make(map[string]int, 2)
				}
				ttlEvicted[algoOf(key.tenant)]++
			}
		}
	}
	for sk, s := range i.stats {
		ttl := ttlOf(sk.tenant)
		if ttl > 0 && now.Sub(s.lastSeen) >= ttl {
			delete(i.stats, sk)
			i.statsScopeRemoveLocked(modelKey{sk.tenant, sk.model}, sk.replicaID)
		}
	}
	i.mu.Unlock()

	if i.metrics != nil {
		for algo, n := range ttlEvicted {
			i.metrics.AddIndexEvictions(algo, indexEvictionReasonTTL, n)
		}
	}
	i.reportEntries()
}

// enforceCapLocked evicts entries until totalEntries is within maxEntries,
// choosing victims by each entry's per-namespace algorithm under a single global
// cap. Victims are ordered by a unified key — effectiveCount ASC, then lastSeen
// ASC — where effectiveCount is the entry's LFU access count in an LFU namespace
// and 0 in an LRU namespace. So an all-LRU cap degenerates to pure
// oldest-by-lastSeen (the historical behavior), an all-LFU cap to
// lowest-count-first with an oldest-lastSeen tie-break, and mixed namespaces
// interleave LRU and low-count LFU entries by recency. The algorithm is resolved
// at sort time and never stored on the entry, so a policy switch takes effect on
// the next sweep with no counter migration. A final stable tie-break on the
// entry's identity keeps the victim set deterministic on full (count, lastSeen)
// ties regardless of map iteration order.
//
// Returns the count of entries evicted per algorithm ("lru"/"lfu") so the caller
// can emit the eviction metric AFTER releasing the lock (nil when nothing was
// over cap). Caller holds the write lock. maxEntries == 0 means unbounded. The
// sort is O(n log n); it only runs while over the cap.
func (i *Index) enforceCapLocked() map[string]int {
	// Reserved-tenant entries (the probe's synthetic state, etc.) are excluded
	// from the cap accounting AND the victim candidate set — so a concurrent
	// real-workload Ingest that fires while a probe is in flight cannot evict
	// a real-workload entry to make room for a transient probe entry that
	// cleanup is about to remove. effectiveTotal is the over-cap measurement.
	effectiveTotal := i.totalEntries - i.reservedEntries
	if i.maxEntries <= 0 || effectiveTotal <= i.maxEntries {
		return nil
	}
	// Resolve each tenant's algorithm once per sweep (the resolver takes the
	// policy-store lock; evictExpired already nests it under i.mu the same way).
	algoCache := make(map[string]string)
	algoOf := func(tenant string) string {
		if a, ok := algoCache[tenant]; ok {
			return a
		}
		a := i.evictionFor(tenant)
		algoCache[tenant] = a
		return a
	}
	type ref struct {
		key            prefixKey
		replica        string
		algo           string
		effectiveCount int64
		lastSeen       time.Time
	}
	all := make([]ref, 0, effectiveTotal)
	for key, replicas := range i.prefixes {
		// Skip reserved-tenant entries from the victim candidate set entirely.
		if i.isReservedTenant(key.tenant) {
			continue
		}
		algo := algoOf(key.tenant)
		for id, e := range replicas {
			var eff int64
			if algo == EvictionLFU {
				eff = e.accessCount.Load()
			}
			all = append(all, ref{key, id, algo, eff, e.lastSeen})
		}
	}
	sort.Slice(all, func(a, b int) bool {
		x, y := all[a], all[b]
		if x.effectiveCount != y.effectiveCount {
			return x.effectiveCount < y.effectiveCount
		}
		if !x.lastSeen.Equal(y.lastSeen) {
			return x.lastSeen.Before(y.lastSeen)
		}
		// Deterministic full-tie order (locked decision): break on the entry's
		// stable identity so the victim set never depends on map iteration order.
		if x.key != y.key {
			if x.key.tenant != y.key.tenant {
				return x.key.tenant < y.key.tenant
			}
			if x.key.model != y.key.model {
				return x.key.model < y.key.model
			}
			if x.key.hashScheme != y.key.hashScheme {
				return x.key.hashScheme < y.key.hashScheme
			}
			// The adapter partition is part of the key, so it must be part of
			// the tie-break too: two adapters can hold the SAME prefixHash
			// (the fingerprint is token-only), and without this the comparator
			// would report "equal" for two distinct keys and reintroduce
			// map-iteration-order dependence in the victim set.
			if x.key.adapter != y.key.adapter {
				return x.key.adapter < y.key.adapter
			}
			return x.key.prefixHash < y.key.prefixHash
		}
		return x.replica < y.replica
	})
	var evicted map[string]int
	for _, r := range all {
		if i.totalEntries-i.reservedEntries <= i.maxEntries {
			break
		}
		i.removeReplicaLocked(r.key, i.prefixes[r.key], r.replica)
		if evicted == nil {
			evicted = make(map[string]int, 2)
		}
		evicted[r.algo]++
	}
	return evicted
}

// isReservedTenant reports whether the given tenant id was declared as
// reserved via WithReservedTenants. Tight inlining matters because this is
// checked on every prefix-entry insert/remove. Returns false on nil sets so
// the default index (no reservations) pays exactly one extra map-nil check
// per call.
func (i *Index) isReservedTenant(tenant string) bool {
	if len(i.reservedTenants) == 0 {
		return false
	}
	_, ok := i.reservedTenants[tenant]
	return ok
}

// tenantQuotaFor returns the tenant's index-entry budget and whether one is
// configured. A nil resolver (or no matching CacheTenant) reports ok=false →
// the index leaves the tenant unbounded (fail open), identical to the behavior
// before any CacheTenant exists. Mirrors ttlFor.
func (i *Index) tenantQuotaFor(tenant string) (maxEntries int64, ok bool) {
	if i.quotaResolver == nil {
		return 0, false
	}
	return i.quotaResolver.TenantQuota(tenant)
}

// evictOldestForTenantLocked evicts the tenant's oldest distinct prefixes until
// its prefix count is within maxPrefixes, returning how many prefixes it
// removed. Caller holds the write lock.
//
// This is the Fairness-mode primitive: it touches ONLY the named tenant's
// prefixes, never another tenant's, so one tenant overrunning its budget can't
// evict a well-behaved tenant's hints. A prefix's age is its freshest replica's
// lastSeen (the most recent time any replica refreshed it); the oldest such
// prefixes go first. Removing a prefix drops ALL its replicas — the quota unit
// is the distinct prefix key, so a prefix counts once no matter how many
// replicas hold it. Ties on age break by prefix hash for deterministic order.
func (i *Index) evictOldestForTenantLocked(tenant string, maxPrefixes int64) int {
	if maxPrefixes < 0 {
		return 0
	}
	if int64(i.prefixesByTenant[tenant]) <= maxPrefixes {
		return 0
	}
	type ref struct {
		key prefixKey
		age time.Time // freshest replica lastSeen across the prefix's holders
	}
	all := make([]ref, 0, i.prefixesByTenant[tenant])
	for key, replicas := range i.prefixes {
		if key.tenant != tenant {
			continue
		}
		var newest time.Time
		for _, e := range replicas {
			if e.lastSeen.After(newest) {
				newest = e.lastSeen
			}
		}
		all = append(all, ref{key, newest})
	}
	sort.Slice(all, func(a, b int) bool {
		x, y := all[a].key, all[b].key
		if !all[a].age.Equal(all[b].age) {
			return all[a].age.Before(all[b].age)
		}
		// Break age ties on the FULL remaining key so victim selection never
		// depends on map iteration order. tenant is constant here (this helper
		// only scans one tenant's prefixes), but model, hashScheme, and adapter
		// are all free to differ within it — and two keys can even share a
		// prefixHash across adapters (the fingerprint is token-only). Compare
		// every field that completes the key, mirroring the cap-sweep comparator.
		if x.model != y.model {
			return x.model < y.model
		}
		if x.hashScheme != y.hashScheme {
			return x.hashScheme < y.hashScheme
		}
		if x.adapter != y.adapter {
			return x.adapter < y.adapter
		}
		return x.prefixHash < y.prefixHash
	})
	removed := 0
	for _, r := range all {
		if int64(i.prefixesByTenant[tenant]) <= maxPrefixes {
			break
		}
		// Drop the whole prefix: collect replica ids first (removeReplicaLocked
		// mutates the inner map and deletes the key on the last removal).
		replicas := i.prefixes[r.key]
		ids := make([]string, 0, len(replicas))
		for id := range replicas {
			ids = append(ids, id)
		}
		for _, id := range ids {
			i.removeReplicaLocked(r.key, i.prefixes[r.key], id)
		}
		removed++
	}
	return removed
}

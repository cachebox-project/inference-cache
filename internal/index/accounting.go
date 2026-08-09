// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"math"
	"time"
)

// upsertReplicaLocked refreshes (or inserts) a replica's hold on one prefix
// key. Caller holds the write lock. Bumps totalEntries on first insert so the
// memory cap stays accurate when chains expand into N per-block entries, and
// bumps the (tenant, model, hash_scheme) → replica serving count in lockstep
// so tenantHotCandidates' O(1) scope lookup stays consistent with i.prefixes.
func (i *Index) upsertReplicaLocked(key prefixKey, replicaID string, tokenCount int32, tier CacheTier, ts time.Time) {
	replicas := i.prefixes[key]
	if replicas == nil {
		replicas = make(map[string]*replicaEntry)
		i.prefixes[key] = replicas
		// First replica of a brand-new prefix key → one more distinct prefix
		// for this tenant (the maxIndexEntries unit), and one more for the
		// (tenant, model) bucket the miss-classifier's UNKNOWN_MODEL check
		// reads.
		i.prefixesByTenant[key.tenant]++
		i.prefixesByTenantModel[modelKey{key.tenant, key.model}]++
	}
	e, existed := replicas[replicaID]
	if !existed {
		// First sight of this (prefix, replica): allocate a fresh entry whose
		// accessCount starts at zero. A refresh below mutates this SAME pointer
		// in place, so re-ingesting an existing entry never resets its LFU
		// counter (the counter tracks lookup usefulness, not ingest recency).
		e = &replicaEntry{}
		replicas[replicaID] = e
		i.totalEntries++
		if i.isReservedTenant(key.tenant) {
			i.reservedEntries++
		}
		i.scopeIncLocked(scopeKey{key.tenant, key.model, key.hashScheme}, replicaID)
	}
	e.tokenCount = tokenCount
	e.lastSeen = ts
	e.tier = tier
}

// removeReplicaLocked drops a replica from a prefix, deleting the prefix if it
// becomes empty. Caller holds the write lock.
func (i *Index) removeReplicaLocked(key prefixKey, replicas map[string]*replicaEntry, replicaID string) {
	if _, ok := replicas[replicaID]; !ok {
		return
	}
	delete(replicas, replicaID)
	i.totalEntries--
	if i.isReservedTenant(key.tenant) {
		i.reservedEntries--
	}
	if len(replicas) == 0 {
		delete(i.prefixes, key)
		// Last replica gone → the prefix key is removed → one fewer distinct
		// prefix for this tenant AND for the (tenant, model) bucket.
		if n := i.prefixesByTenant[key.tenant] - 1; n <= 0 {
			delete(i.prefixesByTenant, key.tenant)
		} else {
			i.prefixesByTenant[key.tenant] = n
		}
		mk := modelKey{key.tenant, key.model}
		if n := i.prefixesByTenantModel[mk] - 1; n <= 0 {
			delete(i.prefixesByTenantModel, mk)
		} else {
			i.prefixesByTenantModel[mk] = n
		}
	}
	// Drop the replica from the (tenant, model, hash_scheme) serving count
	// in lockstep with the prefix removal so TENANT_HOT's O(1) check stays
	// consistent with what's actually in i.prefixes.
	i.scopeDecLocked(scopeKey{key.tenant, key.model, key.hashScheme}, replicaID)
}

// scopeIncLocked increments the (scope, replica) serving count, creating
// the inner map on first sight. Caller holds the write lock.
func (i *Index) scopeIncLocked(scope scopeKey, replicaID string) {
	m := i.servingByScope[scope]
	if m == nil {
		m = make(map[string]int)
		i.servingByScope[scope] = m
	}
	m[replicaID]++
}

// scopeDecLocked decrements the (scope, replica) serving count and removes
// the entry once it reaches zero (and the outer scope once it's empty), so
// the map doesn't accumulate dead keys. Caller holds the write lock.
func (i *Index) scopeDecLocked(scope scopeKey, replicaID string) {
	m := i.servingByScope[scope]
	if m == nil {
		return
	}
	n := m[replicaID] - 1
	if n <= 0 {
		delete(m, replicaID)
		if len(m) == 0 {
			delete(i.servingByScope, scope)
		}
		return
	}
	m[replicaID] = n
}

// statsScopeAddLocked records replicaID as having stats reported in (tenant,
// model) so tenantHotCandidates can scan only the relevant subset rather
// than the whole i.stats map. Caller holds the write lock.
func (i *Index) statsScopeAddLocked(mk modelKey, replicaID string) {
	m := i.replicasByModel[mk]
	if m == nil {
		m = make(map[string]struct{})
		i.replicasByModel[mk] = m
	}
	m[replicaID] = struct{}{}
}

// statsScopeRemoveLocked drops replicaID from the (tenant, model) set when
// its stats entry has been deleted (event-driven clear or TTL sweep).
// Caller holds the write lock.
func (i *Index) statsScopeRemoveLocked(mk modelKey, replicaID string) {
	m := i.replicasByModel[mk]
	if m == nil {
		return
	}
	delete(m, replicaID)
	if len(m) == 0 {
		delete(i.replicasByModel, mk)
	}
}

// reportEntries pushes live per-model counts to the metrics sink, if wired.
// Models that have drained to zero since the last report are explicitly set to
// 0 so their gauge series doesn't go stale.
//
// The snapshot is taken while holding reportMu so concurrent reporters can't
// publish out of order: reportMu serializes them, and because each snapshot
// reads live index state at publish time, whichever reporter runs last writes
// the current count (mutations complete under i.mu before reportEntries is
// called). Lock order is always reportMu → i.mu, never the reverse.
func (i *Index) reportEntries() {
	if i.metrics == nil {
		return
	}

	i.reportMu.Lock()
	defer i.reportMu.Unlock()
	counts := i.EntryCountsByModel()
	for model := range i.reportedModels {
		if _, ok := counts[model]; !ok {
			i.metrics.SetIndexEntries(model, 0)
			delete(i.reportedModels, model)
		}
	}
	for model, n := range counts {
		i.metrics.SetIndexEntries(model, n)
		i.reportedModels[model] = struct{}{}
	}
}

// sanitizeRate clamps non-finite values (NaN, ±Inf) to 0. Engine adapters can
// produce these (e.g. hit_rate = hits/(hits+misses) with 0 total). encoding/json
// rejects them, so letting them into the index would later break /snapshot.
func sanitizeRate(f float32) float32 {
	x := float64(f)
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return 0
	}
	return f
}

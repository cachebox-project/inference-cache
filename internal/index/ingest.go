// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package index

// normalizeIngestTier applies the default-ingest rule: an entry that arrives
// without a tier is tagged T1 (the local engine KV cache). A producer that
// already classified the hold keeps its tier untouched.
func normalizeIngestTier(t CacheTier) CacheTier {
	if t == TierUnspecified {
		return TierT1
	}
	return t
}

// Ingest applies a replica update (from ReportCacheState). Updates are
// additive deltas, NOT snapshots: each call adds or refreshes the reported
// prefixes (idempotent on (replica_id, hash_scheme, prefix_hash)). A prefix's
// absence from a later update does NOT remove it — removals arrive as explicit
// CacheEvents (PREFIX_EVICTED / ALL_CLEARED) or expire via TTL. This matches the
// engine KV-event model (e.g. vLLM BlockStored / BlockRemoved) and the soft-state
// guarantee: a stale hint causes a cache miss, never a wrong answer.
func (i *Index) Ingest(u Update) {
	ts := u.Timestamp
	if ts.IsZero() {
		ts = i.now()
	}

	// Resolve the tenant's entry budget before locking: the resolver owns its
	// own lock, and calling it under i.mu would nest the index lock with the
	// policy-store lock on the hot ingest path. ok=false → no quota → unbounded.
	maxEntries, hasQuota := i.tenantQuotaFor(u.Tenant)

	i.mu.Lock()
	// prefix_hash is engine-opaque and only safe within a known hash_scheme; an
	// empty/unspecified scheme would collapse all engines into one domain, so we
	// do not index prefixes without one (fail open). Stats are scheme-independent.
	if u.HashScheme != "" {
		for _, p := range u.Prefixes {
			// Default-ingest rule: an entry with no tier is tagged T1 (the
			// engine KV cache). Resolved once per prefix so the chain's
			// per-block entries and the preserved legacy blob share one tier.
			tier := normalizeIngestTier(p.Tier)
			// Adapter partition for this entry: its own, else the update-level
			// default, else "" (the default partition — pre-adapter behavior).
			// Resolved once per prefix so a chain's per-block entries and the
			// preserved legacy blob all land in the same partition.
			adapter := p.Adapter
			if adapter == "" {
				adapter = u.Adapter
			}
			// Chain form: expand into one per-block entry per hash, keyed by
			// the block hash with cumulative tokenCount so a legacy exact-match
			// against any single block hash still works. The parallel arrays
			// must agree in length AND both be non-empty for the chain path
			// to engage; a chain whose two arrays disagree (including the
			// one-sided cases — hashes set with no counts, or counts set with
			// no hashes) is dropped silently (fail-soft — a stale hint is OK,
			// a wrong one isn't) and is NOT downgraded to the legacy single-
			// blob path. Only an entry that sets neither chain field falls
			// through to the legacy PrefixHash path.
			if len(p.BlockHashes) > 0 || len(p.BlockTokenCounts) > 0 {
				if len(p.BlockHashes) != len(p.BlockTokenCounts) {
					continue
				}
				var cumulative int32
				for j, h := range p.BlockHashes {
					cumulative += p.BlockTokenCounts[j]
					i.upsertReplicaLocked(prefixKey{u.Tenant, u.Model, u.HashScheme, adapter, string(h)}, u.ReplicaID, cumulative, tier, ts)
				}
				// Preserve the legacy single-blob key too when the producer
				// set both representations on the same entry — so legacy
				// LookupRoute callers (no chain in the request) still hit
				// via exact-match on PrefixHash. The chain path otherwise
				// silently breaks backward-compat for unmigrated clients.
				if len(p.PrefixHash) > 0 {
					i.upsertReplicaLocked(prefixKey{u.Tenant, u.Model, u.HashScheme, adapter, string(p.PrefixHash)}, u.ReplicaID, p.TokenCount, tier, ts)
				}
				continue
			}
			// Legacy single-blob exact-match entry. The helper does the
			// totalEntries + scope bookkeeping that main's inline form did,
			// so the chain and legacy paths agree on accounting.
			i.upsertReplicaLocked(prefixKey{u.Tenant, u.Model, u.HashScheme, adapter, string(p.PrefixHash)}, u.ReplicaID, p.TokenCount, tier, ts)
		}
	}
	if u.Stats != nil {
		st := *u.Stats
		st.ReplicaID = u.ReplicaID // top-level replica id is authoritative — it is the index key
		// Clamp non-finite rates to 0 so a bad engine stat can't poison /snapshot:
		// encoding/json rejects NaN/±Inf and would 500 the endpoint until the
		// stat expires (TTL), stalling the CacheIndex poller.
		st.HitRate = sanitizeRate(st.HitRate)
		st.Pressure = sanitizeRate(st.Pressure)
		i.stats[statsKey{u.Tenant, u.Model, u.ReplicaID}] = statEntry{
			stats:         st,
			lastSeen:      ts,
			statsReported: ts,
		}
		i.statsScopeAddLocked(modelKey{u.Tenant, u.Model}, u.ReplicaID)
	}
	// Enforce the tenant's maxIndexEntries budget on the freshly-ingested state.
	// Fairness mode: evict only THIS tenant's own oldest distinct prefixes down to
	// budget; other tenants are untouched. Memory budgets are not enforced here
	// (the engine owns KV memory) — distinct-prefix count is the only dimension we
	// control.
	evictedPrefixes := 0
	if hasQuota {
		evictedPrefixes = i.evictOldestForTenantLocked(u.Tenant, maxEntries)
	}
	// enforceCapLocked is a no-op for reserved-tenant writes that don't push
	// the effective total (totalEntries - reservedEntries) over maxEntries —
	// reserved tenants do not fill the cap, so a probe ingest never triggers
	// eviction here, and a concurrent real-workload ingest sees the probe
	// entry as cap-invisible too. See WithReservedTenants.
	capEvicted := i.enforceCapLocked()
	i.mu.Unlock()

	if i.metrics != nil {
		if evictedPrefixes > 0 {
			i.metrics.AddTenantEvictions(u.Tenant, tenantEvictionReasonOverEntries, evictedPrefixes)
		}
		// Cap evictions are tallied per resolved algorithm so dashboards can tell
		// LRU from LFU pressure. Emitted after the lock, mirroring AddTenantEvictions.
		for algo, n := range capEvicted {
			i.metrics.AddIndexEvictions(algo, indexEvictionReasonCap, n)
		}
	}
	i.reportEntries()
}

// ApplyEvent applies a delta from PublishEvent. CacheEvent carries no
// hash_scheme, and prefix_hash is only meaningful within a scheme, so events
// never refresh scheme-specific prefix freshness — that is owned by
// ReportCacheState (authoritative). Events only do scheme-safe work: removals
// (conservative — at worst a cache miss, soft state) and replica liveness.
func (i *Index) ApplyEvent(ev Event) {
	ts := ev.Timestamp
	if ts.IsZero() {
		ts = i.now()
	}
	hash := string(ev.PrefixHash)

	i.mu.Lock()
	switch ev.Type {
	case EventReplicaUpdated:
		// Replica liveness only: keep its stats entry from expiring. Prefix
		// freshness is not touched here (no hash_scheme to target it safely).
		if s, ok := i.stats[statsKey{ev.Tenant, ev.Model, ev.ReplicaID}]; ok {
			s.lastSeen = ts
			i.stats[statsKey{ev.Tenant, ev.Model, ev.ReplicaID}] = s
		}
	case EventPrefixEvicted:
		// Remove the replica from the prefix across schemes — removal is
		// conservative, so matching opaque bytes without a scheme is safe.
		//
		// Adapter, unlike scheme, IS narrowed when the producer supplies it. The
		// fingerprint is token-only, so one prefix hash can be live under several
		// adapters at once on the same replica; dropping every partition for one
		// adapter's GPU eviction would throw away hints that are still valid.
		//
		// An empty ev.Adapter falls back to the original cross-partition sweep.
		// That is exact for a pre-adapter producer (all its entries are in the ""
		// partition anyway), but an adapter-aware producer ALSO emits "" for a
		// genuine base-model eviction — and the sweep then drops live LoRA-
		// partition hints for the same prefix hash too. That is conservative soft
		// state (at worst a cache miss, re-added by the authoritative
		// ReportCacheState path) and a strict non-regression (pre-partition, base
		// and LoRA shared one partition, so a base eviction was already total).
		// Making "" mean the base partition only, without breaking the legacy
		// wildcard, needs explicit adapter_id presence on the event — a tracked
		// follow-up.
		for key, replicas := range i.prefixes {
			if key.tenant != ev.Tenant || key.model != ev.Model || key.prefixHash != hash {
				continue
			}
			if ev.Adapter != "" && key.adapter != ev.Adapter {
				continue
			}
			i.removeReplicaLocked(key, replicas, ev.ReplicaID)
		}
	case EventAllCleared:
		for key, replicas := range i.prefixes {
			if key.tenant != ev.Tenant || key.model != ev.Model {
				continue
			}
			i.removeReplicaLocked(key, replicas, ev.ReplicaID)
		}
		delete(i.stats, statsKey{ev.Tenant, ev.Model, ev.ReplicaID})
		i.statsScopeRemoveLocked(modelKey{ev.Tenant, ev.Model}, ev.ReplicaID)
	}
	// EventPrefixAdded is intentionally a no-op: ReportCacheState is the
	// authoritative add/refresh path, and the event lacks hash_scheme +
	// token_count to create or refresh a scheme-specific entry without risking
	// a cross-scheme false match.
	i.mu.Unlock()

	i.reportEntries()
}

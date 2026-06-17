package engine

import (
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// This file stamps the replica/model/tenant/hash_scheme identity onto the gRPC
// contract messages the subscriber forwards:
//   - CacheStateUpdate (additive ingest via ReportCacheState) wraps the
//     PrefixEntry list produced by positionalIndex.Stored
//   - EvictedEvent / ClearedEvent carry the PREFIX_EVICTED / ALL_CLEARED removals
//     (via PublishEvent)
// Only hashes + counts cross the wire — never tokens or prompt text. The prefix
// keys are our deterministic content fingerprints, derived in positional.go.

// microsFromSeconds converts vLLM's float-seconds timestamp to the contract's
// timestamp_us. A non-positive input yields 0 (server treats 0 as "now").
func microsFromSeconds(s float64) int64 {
	if s <= 0 {
		return 0
	}
	return int64(s * 1e6)
}

// Update stamps the replica/model/tenant/hash_scheme identity onto a set of
// prefixes. Returns nil for an empty prefix set (nothing to report).
func (c Config) Update(tsUs int64, prefixes []*icpb.PrefixEntry) *icpb.CacheStateUpdate {
	if len(prefixes) == 0 {
		return nil
	}
	return &icpb.CacheStateUpdate{
		ReplicaId:   c.ReplicaID,
		ModelId:     c.ModelID,
		TenantId:    c.TenantID,
		HashScheme:  c.HashScheme,
		TimestampUs: tsUs,
		Prefixes:    prefixes,
	}
}

// StatsUpdate stamps the replica/model/tenant/hash_scheme identity onto a
// scraped ReplicaStats and produces a stats-only CacheStateUpdate (empty
// prefixes). The contract treats a CSU as an additive delta, so a stats-only
// update refreshes liveness + the per-replica stats without touching prefixes.
// Returns nil if stats is nil.
//
// The nested stats are rebuilt by-field rather than copied — proto messages
// embed a sync.Mutex via MessageState, so go vet rejects value copies.
func (c Config) StatsUpdate(tsUs int64, stats *icpb.ReplicaStats) *icpb.CacheStateUpdate {
	if stats == nil {
		return nil
	}
	return &icpb.CacheStateUpdate{
		ReplicaId:   c.ReplicaID,
		ModelId:     c.ModelID,
		TenantId:    c.TenantID,
		HashScheme:  c.HashScheme,
		TimestampUs: tsUs,
		Stats: &icpb.ReplicaStats{
			// The top-level replica_id is authoritative server-side; mirror it
			// onto the nested ReplicaStats so wire captures are self-describing.
			ReplicaId:        c.ReplicaID,
			CacheMemoryBytes: stats.GetCacheMemoryBytes(),
			HitRate:          stats.GetHitRate(),
			Pressure:         stats.GetPressure(),
			T2HitTokens:      stats.GetT2HitTokens(),
			T2QueryTokens:    stats.GetT2QueryTokens(),
		},
	}
}

// ClearedEvent builds the ALL_CLEARED CacheEvent for an AllBlocksCleared event.
func (c Config) ClearedEvent(tsSeconds float64) *icpb.CacheEvent {
	return &icpb.CacheEvent{
		Type:        icpb.CacheEvent_ALL_CLEARED,
		ReplicaId:   c.ReplicaID,
		ModelId:     c.ModelID,
		TenantId:    c.TenantID,
		TimestampUs: microsFromSeconds(tsSeconds),
	}
}

// EvictedEvent builds one PREFIX_EVICTED CacheEvent for an already-derived prefix
// hash (our content fingerprint, the index key to drop). The subscriber maps an
// evicted engine block hash to this key via positionalIndex.Removed.
func (c Config) EvictedEvent(prefixHash []byte, tsSeconds float64) *icpb.CacheEvent {
	return &icpb.CacheEvent{
		Type:        icpb.CacheEvent_PREFIX_EVICTED,
		ReplicaId:   c.ReplicaID,
		ModelId:     c.ModelID,
		TenantId:    c.TenantID,
		PrefixHash:  prefixHash,
		TimestampUs: microsFromSeconds(tsSeconds),
	}
}

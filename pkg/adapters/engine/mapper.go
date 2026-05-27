package engine

import (
	"encoding/binary"

	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// This file translates decoded vLLM events into the gRPC contract. The mapping
// follows the index's state model:
//   - BlockStored  -> CacheStateUpdate (additive ingest via ReportCacheState)
//   - BlockRemoved -> PREFIX_EVICTED CacheEvent(s) (removal via PublishEvent)
//   - AllBlocksCleared -> ALL_CLEARED CacheEvent
// Only hashes + counts cross the wire — never tokens or prompt text.

// encodeHash renders a vLLM uint64 block hash as the contract's opaque
// prefix_hash bytes: 8-byte big-endian. Producer and consumer must agree on this
// encoding; big-endian keeps the bytes comparable/stable across processes.
func encodeHash(h uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, h)
	return b
}

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

// StoredPrefixes renders a BlockStored event as PrefixEntry values: one per block
// hash, each covering BlockSize tokens.
func StoredPrefixes(ev BlockStored) []*icpb.PrefixEntry {
	prefixes := make([]*icpb.PrefixEntry, 0, len(ev.BlockHashes))
	for _, h := range ev.BlockHashes {
		prefixes = append(prefixes, &icpb.PrefixEntry{
			PrefixHash: encodeHash(h),
			TokenCount: ev.BlockSize,
		})
	}
	return prefixes
}

// StoredUpdate builds the CacheStateUpdate for a single BlockStored event.
// Returns nil if the event carries no hashes (nothing to report).
func (c Config) StoredUpdate(ev BlockStored, tsSeconds float64) *icpb.CacheStateUpdate {
	return c.Update(microsFromSeconds(tsSeconds), StoredPrefixes(ev))
}

// RemovedEvents builds one PREFIX_EVICTED CacheEvent per removed block hash.
// CacheEvent carries a single prefix_hash, so a BlockRemoved with N hashes maps
// to N events.
func (c Config) RemovedEvents(ev BlockRemoved, tsSeconds float64) []*icpb.CacheEvent {
	if len(ev.BlockHashes) == 0 {
		return nil
	}
	us := microsFromSeconds(tsSeconds)
	out := make([]*icpb.CacheEvent, 0, len(ev.BlockHashes))
	for _, h := range ev.BlockHashes {
		out = append(out, &icpb.CacheEvent{
			Type:        icpb.CacheEvent_PREFIX_EVICTED,
			ReplicaId:   c.ReplicaID,
			ModelId:     c.ModelID,
			TenantId:    c.TenantID,
			PrefixHash:  encodeHash(h),
			TimestampUs: us,
		})
	}
	return out
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

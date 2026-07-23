package engine

import (
	"github.com/cachebox-project/inference-cache/pkg/fingerprint"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// positionalIndex turns the engine's incremental, parent-chained block-store
// events into our own deterministic, positional prefix hashes.
//
// The engine reports a brand-new prefix as one event (parent = its random root)
// but reports a prefix that extends an already-cached one as just the new suffix
// blocks, with parent = the existing prefix's last block hash. To keep our prefix
// hash positional — block i identifies the whole prefix 0..i, matching the
// engine's chained block hashes and the gateway's full-prompt roll — we chain
// across events: a small reverse map remembers, per engine block hash, the rolling
// prefix hash and cumulative token count we derived for it.
//
// This mirrors SMG's PositionalIndexer write path (event_tree.rs apply_stored /
// apply_removed / apply_cleared). One instance per replica; the owning
// Reporter.Run loop is single-goroutine, so no locking is needed.
type positionalIndex struct {
	// engine block hash (opaque bytes, as map key) -> the entry we derived for it.
	blocks map[string]posEntry
}

type posEntry struct {
	prefixHash uint64 // our rolling positional prefix hash
	tokenCount int32  // cumulative tokens of the prefix up to and including this block
}

func newPositionalIndex() *positionalIndex {
	return &positionalIndex{blocks: make(map[string]posEntry)}
}

// Stored derives the positional PrefixEntry list for a BlockStored event and
// records each block in the reverse map. It chains from the parent when that
// parent is known; otherwise it starts a fresh sequence at position 0.
//
// Starting fresh on an unknown parent is correct for the common case — the parent
// is the engine's random root (NONE_HASH), which we never store, so every genuine
// fresh prefix lands here. It is also reached, rarely, when a parent's event was
// dropped or the subscriber restarted mid-stream: that suffix block is then keyed
// as if it were a root, so if its tokens happen to equal some real root prefix's
// leading tokens it can yield a false PREFIX_MATCH. That is a bounded, soft-state
// cost (a wrong hint degrades to a cache miss, never a wrong answer) and does not
// occur on a clean cold start (no gaps). Hardening — learn NONE_HASH and drop
// true gaps — is tracked as a follow-up. Returns nil when nothing is indexable.
func (p *positionalIndex) Stored(ev BlockStored) []*icpb.PrefixEntry {
	bs := int(ev.BlockSize)
	if bs <= 0 || len(ev.BlockHashes) == 0 {
		return nil
	}

	var parentPrefix uint64
	var parentTokens int32
	hasParent := false
	if ev.ParentBlockHash != nil {
		if pe, ok := p.blocks[string(ev.ParentBlockHash)]; ok {
			parentPrefix, parentTokens, hasParent = pe.prefixHash, pe.tokenCount, true
		}
	}

	// One rolling prefix hash per full block of token_ids. token_ids must cover
	// exactly the event's blocks; a length mismatch is a malformed event — drop it
	// rather than partially index, which would desync our keys from the engine's
	// block identities and corrupt a later removal.
	phs := fingerprint.PrefixHashesFrom(ev.TokenIDs, bs, parentPrefix, hasParent)
	if len(phs) != len(ev.BlockHashes) {
		return nil
	}
	n := len(phs)

	out := make([]*icpb.PrefixEntry, 0, n)
	tokens := parentTokens
	for i := 0; i < n; i++ {
		tokens += int32(bs)
		out = append(out, &icpb.PrefixEntry{
			PrefixHash: fingerprint.Bytes(phs[i]),
			TokenCount: tokens,
			// A stored block is resident in the engine KV cache — tier T1. Stamped
			// explicitly so the wire is self-describing; the server would default
			// an unset tier to T1 anyway, but being explicit keeps a captured
			// CacheStateUpdate unambiguous and symmetric with the T2 downgrade the
			// reporter emits on eviction (see forwarder.go BlockRemoved).
			Tier: icpb.CacheTier_CACHE_TIER_T1,
		})
		p.blocks[string(ev.BlockHashes[i])] = posEntry{prefixHash: phs[i], tokenCount: tokens}
	}
	return out
}

// evictedPrefix is one block the engine evicted, resolved back to the index key
// we derived for it (our content fingerprint) plus its cumulative token count.
// The token count lets the caller re-report the prefix at a colder tier (T2) when
// a paired L2 store still holds it, instead of only being able to delete it.
type evictedPrefix struct {
	prefixHash []byte
	tokenCount int32
}

// Removed maps each evicted engine block hash back to the index entry we derived
// for it (prefix hash + token count) and forgets it. Unknown hashes are skipped.
// The caller either forwards each as a PREFIX_EVICTED (single-tier: the block is
// gone) or re-reports it at T2 (L2 tier present: the block moved tiers, not gone).
func (p *positionalIndex) Removed(ev BlockRemoved) []evictedPrefix {
	if len(ev.BlockHashes) == 0 {
		return nil
	}
	out := make([]evictedPrefix, 0, len(ev.BlockHashes))
	for _, h := range ev.BlockHashes {
		key := string(h)
		if pe, ok := p.blocks[key]; ok {
			out = append(out, evictedPrefix{prefixHash: fingerprint.Bytes(pe.prefixHash), tokenCount: pe.tokenCount})
			delete(p.blocks, key)
		}
	}
	return out
}

// Cleared forgets all remembered blocks (the engine flushed its KV cache).
func (p *positionalIndex) Cleared() {
	p.blocks = make(map[string]posEntry)
}

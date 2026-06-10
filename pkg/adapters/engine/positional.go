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
// parent is known; otherwise it starts a fresh sequence at position 0. A fresh
// sequence is the common case (the parent is the engine's random root, which we
// never store); it can also happen, rarely, if a parent's event was dropped — in
// which case starting fresh is the safe choice, since a wrong-positioned hash
// simply never matches a query rather than mis-routing. Returns nil when there is
// nothing to index.
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

	// One rolling prefix hash per full block of token_ids; align with block_hashes.
	phs := fingerprint.PrefixHashesFrom(ev.TokenIDs, bs, parentPrefix, hasParent)
	n := len(phs)
	if len(ev.BlockHashes) < n {
		n = len(ev.BlockHashes)
	}
	if n == 0 {
		return nil
	}

	out := make([]*icpb.PrefixEntry, 0, n)
	tokens := parentTokens
	for i := 0; i < n; i++ {
		tokens += int32(bs)
		out = append(out, &icpb.PrefixEntry{
			PrefixHash: fingerprint.Bytes(phs[i]),
			TokenCount: tokens,
		})
		p.blocks[string(ev.BlockHashes[i])] = posEntry{prefixHash: phs[i], tokenCount: tokens}
	}
	return out
}

// Removed maps each evicted engine block hash to our prefix hash (the index key to
// drop) and forgets it. Unknown hashes are skipped. The caller turns each returned
// hash into a PREFIX_EVICTED event via Config.EvictedEvent.
func (p *positionalIndex) Removed(ev BlockRemoved) [][]byte {
	if len(ev.BlockHashes) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(ev.BlockHashes))
	for _, h := range ev.BlockHashes {
		key := string(h)
		if pe, ok := p.blocks[key]; ok {
			out = append(out, fingerprint.Bytes(pe.prefixHash))
			delete(p.blocks, key)
		}
	}
	return out
}

// Cleared forgets all remembered blocks (the engine flushed its KV cache).
func (p *positionalIndex) Cleared() {
	p.blocks = make(map[string]posEntry)
}

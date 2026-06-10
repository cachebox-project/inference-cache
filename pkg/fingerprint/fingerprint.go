// Package fingerprint computes deterministic, content-addressed prefix hashes
// from token IDs.
//
// The engine's own KV-block hash is seeded by a per-process random value
// (vLLM's NONE_HASH = os.urandom when PYTHONHASHSEED is unset), so it is not
// reproducible across replicas or by an external consumer. This package instead
// derives the index key from token content alone, so the kvevent subscriber, the
// lookup server, and any gateway all agree on the same key regardless of the
// engine's internal hashing.
//
// The construction matches SMG's event_tree.rs so the two systems are
// interoperable on one index:
//
//	seed              = 1337 (XXH3-64)
//	content_hash(blk) = XXH3_64(seed) over concat(token.to_le_bytes() as u32)
//	prefix_hash[0]    = content_hash[0]
//	prefix_hash[i]    = XXH3_64(seed) over prev.le8 ++ content_hash[i].le8
//
// prefix_hash is positional (it chains the parent), matching the engine's
// chained block hashes: prefix_hash[i] identifies the whole prefix up to and
// including block i.
package fingerprint

import (
	"encoding/binary"

	"github.com/zeebo/xxh3"
)

// Seed is the XXH3-64 seed shared with SMG (event_tree.rs XXH3_SEED). It must not
// change without coordinating every producer and consumer of these hashes.
const Seed uint64 = 1337

// ContentHash returns the position-independent content hash of one block's token
// IDs: XXH3-64(Seed) over the little-endian u32 encoding of each token. XXH3
// streaming and one-shot are equivalent, so this matches SMG's streaming hasher.
func ContentHash(blockTokens []uint32) uint64 {
	buf := make([]byte, len(blockTokens)*4)
	for i, t := range blockTokens {
		binary.LittleEndian.PutUint32(buf[i*4:], t)
	}
	return xxh3.HashSeed(buf, Seed)
}

// nextSeqHash rolls the prefix hash forward by one block: XXH3-64(Seed) over
// prev.le8 ++ content.le8 (matches event_tree.rs:compute_next_seq_hash).
func nextSeqHash(prev, content uint64) uint64 {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:8], prev)
	binary.LittleEndian.PutUint64(b[8:16], content)
	return xxh3.HashSeed(b[:], Seed)
}

// PrefixHashes chunks tokens into full blocks of blockSize and returns the
// rolling positional prefix hash per block, starting a fresh sequence at block 0.
// Partial trailing tokens (fewer than blockSize) are discarded, matching engines
// that cache only full blocks. Returns nil for blockSize <= 0 or no full block.
func PrefixHashes(tokens []uint32, blockSize int) []uint64 {
	return PrefixHashesFrom(tokens, blockSize, 0, false)
}

// PrefixHashesFrom is PrefixHashes with an explicit parent. When hasParent is
// true the first block chains from parentPrefix instead of starting a new
// sequence, keeping hashes globally positional when an event carries only the
// new suffix blocks of a longer, already-cached prefix.
func PrefixHashesFrom(tokens []uint32, blockSize int, parentPrefix uint64, hasParent bool) []uint64 {
	if blockSize <= 0 {
		return nil
	}
	nFull := len(tokens) / blockSize
	if nFull == 0 {
		return nil
	}
	out := make([]uint64, 0, nFull)
	prev := parentPrefix
	have := hasParent
	for i := 0; i < nFull; i++ {
		ch := ContentHash(tokens[i*blockSize : (i+1)*blockSize])
		ph := ch
		if have {
			ph = nextSeqHash(prev, ch)
		}
		out = append(out, ph)
		prev = ph
		have = true
	}
	return out
}

// Bytes serializes a prefix hash as 8-byte big-endian — the opaque key form used
// for PrefixEntry.prefix_hash, matching the int-hash normalization in the event
// decoder (uint64BE). Producers and consumers must agree on this encoding.
func Bytes(h uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, h)
	return b
}

package fingerprint

// Chain turns a flat token-ID slice into the block-hash chain a
// LookupRouteRequest carries for longest-prefix matching: blockHashes[i] is the
// 8-byte big-endian positional prefix hash covering tokens 0..(i+1)*blockSize
// (block i is the i-th full block, so block 0 covers the first blockSize tokens),
// and blockTokenCounts[i] is the per-block token count (always blockSize for a
// full block). It is the query-side mirror of the subscriber's per-block PrefixEntry
// ingest (pkg/adapters/engine/positional.go) — so a lookup built from the same
// tokens the engine cached matches the ingested keys by construction.
//
// Partial trailing tokens (fewer than blockSize) are discarded, matching engines
// that cache only full blocks and the PrefixHashes contract. Returns (nil, nil)
// for blockSize <= 0 or when there is no full block.
func Chain(tokens []uint32, blockSize int) (blockHashes [][]byte, blockTokenCounts []int32) {
	phs := PrefixHashes(tokens, blockSize)
	if len(phs) == 0 {
		return nil, nil
	}
	blockHashes = make([][]byte, len(phs))
	blockTokenCounts = make([]int32, len(phs))
	for i, ph := range phs {
		blockHashes[i] = Bytes(ph)
		blockTokenCounts[i] = int32(blockSize)
	}
	return blockHashes, blockTokenCounts
}

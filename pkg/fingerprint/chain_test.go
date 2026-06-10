package fingerprint

import (
	"bytes"
	"testing"
)

// Chain bundles the positional prefix-hash chain into the wire form a
// LookupRouteRequest carries: block_hashes[i] = Bytes(prefix_hash[i]) and a
// parallel per-block token count (every full block covers blockSize tokens).
// This is the server/gateway query-construction mirror of the subscriber's
// per-block PrefixEntry ingest, so a content-fingerprint lookup matches the
// ingested keys by construction.
func TestChainMatchesPrefixHashes(t *testing.T) {
	cases := []struct {
		name      string
		tokens    []uint32
		blockSize int
		wantLen   int
	}{
		{"seq_1_32_bs16", seq(1, 32), 16, 2},
		{"seq_100_20_bs16", seq(100, 20), 16, 1}, // 4 trailing tokens discarded
		{"seq_0_64_bs16", seq(0, 64), 16, 4},
	}
	for _, c := range cases {
		hashes, counts := Chain(c.tokens, c.blockSize)
		if len(hashes) != c.wantLen || len(counts) != c.wantLen {
			t.Fatalf("%s: got %d hashes / %d counts, want %d each", c.name, len(hashes), len(counts), c.wantLen)
		}
		phs := PrefixHashes(c.tokens, c.blockSize)
		for i := range phs {
			if !bytes.Equal(hashes[i], Bytes(phs[i])) {
				t.Errorf("%s: hashes[%d] = %x, want %x", c.name, i, hashes[i], Bytes(phs[i]))
			}
			if counts[i] != int32(c.blockSize) {
				t.Errorf("%s: counts[%d] = %d, want %d", c.name, i, counts[i], c.blockSize)
			}
		}
	}
}

func TestChainEmptyOnNoFullBlock(t *testing.T) {
	cases := []struct {
		name      string
		tokens    []uint32
		blockSize int
	}{
		{"partial_only", seq(0, 10), 16},
		{"blocksize_zero", seq(0, 32), 0},
		{"no_tokens", nil, 16},
	}
	for _, c := range cases {
		hashes, counts := Chain(c.tokens, c.blockSize)
		if hashes != nil || counts != nil {
			t.Errorf("%s: got (%v, %v), want (nil, nil)", c.name, hashes, counts)
		}
	}
}

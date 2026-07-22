package engine

import (
	"encoding/binary"
	"testing"

	"github.com/cachebox-project/inference-cache/pkg/fingerprint"
)

func engHash(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func beU64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }

func tokSeq(start, n int) []uint32 {
	out := make([]uint32, n)
	for i := range out {
		out[i] = uint32(start + i)
	}
	return out
}

// The core correctness property: chaining the prefix hash across an incremental
// (suffix-only) event must yield exactly the hashes a gateway computes by rolling
// the FULL prompt — otherwise ingest keys and query keys never match.
func TestStoredChainMatchesFullRoll(t *testing.T) {
	const bs = 16
	full := tokSeq(1000, 5*bs) // 5 blocks
	want := fingerprint.PrefixHashes(full, bs)

	p := newPositionalIndex()
	// Event 1: fresh, blocks 0..2 (parent is the engine root, modeled as nil).
	got1 := p.Stored(BlockStored{
		BlockHashes: [][]byte{engHash(10), engHash(11), engHash(12)},
		TokenIDs:    full[:3*bs],
		BlockSize:   bs,
	}, "")
	// Event 2: incremental, blocks 3..4, parent = engine hash of block 2.
	got2 := p.Stored(BlockStored{
		BlockHashes:     [][]byte{engHash(13), engHash(14)},
		ParentBlockHash: engHash(12),
		TokenIDs:        full[3*bs : 5*bs],
		BlockSize:       bs,
	}, "")

	var got []uint64
	for _, e := range got1 {
		got = append(got, beU64(e.PrefixHash))
	}
	for _, e := range got2 {
		got = append(got, beU64(e.PrefixHash))
	}
	if len(got) != len(want) {
		t.Fatalf("got %d hashes, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("prefix[%d]=%d, want %d (cross-event chain != full roll)", i, got[i], want[i])
		}
	}
	// Cumulative token count must be global: block 4 covers all 5 blocks.
	if tc := got2[len(got2)-1].TokenCount; tc != int32(5*bs) {
		t.Fatalf("last token_count=%d, want %d", tc, 5*bs)
	}
}

// A fresh event (no parent) hashes block 0 as content[0].
func TestStoredFreshPrefixZero(t *testing.T) {
	const bs = 16
	toks := tokSeq(1, bs)
	p := newPositionalIndex()
	got := p.Stored(BlockStored{BlockHashes: [][]byte{engHash(1)}, TokenIDs: toks, BlockSize: bs}, "")
	if len(got) != 1 || beU64(got[0].PrefixHash) != fingerprint.ContentHash(toks) {
		t.Fatal("fresh prefix[0] != content[0]")
	}
}

// An incremental event whose parent is unknown (dropped event, or the engine
// root) must start a fresh sequence — never chain from stale state, never panic.
func TestStoredUnknownParentStartsFresh(t *testing.T) {
	const bs = 16
	toks := tokSeq(7, bs)
	p := newPositionalIndex()
	got := p.Stored(BlockStored{
		BlockHashes:     [][]byte{engHash(99)},
		ParentBlockHash: engHash(0xDEAD), // never stored
		TokenIDs:        toks,
		BlockSize:       bs,
	}, "")
	if len(got) != 1 || beU64(got[0].PrefixHash) != fingerprint.ContentHash(toks) {
		t.Fatal("unknown parent did not start a fresh sequence")
	}
}

// Removed maps the engine hash to our prefix hash and forgets it.
func TestRemovedMapsAndForgets(t *testing.T) {
	const bs = 16
	toks := tokSeq(1, bs)
	p := newPositionalIndex()
	stored := p.Stored(BlockStored{BlockHashes: [][]byte{engHash(5)}, TokenIDs: toks, BlockSize: bs}, "")
	our := stored[0].PrefixHash

	rm := p.Removed(BlockRemoved{BlockHashes: [][]byte{engHash(5)}})
	if len(rm) != 1 || beU64(rm[0].PrefixHash) != beU64(our) {
		t.Fatalf("Removed = %v, want our prefix hash %d", rm, beU64(our))
	}
	if rm[0].AdapterID != "" {
		t.Errorf("AdapterID = %q, want \"\" (block stored without an adapter)", rm[0].AdapterID)
	}
	// Re-removing the now-forgotten hash yields nothing.
	if rm2 := p.Removed(BlockRemoved{BlockHashes: [][]byte{engHash(5)}}); len(rm2) != 0 {
		t.Fatalf("re-remove returned %d, want 0", len(rm2))
	}
}

// Cleared forgets everything, so a later reference to a pre-clear parent falls
// back to a fresh sequence.
func TestClearedResets(t *testing.T) {
	const bs = 16
	full := tokSeq(1, 2*bs)
	p := newPositionalIndex()
	p.Stored(BlockStored{BlockHashes: [][]byte{engHash(1), engHash(2)}, TokenIDs: full, BlockSize: bs}, "")
	p.Cleared()
	suffix := tokSeq(500, bs)
	got := p.Stored(BlockStored{
		BlockHashes:     [][]byte{engHash(3)},
		ParentBlockHash: engHash(2), // gone after Cleared
		TokenIDs:        suffix,
		BlockSize:       bs,
	}, "")
	if len(got) != 1 || beU64(got[0].PrefixHash) != fingerprint.ContentHash(suffix) {
		t.Fatal("after Cleared, a stale parent should have forced a fresh sequence")
	}
}

package main

import (
	"bytes"
	"testing"

	"github.com/cachebox-project/inference-cache/pkg/adapters/engine"
	"github.com/cachebox-project/inference-cache/pkg/fingerprint"
)

// decodeStored decodes one payload through the REAL subscriber decoder and
// returns its BlockStored events. If the synthetic encoding drifts from what
// the subscriber decodes, a smoke would assert against a key the subscriber
// never produced (false green) — so every shape change must round-trip here.
func decodeStored(t *testing.T, payload []byte) []engine.BlockStored {
	t.Helper()
	batch, err := engine.DecodeEventBatch(payload)
	if err != nil {
		t.Fatalf("DecodeEventBatch: %v", err)
	}
	out := make([]engine.BlockStored, 0, len(batch.Events))
	for i, ev := range batch.Events {
		bs, ok := ev.(engine.BlockStored)
		if !ok {
			t.Fatalf("event %d = %T, want BlockStored", i, ev)
		}
		out = append(out, bs)
	}
	return out
}

// Single-event form: the decoded token_ids and the fingerprints derived from
// them must equal what the publisher logs/queries for.
func TestBuildBatchPayloadRoundTrips(t *testing.T) {
	const blockSize = 128
	tokens := tokenSeq(0, blockSize*2) // 2 blocks, one event

	payload, err := buildBatchPayload(tokens, blockSize, 1, false)
	if err != nil {
		t.Fatalf("buildBatchPayload: %v", err)
	}
	evs := decodeStored(t, payload)
	if len(evs) != 1 {
		t.Fatalf("got %d events, want 1", len(evs))
	}
	bs := evs[0]

	if bs.BlockSize != blockSize {
		t.Errorf("BlockSize = %d, want %d", bs.BlockSize, blockSize)
	}
	if bs.ParentBlockHash != nil {
		t.Errorf("ParentBlockHash = %x, want nil for a sequence root", bs.ParentBlockHash)
	}
	if len(bs.BlockHashes) != 2 {
		t.Fatalf("got %d block hashes, want 2 (one per block)", len(bs.BlockHashes))
	}
	if len(bs.TokenIDs) != len(tokens) {
		t.Fatalf("decoded %d token_ids, want %d", len(bs.TokenIDs), len(tokens))
	}
	for i := range tokens {
		if bs.TokenIDs[i] != tokens[i] {
			t.Fatalf("token[%d] = %d, want %d", i, bs.TokenIDs[i], tokens[i])
		}
	}

	// The fingerprint derived from the decoded tokens must equal what we log/query.
	want := fingerprint.PrefixHashes(tokens, blockSize)
	got := fingerprint.PrefixHashes(bs.TokenIDs, blockSize)
	if len(got) != len(want) {
		t.Fatalf("got %d prefix hashes, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("prefix_hash[%d] mismatch after round-trip", i)
		}
	}
}

// Multi-event form: blocks split across events inside ONE batch must chain —
// event N+1's parent_block_hash decodes to event N's last block hash, so the
// subscriber's reverse map can resume the rolling prefix hash across events,
// and a replayed message carries the whole chain atomically (parent-first).
func TestBuildBatchPayloadChainsAcrossEvents(t *testing.T) {
	const blockSize = 16
	tokens := tokenSeq(100, blockSize*3) // 3 blocks over 2 events: [b0,b1] then [b2]

	payload, err := buildBatchPayload(tokens, blockSize, 2, false)
	if err != nil {
		t.Fatalf("buildBatchPayload: %v", err)
	}
	evs := decodeStored(t, payload)
	if len(evs) != 2 {
		t.Fatalf("got %d events, want 2", len(evs))
	}
	if got, want := len(evs[0].BlockHashes), 2; got != want {
		t.Fatalf("event 0: %d blocks, want %d", got, want)
	}
	if got, want := len(evs[1].BlockHashes), 1; got != want {
		t.Fatalf("event 1: %d blocks, want %d", got, want)
	}
	if evs[0].ParentBlockHash != nil {
		t.Errorf("event 0 parent = %x, want nil (sequence root)", evs[0].ParentBlockHash)
	}
	last := evs[0].BlockHashes[len(evs[0].BlockHashes)-1]
	if !bytes.Equal(evs[1].ParentBlockHash, last) {
		t.Errorf("event 1 parent = %x, want event 0's last block hash %x", evs[1].ParentBlockHash, last)
	}
	// Token coverage: event 0 carries blocks 0-1, event 1 carries block 2.
	if got, want := len(evs[0].TokenIDs), 2*blockSize; got != want {
		t.Errorf("event 0: %d token_ids, want %d", got, want)
	}
	if got, want := len(evs[1].TokenIDs), blockSize; got != want {
		t.Errorf("event 1: %d token_ids, want %d", got, want)
	}
	if evs[1].TokenIDs[0] != tokens[2*blockSize] {
		t.Errorf("event 1 first token = %d, want %d", evs[1].TokenIDs[0], tokens[2*blockSize])
	}
}

// Omit-tokens form: the payload still carries block hashes and block size but
// no token_ids — the engine-regression shape the subscriber must refuse to
// index (the e2e asserts the warn + nothing-indexed contract on top of this).
func TestBuildBatchPayloadOmitTokenIDs(t *testing.T) {
	const blockSize = 128
	tokens := tokenSeq(0, blockSize)

	payload, err := buildBatchPayload(tokens, blockSize, 1, true)
	if err != nil {
		t.Fatalf("buildBatchPayload: %v", err)
	}
	evs := decodeStored(t, payload)
	if len(evs) != 1 {
		t.Fatalf("got %d events, want 1", len(evs))
	}
	if len(evs[0].BlockHashes) != 1 {
		t.Fatalf("got %d block hashes, want 1", len(evs[0].BlockHashes))
	}
	if len(evs[0].TokenIDs) != 0 {
		t.Fatalf("decoded %d token_ids, want 0 when omitted", len(evs[0].TokenIDs))
	}
}

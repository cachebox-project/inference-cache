package engine

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// encodeVLLMBatch builds a msgpack payload in vLLM's wire format:
// [ts, [ [tag, ...fields], ... ]]. Used to exercise the decoder without a
// running engine.
func encodeVLLMBatch(t *testing.T, ts float64, events ...[]interface{}) []byte {
	t.Helper()
	evs := make([]interface{}, len(events))
	for i, e := range events {
		evs[i] = e
	}
	b, err := msgpack.Marshal([]interface{}{ts, evs})
	if err != nil {
		t.Fatalf("encode fixture: %v", err)
	}
	return b
}

func TestDecodeEventBatch(t *testing.T) {
	// A BlockStored carrying token_ids (which must be dropped), a BlockRemoved,
	// an AllBlocksCleared, and an unknown event type (must be skipped).
	payload := encodeVLLMBatch(t, 1779901681.5,
		[]interface{}{"BlockStored", []uint64{10, 11}, nil, []int64{0, 1, 2, 3}, int32(128), nil},
		[]interface{}{"BlockRemoved", []uint64{4}},
		[]interface{}{"AllBlocksCleared"},
		[]interface{}{"SomeFutureEvent", 42},
	)

	batch, err := DecodeEventBatch(payload)
	if err != nil {
		t.Fatalf("DecodeEventBatch: %v", err)
	}
	if batch.TimestampSeconds != 1779901681.5 {
		t.Errorf("ts = %v, want 1779901681.5", batch.TimestampSeconds)
	}
	if len(batch.Events) != 3 {
		t.Fatalf("got %d events, want 3 (unknown skipped)", len(batch.Events))
	}

	stored, ok := batch.Events[0].(BlockStored)
	if !ok {
		t.Fatalf("event[0] = %T, want BlockStored", batch.Events[0])
	}
	// Integer hashes normalize to 8-byte big-endian opaque bytes.
	if len(stored.BlockHashes) != 2 ||
		binary.BigEndian.Uint64(stored.BlockHashes[0]) != 10 ||
		binary.BigEndian.Uint64(stored.BlockHashes[1]) != 11 {
		t.Errorf("BlockHashes = %v, want big-endian [10 11]", stored.BlockHashes)
	}
	if stored.BlockSize != 128 {
		t.Errorf("BlockSize = %d, want 128", stored.BlockSize)
	}

	if _, ok := batch.Events[1].(BlockRemoved); !ok {
		t.Errorf("event[1] = %T, want BlockRemoved", batch.Events[1])
	}
	if _, ok := batch.Events[2].(AllBlocksCleared); !ok {
		t.Errorf("event[2] = %T, want AllBlocksCleared", batch.Events[2])
	}
}

// vLLM block hashes routinely exceed 2^63; the uint64 must survive intact.
func TestDecodeLargeUint64Hash(t *testing.T) {
	const big = uint64(17927488143086849986) // > math.MaxInt64
	payload := encodeVLLMBatch(t, 1,
		[]interface{}{"BlockStored", []uint64{big}, nil, []int64{}, int32(16), nil},
	)
	batch, err := DecodeEventBatch(payload)
	if err != nil {
		t.Fatalf("DecodeEventBatch: %v", err)
	}
	stored := batch.Events[0].(BlockStored)
	if binary.BigEndian.Uint64(stored.BlockHashes[0]) != big {
		t.Errorf("hash = %d, want %d", binary.BigEndian.Uint64(stored.BlockHashes[0]), big)
	}
}

// vLLM's ExternalBlockHash can also be raw bytes; those must pass through opaque.
func TestDecodeByteHashes(t *testing.T) {
	h0 := []byte{0xde, 0xad, 0xbe, 0xef}
	h1 := []byte{0x01, 0x02}
	payload := encodeVLLMBatch(t, 1,
		[]interface{}{"BlockStored", [][]byte{h0, h1}, nil, []int64{}, int32(16), nil},
		[]interface{}{"BlockRemoved", [][]byte{h0}},
	)
	batch, err := DecodeEventBatch(payload)
	if err != nil {
		t.Fatalf("DecodeEventBatch: %v", err)
	}
	stored := batch.Events[0].(BlockStored)
	if len(stored.BlockHashes) != 2 || !bytes.Equal(stored.BlockHashes[0], h0) || !bytes.Equal(stored.BlockHashes[1], h1) {
		t.Errorf("BlockHashes = %v, want [%x %x] verbatim", stored.BlockHashes, h0, h1)
	}
	removed := batch.Events[1].(BlockRemoved)
	if len(removed.BlockHashes) != 1 || !bytes.Equal(removed.BlockHashes[0], h0) {
		t.Errorf("removed hashes = %v, want [%x]", removed.BlockHashes, h0)
	}
}

// A known tag with a truncated tuple (no block_size) must be rejected, not
// silently indexed with token_count=0.
func TestDecodeBlockStoredMissingBlockSizeIsError(t *testing.T) {
	payload := encodeVLLMBatch(t, 1, []interface{}{"BlockStored", []uint64{1}, nil})
	if _, err := DecodeEventBatch(payload); err == nil {
		t.Error("expected error for truncated BlockStored (missing block_size)")
	}
}

// A nil element in block_hashes must be rejected cleanly (returned as an error
// and skipped by the subscriber), never panic the sidecar.
func TestDecodeNilBlockHashIsError(t *testing.T) {
	payload := encodeVLLMBatch(t, 1,
		[]interface{}{"BlockStored", []interface{}{nil}, nil, []int64{}, int32(16), nil})
	if _, err := DecodeEventBatch(payload); err == nil {
		t.Error("expected error for nil block hash")
	}
}

// A non-positive block_size is malformed; reject it so a bogus PREFIX_MATCH hint
// (token_count <= 0) is never produced.
func TestDecodeBlockStoredNonPositiveBlockSizeIsError(t *testing.T) {
	for _, bs := range []int32{0, -1} {
		payload := encodeVLLMBatch(t, 1,
			[]interface{}{"BlockStored", []uint64{1}, nil, []int64{}, bs, nil})
		if _, err := DecodeEventBatch(payload); err == nil {
			t.Errorf("block_size=%d: expected error", bs)
		}
	}
}

func TestDecodeMalformed(t *testing.T) {
	if _, err := DecodeEventBatch([]byte{0xff, 0x00, 0x01}); err == nil {
		t.Error("expected error decoding garbage payload")
	}
	// Top-level array too short.
	short, _ := msgpack.Marshal([]interface{}{1.0})
	if _, err := DecodeEventBatch(short); err == nil {
		t.Error("expected error for short batch envelope")
	}
}

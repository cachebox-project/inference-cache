package engine

import (
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
	if len(stored.BlockHashes) != 2 || stored.BlockHashes[0] != 10 || stored.BlockHashes[1] != 11 {
		t.Errorf("BlockHashes = %v, want [10 11]", stored.BlockHashes)
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

// vLLM block hashes routinely exceed 2^63; they must survive as uint64.
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
	if stored.BlockHashes[0] != big {
		t.Errorf("hash = %d, want %d", stored.BlockHashes[0], big)
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

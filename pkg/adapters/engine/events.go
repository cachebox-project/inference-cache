package engine

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
)

// This file decodes vLLM's KV-cache event stream. vLLM publishes an EventBatch
// per step as msgpack over ZMQ, using msgspec array-like tagged structs:
//
//	EventBatch  = [ts(float), events([...])]
//	event       = [tag(string), ...fields]   // tag is the struct name
//	  BlockStored      = ["BlockStored", block_hashes, parent_block_hash, token_ids, block_size, lora_id]
//	  BlockRemoved     = ["BlockRemoved", block_hashes]
//	  AllBlocksCleared = ["AllBlocksCleared"]
//
// We decode ONLY the metadata we forward (hashes, block size). token_ids,
// parent_block_hash, and lora_id are intentionally never materialized into our
// types — the contract is metadata-only, never prompt-derived token content.

// Event is one decoded KV-cache event. The concrete types are BlockStored,
// BlockRemoved, and AllBlocksCleared.
type Event interface{ isEvent() }

// BlockStored reports KV blocks that became resident. BlockSize is the number of
// tokens per block; a block covers BlockSize tokens of the prefix.
//
// BlockHashes are opaque prefix-hash bytes. vLLM's ExternalBlockHash is a union
// of bytes and int — integer hashes are normalized to 8-byte big-endian here, so
// downstream only ever sees opaque bytes (matching the contract's prefix_hash).
type BlockStored struct {
	BlockHashes [][]byte
	BlockSize   int32
}

// BlockRemoved reports KV blocks that were evicted. BlockHashes are opaque bytes
// (see BlockStored).
type BlockRemoved struct {
	BlockHashes [][]byte
}

// AllBlocksCleared reports that the engine flushed its entire KV cache.
type AllBlocksCleared struct{}

func (BlockStored) isEvent()      {}
func (BlockRemoved) isEvent()     {}
func (AllBlocksCleared) isEvent() {}

// EventBatch is one decoded vLLM event batch.
type EventBatch struct {
	// TimestampSeconds is the engine's batch timestamp (Unix seconds, float).
	TimestampSeconds float64
	Events           []Event
}

// DecodeEventBatch decodes one msgpack EventBatch payload (the last ZMQ frame).
// Unknown event tags are skipped (forward-compatible); a malformed batch is an
// error so the caller can drop it without corrupting state.
func DecodeEventBatch(payload []byte) (*EventBatch, error) {
	var top []msgpack.RawMessage
	if err := msgpack.Unmarshal(payload, &top); err != nil {
		return nil, fmt.Errorf("decode batch envelope: %w", err)
	}
	if len(top) < 2 {
		return nil, fmt.Errorf("event batch: want [ts, events], got %d elements", len(top))
	}

	var ts float64
	if err := msgpack.Unmarshal(top[0], &ts); err != nil {
		return nil, fmt.Errorf("decode batch ts: %w", err)
	}

	var rawEvents []msgpack.RawMessage
	if err := msgpack.Unmarshal(top[1], &rawEvents); err != nil {
		return nil, fmt.Errorf("decode batch events: %w", err)
	}

	out := &EventBatch{TimestampSeconds: ts, Events: make([]Event, 0, len(rawEvents))}
	for _, re := range rawEvents {
		ev, err := decodeEvent(re)
		if err != nil {
			return nil, err
		}
		if ev != nil {
			out.Events = append(out.Events, ev)
		}
	}
	return out, nil
}

// decodeEvent decodes a single [tag, ...fields] event. Returns (nil, nil) for an
// unknown tag so new vLLM event types don't break older subscribers.
func decodeEvent(raw msgpack.RawMessage) (Event, error) {
	var fields []msgpack.RawMessage
	if err := msgpack.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("decode event tuple: %w", err)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("event tuple is empty")
	}
	var tag string
	if err := msgpack.Unmarshal(fields[0], &tag); err != nil {
		return nil, fmt.Errorf("decode event tag: %w", err)
	}

	switch tag {
	case "BlockStored":
		// [tag, block_hashes, parent_block_hash, token_ids, block_size, lora_id].
		// Require through block_size so we never index a zero token_count from a
		// truncated/malformed tuple.
		if len(fields) < 5 {
			return nil, fmt.Errorf("BlockStored: want >=5 fields, got %d", len(fields))
		}
		hashes, err := decodeHashes(fields[1])
		if err != nil {
			return nil, fmt.Errorf("BlockStored.block_hashes: %w", err)
		}
		var bs int32
		if err := msgpack.Unmarshal(fields[4], &bs); err != nil {
			return nil, fmt.Errorf("BlockStored.block_size: %w", err)
		}
		return BlockStored{BlockHashes: hashes, BlockSize: bs}, nil
	case "BlockRemoved":
		// [tag, block_hashes]
		if len(fields) < 2 {
			return nil, fmt.Errorf("BlockRemoved: want >=2 fields, got %d", len(fields))
		}
		hashes, err := decodeHashes(fields[1])
		if err != nil {
			return nil, fmt.Errorf("BlockRemoved.block_hashes: %w", err)
		}
		return BlockRemoved{BlockHashes: hashes}, nil
	case "AllBlocksCleared":
		return AllBlocksCleared{}, nil
	default:
		return nil, nil // unknown event type — skip
	}
}

// decodeHashes decodes a msgpack array of block hashes into opaque bytes. Each
// element is either binary (used as-is) or an integer (vLLM's int hash variant,
// normalized to 8-byte big-endian).
func decodeHashes(raw msgpack.RawMessage) ([][]byte, error) {
	var elems []msgpack.RawMessage
	if err := msgpack.Unmarshal(raw, &elems); err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(elems))
	for _, e := range elems {
		var v interface{}
		if err := msgpack.Unmarshal(e, &v); err != nil {
			return nil, err
		}
		b, err := hashToBytes(v)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, nil
}

// hashToBytes normalizes one decoded block hash to opaque bytes. Binary/string
// hashes pass through; integer hashes become 8-byte big-endian.
func hashToBytes(v interface{}) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return x, nil
	case string:
		return []byte(x), nil
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return uint64BE(rv.Uint()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return uint64BE(uint64(rv.Int())), nil
	default:
		return nil, fmt.Errorf("unsupported block-hash type %T", v)
	}
}

func uint64BE(u uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, u)
	return b
}

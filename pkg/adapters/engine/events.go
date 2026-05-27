package engine

import (
	"fmt"

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
type BlockStored struct {
	BlockHashes []uint64
	BlockSize   int32
}

// BlockRemoved reports KV blocks that were evicted.
type BlockRemoved struct {
	BlockHashes []uint64
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
		// [tag, block_hashes, parent_block_hash, token_ids, block_size, lora_id]
		ev := BlockStored{}
		if len(fields) > 1 {
			hashes, err := decodeHashes(fields[1])
			if err != nil {
				return nil, fmt.Errorf("BlockStored.block_hashes: %w", err)
			}
			ev.BlockHashes = hashes
		}
		if len(fields) > 4 {
			var bs int32
			if err := msgpack.Unmarshal(fields[4], &bs); err != nil {
				return nil, fmt.Errorf("BlockStored.block_size: %w", err)
			}
			ev.BlockSize = bs
		}
		return ev, nil
	case "BlockRemoved":
		// [tag, block_hashes]
		ev := BlockRemoved{}
		if len(fields) > 1 {
			hashes, err := decodeHashes(fields[1])
			if err != nil {
				return nil, fmt.Errorf("BlockRemoved.block_hashes: %w", err)
			}
			ev.BlockHashes = hashes
		}
		return ev, nil
	case "AllBlocksCleared":
		return AllBlocksCleared{}, nil
	default:
		return nil, nil // unknown event type — skip
	}
}

// decodeHashes decodes a msgpack array of block hashes. vLLM block hashes are
// unsigned 64-bit; we read them as uint64 regardless of msgpack int/uint coding.
func decodeHashes(raw msgpack.RawMessage) ([]uint64, error) {
	var hashes []uint64
	if err := msgpack.Unmarshal(raw, &hashes); err != nil {
		return nil, err
	}
	return hashes, nil
}

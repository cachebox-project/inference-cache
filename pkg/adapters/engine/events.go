package engine

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
)

// This file decodes vLLM's KV-cache event stream. vLLM publishes an EventBatch
// per step as msgpack over ZMQ, using msgspec array-like tagged structs:
//
//	EventBatch  = [ts(float), events([...]), ...]  // trailing fields (e.g. a
//	                                                // data-parallel rank) are ignored
//	event       = [tag(string), ...fields]   // tag is the struct name
//	  BlockStored      = ["BlockStored", block_hashes, parent_block_hash, token_ids, block_size, lora_id]
//	  BlockRemoved     = ["BlockRemoved", block_hashes]
//	  AllBlocksCleared = ["AllBlocksCleared"]
//
// We decode the metadata we forward (hashes, block size) plus token_ids and
// parent_block_hash. The subscriber uses those two LOCALLY, in-pod, to derive its
// own deterministic content fingerprint for the index key (see positional.go) —
// the token IDs are hashed and never leave the pod, so only hashes ever reach the
// server and the control plane stays metadata-only. lora_id IS decoded (see
// BlockStored.LoRAID): the fingerprint is token-only, so two prompts with the
// same tokens under different adapters hash identically and would alias in the
// index — the id becomes the index PARTITION, never part of the hash.

// Event is one decoded KV-cache event. The concrete types are BlockStored,
// BlockRemoved, and AllBlocksCleared.
type Event interface{ isEvent() }

// BlockStored reports KV blocks that became resident. BlockSize is the number of
// tokens per block; a block covers BlockSize tokens of the prefix.
//
// BlockHashes are the engine's opaque per-block hashes (int variants normalized
// to 8-byte big-endian). They serve only as a stable per-block identity for the
// reverse map; the index key itself is our own content fingerprint derived from
// TokenIDs (see positional.go).
//
// ParentBlockHash is the engine hash of the block preceding this event's first
// block — nil for a sequence root or when absent — normalized like BlockHashes.
// It lets the subscriber chain its rolling prefix hash across events.
//
// TokenIDs are the flat token IDs of this event's blocks (BlockSize tokens per
// block, in block order). They are hashed in-pod to derive the fingerprint and
// never leave the pod.
//
// LoRAID is the engine's LoRA adapter id for these blocks — nil when the event
// carries no adapter (msgpack nil, a truncated 5-field tuple, or a base-model
// request). It is the engine's INTERNAL integer id, assigned in adapter load
// order, so Config.AdapterID maps it to the stable adapter identity that becomes
// the index partition; it is never mixed into the content fingerprint.
type BlockStored struct {
	BlockHashes     [][]byte
	ParentBlockHash []byte
	TokenIDs        []uint32
	BlockSize       int32
	LoRAID          *int64
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
		parent, err := decodeParent(fields[2])
		if err != nil {
			return nil, fmt.Errorf("BlockStored.parent_block_hash: %w", err)
		}
		tokenIDs, err := decodeTokenIDs(fields[3])
		if err != nil {
			return nil, fmt.Errorf("BlockStored.token_ids: %w", err)
		}
		var bs int32
		if err := msgpack.Unmarshal(fields[4], &bs); err != nil {
			return nil, fmt.Errorf("BlockStored.block_size: %w", err)
		}
		if bs <= 0 {
			return nil, fmt.Errorf("BlockStored.block_size must be positive, got %d", bs)
		}
		// lora_id is OPTIONAL on the wire: the 5-field tuple (no adapter field at
		// all) and an explicit nil both mean "no adapter", so a truncated tuple is
		// not an error here — only a present-but-undecodable value is.
		var loraID *int64
		if len(fields) >= 6 {
			loraID, err = decodeLoRAID(fields[5])
			if err != nil {
				return nil, fmt.Errorf("BlockStored.lora_id: %w", err)
			}
		}
		return BlockStored{
			BlockHashes:     hashes,
			ParentBlockHash: parent,
			TokenIDs:        tokenIDs,
			BlockSize:       bs,
			LoRAID:          loraID,
		}, nil
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
	if v == nil { // a nil element (msgpack nil) is malformed — reject, don't reflect on it
		return nil, fmt.Errorf("block hash is nil")
	}
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

// decodeParent decodes the optional parent_block_hash. A msgpack nil yields a nil
// parent (sequence root); an int or bytes hash is normalized like a block hash so
// it can be matched against BlockHashes in the reverse map.
func decodeParent(raw msgpack.RawMessage) ([]byte, error) {
	if len(raw) == 0 {
		return nil, nil // a msgpack nil decodes to an empty RawMessage — no parent
	}
	var v interface{}
	if err := msgpack.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	return hashToBytes(v)
}

// decodeLoRAID decodes the optional lora_id. A msgpack nil (or an absent field)
// yields nil — no adapter. Anything present but not an integer is an error
// rather than a silent "no adapter": treating an unrecognized adapter id as the
// base model would put that adapter's blocks in the default partition, where
// they alias against every other adapter's identical token content — the exact
// failure this field exists to prevent. Dropping the batch loses a routing hint
// (soft state, a cache miss at worst); aliasing returns a wrong hint.
func decodeLoRAID(raw msgpack.RawMessage) (*int64, error) {
	if len(raw) == 0 {
		return nil, nil // a msgpack nil decodes to an empty RawMessage — no adapter
	}
	var v interface{}
	if err := msgpack.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		id := rv.Int()
		return &id, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		u := rv.Uint()
		if u > math.MaxInt64 {
			return nil, fmt.Errorf("lora id %d out of int64 range", u)
		}
		id := int64(u)
		return &id, nil
	default:
		return nil, fmt.Errorf("unsupported lora_id type %T", v)
	}
}

// decodeTokenIDs decodes the flat token_ids array to uint32 (vLLM token IDs fit in
// 32 bits). Hashed in-pod for the content fingerprint; never forwarded.
func decodeTokenIDs(raw msgpack.RawMessage) ([]uint32, error) {
	if len(raw) == 0 {
		return nil, nil // a msgpack nil decodes to an empty RawMessage — no tokens
	}
	var ids []int64
	if err := msgpack.Unmarshal(raw, &ids); err != nil {
		return nil, err
	}
	out := make([]uint32, len(ids))
	for i, t := range ids {
		if t < 0 || t > math.MaxUint32 {
			return nil, fmt.Errorf("token id %d out of uint32 range", t)
		}
		out[i] = uint32(t)
	}
	return out, nil
}

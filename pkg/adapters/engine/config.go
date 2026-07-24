package engine

import (
	"fmt"
	"strconv"
	"strings"
)

// Config is the per-engine-replica identity the subscriber stamps onto every
// report. vLLM's KV-cache events carry none of this, so the subscriber must be
// told which replica/model/tenant it watches and which hash scheme the engine
// uses (so the server keeps different engines' hashes in disjoint domains).
type Config struct {
	// ReplicaID identifies the engine replica these events come from. Required;
	// it is the index key the server attributes prefixes/stats to.
	ReplicaID string
	// ModelID is the served model identifier. Required.
	ModelID string
	// TenantID is the tenant namespace. Optional (empty = shared/default).
	TenantID string
	// HashScheme names the engine's prefix-hash domain (e.g. "vllm"). Required
	// and non-empty: an empty scheme fails open server-side (dropped on ingest),
	// so reporting with one would silently lose the data.
	HashScheme string
	// AdapterNames maps the engine's INTERNAL LoRA id (BlockStored.lora_id) to
	// the stable adapter identity used as the index partition — the same string
	// a gateway puts in LookupRouteRequest.adapter_id.
	//
	// The mapping is needed because the engine's id is a load-order integer, not
	// a name: vLLM assigns lora_int_id from the ordered --lora-modules list (and
	// increments it for adapters loaded at runtime), so the SAME integer can mean
	// different adapters on two replicas whose load order differs. The index is
	// shared across replicas, so partitioning on the raw integer would re-create
	// the aliasing this partition exists to prevent — across replicas instead of
	// within one.
	//
	// Only a NIL lora_id (base model / no LoRA) uses the default ("") partition,
	// and that path needs no configuration. A non-nil lora_id with NO mapping is
	// FAIL-CLOSED at ingest — AdapterID reports ok=false and the subscriber drops
	// the event rather than partitioning it under a replica-local "lora:<id>",
	// which could place different adapters in one partition across replicas whose
	// --lora-modules order differs (the very cross-replica alias this partition
	// exists to prevent).
	//
	// So LoRA caching REQUIRES this map: supply it from the same --lora-modules
	// ordering the engine gets, and send the matching adapter_id from the gateway
	// (whatever identity is ingested here MUST match the gateway's query id — the
	// same producer/consumer agreement HashScheme requires). An unmapped adapter
	// is neither aliased nor cached — its prefixes get no hint (a miss, never a
	// wrong replica) until it is mapped. Nil/empty AdapterNames is correct only
	// for base-model / non-LoRA traffic.
	AdapterNames map[int64]string
}

// AdapterID resolves an engine LoRA id to the stable adapter identity that
// partitions the index, and reports whether that id is safe to ingest:
//
//   - a nil id (base model / no adapter) → ("", true): the default partition,
//     the behavior for every non-LoRA deployment;
//   - a mapped id → (name, true): its configured stable identity;
//   - an unmapped non-nil id → ("", false): FAIL CLOSED. The engine's id is a
//     replica-local load-order integer with no globally stable meaning, so
//     indexing it under "lora:<id>" could place different adapters in the same
//     partition on replicas whose --lora-modules order differs — the very
//     cross-replica alias this partition exists to prevent. The caller drops the
//     ingest; the adapter is cached only once --lora-adapter-names maps its id.
func (c Config) AdapterID(loraID *int64) (string, bool) {
	if loraID == nil {
		return "", true
	}
	if name, ok := c.AdapterNames[*loraID]; ok && name != "" {
		return name, true
	}
	return "", false
}

// ParseAdapterNames parses a comma-separated "id=name" list into the
// Config.AdapterNames map (e.g. "1=sql-lora,2=chat-lora"). Empty input yields a
// nil map — no mapping, so every non-nil lora_id is fail-closed (dropped, not
// cached) at ingest until it is mapped. Blank list entries are skipped so a
// trailing comma is tolerated; a malformed pair, a non-integer id, an empty
// name, or a duplicate id is an error, because silently dropping one would leave
// that adapter unmapped — uncached where the operator asked for caching.
func ParseAdapterNames(s string) (map[int64]string, error) {
	var out map[int64]string
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		idStr, name, ok := strings.Cut(pair, "=")
		if !ok {
			return nil, fmt.Errorf("adapter names: %q is not id=name", pair)
		}
		id, err := strconv.ParseInt(strings.TrimSpace(idStr), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("adapter names: %q has a non-integer lora id: %w", pair, err)
		}
		name = strings.TrimSpace(name)
		if name == "" {
			return nil, fmt.Errorf("adapter names: %q has an empty adapter name", pair)
		}
		if _, dup := out[id]; dup {
			return nil, fmt.Errorf("adapter names: lora id %d mapped more than once", id)
		}
		if out == nil {
			out = make(map[int64]string)
		}
		out[id] = name
	}
	return out, nil
}

// Validate checks the required identity fields are set.
func (c Config) Validate() error {
	if c.ReplicaID == "" {
		return fmt.Errorf("engine config: ReplicaID is required")
	}
	if c.ModelID == "" {
		return fmt.Errorf("engine config: ModelID is required")
	}
	if c.HashScheme == "" {
		return fmt.Errorf("engine config: HashScheme is required (an empty scheme is dropped server-side)")
	}
	return nil
}

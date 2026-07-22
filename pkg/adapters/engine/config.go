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
	// Optional. An id with no mapping falls back to "lora:<id>", which is exact
	// within a replica and agrees ACROSS replicas whenever they share the same
	// adapter load order (the homogeneous-Deployment case). Supply the map — from
	// the same --lora-modules ordering the engine gets — for deployments whose
	// replicas can diverge (runtime load/unload, rolling updates), and supply the
	// matching adapter_id from the gateway. Nil/empty is the common single-adapter
	// or LoRA-free deployment: every event has no lora_id and lands in the default
	// ("") partition, i.e. exactly the pre-adapter behavior.
	AdapterNames map[int64]string
}

// AdapterID resolves an engine LoRA id to the stable adapter identity that
// partitions the index. A nil id (base model / no adapter) yields "" — the
// default partition, which is the behavior for every non-LoRA deployment.
// A mapped id yields its configured name; an unmapped one yields "lora:<id>"
// (see AdapterNames for why that fallback is exact per-replica but only
// load-order-stable across replicas).
func (c Config) AdapterID(loraID *int64) string {
	if loraID == nil {
		return ""
	}
	if name, ok := c.AdapterNames[*loraID]; ok && name != "" {
		return name
	}
	return "lora:" + strconv.FormatInt(*loraID, 10)
}

// ParseAdapterNames parses a comma-separated "id=name" list into the
// Config.AdapterNames map (e.g. "1=sql-lora,2=chat-lora"). Empty input yields a
// nil map — no mapping, so every adapter id falls back to "lora:<id>". Blank
// list entries are skipped so a trailing comma is tolerated; a malformed pair,
// a non-integer id, an empty name, or a duplicate id is an error, because
// silently dropping one would put that adapter's blocks in a different partition
// than the gateway looks up and turn every one of its lookups into a miss.
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

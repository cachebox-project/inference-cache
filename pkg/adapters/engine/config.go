package engine

import "fmt"

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

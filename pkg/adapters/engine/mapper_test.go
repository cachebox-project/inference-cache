package engine

import (
	"bytes"
	"testing"

	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

func testConfig() Config {
	return Config{ReplicaID: "vllm-0", ModelID: "llama", TenantID: "tenant-a", HashScheme: "vllm"}
}

func TestEvictedEvent(t *testing.T) {
	e := testConfig().EvictedEvent([]byte{0x01, 0x02}, "", 1.0)
	if e.Type != icpb.CacheEvent_PREFIX_EVICTED {
		t.Errorf("type = %v, want PREFIX_EVICTED", e.Type)
	}
	if e.ReplicaId != "vllm-0" || e.ModelId != "llama" {
		t.Errorf("identity not stamped: %+v", e)
	}
	if !bytes.Equal(e.PrefixHash, []byte{0x01, 0x02}) {
		t.Errorf("prefix hash = %x, want 0102", e.PrefixHash)
	}
	// A base-model eviction carries adapter_id="" WITH presence, so the server
	// drops only the base ("") partition instead of sweeping every adapter.
	if e.AdapterId == nil {
		t.Error("adapter id absent; a base eviction must set it WITH presence to scope removal to the base partition")
	} else if got := e.GetAdapterId(); got != "" {
		t.Errorf("adapter id = %q, want \"\" (base partition)", got)
	}
}

// An adapter-scoped eviction must name its partition, so the server drops the
// prefix only there — the same hash can be live under another adapter.
func TestEvictedEventCarriesAdapter(t *testing.T) {
	e := testConfig().EvictedEvent([]byte{0x01}, "sql-lora", 1.0)
	if e.AdapterId == nil || e.GetAdapterId() != "sql-lora" {
		t.Errorf("adapter id = %v, want sql-lora (present)", e.AdapterId)
	}
}

func TestClearedEvent(t *testing.T) {
	e := testConfig().ClearedEvent(1.0)
	if e.Type != icpb.CacheEvent_ALL_CLEARED {
		t.Errorf("type = %v, want ALL_CLEARED", e.Type)
	}
}

func TestConfigValidate(t *testing.T) {
	cases := map[string]Config{
		"missing replica": {ModelID: "m", HashScheme: "vllm"},
		"missing model":   {ReplicaID: "r", HashScheme: "vllm"},
		"missing scheme":  {ReplicaID: "r", ModelID: "m"},
	}
	for name, c := range cases {
		if err := c.Validate(); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
	if err := testConfig().Validate(); err != nil {
		t.Errorf("valid config rejected: %v", err)
	}
}

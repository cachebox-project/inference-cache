package engine

import (
	"bytes"
	"testing"

	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

func testConfig() Config {
	return Config{ReplicaID: "vllm-0", ModelID: "llama", TenantID: "tenant-a", HashScheme: "vllm"}
}

func TestStoredUpdate(t *testing.T) {
	c := testConfig()
	h0, h1 := []byte{0xaa, 0xbb}, []byte{0xcc, 0xdd}
	u := c.StoredUpdate(BlockStored{BlockHashes: [][]byte{h0, h1}, BlockSize: 128}, 2.5)
	if u == nil {
		t.Fatal("StoredUpdate returned nil")
	}
	if u.ReplicaId != "vllm-0" || u.ModelId != "llama" || u.TenantId != "tenant-a" || u.HashScheme != "vllm" {
		t.Errorf("identity not stamped: %+v", u)
	}
	if u.TimestampUs != 2_500_000 {
		t.Errorf("TimestampUs = %d, want 2500000", u.TimestampUs)
	}
	if len(u.Prefixes) != 2 {
		t.Fatalf("got %d prefixes, want 2", len(u.Prefixes))
	}
	if u.Prefixes[0].TokenCount != 128 {
		t.Errorf("TokenCount = %d, want 128 (block size)", u.Prefixes[0].TokenCount)
	}
	if !bytes.Equal(u.Prefixes[0].PrefixHash, h0) || !bytes.Equal(u.Prefixes[1].PrefixHash, h1) {
		t.Errorf("prefix hashes not passed through opaque: %x %x", u.Prefixes[0].PrefixHash, u.Prefixes[1].PrefixHash)
	}
}

func TestStoredUpdateEmptyIsNil(t *testing.T) {
	if u := testConfig().StoredUpdate(BlockStored{BlockSize: 16}, 1); u != nil {
		t.Errorf("expected nil update for zero hashes, got %+v", u)
	}
}

func TestRemovedEvents(t *testing.T) {
	c := testConfig()
	evs := c.RemovedEvents(BlockRemoved{BlockHashes: [][]byte{{1}, {2}, {3}}}, 1.0)
	if len(evs) != 3 {
		t.Fatalf("got %d events, want 3", len(evs))
	}
	for _, e := range evs {
		if e.Type != icpb.CacheEvent_PREFIX_EVICTED {
			t.Errorf("type = %v, want PREFIX_EVICTED", e.Type)
		}
		if e.ReplicaId != "vllm-0" || e.ModelId != "llama" {
			t.Errorf("identity not stamped: %+v", e)
		}
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

package engine

import (
	"encoding/binary"
	"testing"

	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

func testConfig() Config {
	return Config{ReplicaID: "vllm-0", ModelID: "llama", TenantID: "tenant-a", HashScheme: "vllm"}
}

func TestEncodeHashBigEndian(t *testing.T) {
	got := encodeHash(0x0102030405060708)
	want := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	if len(got) != 8 {
		t.Fatalf("len = %d, want 8", len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("encodeHash = %v, want %v", got, want)
		}
	}
	if binary.BigEndian.Uint64(got) != 0x0102030405060708 {
		t.Errorf("round-trip mismatch")
	}
}

func TestStoredUpdate(t *testing.T) {
	c := testConfig()
	u := c.StoredUpdate(BlockStored{BlockHashes: []uint64{10, 11}, BlockSize: 128}, 2.5)
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
	if binary.BigEndian.Uint64(u.Prefixes[0].PrefixHash) != 10 {
		t.Errorf("PrefixHash[0] decodes to %d, want 10", binary.BigEndian.Uint64(u.Prefixes[0].PrefixHash))
	}
}

func TestStoredUpdateEmptyIsNil(t *testing.T) {
	if u := testConfig().StoredUpdate(BlockStored{BlockSize: 16}, 1); u != nil {
		t.Errorf("expected nil update for zero hashes, got %+v", u)
	}
}

func TestRemovedEvents(t *testing.T) {
	c := testConfig()
	evs := c.RemovedEvents(BlockRemoved{BlockHashes: []uint64{1, 2, 3}}, 1.0)
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

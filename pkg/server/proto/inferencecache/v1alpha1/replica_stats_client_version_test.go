package inferencecachev1alpha1pb_test

// Wire-level backward-compat coverage for the ReplicaStats.client_version
// (field 5) addition: a new client can roundtrip the field; an OLD client (one
// that only knows fields 1-4) emits wire bytes whose decoded form leaves
// ClientVersion at its zero value. This is the safety-net check before the
// follow-up consumer code lands — older subscribers must never get rejected
// or mis-decoded when a newer server starts reading client_version.

import (
	"math"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

func TestReplicaStats_ClientVersion_RoundtripSet(t *testing.T) {
	in := &icpb.ReplicaStats{
		ReplicaId:        "vllm-0",
		CacheMemoryBytes: 1 << 30,
		HitRate:          0.42,
		Pressure:         0.03125,
		ClientVersion:    "lmcache==0.4.2",
	}
	raw, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out icpb.ReplicaStats
	if err := proto.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !proto.Equal(in, &out) {
		t.Fatalf("roundtrip mismatch:\n in  = %+v\n out = %+v", in, &out)
	}
	if out.GetClientVersion() != "lmcache==0.4.2" {
		t.Errorf("client_version after roundtrip = %q, want %q", out.GetClientVersion(), "lmcache==0.4.2")
	}
}

func TestReplicaStats_ClientVersion_RoundtripUnset(t *testing.T) {
	in := &icpb.ReplicaStats{
		ReplicaId:        "vllm-0",
		CacheMemoryBytes: 1 << 30,
		HitRate:          0.42,
		Pressure:         0.03125,
	}
	raw, err := proto.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out icpb.ReplicaStats
	if err := proto.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := out.GetClientVersion(); got != "" {
		t.Errorf("unset client_version after roundtrip = %q, want empty string", got)
	}
	// Empty proto3 string fields are not emitted on the wire — confirm so we
	// know the legacy-shape wire bytes a pre-field-5 client would emit are
	// what this case actually exercises.
	for off := 0; off < len(raw); {
		num, _, n := protowire.ConsumeField(raw[off:])
		if n < 0 {
			t.Fatalf("malformed wire bytes at offset %d: %v", off, protowire.ParseError(n))
		}
		if num == 5 {
			t.Fatalf("unset ClientVersion still serialized field 5 on the wire: % x", raw)
		}
		off += n
	}
}

func TestReplicaStats_ClientVersion_LegacyWireDecodesSafely(t *testing.T) {
	// Construct the wire bytes a pre-field-5 client would emit: fields 1-4
	// only, no field 5. Then decode with the new generated type and assert
	// (a) decode succeeds, (b) the original fields are preserved, and
	// (c) ClientVersion is the empty zero-value (treated as "unknown" by the
	// server-side skew check downstream).
	var raw []byte
	raw = protowire.AppendTag(raw, 1, protowire.BytesType)
	raw = protowire.AppendString(raw, "legacy-replica-0")
	raw = protowire.AppendTag(raw, 2, protowire.VarintType)
	raw = protowire.AppendVarint(raw, 1<<30)
	raw = protowire.AppendTag(raw, 3, protowire.Fixed32Type)
	raw = protowire.AppendFixed32(raw, math.Float32bits(0.5))
	raw = protowire.AppendTag(raw, 4, protowire.Fixed32Type)
	raw = protowire.AppendFixed32(raw, math.Float32bits(0.25))

	var out icpb.ReplicaStats
	if err := proto.Unmarshal(raw, &out); err != nil {
		t.Fatalf("legacy wire failed to decode under new schema: %v", err)
	}
	if got, want := out.GetReplicaId(), "legacy-replica-0"; got != want {
		t.Errorf("ReplicaId = %q, want %q", got, want)
	}
	if got, want := out.GetCacheMemoryBytes(), int64(1<<30); got != want {
		t.Errorf("CacheMemoryBytes = %d, want %d", got, want)
	}
	if got, want := out.GetHitRate(), float32(0.5); got != want {
		t.Errorf("HitRate = %v, want %v", got, want)
	}
	if got, want := out.GetPressure(), float32(0.25); got != want {
		t.Errorf("Pressure = %v, want %v", got, want)
	}
	if got := out.GetClientVersion(); got != "" {
		t.Errorf("ClientVersion from legacy wire = %q, want empty string (unknown)", got)
	}
}

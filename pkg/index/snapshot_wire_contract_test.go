package index

// Frozen wire-shape contract for pkg/index.Snapshot — the JSON the policy
// server publishes at /snapshot and the controller decodes in
// CacheIndexPoller. A silent rename of any JSON tag (e.g.
// json:"replicaId" → json:"replica_id") would still pass a round-trip test
// because the rename applies to both encoder and decoder. This test
// freezes the actual on-wire key names so the rename surface is
// observable in code review: bump THIS list when you bump the contract.

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestSnapshotJSONTagsAreFrozen pins the on-wire JSON tag
// names used by Snapshot, ReplicaSnapshot, and TenantSnapshot. The
// CacheIndex poller in internal/controller depends on these EXACT strings
// when decoding /snapshot — and any older controller deployed alongside a
// newer server (or the other way around) would silently degrade if a tag
// was renamed because the structs would just deserialize zero values for
// the renamed fields and the per-backend status writer would publish
// drained-looking participation. A "compile-time round-trip" check does
// not catch this; the rename has to be visible at the string level.
//
// To update the contract:
//   - Add the new tag(s) to the wantTop, wantReplica, or wantTenant slices.
//   - Make sure the change is intentional and the consumer side
//     (internal/controller/cacheindex_controller.go fetchSnapshot path) is
//     updated in the same commit. Coordinate with anything else that
//     decodes /snapshot.
func TestSnapshotJSONTagsAreFrozen(t *testing.T) {
	// Construct a Snapshot whose every leaf field is non-zero so the
	// json encoder emits all keys (including ones marked omitempty).
	// Tags marked omitempty whose values would be zero are excluded from
	// the frozen list below ON PURPOSE — they're documented optional and
	// their absence on the wire is part of the contract.
	snap := Snapshot{
		Replicas: []ReplicaSnapshot{{
			ReplicaID:        "vllm-0",
			Tenant:           "ns-a",
			CacheMemoryBytes: 100,
			HitRate:          0.5,
			Pressure:         0.25,
			LastUpdate:       time.Unix(1_700_000_000, 0).UTC(),
			PrefixCount:      3,
			LastEventAt:      time.Unix(1_700_000_500, 0).UTC(),
			T2HitTokens:      600,
			T2QueryTokens:    1000,
		}},
		Tenants: []TenantSnapshot{{
			TenantID:     "team-a",
			MemoryUsed:   100,
			IndexEntries: 3,
			HitRate:      0.5,
		}},
		TotalPrefixes: 3,
		HotPrefixes:   0,
	}
	raw, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}

	// Top-level Snapshot keys.
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		t.Fatalf("unmarshal top: %v", err)
	}
	wantTop := []string{"replicas", "tenants", "totalPrefixes", "hotPrefixes"}
	assertExactKeys(t, "Snapshot", top, wantTop)

	// Per-replica keys.
	var replicas []map[string]json.RawMessage
	if err := json.Unmarshal(top["replicas"], &replicas); err != nil {
		t.Fatalf("unmarshal replicas: %v", err)
	}
	if len(replicas) != 1 {
		t.Fatalf("expected one replica row; got %d", len(replicas))
	}
	wantReplica := []string{
		"replicaId", "tenant", "cacheMemoryBytes", "hitRate", "pressure",
		"lastUpdate", "prefixCount", "lastEventAt", "t2HitTokens", "t2QueryTokens",
	}
	assertExactKeys(t, "ReplicaSnapshot", replicas[0], wantReplica)

	// Per-tenant keys.
	var tenants []map[string]json.RawMessage
	if err := json.Unmarshal(top["tenants"], &tenants); err != nil {
		t.Fatalf("unmarshal tenants: %v", err)
	}
	if len(tenants) != 1 {
		t.Fatalf("expected one tenant row; got %d", len(tenants))
	}
	wantTenant := []string{"tenantId", "memoryUsed", "indexEntries", "hitRate"}
	assertExactKeys(t, "TenantSnapshot", tenants[0], wantTenant)
}

// TestSnapshotJSONOptionalTagWireShape pins the actual wire-emission
// behaviour for the two ReplicaSnapshot fields tagged ",omitempty":
//
//   - `Tenant string` — omitempty works on strings, so an empty Tenant
//     omits the key entirely. Older controllers built against the
//     pre-tenant snapshot shape interpreted the key's absence as "no
//     tenant context", and that interpretation must keep working.
//   - `LastEventAt time.Time` — omitempty does NOT actually omit time.Time
//     because Go's encoding/json treats only basic-type zero values and
//     empty containers as omitable; time.Time is a struct. The field
//     consequently emits as `"0001-01-01T00:00:00Z"` when zero. The
//     controller-side consumer uses `IsZero()` (not key-absence) to
//     branch, so this is wire-shape oddity rather than a bug. Pin both
//     the present-key and the sentinel value so a future "fix" (e.g.
//     switching to *time.Time) is a deliberate joint change with the
//     consumer, not a silent break.
//
// To genuinely make LastEventAt absent on zero, the struct would need to
// switch to *time.Time — out of scope for this test sweep (the prompt
// excludes shape changes); flag and re-frame as its own ticket if
// downstream consumers require true absence semantics.
func TestSnapshotJSONOptionalTagWireShape(t *testing.T) {
	snap := Snapshot{
		Replicas: []ReplicaSnapshot{{
			ReplicaID:  "vllm-0",
			LastUpdate: time.Unix(1_700_000_000, 0).UTC(),
			// Tenant left empty; LastEventAt left zero.
		}},
	}
	raw, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(raw)
	if strings.Contains(s, `"tenant"`) {
		t.Fatalf("tenant should be omitted when empty (string omitempty works); got %s", s)
	}
	if !strings.Contains(s, `"lastEventAt":"0001-01-01T00:00:00Z"`) {
		t.Fatalf("lastEventAt is expected to emit the zero-time sentinel because Go's omitempty does not omit time.Time; got %s", s)
	}
}

// assertExactKeys verifies the set of top-level JSON keys in `got` is
// exactly `want`, no more no less. Order doesn't matter; missing or
// extra keys both fail.
func assertExactKeys(t *testing.T, surface string, got map[string]json.RawMessage, want []string) {
	t.Helper()
	gotKeys := make([]string, 0, len(got))
	for k := range got {
		gotKeys = append(gotKeys, k)
	}
	sort.Strings(gotKeys)
	wantSorted := append([]string(nil), want...)
	sort.Strings(wantSorted)
	if !reflect.DeepEqual(gotKeys, wantSorted) {
		t.Fatalf("%s JSON keys = %v, want %v (the frozen wire-shape — update both the struct tag and this list together)",
			surface, gotKeys, wantSorted)
	}
}

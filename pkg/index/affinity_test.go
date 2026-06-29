package index

import (
	"math/rand"
	"sort"
	"testing"
	"time"
)

// TestAffinityHintEmptyReplicaSet returns ok=false when no replicas are
// known for (tenant, model, hash_scheme).
func TestAffinityHintEmptyReplicaSet(t *testing.T) {
	idx := New()
	id, ok := idx.AffinityHint("tenantA", "modelX", "vllm", []byte("seed"))
	if ok || id != "" {
		t.Fatalf("expected ok=false id=\"\", got ok=%v id=%q", ok, id)
	}
}

// TestAffinityHintEmptyHashSchemeReturnsFalse: an empty hash_scheme has
// no engine domain to be stable in; AffinityHint must refuse.
func TestAffinityHintEmptyHashSchemeReturnsFalse(t *testing.T) {
	idx := newIndexWithReplicas(t, "tenantA", "modelX", "vllm", []string{"r-1", "r-2"})
	id, ok := idx.AffinityHint("tenantA", "modelX", "", []byte("seed"))
	if ok || id != "" {
		t.Fatalf("empty hash_scheme: expected ok=false, got ok=%v id=%q", ok, id)
	}
}

// TestAffinityHintEmptySeedReturnsFalse: an empty seed yields ok=false so
// the caller falls through to NO_HINT.
func TestAffinityHintEmptySeedReturnsFalse(t *testing.T) {
	idx := newIndexWithReplicas(t, "tenantA", "modelX", "vllm", []string{"r-1", "r-2"})
	id, ok := idx.AffinityHint("tenantA", "modelX", "vllm", nil)
	if ok || id != "" {
		t.Fatalf("nil seed: expected ok=false id=\"\", got ok=%v id=%q", ok, id)
	}
	id, ok = idx.AffinityHint("tenantA", "modelX", "vllm", []byte{})
	if ok || id != "" {
		t.Fatalf("empty seed: expected ok=false id=\"\", got ok=%v id=%q", ok, id)
	}
}

// TestAffinityHintSchemeDisjoint: AffinityHint must NEVER pick a replica
// that doesn't serve the requested (tenant, model, hash_scheme) engine
// domain. Seed r-vllm under "vllm" only; seed r-sglang under "sglang"
// only. A lookup under "vllm" must return r-vllm; a lookup under
// "sglang" must return r-sglang; a lookup under "missing" must return
// ok=false. This is the regression test for the scheme-blind bug
// (replicasByModel-keyed routing would have routed a vllm request to
// r-sglang half the time).
func TestAffinityHintSchemeDisjoint(t *testing.T) {
	idx := New(WithTTL(time.Hour), WithSweepInterval(time.Hour))
	idx.Ingest(Update{
		Tenant: "tenantA", Model: "modelX", ReplicaID: "r-vllm", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: []byte("p"), TokenCount: 32}},
		Stats:    &ReplicaStats{ReplicaID: "r-vllm"},
	})
	idx.Ingest(Update{
		Tenant: "tenantA", Model: "modelX", ReplicaID: "r-sglang", HashScheme: "sglang",
		Prefixes: []PrefixRef{{PrefixHash: []byte("q"), TokenCount: 32}},
		Stats:    &ReplicaStats{ReplicaID: "r-sglang"},
	})
	seed := []byte("any-seed")

	if id, ok := idx.AffinityHint("tenantA", "modelX", "vllm", seed); !ok || id != "r-vllm" {
		t.Fatalf("vllm lookup: expected ok=true id=r-vllm, got ok=%v id=%q", ok, id)
	}
	if id, ok := idx.AffinityHint("tenantA", "modelX", "sglang", seed); !ok || id != "r-sglang" {
		t.Fatalf("sglang lookup: expected ok=true id=r-sglang, got ok=%v id=%q", ok, id)
	}
	if id, ok := idx.AffinityHint("tenantA", "modelX", "missing", seed); ok || id != "" {
		t.Fatalf("missing-scheme lookup: expected ok=false id=\"\", got ok=%v id=%q", ok, id)
	}
}

// TestAffinityHintStable: identical (tenant, model, hash_scheme, seed)
// always returns the same replicaID across repeated calls.
func TestAffinityHintStable(t *testing.T) {
	idx := newIndexWithReplicas(t, "tenantA", "modelX", "vllm", []string{"r-1", "r-2", "r-3", "r-4"})
	seed := []byte("prompt-fingerprint")
	first, ok := idx.AffinityHint("tenantA", "modelX", "vllm", seed)
	if !ok {
		t.Fatalf("first call: expected ok=true")
	}
	for i := 0; i < 100; i++ {
		got, ok := idx.AffinityHint("tenantA", "modelX", "vllm", seed)
		if !ok || got != first {
			t.Fatalf("call %d: expected ok=true id=%q, got ok=%v id=%q", i, first, ok, got)
		}
	}
}

// TestAffinityHintSingleReplica: with a single replica known, every seed
// hashes to it.
func TestAffinityHintSingleReplica(t *testing.T) {
	idx := newIndexWithReplicas(t, "tenantA", "modelX", "vllm", []string{"r-1"})
	for i := 0; i < 10; i++ {
		seed := []byte{byte(i + 1)} // non-empty
		id, ok := idx.AffinityHint("tenantA", "modelX", "vllm", seed)
		if !ok || id != "r-1" {
			t.Fatalf("seed %d: expected ok=true id=r-1, got ok=%v id=%q", i, ok, id)
		}
	}
}

// TestAffinityHintBalanced: with 1000 deterministic-random seeds across 8
// replicas, no replica gets fewer than 80 or more than 220 (a loose 5σ-ish
// band around the expected 125 — modulo of SHA-256 should not
// pathologically concentrate).
func TestAffinityHintBalanced(t *testing.T) {
	replicas := []string{"r-1", "r-2", "r-3", "r-4", "r-5", "r-6", "r-7", "r-8"}
	idx := newIndexWithReplicas(t, "tenantA", "modelX", "vllm", replicas)

	counts := make(map[string]int, len(replicas))
	rng := rand.New(rand.NewSource(42)) // fixed seed → reproducible
	for i := 0; i < 1000; i++ {
		buf := make([]byte, 32)
		rng.Read(buf)
		id, ok := idx.AffinityHint("tenantA", "modelX", "vllm", buf)
		if !ok {
			t.Fatalf("seed %d: expected ok=true", i)
		}
		counts[id]++
	}
	for _, r := range replicas {
		c := counts[r]
		if c < 80 || c > 220 {
			t.Fatalf("replica %q got %d seeds (expected within [80, 220])", r, c)
		}
	}
}

// TestAffinityHintSortOrderIndependent: insertion order of replicas must
// not change the assignment — only the sorted replicaID list matters.
func TestAffinityHintSortOrderIndependent(t *testing.T) {
	a := newIndexWithReplicas(t, "tenantA", "modelX", "vllm", []string{"r-1", "r-2", "r-3"})
	b := newIndexWithReplicas(t, "tenantA", "modelX", "vllm", []string{"r-3", "r-1", "r-2"})
	seed := []byte("any-fingerprint")
	gotA, _ := a.AffinityHint("tenantA", "modelX", "vllm", seed)
	gotB, _ := b.AffinityHint("tenantA", "modelX", "vllm", seed)
	if gotA != gotB {
		t.Fatalf("sort-order dependence: a=%q b=%q", gotA, gotB)
	}
}

// newIndexWithReplicas builds a fresh Index and primes servingByScope by
// ingesting one PrefixRef per replica under the given hash_scheme — that
// is what registers each replica AS SERVING the engine domain. The
// stats values don't matter; only the replicaID set does. Each replica
// gets a distinct PrefixHash so they all show up in servingByScope.
func newIndexWithReplicas(t *testing.T, tenant, model, hashScheme string, replicaIDs []string) *Index {
	t.Helper()
	idx := New(WithTTL(time.Hour), WithSweepInterval(time.Hour))
	for n, rid := range replicaIDs {
		idx.Ingest(Update{
			Tenant: tenant, Model: model, ReplicaID: rid, HashScheme: hashScheme,
			Prefixes: []PrefixRef{{PrefixHash: []byte{byte(n + 1)}, TokenCount: 32}},
			Stats:    &ReplicaStats{ReplicaID: rid},
		})
	}
	got := idx.replicasForRouting(tenant, model, hashScheme)
	want := make([]string, len(replicaIDs))
	copy(want, replicaIDs)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("setup: expected %d replicas, got %d", len(want), len(got))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("setup: replicasForRouting not sorted as expected: got %v, want %v", got, want)
		}
	}
	return idx
}

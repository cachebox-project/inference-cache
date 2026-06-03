package controller

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/index"
	cacheserver "github.com/cachebox-project/inference-cache/pkg/server"
)

// TestIntegrationCachePolicyEvictionAlgorithm exercises the full
// CachePolicy.spec.eviction loop against a real apiserver: the reconciler
// flattens the CRD enum (lower-cased) into the PolicyStore, and an index wired
// with that store as its EvictionResolver (exactly as pkg/server.New does)
// picks LFU vs LRU victims accordingly when the entry cap is exceeded.
//
// One index per namespace/algorithm, each holding only its own tenant's
// entries, so the global cap targets that tenant deterministically.
func TestIntegrationCachePolicyEvictionAlgorithm(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, _, _ := startEnv(t)
	ctx := context.Background()

	store := cacheserver.NewPolicyStore()
	policySrv := httptest.NewServer(cacheserver.NewPolicyHTTPHandler(store))
	defer policySrv.Close()
	reconciler := &ControlPlaneReconciler{Client: k8s, ServerPolicyURL: policySrv.URL, HTTPClient: policySrv.Client()}
	push := func() {
		t.Helper()
		if _, err := reconciler.Reconcile(ctx, ctrl.Request{}); err != nil {
			t.Fatalf("push: %v", err)
		}
	}

	nsLFU := freshNS(t, k8s)
	nsLRU := freshNS(t, k8s)
	mkPolicy := func(ns string, algo cachev1alpha1.CachePolicyEvictionAlgorithm) {
		t.Helper()
		cp := &cachev1alpha1.CachePolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "policy", Namespace: ns},
			Spec:       cachev1alpha1.CachePolicySpec{Eviction: algo},
		}
		if err := k8s.Create(ctx, cp); err != nil {
			t.Fatalf("create CachePolicy(%s): %v", algo, err)
		}
	}
	mkPolicy(nsLFU, cachev1alpha1.CachePolicyEvictionAlgorithmLFU)
	mkPolicy(nsLRU, cachev1alpha1.CachePolicyEvictionAlgorithmLRU)
	push()

	// The reconciler flattened both CRDs (upper-case enum → lower-case wire form)
	// into the store the index will consult.
	if got := store.Eviction(nsLFU); got != index.EvictionLFU {
		t.Fatalf("Eviction(%s) = %q, want %q", nsLFU, got, index.EvictionLFU)
	}
	if got := store.Eviction(nsLRU); got != index.EvictionLRU {
		t.Fatalf("Eviction(%s) = %q, want %q", nsLRU, got, index.EvictionLRU)
	}

	// Timestamps near now so a 1h TTL keeps every entry fresh (the index's clock
	// is real time across package boundaries); millisecond offsets give distinct,
	// ordered lastSeen values for the tie-break.
	base := time.Now()
	ingest := func(idx *index.Index, tenant, prefix string, offsetMs int) {
		idx.Ingest(index.Update{
			ReplicaID: "r", Model: "m", Tenant: tenant, HashScheme: "vllm",
			Timestamp: base.Add(time.Duration(offsetMs) * time.Millisecond),
			Prefixes:  []index.PrefixRef{{PrefixHash: []byte(prefix), TokenCount: 10}},
		})
	}
	lookupN := func(idx *index.Index, tenant, prefix string, n int) {
		for i := 0; i < n; i++ {
			// LookupRoute + CreditHits models the gRPC handler crediting a
			// delivered hit (the only path that bumps the LFU counter).
			idx.LookupRoute(index.LookupRequest{Tenant: tenant, Model: "m", HashScheme: "vllm", PrefixHash: []byte(prefix)}).CreditHits()
		}
	}
	present := func(idx *index.Index, tenant, prefix string) bool {
		return len(idx.Lookup(index.LookupRequest{Tenant: tenant, Model: "m", HashScheme: "vllm", PrefixHash: []byte(prefix)})) > 0
	}

	// --- LFU namespace: lowest access count is evicted; old-but-hot survives. ---
	idxLFU := index.New(index.WithTTL(time.Hour), index.WithMaxEntries(3), index.WithEvictionResolver(store))
	ingest(idxLFU, nsLFU, "hot", 0)  // oldest
	ingest(idxLFU, nsLFU, "warm", 1) //
	ingest(idxLFU, nsLFU, "cold", 2) // newest of the initial three
	lookupN(idxLFU, nsLFU, "hot", 5) // LFU namespace → these bump the counter
	lookupN(idxLFU, nsLFU, "warm", 2)
	ingest(idxLFU, nsLFU, "new", 3) // 4th entry → over the cap of 3

	if present(idxLFU, nsLFU, "cold") {
		t.Fatalf("LFU: cold (lowest count, oldest of the zero-count entries) should be evicted")
	}
	if !present(idxLFU, nsLFU, "hot") {
		t.Fatalf("LFU: hot is the OLDEST entry but the most-used; it must survive (proves LFU, not LRU)")
	}
	for _, p := range []string{"warm", "new"} {
		if !present(idxLFU, nsLFU, p) {
			t.Fatalf("LFU: %q should survive cap eviction", p)
		}
	}

	// --- LRU namespace: oldest is evicted regardless of how often it was used. ---
	idxLRU := index.New(index.WithTTL(time.Hour), index.WithMaxEntries(3), index.WithEvictionResolver(store))
	ingest(idxLRU, nsLRU, "A", 0) // oldest
	ingest(idxLRU, nsLRU, "B", 1)
	ingest(idxLRU, nsLRU, "C", 2)
	lookupN(idxLRU, nsLRU, "A", 5) // LRU namespace → lookups must NOT protect A
	ingest(idxLRU, nsLRU, "D", 3)  // over the cap of 3

	if present(idxLRU, nsLRU, "A") {
		t.Fatalf("LRU: A is the oldest entry and must be evicted even though it was looked up most")
	}
	if !present(idxLRU, nsLRU, "D") {
		t.Fatalf("LRU: the newest entry D must survive")
	}
	for _, p := range []string{"B", "C"} {
		if !present(idxLRU, nsLRU, p) {
			t.Fatalf("LRU: %q should survive cap eviction", p)
		}
	}
}

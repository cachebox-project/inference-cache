package server

import (
	"context"
	"testing"
	"time"

	"github.com/cachebox-project/inference-cache/pkg/index"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// lfuCreditService builds a service whose index runs LFU for tenant "t" with a
// 1-entry cap, so a cap eviction reveals which entries the handler credited.
func lfuCreditService(t *testing.T) *inferenceCacheService {
	t.Helper()
	policies := NewPolicyStore()
	policies.Replace([]ResolvedPolicy{{Namespace: "t", Eviction: "lfu", LookupTimeoutMs: 5}})
	idx := index.New(
		index.WithTTL(time.Hour),
		index.WithMaxEntries(1),
		index.WithEvictionResolver(policies),
	)
	return newInferenceCacheService(idx, newServerMetrics(), policies)
}

func presentInIndex(idx *index.Index, prefix string) bool {
	// idx.Lookup is the prefix-match-only path (no TENANT_HOT fallback), so a
	// non-empty result means the entry survived eviction.
	return len(idx.Lookup(index.LookupRequest{Tenant: "t", Model: "m", HashScheme: "vllm", PrefixHash: []byte(prefix)})) > 0
}

// TestLookupRouteCreditsDeliveredLFUHitOverHandler proves the gRPC handler wires
// LookupResult.CreditHits — a delivered prefix-match hit credits the entry's LFU
// counter, so the credited (but OLDER) entry survives a cap eviction that pure
// LRU would have dropped. If CreditHits were removed from buildLookupResponse,
// both entries would be count 0 and the older one (A) would be evicted instead —
// flipping this assertion. This is the regression guard the index-level test
// (which calls CreditHits directly) cannot provide.
func TestLookupRouteCreditsDeliveredLFUHitOverHandler(t *testing.T) {
	svc := lfuCreditService(t)
	base := time.Now()
	// A is the older entry; under pure LRU it would be the first cap victim.
	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm", Timestamp: base,
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("A"), TokenCount: 32}},
	})

	// Credit A through the REAL handler path (buildLookupResponse -> CreditHits).
	for i := 0; i < 3; i++ {
		resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
			ModelId: "m", TenantId: "t", HashScheme: "vllm", PrefixHash: []byte("A"),
		})
		if err != nil {
			t.Fatalf("LookupRoute(A): %v", err)
		}
		if resp.GetReasonCode() != "PREFIX_MATCH" {
			t.Fatalf("LookupRoute(A) reason = %q, want PREFIX_MATCH", resp.GetReasonCode())
		}
	}

	// Ingesting B (newer) puts the index over the 1-entry cap, triggering eviction.
	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm", Timestamp: base.Add(time.Millisecond),
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("B"), TokenCount: 32}},
	})

	if !presentInIndex(svc.index, "A") {
		t.Fatalf("A was credited via the handler (count 3) but got evicted — CreditHits not wired into the delivered path?")
	}
	if presentInIndex(svc.index, "B") {
		t.Fatalf("B (count 0) should have been the LFU cap victim, but survived")
	}
}

// TestLookupRouteTimeoutDoesNotCreditLFU proves a LookupRoute that the handler
// discards as TIMEOUT credits nothing: the timed-out entry stays count 0 and is
// evicted exactly as if it had never been looked up. Guards the "deliver-only"
// half of the design — buildLookupResponse (which credits) must not run on the
// TIMEOUT path.
func TestLookupRouteTimeoutDoesNotCreditLFU(t *testing.T) {
	svc := lfuCreditService(t) // policy carries LookupTimeoutMs: 5
	base := time.Now()
	// A is the older entry again.
	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm", Timestamp: base,
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("A"), TokenCount: 32}},
	})

	// Wrap the real lookup so it collects hits (no bump) but overruns the 5ms
	// budget, forcing the handler down the TIMEOUT path where CreditHits never runs.
	real := svc.index.LookupRoute
	svc.lookupFn = func(r index.LookupRequest) index.LookupResult {
		res := real(r)
		time.Sleep(50 * time.Millisecond)
		return res
	}
	for i := 0; i < 3; i++ {
		resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
			ModelId: "m", TenantId: "t", HashScheme: "vllm", PrefixHash: []byte("A"),
		})
		if err != nil {
			t.Fatalf("LookupRoute(A): %v", err)
		}
		if resp.GetReasonCode() != "TIMEOUT" {
			t.Fatalf("LookupRoute(A) reason = %q, want TIMEOUT", resp.GetReasonCode())
		}
	}

	// B (newer) triggers the cap eviction. A was NOT credited (timed out), so both
	// are count 0 → the oldest (A) is evicted. If a timed-out lookup had wrongly
	// credited A, A (count 3) would survive and B would be evicted instead.
	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm", Timestamp: base.Add(time.Millisecond),
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("B"), TokenCount: 32}},
	})

	if presentInIndex(svc.index, "A") {
		t.Fatalf("A survived, so a TIMEOUT'd lookup credited it — CreditHits leaked onto the timeout path")
	}
	if !presentInIndex(svc.index, "B") {
		t.Fatalf("B (newer, uncredited) should have survived once A was evicted")
	}
}

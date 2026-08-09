// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"sync"
	"testing"
	"time"
)

func TestEvictExpiredRemovesStaleEntries(t *testing.T) {
	clk := &fakeClock{t: time.Unix(2_000_000, 0)}
	idx := New(withClock(clk.now), WithTTL(10*time.Minute))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}}})

	clk.add(11 * time.Minute) // past TTL
	idx.evictExpired()

	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p")}); len(got) != 0 {
		t.Fatalf("stale entry should be evicted, got %d scores", len(got))
	}
	if n := idx.EntryCountsByModel()["m"]; n != 0 {
		t.Fatalf("entry count after eviction = %d, want 0", n)
	}
}

func TestMaxEntriesCapEvictsOldest(t *testing.T) {
	clk := &fakeClock{t: time.Unix(3_000_000, 0)}
	idx := New(withClock(clk.now), WithTTL(time.Hour), WithMaxEntries(2))

	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("old"), TokenCount: 1}}})
	clk.add(time.Minute)
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("mid"), TokenCount: 1}}})
	clk.add(time.Minute)
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("new"), TokenCount: 1}}}) // exceeds cap of 2

	if total := idx.EntryCountsByModel()["m"]; total != 2 {
		t.Fatalf("expected cap to hold total at 2, got %d", total)
	}
	// Oldest ("old") should be gone; "new" present.
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("old")}); len(got) != 0 {
		t.Fatalf("oldest entry should have been evicted by the cap")
	}
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("new")}); len(got) != 1 {
		t.Fatalf("newest entry should be retained under the cap")
	}
}

func TestApplyEventEvictAndClear(t *testing.T) {
	idx := New()
	ingest := func(replica, h string) {
		idx.Ingest(Update{ReplicaID: replica, Model: "m", Tenant: "t", HashScheme: "vllm",
			Prefixes: []PrefixRef{{PrefixHash: hash(h), TokenCount: 10}}})
	}
	ingest("r1", "p1")
	ingest("r1", "p2")
	ingest("r2", "p1")

	// PREFIX_EVICTED for r1/p1 removes only that replica from that prefix.
	idx.ApplyEvent(Event{Type: EventPrefixEvicted, ReplicaID: "r1", Model: "m", Tenant: "t", PrefixHash: hash("p1")})
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p1")}); len(got) != 1 || got[0].ReplicaID != "r2" {
		t.Fatalf("after evict, p1 should only have r2; got %+v", got)
	}

	// ALL_CLEARED for r1 drops the remainder of r1's entries.
	idx.ApplyEvent(Event{Type: EventAllCleared, ReplicaID: "r1", Model: "m", Tenant: "t"})
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p2")}); len(got) != 0 {
		t.Fatalf("ALL_CLEARED should remove r1/p2; got %+v", got)
	}
}

func TestPerTenantTTLDrivesFreshnessAndEviction(t *testing.T) {
	clk := &fakeClock{t: time.Unix(4_000_000, 0)}
	// Global TTL is long; tenant-short overrides to 5m, tenant-long uses default.
	resolver := staticTTL{"tenant-short": 5 * time.Minute}
	idx := New(
		withClock(clk.now),
		WithTTL(time.Hour),
		WithTTLResolver(resolver),
	)

	for _, tenant := range []string{"tenant-short", "tenant-long"} {
		idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: tenant, HashScheme: "vllm",
			Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}}})
	}

	// Advance 10m: tenant-short's TTL (5m) has elapsed; tenant-long's (1h) has not.
	clk.add(10 * time.Minute)

	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "tenant-short", HashScheme: "vllm", PrefixHash: hash("p")}); len(got) != 0 {
		t.Fatalf("tenant-short entry should be stale under 5m TTL, got %+v", got)
	}
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "tenant-long", HashScheme: "vllm", PrefixHash: hash("p")}); len(got) != 1 {
		t.Fatalf("tenant-long entry should still be fresh under 1h TTL, got %+v", got)
	}

	// Eviction sweep removes only tenant-short; tenant-long survives.
	idx.evictExpired()
	if n := idx.EntryCountsByModel()["m"]; n != 1 {
		t.Fatalf("after sweep, only tenant-long should remain (count = %d, want 1)", n)
	}
}

func TestNilTTLResolverFallsBackToGlobalTTL(t *testing.T) {
	clk := &fakeClock{t: time.Unix(4_500_000, 0)}
	idx := New(withClock(clk.now), WithTTL(time.Hour), WithTTLResolver(staticTTL{}))

	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "anything", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 10}}})

	clk.add(30 * time.Minute) // half the global TTL → still fresh
	if got := idx.Lookup(LookupRequest{Model: "m", Tenant: "anything", HashScheme: "vllm", PrefixHash: hash("p")}); len(got) != 1 {
		t.Fatalf("resolver returning 0 should fall back to global TTL (entry should be fresh), got %+v", got)
	}
}

// TestConcurrentTTLResolverMutation hammers Lookup while a writer flips the
// per-tenant TTL — the race detector catches a missing lock in the resolver
// path.
func TestConcurrentTTLResolverMutation(t *testing.T) {
	r := &dynamicTTL{v: time.Hour}
	idx := New(WithTTL(time.Hour), WithTTLResolver(r))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 1}}})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p")})
				}
			}
		}()
	}

	for i := 0; i < 200; i++ {
		if i%2 == 0 {
			r.set(time.Minute)
		} else {
			r.set(time.Hour)
		}
	}
	close(stop)
	wg.Wait()
}

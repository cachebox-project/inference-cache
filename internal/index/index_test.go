// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually-advanced time source for deterministic freshness/TTL tests.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time      { return c.t }
func (c *fakeClock) add(d time.Duration) { c.t = c.t.Add(d) }

func hash(s string) []byte { return []byte(s) }

func TestReadyReflectsStartAndStop(t *testing.T) {
	idx := New(WithSweepInterval(10 * time.Millisecond))
	if idx.Ready() {
		t.Fatal("index should not be ready before Start")
	}
	ctx, cancel := context.WithCancel(context.Background())
	idx.Start(ctx)
	if !idx.Ready() {
		t.Fatal("index should be ready after Start")
	}
	cancel()
	// Ready flips to false once the loop observes cancellation.
	deadline := time.After(time.Second)
	for idx.Ready() {
		select {
		case <-deadline:
			t.Fatal("index still ready well after context cancel")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
}

// countingMetrics records the latest reported entry count per model and the
// running total of tenant evictions per (tenant, reason) and index evictions
// per (algorithm, reason).
type countingMetrics struct {
	mu             sync.Mutex
	last           map[string]int
	evictions      map[string]int // key: tenantID + "|" + reason
	indexEvictions map[string]int // key: algorithm + "|" + reason
}

func (c *countingMetrics) SetIndexEntries(model string, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.last == nil {
		c.last = map[string]int{}
	}
	c.last[model] = n
}

func (c *countingMetrics) AddTenantEvictions(tenantID, reason string, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.evictions == nil {
		c.evictions = map[string]int{}
	}
	c.evictions[tenantID+"|"+reason] += n
}

func (c *countingMetrics) AddIndexEvictions(algorithm, reason string, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.indexEvictions == nil {
		c.indexEvictions = map[string]int{}
	}
	c.indexEvictions[algorithm+"|"+reason] += n
}

// indexEvictionCount returns the recorded index-eviction total for an
// (algorithm, reason) pair.
func (c *countingMetrics) indexEvictionCount(algorithm, reason string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.indexEvictions[algorithm+"|"+reason]
}

func TestNonPositiveDurationsClampToDefaults(t *testing.T) {
	// WithSweepInterval(0) must not panic time.NewTicker(0); both clamp to defaults.
	idx := New(WithTTL(0), WithSweepInterval(0))
	if idx.ttl != DefaultTTL {
		t.Fatalf("ttl = %v, want default %v", idx.ttl, DefaultTTL)
	}
	if idx.sweepInterval != DefaultSweepInterval {
		t.Fatalf("sweepInterval = %v, want default %v", idx.sweepInterval, DefaultSweepInterval)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	idx.Start(ctx) // would panic if sweepInterval were 0
}

// staticTTL is a TTLResolver returning fixed per-tenant TTLs for tests.
type staticTTL map[string]time.Duration

func (s staticTTL) TTL(tenant string) time.Duration { return s[tenant] }

// dynamicTTL exposes a setter so the test can mutate while the index reads.
type dynamicTTL struct {
	mu sync.RWMutex
	v  time.Duration
}

func (d *dynamicTTL) set(v time.Duration) {
	d.mu.Lock()
	d.v = v
	d.mu.Unlock()
}
func (d *dynamicTTL) TTL(string) time.Duration {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.v
}

// chain assembles a parallel (hash, tokenCount) chain for the table-driven
// tests below. Block hashes are opaque bytes so we use short strings.
func chain(blocks ...string) (hashes [][]byte, counts []int32) {
	hashes = make([][]byte, len(blocks))
	counts = make([]int32, len(blocks))
	for i, b := range blocks {
		hashes[i] = []byte(b)
		counts[i] = 16 // uniform per-block token count for the test
	}
	return hashes, counts
}

// TestLegacyExactMatchPathUnchanged locks in the migration-window guarantee:
// legacy single-blob ingest + lookup behavior is unchanged from the B6 path.
func TestLegacyExactMatchPathUnchanged(t *testing.T) {
	idx := New(WithTTL(time.Hour))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 128}}})
	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: hash("p")})
	if len(got) != 1 || got[0].ReplicaID != "r" || got[0].MatchedTokens != 128 {
		t.Fatalf("legacy exact-match path changed: got %+v", got)
	}
}

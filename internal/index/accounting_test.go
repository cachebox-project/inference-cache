// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"
)

func TestMetricsSinkReceivesCounts(t *testing.T) {
	m := &countingMetrics{}
	idx := New(WithMetrics(m))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("a"), TokenCount: 1}, {PrefixHash: hash("b"), TokenCount: 1}}})
	if m.last["m"] != 2 {
		t.Fatalf("metrics sink got %d entries for model m, want 2", m.last["m"])
	}
}

func TestStatsKeyedByTopLevelReplicaID(t *testing.T) {
	idx := New()
	// The nested stats.ReplicaID disagrees with the authoritative top-level one;
	// CacheState must report the top-level id (the key), not the nested value.
	idx.Ingest(Update{ReplicaID: "real-replica", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 1}},
		Stats:    &ReplicaStats{ReplicaID: "mismatched", CacheMemoryBytes: 42}})

	replicas, total := idx.CacheState("t", "m")
	if total != 1 {
		t.Fatalf("total prefixes = %d, want 1", total)
	}
	if len(replicas) != 1 || replicas[0].ReplicaID != "real-replica" {
		t.Fatalf("stats should carry the top-level replica id, got %+v", replicas)
	}
	if replicas[0].CacheMemoryBytes != 42 {
		t.Fatalf("stats payload lost: cacheMemoryBytes = %d, want 42", replicas[0].CacheMemoryBytes)
	}
}

func TestConcurrentIngestReportsFinalCount(t *testing.T) {
	m := &countingMetrics{}
	idx := New(WithMetrics(m))

	const n = 50
	var wg sync.WaitGroup
	for k := 0; k < n; k++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
				Prefixes: []PrefixRef{{PrefixHash: []byte(fmt.Sprintf("p%d", k)), TokenCount: 1}}})
		}(k)
	}
	wg.Wait()

	if got := idx.EntryCountsByModel()["m"]; got != n {
		t.Fatalf("index has %d entries, want %d", got, n)
	}
	// After all reporters have run (serialized by reportMu), the gauge must equal
	// the live count — never a stale earlier snapshot.
	if m.last["m"] != n {
		t.Fatalf("reported gauge = %d, want %d (stale report ordering)", m.last["m"], n)
	}
}

func TestMetricsZeroedWhenModelDrains(t *testing.T) {
	clk := &fakeClock{t: time.Unix(4_000_000, 0)}
	m := &countingMetrics{}
	idx := New(withClock(clk.now), WithTTL(10*time.Minute), WithMetrics(m))
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("a"), TokenCount: 1}}})
	if m.last["m"] != 1 {
		t.Fatalf("expected 1 entry reported, got %d", m.last["m"])
	}

	clk.add(11 * time.Minute) // expire everything
	idx.evictExpired()

	// The drained model's gauge must be reset to 0, not left stale at 1.
	if m.last["m"] != 0 {
		t.Fatalf("drained model gauge = %d, want 0", m.last["m"])
	}
}

func TestIngestSanitizesNonFiniteStats(t *testing.T) {
	idx := New()
	// NaN / +Inf / -Inf would later make /snapshot's JSON encode fail
	// (and 500 the endpoint) — Ingest must clamp them to 0 at the boundary.
	idx.Ingest(Update{
		ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 1}},
		Stats: &ReplicaStats{
			HitRate:  float32(math.NaN()),
			Pressure: float32(math.Inf(1)),
		},
	})
	replicas, _ := idx.CacheState("t", "m")
	if len(replicas) != 1 {
		t.Fatalf("expected 1 replica, got %d", len(replicas))
	}
	r := replicas[0]
	if x := float64(r.HitRate); math.IsNaN(x) || math.IsInf(x, 0) {
		t.Fatalf("HitRate not sanitized: %v", r.HitRate)
	}
	if x := float64(r.Pressure); math.IsNaN(x) || math.IsInf(x, 0) {
		t.Fatalf("Pressure not sanitized: %v", r.Pressure)
	}
	// The whole snapshot must JSON-encode cleanly — that's the failure mode
	// this guards: encoding/json rejects non-finite floats.
	if _, err := json.Marshal(idx.Snapshot()); err != nil {
		t.Fatalf("snapshot encode after sanitization: %v", err)
	}
}

func TestIngestSanitizesNegativeInfinity(t *testing.T) {
	idx := New()
	idx.Ingest(Update{
		ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Stats: &ReplicaStats{HitRate: float32(math.Inf(-1))},
	})
	replicas, _ := idx.CacheState("t", "m")
	if len(replicas) != 1 || replicas[0].HitRate != 0 {
		t.Fatalf("-Inf HitRate should be clamped to 0, got %+v", replicas)
	}
}

// TestTenantHotRequiresRecentStats pins the recency cutoff: a warm replica
// whose stats are older than TenantHotMaxAge does NOT trigger the fallback —
// the index would otherwise hint based on stale information.
func TestTenantHotRequiresRecentStats(t *testing.T) {
	clk := &fakeClock{t: time.Unix(10_000_000, 0)}
	cfg := DefaultRankerConfig()
	cfg.TenantHotMaxAge = 5 * time.Minute
	idx := New(withClock(clk.now), WithTTL(time.Hour), WithRanker(cfg))

	// Ingest a prefix entry so the engine-domain guard is satisfied; the
	// test is about stats recency, not the domain check.
	idx.Ingest(Update{ReplicaID: "stale-warm", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("other"), TokenCount: 1}},
		Stats:    &ReplicaStats{HitRate: 0.9}})

	// Advance past TenantHotMaxAge — the stats are now "old" for fallback
	// purposes (even though they're still inside the global TTL).
	clk.add(10 * time.Minute)

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone {
		t.Fatalf("stale stats should NOT trigger TENANT_HOT, got %v (%+v)", res.Strategy, res.Scores)
	}
}

// TestLookupIgnoresStaleStatsPressurePenalty guards a symmetric freshness
// rule for the prefix-match path: a stats entry that has aged past the
// index TTL (but not yet swept) must NOT demote a freshly refreshed prefix
// score. Otherwise a high-pressure reading from minutes ago could zero a
// replica that's actually idle right now, just because the sweeper hasn't
// run yet. The fresh-prefix replica below has stale high-pressure stats;
// its score must equal the unpenalized baseline (matched_tokens × freshness).
func TestLookupIgnoresStaleStatsPressurePenalty(t *testing.T) {
	clk := &fakeClock{t: time.Unix(12_000_000, 0)}
	idx := New(withClock(clk.now), WithTTL(10*time.Minute), WithRanker(DefaultRankerConfig()))

	// Ingest stats first: high pressure, will be stale by the time the
	// prefix is refreshed and looked up.
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Stats: &ReplicaStats{Pressure: 0.9}})

	// Advance past the stats freshness window.
	clk.add(15 * time.Minute)

	// Now ingest a fresh prefix entry. The stats are stale at this point.
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 50}}})

	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("p")})
	if len(got) != 1 {
		t.Fatalf("expected 1 score, got %d (%+v)", len(got), got)
	}
	// Stale pressure must NOT be applied. Score should equal the baseline
	// (50 tokens × 1.0 freshness × 1.0 pressure factor × 1.0 SLO bias) = 50.
	if got[0].Score != 50 {
		t.Fatalf("stale pressure leaked into score: got %v, want 50 (no penalty)", got[0].Score)
	}
}

// TestTenantHotIgnoresStatsOnlyReplicas guards the same engine-domain check
// for a more subtle case: an update that carries stats but NO prefix entry
// (regardless of HashScheme) cannot become a TENANT_HOT candidate, because
// the index has no evidence the replica serves any prefix at all.
//
// Stats-only ingest registers no prefix entries → prefix map globally empty
// → cold-start carve-out keeps the response on NO_HINT. The original
// guarantee (stats-only replica never appears in Scores) is preserved.
func TestTenantHotIgnoresStatsOnlyReplicas(t *testing.T) {
	idx := New(WithRanker(DefaultRankerConfig()))
	idx.Ingest(Update{ReplicaID: "stats-only", Model: "m", Tenant: "t", HashScheme: "vllm",
		Stats: &ReplicaStats{HitRate: 0.95}})

	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone {
		t.Fatalf("stats-only update must NOT surface in TENANT_HOT; got %v (%+v)",
			res.Strategy, res.Scores)
	}
	if len(res.Scores) != 0 {
		t.Fatalf("stats-only response must carry no scores; got %+v", res.Scores)
	}
}

// TestReplicaUpdatedEventDoesNotKeepStaleStatsFresh guards a subtle
// interaction between liveness events and the ranker's stats freshness
// check: REPLICA_UPDATED refreshes the index's liveness timestamp without
// supplying new stat values. The ranker uses a separate statsReported
// timestamp so a stale high-pressure / high-hit_rate payload kept "alive"
// by liveness events can't keep demoting prefix scores or qualifying for
// TENANT_HOT indefinitely.
//
// Two assertions in one test, with the same setup, so the bug is easy to
// recognise if either path regresses.
func TestReplicaUpdatedEventDoesNotKeepStaleStatsFresh(t *testing.T) {
	clk := &fakeClock{t: time.Unix(13_000_000, 0)}
	cfg := DefaultRankerConfig()
	cfg.TenantHotMaxAge = 5 * time.Minute
	idx := New(withClock(clk.now), WithTTL(10*time.Minute), WithRanker(cfg))

	// Replica reports an initial state with high pressure and high hit_rate.
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 50}},
		Stats:    &ReplicaStats{Pressure: 0.9, HitRate: 0.9}})

	// Advance past both freshness windows so the stats payload is stale.
	clk.add(20 * time.Minute)

	// Now a stream of REPLICA_UPDATED liveness events keeps refreshing
	// the in-index lastSeen — but NOT the stats payload.
	for k := 0; k < 5; k++ {
		idx.ApplyEvent(Event{Type: EventReplicaUpdated,
			ReplicaID: "r", Model: "m", Tenant: "t"})
		clk.add(time.Minute)
	}

	// Refresh the prefix so the prefix-match path has a candidate to score.
	idx.Ingest(Update{ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []PrefixRef{{PrefixHash: hash("p"), TokenCount: 50}}})

	// Prefix-match path: stale Pressure must NOT be applied. The score
	// should equal the unpenalized baseline (50 × 1.0 × 1.0 × 1.0 = 50).
	got := idx.Lookup(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("p")})
	if len(got) != 1 || got[0].Score != 50 {
		t.Fatalf("liveness-refreshed stale pressure leaked into score: got %+v, want score 50", got)
	}

	// TENANT_HOT path: the stale HitRate must NOT qualify the replica for
	// the fallback either. Look up a novel prefix to force the miss.
	res := idx.LookupRoute(LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm",
		PrefixHash: hash("novel")})
	if res.Strategy != StrategyNone {
		t.Fatalf("liveness-refreshed stale hit_rate leaked into TENANT_HOT: got %v (%+v)",
			res.Strategy, res.Scores)
	}
}

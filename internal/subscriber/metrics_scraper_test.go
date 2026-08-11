// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package subscriber

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fixtureServer returns the bytes for /metrics from a sequence of testdata
// files. Each request advances to the next file; once exhausted, the last file
// is returned indefinitely.
func fixtureServer(t *testing.T, files ...string) *httptest.Server {
	t.Helper()
	if len(files) == 0 {
		t.Fatal("fixtureServer: at least one file required")
	}
	var idx atomic.Int32
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := int(idx.Load())
		if i >= len(files) {
			i = len(files) - 1
		} else {
			idx.Add(1)
		}
		body, err := os.ReadFile(filepath.Join("testdata", files[i]))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write(body)
	}))
}

func TestScraperFirstTickReturnsZeroHitRate(t *testing.T) {
	srv := fixtureServer(t, "vllm_metrics_cpu.txt")
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL:                   srv.URL,
		Tier:                  CacheTierAuto,
		CacheSizeBytes:        1 << 30, // 1 GiB
		MaxConcurrencyCeiling: 256,
	}, nil)

	stats, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	if stats.GetHitRate() != 0 {
		t.Errorf("first-tick hit_rate = %v, want 0 (no delta available)", stats.GetHitRate())
	}
	// vLLM 0.21: unified kv_cache_usage_perc = 0.42 → bytes = 0.42 × 1 GiB
	cap1GiB := float64(int64(1) << 30)
	wantBytes := int64(cap1GiB * 0.42)
	if got := stats.GetCacheMemoryBytes(); got < wantBytes-1 || got > wantBytes+1 {
		t.Errorf("cacheMemoryBytes = %d, want ~%d", got, wantBytes)
	}
	// pressure = (3 + 5) / 256 = 0.03125
	if got, want := stats.GetPressure(), float32(8.0/256.0); got < want-1e-4 || got > want+1e-4 {
		t.Errorf("pressure = %v, want %v", got, want)
	}
}

func TestScraperSecondTickHasHitRateDelta(t *testing.T) {
	srv := fixtureServer(t, "vllm_metrics_cpu.txt", "vllm_metrics_cpu_tick2.txt")
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL:                   srv.URL,
		Tier:                  CacheTierAuto,
		CacheSizeBytes:        1 << 30,
		MaxConcurrencyCeiling: 256,
	}, nil)

	// Prime prev_*.
	if _, err := s.Scrape(context.Background()); err != nil {
		t.Fatalf("scrape 1: %v", err)
	}
	stats, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape 2: %v", err)
	}
	// dHits = 95 - 25 = 70; dQueries = 200 - 100 = 100 → 0.7
	if got, want := stats.GetHitRate(), float32(0.7); got < want-1e-4 || got > want+1e-4 {
		t.Errorf("delta hit_rate = %v, want %v", got, want)
	}
}

func TestScraperReadsT2ExternalCounters(t *testing.T) {
	srv := fixtureServer(t, "vllm_metrics_t2.txt")
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{URL: srv.URL, ModelLabel: "m"}, nil)
	stats, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	// External counters are forwarded cumulatively (no delta) so the first
	// scrape already carries them — the server derives presence from them.
	if got := stats.GetT2HitTokens(); got != 750 {
		t.Errorf("t2_hit_tokens = %d, want 750", got)
	}
	if got := stats.GetT2QueryTokens(); got != 1000 {
		t.Errorf("t2_query_tokens = %d, want 1000", got)
	}
}

func TestScraperT2CountersAbsentAreZero(t *testing.T) {
	// An engine with no external offload tier exposes no external_prefix_cache_*
	// metrics; the scraper reports 0/0, which the server reads as "tier-2 not
	// exercised" — never a fabricated 0% hit-rate.
	srv := fixtureServer(t, "vllm_metrics_cpu.txt")
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{URL: srv.URL}, nil)
	stats, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	if stats.GetT2HitTokens() != 0 || stats.GetT2QueryTokens() != 0 {
		t.Errorf("t2 = (%d,%d), want (0,0)", stats.GetT2HitTokens(), stats.GetT2QueryTokens())
	}
}

func TestScraperHandlesCounterReset(t *testing.T) {
	// tick2 then cpu.txt (which has smaller counters) simulates an engine
	// restart that resets the prefix-cache counters to a lower value.
	srv := fixtureServer(t, "vllm_metrics_cpu_tick2.txt", "vllm_metrics_cpu.txt", "vllm_metrics_cpu_tick2.txt")
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Tier: CacheTierAuto,
	}, nil)

	if _, err := s.Scrape(context.Background()); err != nil {
		t.Fatalf("prime: %v", err)
	}
	resetTick, err := s.Scrape(context.Background()) // counters went down
	if err != nil {
		t.Fatalf("reset tick: %v", err)
	}
	if resetTick.GetHitRate() != 0 {
		t.Errorf("reset tick hit_rate = %v, want 0", resetTick.GetHitRate())
	}
	// Next tick rebases against the post-reset baseline and produces a delta.
	next, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("post-reset tick: %v", err)
	}
	if next.GetHitRate() == 0 {
		t.Errorf("post-reset hit_rate = 0, want > 0 (fresh delta)")
	}
}

func TestScraperMissingMetricsDegradeGracefully(t *testing.T) {
	srv := fixtureServer(t, "vllm_metrics_partial.txt")
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Tier: CacheTierAuto, CacheSizeBytes: 1 << 30, MaxConcurrencyCeiling: 256,
	}, nil)

	stats, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	// Both load gauges present (running=0, waiting=0) so the scrape succeeds;
	// usage missing → cache bytes 0; hit/queries missing → hit_rate 0; pressure=0.
	if stats.GetCacheMemoryBytes() != 0 {
		t.Errorf("cacheMemoryBytes = %d, want 0", stats.GetCacheMemoryBytes())
	}
	if stats.GetHitRate() != 0 {
		t.Errorf("hit_rate = %v, want 0", stats.GetHitRate())
	}
	if stats.GetPressure() != 0 {
		t.Errorf("pressure = %v, want 0", stats.GetPressure())
	}
}

func TestScraperUnrecognizedMetricsFailLoud(t *testing.T) {
	// An SGLang endpoint exposes sglang:* names — none recognized by
	// this vLLM-oriented scraper. A 200 OK with zero recognized metric families
	// must fail loud (error) so the StatsReporter marks the load signal stale,
	// rather than emit a fabricated all-zero ReplicaStats that gets marked
	// delivered and silently disables load-aware routing. Contrast
	// TestScraperMissingMetricsDegradeGracefully, where a vLLM family IS present.
	srv := fixtureServer(t, "sglang_metrics.txt")
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Tier: CacheTierAuto, CacheSizeBytes: 1 << 30, MaxConcurrencyCeiling: 256,
	}, nil)

	if stats, err := s.Scrape(context.Background()); err == nil {
		t.Fatalf("want error on unrecognized (sglang:*) metrics, got nil (stats=%+v)", stats)
	}
}

func TestScraperSGLangProfileExtractsNativeMetrics(t *testing.T) {
	// With Scheme:"sglang" the scraper reads sglang:* names and
	// derives hit_rate from the DIRECT 0-1 gauge (sglang:cache_hit_rate), so it is
	// meaningful on the FIRST scrape — unlike vLLM's counter delta, which is 0
	// until primed. KV utilization comes from sglang:token_usage (0-1).
	srv := fixtureServer(t, "sglang_metrics.txt")
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Scheme: "sglang", Tier: CacheTierAuto,
		CacheSizeBytes: 1 << 30, MaxConcurrencyCeiling: 256,
	}, nil)

	stats, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	// pressure = (num_running_reqs 3 + num_queue_reqs 5) / 256
	if got, want := stats.GetPressure(), float32(8.0/256.0); got < want-1e-4 || got > want+1e-4 {
		t.Errorf("pressure = %v, want %v (sglang:num_running_reqs + num_queue_reqs)", got, want)
	}
	// hit_rate read DIRECTLY from sglang:cache_hit_rate = 0.75, on the first scrape.
	if got, want := stats.GetHitRate(), float32(0.75); got < want-1e-4 || got > want+1e-4 {
		t.Errorf("hit_rate = %v, want %v (direct gauge, not counter delta)", got, want)
	}
	// cache bytes = sglang:token_usage 0.63 × 1 GiB.
	cap1GiB := float64(int64(1) << 30)
	wantBytes := int64(cap1GiB * 0.63)
	if got := stats.GetCacheMemoryBytes(); got < wantBytes-2 || got > wantBytes+2 {
		t.Errorf("cacheMemoryBytes = %d, want ~%d (token_usage × capacity)", got, wantBytes)
	}
}

func TestScraperSGLangDataParallelAggregatesLoad(t *testing.T) {
	// A data-parallel SGLang node emits one series per scheduler (dp_rank), plus a
	// priority="" total and a priority="<int>" breakdown that duplicates it. Load
	// must SUM the per-rank totals and EXCLUDE the priority breakdown; usage and
	// hit-rate take the max across ranks. A single-series read would report one
	// scheduler's load and could hint toward an overloaded pod.
	const body = `# TYPE sglang:num_running_reqs gauge
sglang:num_running_reqs{model_name="m",dp_rank="0",priority=""} 3
sglang:num_running_reqs{model_name="m",dp_rank="1",priority=""} 5
sglang:num_running_reqs{model_name="m",dp_rank="0",priority="0"} 3
# TYPE sglang:num_queue_reqs gauge
sglang:num_queue_reqs{model_name="m",dp_rank="0",priority=""} 1
sglang:num_queue_reqs{model_name="m",dp_rank="1",priority=""} 2
# TYPE sglang:token_usage gauge
sglang:token_usage{model_name="m",dp_rank="0"} 0.40
sglang:token_usage{model_name="m",dp_rank="1"} 0.70
# TYPE sglang:cache_hit_rate gauge
sglang:cache_hit_rate{model_name="m",dp_rank="0"} 0.60
sglang:cache_hit_rate{model_name="m",dp_rank="1"} 0.90
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Scheme: "sglang", Tier: CacheTierAuto,
		CacheSizeBytes: 1 << 30, MaxConcurrencyCeiling: 256,
	}, nil)

	stats, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	// running = 3+5 (priority="0" breakdown excluded); waiting = 1+2 → 11/256.
	if got, want := stats.GetPressure(), float32(11.0/256.0); got < want-1e-4 || got > want+1e-4 {
		t.Errorf("pressure = %v, want %v (sum of per-rank totals, priority breakdown excluded)", got, want)
	}
	// hit_rate = max(0.60, 0.90).
	if got, want := stats.GetHitRate(), float32(0.90); got < want-1e-4 || got > want+1e-4 {
		t.Errorf("hit_rate = %v, want %v (max across ranks)", got, want)
	}
	// token_usage = max(0.40, 0.70) → cache bytes.
	cap1GiB := float64(int64(1) << 30)
	wantBytes := int64(cap1GiB * 0.70)
	if got := stats.GetCacheMemoryBytes(); got < wantBytes-2 || got > wantBytes+2 {
		t.Errorf("cacheMemoryBytes = %d, want ~%d (max token_usage × capacity)", got, wantBytes)
	}
}

func TestScraperRejectedScrapeDoesNotPoisonHitRateBaseline(t *testing.T) {
	// The load-gauge validation runs BEFORE hitRate, so a rejected partial scrape
	// (hits/queries present but load gauges absent) must NOT advance the vLLM
	// hit-rate counter baseline — otherwise the next good scrape computes a wrong
	// delta against a baseline no sample was ever delivered for.
	full1 := "# TYPE vllm:prefix_cache_hits_total counter\n" +
		"vllm:prefix_cache_hits_total{model_name=\"m\"} 25\n" +
		"# TYPE vllm:prefix_cache_queries_total counter\n" +
		"vllm:prefix_cache_queries_total{model_name=\"m\"} 100\n" +
		"# TYPE vllm:num_requests_running gauge\nvllm:num_requests_running{model_name=\"m\"} 0\n" +
		"# TYPE vllm:num_requests_waiting gauge\nvllm:num_requests_waiting{model_name=\"m\"} 0\n"
	partial := "# TYPE vllm:prefix_cache_hits_total counter\n" +
		"vllm:prefix_cache_hits_total{model_name=\"m\"} 60\n" +
		"# TYPE vllm:prefix_cache_queries_total counter\n" +
		"vllm:prefix_cache_queries_total{model_name=\"m\"} 120\n" // load gauges absent → rejected
	full2 := "# TYPE vllm:prefix_cache_hits_total counter\n" +
		"vllm:prefix_cache_hits_total{model_name=\"m\"} 95\n" +
		"# TYPE vllm:prefix_cache_queries_total counter\n" +
		"vllm:prefix_cache_queries_total{model_name=\"m\"} 200\n" +
		"# TYPE vllm:num_requests_running gauge\nvllm:num_requests_running{model_name=\"m\"} 0\n" +
		"# TYPE vllm:num_requests_waiting gauge\nvllm:num_requests_waiting{model_name=\"m\"} 0\n"
	bodies := []string{full1, partial, full2}
	var idx atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		i := int(idx.Load())
		if i >= len(bodies) {
			i = len(bodies) - 1
		} else {
			idx.Add(1)
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(bodies[i]))
	}))
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{URL: srv.URL, MaxConcurrencyCeiling: 256}, nil)
	if _, err := s.Scrape(context.Background()); err != nil { // tick1 primes baseline 25/100
		t.Fatalf("tick1: %v", err)
	}
	if _, err := s.Scrape(context.Background()); err == nil { // tick2 rejected (no load gauges)
		t.Fatal("tick2: want error (load gauges absent)")
	}
	stats, err := s.Scrape(context.Background()) // tick3 full again
	if err != nil {
		t.Fatalf("tick3: %v", err)
	}
	// Delta tick1→tick3: (95-25)/(200-100) = 0.70. A baseline poisoned to tick2's
	// 60/120 would give (95-60)/(200-120) = 0.4375.
	if got, want := stats.GetHitRate(), float32(0.70); got < want-1e-3 || got > want+1e-3 {
		t.Errorf("post-reject hit_rate = %v, want %v (rejected tick2 must not advance the baseline)", got, want)
	}
}

func TestScraperSGLangAbsentHitRateReportsZero(t *testing.T) {
	// SGLang load gauges present but cache_hit_rate absent: hit_rate reports 0
	// (conservatively withholding the replica's TENANT_HOT hint) without failing
	// the scrape — the load signal still flows.
	const body = `# TYPE sglang:num_running_reqs gauge
sglang:num_running_reqs{model_name="m"} 2
# TYPE sglang:num_queue_reqs gauge
sglang:num_queue_reqs{model_name="m"} 0
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Scheme: "sglang", Tier: CacheTierAuto, MaxConcurrencyCeiling: 256,
	}, nil)
	stats, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	if stats.GetHitRate() != 0 {
		t.Errorf("hit_rate = %v, want 0 (cache_hit_rate absent)", stats.GetHitRate())
	}
	if got, want := stats.GetPressure(), float32(2.0/256.0); got < want-1e-4 || got > want+1e-4 {
		t.Errorf("pressure = %v, want %v", got, want)
	}
}

func TestScraperUnknownSchemeLogsFallbackWarning(t *testing.T) {
	// An unknown non-empty scheme falls back to the vLLM profile; the fallback
	// must be logged (not silent) so the operator can fix a mistyped scheme.
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	_ = NewMetricsScraper(nil, ScraperConfig{URL: "http://127.0.0.1:9/metrics", Scheme: "sglang-custom"}, logger)
	if !strings.Contains(buf.String(), "unknown --hash-scheme") {
		t.Fatalf("expected a fallback warning for an unknown scheme; log = %q", buf.String())
	}

	buf.Reset()
	_ = NewMetricsScraper(nil, ScraperConfig{URL: "http://127.0.0.1:9/metrics", Scheme: "sglang"}, logger)
	if strings.Contains(buf.String(), "unknown --hash-scheme") {
		t.Fatalf("known scheme must not warn; log = %q", buf.String())
	}
	buf.Reset()
	_ = NewMetricsScraper(nil, ScraperConfig{URL: "http://127.0.0.1:9/metrics"}, logger) // empty scheme
	if strings.Contains(buf.String(), "unknown --hash-scheme") {
		t.Fatalf("empty scheme (vLLM default) must not warn; log = %q", buf.String())
	}
}

func TestScraperNonFiniteLoadGaugeFailsLoud(t *testing.T) {
	// Prometheus permits NaN. A non-finite load gauge is unusable and must not be
	// delivered as pressure — it reads absent, so the scrape fails loud.
	const body = `# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{model_name="m"} NaN
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{model_name="m"} 0
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{URL: srv.URL, MaxConcurrencyCeiling: 256}, nil)
	if stats, err := s.Scrape(context.Background()); err == nil {
		t.Fatalf("want error on NaN load gauge; got nil (stats=%+v)", stats)
	}
}

func TestScraperNonFiniteRankSkippedInAggregate(t *testing.T) {
	// A NaN in one dp_rank series is skipped; the finite ranks still aggregate,
	// so a single bad scheduler reading doesn't drop the whole load signal.
	const body = `# TYPE sglang:num_running_reqs gauge
sglang:num_running_reqs{model_name="m",dp_rank="0"} 4
sglang:num_running_reqs{model_name="m",dp_rank="1"} NaN
# TYPE sglang:num_queue_reqs gauge
sglang:num_queue_reqs{model_name="m",dp_rank="0"} 0
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Scheme: "sglang", Tier: CacheTierAuto, MaxConcurrencyCeiling: 256,
	}, nil)
	stats, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	// running = 4 (NaN rank skipped), waiting = 0 → pressure = 4/256.
	if got, want := stats.GetPressure(), float32(4.0/256.0); got < want-1e-4 || got > want+1e-4 {
		t.Errorf("pressure = %v, want %v (NaN rank excluded, finite rank summed)", got, want)
	}
}

func TestScraperPartialLoadGaugeFailsLoud(t *testing.T) {
	// A scrape that recognizes the engine but is missing a load
	// gauge must fail loud, not emit a fabricated pressure=0. Here an SGLang
	// exposition carries only sglang:cache_hit_rate — no running/queue gauges —
	// so pressure has no basis and the tick must go stale rather than deliver 0.
	const body = `# HELP sglang:cache_hit_rate The prefix cache hit rate.
# TYPE sglang:cache_hit_rate gauge
sglang:cache_hit_rate{model_name="m"} 0.75
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Scheme: "sglang", Tier: CacheTierAuto, MaxConcurrencyCeiling: 256,
	}, nil)

	if stats, err := s.Scrape(context.Background()); err == nil {
		t.Fatalf("want error when load gauges absent (only cache_hit_rate present); got nil (stats=%+v)", stats)
	}
}

func TestScraperModelLabelExcludingLoadGaugesFailsLoud(t *testing.T) {
	// Load gauges present in the exposition but under a different
	// model_name than the filter → after filtering, none match → fail loud rather
	// than report pressure=0 for the configured replica.
	srv := fixtureServer(t, "sglang_metrics.txt") // series carry model_name="m"
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Scheme: "sglang", Tier: CacheTierAuto,
		ModelLabel: "not-m", MaxConcurrencyCeiling: 256,
	}, nil)

	if stats, err := s.Scrape(context.Background()); err == nil {
		t.Fatalf("want error when model_name filter excludes load gauges; got nil (stats=%+v)", stats)
	}
}

func TestScraperSchemeMismatchFailsLoud(t *testing.T) {
	// Mirror of TestScraperUnrecognizedMetricsFailLoud: an sglang-configured
	// scraper pointed at a vLLM endpoint recognizes no sglang:* family and must
	// fail loud, rather than fabricate zeros.
	srv := fixtureServer(t, "vllm_metrics_cpu.txt")
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Scheme: "sglang", Tier: CacheTierAuto, MaxConcurrencyCeiling: 256,
	}, nil)

	if stats, err := s.Scrape(context.Background()); err == nil {
		t.Fatalf("want error: sglang scraper on vLLM metrics, got nil (stats=%+v)", stats)
	}
}

func TestScraperLegacyCPUWithZeroGPU(t *testing.T) {
	// Legacy vLLM exposes both `vllm:gpu_cache_usage_perc` and
	// `vllm:cpu_cache_usage_perc`; the inactive tier reads 0. Auto-tier must
	// pick the non-zero one, not stop at the first present in lookup order.
	srv := fixtureServer(t, "vllm_metrics_legacy_cpu.txt")
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Tier: CacheTierAuto, CacheSizeBytes: 1 << 30, MaxConcurrencyCeiling: 256,
	}, nil)
	stats, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	cap1GiB := float64(int64(1) << 30)
	wantBytes := int64(cap1GiB * 0.42)
	if got := stats.GetCacheMemoryBytes(); got < wantBytes-2 || got > wantBytes+2 {
		t.Errorf("cacheMemoryBytes = %d, want ~%d (active legacy tier is CPU, gpu=0)", got, wantBytes)
	}
}

func TestScraperLegacyGPUFallback(t *testing.T) {
	// Legacy vLLM (pre-0.21) exposes vllm:gpu_cache_usage_perc instead of
	// vllm:kv_cache_usage_perc. Auto-tier must fall through to it.
	srv := fixtureServer(t, "vllm_metrics_gpu.txt")
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Tier: CacheTierAuto, CacheSizeBytes: 1 << 30, MaxConcurrencyCeiling: 32,
	}, nil)
	stats, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	cap1GiB := float64(int64(1) << 30)
	wantBytes := int64(cap1GiB * 0.61)
	if got := stats.GetCacheMemoryBytes(); got < wantBytes-2 || got > wantBytes+2 {
		t.Errorf("cacheMemoryBytes = %d, want ~%d (legacy gpu fallback)", got, wantBytes)
	}
}

func TestScraperExplicitTierPinsLookup(t *testing.T) {
	// kv-only fixture; an explicit --cache-tier=gpu should NOT fall back to kv
	// and therefore reports 0 bytes.
	srv := fixtureServer(t, "vllm_metrics_cpu.txt")
	defer srv.Close()

	gpu := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Tier: CacheTierGPU, CacheSizeBytes: 1 << 30,
	}, nil)
	stats, err := gpu.Scrape(context.Background())
	if err != nil {
		t.Fatalf("gpu scrape: %v", err)
	}
	if stats.GetCacheMemoryBytes() != 0 {
		t.Errorf("explicit GPU tier on kv-only fixture: bytes = %d, want 0", stats.GetCacheMemoryBytes())
	}
}

func TestScraperCountersWithoutTotalSuffix(t *testing.T) {
	// Some prometheus clients expose counters under the unsuffixed family name
	// (no `_total`). The scraper must still find them via the lookup fallback.
	// Two distinct ticks so the hit_rate delta actually proves the counter
	// lookup worked — identical ticks would produce 0 either way.
	srv := fixtureServer(t, "vllm_metrics_openmetrics.txt", "vllm_metrics_openmetrics_tick2.txt")
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{URL: srv.URL, Tier: CacheTierAuto, CacheSizeBytes: 1 << 30, MaxConcurrencyCeiling: 8}, nil)
	if _, err := s.Scrape(context.Background()); err != nil {
		t.Fatalf("prime: %v", err)
	}
	stats, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	// kv_cache_usage_perc = 0.30 → bytes = 0.30 × 1 GiB.
	cap1GiB := float64(int64(1) << 30)
	wantBytes := int64(cap1GiB * 0.30)
	if got := stats.GetCacheMemoryBytes(); got < wantBytes-2 || got > wantBytes+2 {
		t.Errorf("cacheMemoryBytes = %d, want ~%d (unsuffixed counter fixture)", got, wantBytes)
	}
	// pressure = (2+0)/8 = 0.25 — proves gauges were also read.
	if got, want := stats.GetPressure(), float32(0.25); got < want-1e-4 || got > want+1e-4 {
		t.Errorf("pressure = %v, want %v", got, want)
	}
	// Δhits = 70-10 = 60; Δqueries = 120-20 = 100 → 0.6. If sumCounter had
	// failed to find the unsuffixed family this would stay 0.
	if got, want := stats.GetHitRate(), float32(0.6); got < want-1e-4 || got > want+1e-4 {
		t.Errorf("hit_rate = %v, want %v (unsuffixed counter lookup is dark)", got, want)
	}
}

func TestScraperFiltersByModelLabel(t *testing.T) {
	// Two models share one /metrics. The scraper must only read the configured
	// model's series — anything else would pollute /snapshot.replicas[] with
	// another model's load/hit-rate.
	srv := fixtureServer(t, "vllm_metrics_multimodel.txt", "vllm_metrics_multimodel.txt")
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Tier: CacheTierAuto, ModelLabel: "primary",
		CacheSizeBytes: 1 << 30, MaxConcurrencyCeiling: 8,
	}, nil)
	if _, err := s.Scrape(context.Background()); err != nil {
		t.Fatalf("prime: %v", err)
	}
	stats, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	// kv_cache_usage_perc{primary}=0.20 → bytes = 0.20 × 1 GiB
	cap1GiB := float64(int64(1) << 30)
	wantBytes := int64(cap1GiB * 0.20)
	if got := stats.GetCacheMemoryBytes(); got < wantBytes-2 || got > wantBytes+2 {
		t.Errorf("cacheMemoryBytes = %d, want ~%d (other model's 0.95 must NOT leak in)", got, wantBytes)
	}
	// pressure{primary} = (2+0)/8 = 0.25; the other model's (50+30)/8 = 10 →
	// clamp 1.0 must NOT show up.
	if got, want := stats.GetPressure(), float32(0.25); got < want-1e-4 || got > want+1e-4 {
		t.Errorf("pressure = %v, want %v (other model bled through)", got, want)
	}
	// First tick primed prev_{hits=10, queries=40}; second tick is identical
	// (fixtureServer locks on the last file), so dQueries=0 → hit_rate=0.
	// What matters: the filter must not let the OTHER model's huge counters
	// (999/1000) appear in the delta computation.
	if stats.GetHitRate() != 0 {
		t.Errorf("hit_rate = %v, want 0 (identical ticks; if non-zero, other model's counters leaked)", stats.GetHitRate())
	}
}

// TestScraperEmptyModelLabelAggregatesSeriesEvenWhenAliased pins the
// regression that prompted decoupling ScraperConfig.ModelLabel from the cache
// plane's --model-id: when the operator's index key (e.g. "canary") differs
// from the vLLM-side label value (e.g. the served model path), leaving
// ModelLabel empty must aggregate every series and report non-zero stats.
// Previously the subscriber wired --model-id into ModelLabel, so the common
// docs/reference-stack/scripts/canary_e2e.sh setup (MODEL_ID=canary,
// vLLM model_name=Qwen/...) silently dropped every series and emitted zeros.
func TestScraperEmptyModelLabelAggregatesSeriesEvenWhenAliased(t *testing.T) {
	srv := fixtureServer(t, "vllm_metrics_cpu.txt")
	defer srv.Close()
	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Tier: CacheTierAuto, // ModelLabel: "" (operator opt-out)
		CacheSizeBytes: 1 << 30, MaxConcurrencyCeiling: 256,
	}, nil)
	stats, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	if stats.GetCacheMemoryBytes() == 0 {
		t.Error("ModelLabel='' must aggregate; cacheMemoryBytes=0 means the filter rejected every series")
	}
	if stats.GetPressure() == 0 {
		t.Error("ModelLabel='' must aggregate; pressure=0 means the filter rejected the gauges")
	}
}

func TestScraperUnlabeledMetricExcludedWhenFilterSet(t *testing.T) {
	// A series missing the model_name label entirely must NOT be attributed to
	// the configured model. The load gauges ARE labeled (so the scrape clears the
	// both-load-gauges requirement), but the unlabeled usage gauge must be
	// excluded — under-report the cache size rather than misattribute it.
	const body = `# HELP vllm:num_requests_running Number of requests currently running.
# TYPE vllm:num_requests_running gauge
vllm:num_requests_running{model_name="primary"} 0
# HELP vllm:num_requests_waiting Number of requests waiting.
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{model_name="primary"} 0
# HELP vllm:kv_cache_usage_perc KV cache usage.
# TYPE vllm:kv_cache_usage_perc gauge
vllm:kv_cache_usage_perc 0.42
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, ModelLabel: "primary", CacheSizeBytes: 1 << 30,
	}, nil)
	stats, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	if stats.GetCacheMemoryBytes() != 0 {
		t.Errorf("unlabeled series leaked in: cacheMemoryBytes = %d, want 0", stats.GetCacheMemoryBytes())
	}
}

func TestCacheTierIsValid(t *testing.T) {
	for _, ok := range ValidCacheTierNames() {
		if !ok.IsValid() {
			t.Errorf("%q reported invalid", ok)
		}
	}
	for _, bad := range []CacheTier{"", "xpu", "AUTO", "default"} {
		if bad.IsValid() {
			t.Errorf("%q reported valid", bad)
		}
	}
	// ValidCacheTierNames must hand out a fresh slice each call so callers
	// can't clobber the canonical set.
	got := ValidCacheTierNames()
	if len(got) == 0 {
		t.Fatal("ValidCacheTierNames returned empty")
	}
	got[0] = CacheTier("clobber")
	if fresh := ValidCacheTierNames(); fresh[0] == "clobber" {
		t.Error("ValidCacheTierNames returned a shared mutable slice")
	}
}

func TestScraperPartialCounterDoesNotPoisonDelta(t *testing.T) {
	// Tick 1: hits+queries present (good baseline).
	// Tick 2: hits family absent — a transient partial scrape. Hit-rate must
	// be 0 and the baseline must NOT advance to 0; otherwise tick 3 (counters
	// restored at much larger values) would compute against `0`, producing a
	// huge lifetime-ish hit-rate spike.
	srv := fixtureServer(t,
		"vllm_metrics_cpu.txt",       // tick 1: hits=25, queries=100
		"vllm_metrics_partial.txt",   // tick 2: hits/queries absent
		"vllm_metrics_cpu_tick2.txt", // tick 3: hits=95, queries=200
	)
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Tier: CacheTierAuto, MaxConcurrencyCeiling: 256,
	}, nil)
	if _, err := s.Scrape(context.Background()); err != nil { // prime
		t.Fatalf("tick 1: %v", err)
	}
	partial, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("tick 2: %v", err)
	}
	if partial.GetHitRate() != 0 {
		t.Errorf("partial-scrape hit_rate = %v, want 0", partial.GetHitRate())
	}
	restored, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("tick 3: %v", err)
	}
	// Should produce the same delta tick 1→tick 3 produces directly:
	// dHits = 95-25 = 70; dQueries = 200-100 = 100 → 0.7. Anything close to a
	// lifetime ratio (95/200 = 0.475) would mean the baseline was rebased to
	// 0 during tick 2 and the bug Codex flagged is still live.
	if got, want := restored.GetHitRate(), float32(0.7); got < want-1e-3 || got > want+1e-3 {
		t.Errorf("post-partial hit_rate = %v, want %v (baseline poisoned by tick 2 absence)", got, want)
	}
}

func TestScraperZeroCacheSizeEmitsZeroBytes(t *testing.T) {
	srv := fixtureServer(t, "vllm_metrics_cpu.txt")
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Tier: CacheTierAuto, MaxConcurrencyCeiling: 256, // CacheSizeBytes: 0 (unset)
	}, nil)
	stats, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	if stats.GetCacheMemoryBytes() != 0 {
		t.Errorf("unset cache size: bytes = %d, want 0 (honest unknown)", stats.GetCacheMemoryBytes())
	}
	// Other fields should still populate normally.
	if stats.GetPressure() == 0 {
		t.Errorf("pressure should still populate when only cache size is unset")
	}
}

func TestScraperHTTPErrorIsFailSoft(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{URL: srv.URL}, nil)
	stats, err := s.Scrape(context.Background())
	if err == nil {
		t.Fatal("expected scrape error on 500, got nil")
	}
	if stats == nil {
		t.Fatal("expected zero stats on error, got nil")
	}
}

func TestScraperRespectsContextCancel(t *testing.T) {
	// Server never responds; the request must abort on ctx.
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{URL: srv.URL, Timeout: time.Hour}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := s.Scrape(ctx); err == nil {
		t.Fatal("expected context error, got nil")
	}
}

func TestScraperPressureClamps(t *testing.T) {
	// load 8 with ceiling 4 → 2.0 → clamped to 1.
	srv := fixtureServer(t, "vllm_metrics_cpu.txt")
	defer srv.Close()
	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Tier: CacheTierAuto, MaxConcurrencyCeiling: 4,
	}, nil)
	stats, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	if stats.GetPressure() != 1.0 {
		t.Errorf("pressure = %v, want 1.0 (clamped)", stats.GetPressure())
	}
}

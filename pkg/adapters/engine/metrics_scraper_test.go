package engine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	// usage metrics missing → cache bytes 0; hit/queries missing → hit_rate 0;
	// only running=0 present → pressure=0.
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
	// the configured model — silent under-report beats silent misattribution.
	const body = `# HELP vllm:kv_cache_usage_perc KV cache usage.
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

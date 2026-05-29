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
	// usage = 0.42 (CPU; GPU is zero so auto picks CPU)
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

func TestScraperGPUTierAutoDetect(t *testing.T) {
	srv := fixtureServer(t, "vllm_metrics_gpu.txt")
	defer srv.Close()

	s := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Tier: CacheTierAuto, CacheSizeBytes: 1 << 30, MaxConcurrencyCeiling: 32,
	}, nil)
	stats, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	// usage = max(gpu=0.61, cpu=0) → 0.61
	cap1GiB := float64(int64(1) << 30)
	wantBytes := int64(cap1GiB * 0.61)
	if got := stats.GetCacheMemoryBytes(); got < wantBytes-2 || got > wantBytes+2 {
		t.Errorf("cacheMemoryBytes = %d, want ~%d (gpu tier auto-detected)", got, wantBytes)
	}
}

func TestScraperExplicitTier(t *testing.T) {
	srv := fixtureServer(t, "vllm_metrics_cpu.txt")
	defer srv.Close()

	gpu := NewMetricsScraper(srv.Client(), ScraperConfig{
		URL: srv.URL, Tier: CacheTierGPU, CacheSizeBytes: 1 << 30,
	}, nil)
	stats, err := gpu.Scrape(context.Background())
	if err != nil {
		t.Fatalf("gpu scrape: %v", err)
	}
	// CPU fixture has gpu_cache_usage_perc=0 → bytes=0 in explicit gpu mode.
	if stats.GetCacheMemoryBytes() != 0 {
		t.Errorf("explicit GPU tier on CPU fixture: bytes = %d, want 0", stats.GetCacheMemoryBytes())
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

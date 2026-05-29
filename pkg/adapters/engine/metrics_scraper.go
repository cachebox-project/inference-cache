package engine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// CacheTier selects which vLLM cache-usage gauge feeds cache_memory_bytes.
// "auto" picks whichever is non-zero on a given scrape (max of gpu/cpu); this
// correctly degrades to the right tier whether the engine runs on GPU or CPU
// (vLLM 0.21 exposes both metric series — the inactive tier reads 0).
type CacheTier string

const (
	CacheTierAuto CacheTier = "auto"
	CacheTierGPU  CacheTier = "gpu"
	CacheTierCPU  CacheTier = "cpu"
)

const (
	metricGPUUsage = "vllm:gpu_cache_usage_perc"
	metricCPUUsage = "vllm:cpu_cache_usage_perc"
	metricHits     = "vllm:prefix_cache_hits_total"
	metricQueries  = "vllm:prefix_cache_queries_total"
	metricRunning  = "vllm:num_requests_running"
	metricWaiting  = "vllm:num_requests_waiting"

	defaultScrapeTimeout = 5 * time.Second
)

// ScraperConfig tunes the metrics scraper. URL is required.
type ScraperConfig struct {
	// URL is the engine's Prometheus /metrics endpoint.
	URL string
	// Tier selects which cache-usage gauge feeds cache_memory_bytes.
	// Defaults to CacheTierAuto.
	Tier CacheTier
	// CacheSizeBytes is the engine's total KV-cache capacity, used to map a
	// usage_perc gauge (0..1) to bytes. When zero, cache_memory_bytes is
	// emitted as 0 — the ranker doesn't consume this field, so an honest
	// "unknown" is preferred over a fabricated number.
	CacheSizeBytes int64
	// MaxConcurrencyCeiling is the denominator for the pressure proxy:
	//   pressure = clamp01((num_requests_running + num_requests_waiting) / ceiling).
	// 0 disables pressure (it stays 0).
	MaxConcurrencyCeiling int
	// Timeout bounds each scrape; defaults to defaultScrapeTimeout when <= 0.
	Timeout time.Duration
}

// MetricsScraper polls vLLM's Prometheus /metrics endpoint and projects the
// payload into a ReplicaStats. It is fail-soft: any HTTP/parse error returns a
// zero ReplicaStats + the error so the caller logs and retries on the next tick.
//
// Hit rate is a sliding signal — the per-scrape delta of
// (prefix_cache_hits_total / prefix_cache_queries_total) — so the very first
// scrape returns hit_rate=0 (no delta available) and the previous values are
// cached on the scraper for the next tick. A counter reset (engine restart)
// resets the delta state too: the tick that observes the reset returns 0 and
// the subsequent tick produces a fresh delta from the new baseline.
type MetricsScraper struct {
	http   *http.Client
	cfg    ScraperConfig
	logger *slog.Logger

	mu          sync.Mutex
	prevHits    float64
	prevQueries float64
	havePrev    bool
}

// NewMetricsScraper builds a scraper. If httpClient is nil, http.DefaultClient
// with the configured timeout is used.
func NewMetricsScraper(httpClient *http.Client, cfg ScraperConfig, logger *slog.Logger) *MetricsScraper {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Tier == "" {
		cfg.Tier = CacheTierAuto
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultScrapeTimeout
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: cfg.Timeout}
	}
	return &MetricsScraper{http: httpClient, cfg: cfg, logger: logger}
}

// Scrape performs one GET against the engine /metrics endpoint and returns the
// derived ReplicaStats. On error a zero-valued *icpb.ReplicaStats is returned
// alongside the error — the caller logs and skips the tick.
func (s *MetricsScraper) Scrape(ctx context.Context) (*icpb.ReplicaStats, error) {
	rctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(rctx, http.MethodGet, s.cfg.URL, nil)
	if err != nil {
		return &icpb.ReplicaStats{}, fmt.Errorf("build request: %w", err)
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return &icpb.ReplicaStats{}, fmt.Errorf("scrape %s: %w", s.cfg.URL, err)
	}
	defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return &icpb.ReplicaStats{}, fmt.Errorf("scrape %s: status %d", s.cfg.URL, resp.StatusCode)
	}

	// Construct with an explicit scheme — the zero-valued TextParser uses
	// UnsetValidation, which panics in IsValidMetricName. vLLM's `vllm:foo`
	// names are legacy-valid, so either scheme works; pick UTF8 (the common
	// global default) for forward compatibility.
	parser := expfmt.NewTextParser(model.UTF8Validation)
	families, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return &icpb.ReplicaStats{}, fmt.Errorf("parse metrics: %w", err)
	}

	gpuUsage, haveGPU := singleGaugeValue(families[metricGPUUsage])
	cpuUsage, haveCPU := singleGaugeValue(families[metricCPUUsage])
	usage, _ := selectUsage(s.cfg.Tier, gpuUsage, haveGPU, cpuUsage, haveCPU)

	hits := sumCounter(families[metricHits])
	queries := sumCounter(families[metricQueries])
	hitRate := s.consumeHitRate(hits, queries)

	running, _ := singleGaugeValue(families[metricRunning])
	waiting, _ := singleGaugeValue(families[metricWaiting])
	pressure := pressureFrom(running+waiting, s.cfg.MaxConcurrencyCeiling)

	var cacheBytes int64
	if s.cfg.CacheSizeBytes > 0 {
		cacheBytes = int64(usage * float64(s.cfg.CacheSizeBytes))
	}

	return &icpb.ReplicaStats{
		CacheMemoryBytes: cacheBytes,
		HitRate:          float32(hitRate),
		Pressure:         float32(pressure),
	}, nil
}

// consumeHitRate computes Δhits/Δqueries against the previous scrape and
// updates the cached counters. First scrape returns 0 (no delta). A counter
// reset (current < previous) also returns 0 and rebases the deltas.
func (s *MetricsScraper) consumeHitRate(hits, queries float64) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.havePrev {
		s.prevHits, s.prevQueries, s.havePrev = hits, queries, true
		return 0
	}
	dHits := hits - s.prevHits
	dQueries := queries - s.prevQueries
	s.prevHits, s.prevQueries = hits, queries
	if dHits < 0 || dQueries <= 0 {
		// Counter reset (engine restart) or no new queries: drop the tick.
		return 0
	}
	return clamp01(dHits / dQueries)
}

// selectUsage picks the active cache-usage gauge given the tier policy. Returns
// the chosen value plus a flag indicating whether a value was actually present.
// In CacheTierAuto, missing metrics degrade to whichever is present; if both
// are present we take the max — vLLM exposes both series on every platform and
// the inactive tier reads 0, so max() collapses to the active tier in practice.
func selectUsage(tier CacheTier, gpu float64, haveGPU bool, cpu float64, haveCPU bool) (float64, bool) {
	switch tier {
	case CacheTierGPU:
		return gpu, haveGPU
	case CacheTierCPU:
		return cpu, haveCPU
	default: // auto
		switch {
		case haveGPU && haveCPU:
			if gpu >= cpu {
				return gpu, true
			}
			return cpu, true
		case haveGPU:
			return gpu, true
		case haveCPU:
			return cpu, true
		}
		return 0, false
	}
}

// singleGaugeValue returns the single (or first) gauge value in a MetricFamily.
// Returns (0, false) if the family is absent or empty.
func singleGaugeValue(mf *dto.MetricFamily) (float64, bool) {
	if mf == nil || len(mf.Metric) == 0 {
		return 0, false
	}
	g := mf.Metric[0].GetGauge()
	if g == nil {
		return 0, false
	}
	return g.GetValue(), true
}

// sumCounter returns the sum of counter values across all label combinations.
// One vLLM replica generally exposes one series per metric, but summing is the
// safe thing to do under unexpected label cardinality.
func sumCounter(mf *dto.MetricFamily) float64 {
	if mf == nil {
		return 0
	}
	var v float64
	for _, m := range mf.Metric {
		if c := m.GetCounter(); c != nil {
			v += c.GetValue()
		}
	}
	return v
}

func pressureFrom(load float64, ceiling int) float64 {
	if ceiling <= 0 || load <= 0 {
		return 0
	}
	return clamp01(load / float64(ceiling))
}

func clamp01(v float64) float64 {
	switch {
	case v < 0:
		return 0
	case v > 1:
		return 1
	default:
		return v
	}
}

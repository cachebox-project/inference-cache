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
// vLLM 0.21+ exposes a single unified `vllm:kv_cache_usage_perc`; older vLLM
// exposed `vllm:gpu_cache_usage_perc` and (on some builds) `vllm:cpu_cache_usage_perc`.
// "auto" probes that fallback chain — kv → gpu → cpu — so the scraper degrades
// across vLLM releases without operator action.
type CacheTier string

const (
	CacheTierAuto CacheTier = "auto"
	CacheTierKV   CacheTier = "kv"  // vLLM 0.21+: vllm:kv_cache_usage_perc
	CacheTierGPU  CacheTier = "gpu" // legacy: vllm:gpu_cache_usage_perc
	CacheTierCPU  CacheTier = "cpu" // legacy: vllm:cpu_cache_usage_perc
)

// validCacheTiers is the canonical set of accepted --cache-tier values. Kept
// unexported (an exported mutable slice would let callers reorder/clobber the
// shared list); use CacheTier.IsValid() to check membership and
// ValidCacheTierNames() to render the set for help text or error messages.
var validCacheTiers = [...]CacheTier{CacheTierAuto, CacheTierKV, CacheTierGPU, CacheTierCPU}

// IsValid reports whether t is one of the documented tiers.
func (t CacheTier) IsValid() bool {
	for _, v := range validCacheTiers {
		if t == v {
			return true
		}
	}
	return false
}

// ValidCacheTierNames returns the accepted --cache-tier values in fallback
// order. Returns a fresh slice each call so callers cannot mutate the
// canonical set.
func ValidCacheTierNames() []CacheTier {
	out := make([]CacheTier, len(validCacheTiers))
	copy(out, validCacheTiers[:])
	return out
}

const (
	metricKVUsage  = "vllm:kv_cache_usage_perc"  // vLLM 0.21+
	metricGPUUsage = "vllm:gpu_cache_usage_perc" // legacy
	metricCPUUsage = "vllm:cpu_cache_usage_perc" // legacy
	metricHits     = "vllm:prefix_cache_hits"    // Counter; exposition appends _total
	metricQueries  = "vllm:prefix_cache_queries" // Counter; exposition appends _total
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
	// ModelLabel filters every series by the `model_name` Prometheus label
	// (the one vLLM stamps on its metrics). When non-empty, only series whose
	// label matches participate in the scrape — so a /metrics endpoint that
	// happens to expose multiple models cannot attribute another model's
	// usage/load/hit-rate to the replica this scraper is configured for. When
	// empty, every series is included (the legacy aggregate behaviour).
	ModelLabel string
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

const modelLabelName = "model_name"

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

// NewMetricsScraper builds a scraper. If httpClient is nil, a fresh
// *http.Client with the configured Timeout is used.
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

	usage, _ := selectUsage(s.cfg.Tier, families, s.cfg.ModelLabel)

	hits, haveHits := sumCounter(families, metricHits, s.cfg.ModelLabel)
	queries, haveQueries := sumCounter(families, metricQueries, s.cfg.ModelLabel)
	hitRate := s.consumeHitRate(hits, queries, haveHits && haveQueries)

	running, _ := singleGaugeValue(families, metricRunning, s.cfg.ModelLabel)
	waiting, _ := singleGaugeValue(families, metricWaiting, s.cfg.ModelLabel)
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
//
// `present` is false when either counter was missing from this scrape — the
// engine has not exposed it yet, or a partial scrape lost it. In that case
// the previous baseline is preserved (NOT rebased to 0) so when the counter
// reappears we don't manufacture a lifetime-ish hit-rate spike from the
// `current − 0` delta.
func (s *MetricsScraper) consumeHitRate(hits, queries float64, present bool) float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !present {
		// Don't touch prev_*; resume the delta when both counters reappear.
		return 0
	}
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
//
// CacheTierAuto picks the unified `vllm:kv_cache_usage_perc` if present (the
// vLLM 0.21+ canonical metric) and otherwise takes max(gpu, cpu) across the
// legacy gauges — legacy vLLM exposed both series and the inactive tier reads
// 0, so max() collapses to whichever tier is active. Explicit tier values pin
// the lookup to one metric.
func selectUsage(tier CacheTier, families map[string]*dto.MetricFamily, modelLabel string) (float64, bool) {
	switch tier {
	case CacheTierKV:
		return singleGaugeValue(families, metricKVUsage, modelLabel)
	case CacheTierGPU:
		return singleGaugeValue(families, metricGPUUsage, modelLabel)
	case CacheTierCPU:
		return singleGaugeValue(families, metricCPUUsage, modelLabel)
	}
	// CacheTierAuto.
	if v, ok := singleGaugeValue(families, metricKVUsage, modelLabel); ok {
		return v, true
	}
	gpu, haveGPU := singleGaugeValue(families, metricGPUUsage, modelLabel)
	cpu, haveCPU := singleGaugeValue(families, metricCPUUsage, modelLabel)
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

// singleGaugeValue returns the first gauge value for a metric family whose
// `model_name` label matches modelLabel. modelLabel == "" disables the filter
// (every series qualifies). Returns (0, false) if the family is absent or
// nothing matches.
func singleGaugeValue(families map[string]*dto.MetricFamily, name, modelLabel string) (float64, bool) {
	mf := families[name]
	if mf == nil {
		return 0, false
	}
	for _, m := range mf.Metric {
		if !modelLabelMatches(m, modelLabel) {
			continue
		}
		if g := m.GetGauge(); g != nil {
			return g.GetValue(), true
		}
	}
	return 0, false
}

// sumCounter returns the sum of counter values for a metric whose `model_name`
// label matches modelLabel (modelLabel == "" disables the filter), plus a
// presence flag. ok=false means the family was absent (or no series matched
// the filter) — caller distinguishes a real 0 from "we didn't see it" so a
// partial scrape can't poison the hit-rate delta baseline. The Python
// prometheus_client exposes Counter("foo") under the family name "foo_total"
// in the text format, but other Prometheus clients use the unsuffixed form —
// we check both so the scraper is portable across them. One vLLM replica
// generally exposes one series per metric, but summing is the safe thing to
// do under unexpected label cardinality.
func sumCounter(families map[string]*dto.MetricFamily, name, modelLabel string) (float64, bool) {
	mf := families[name+"_total"]
	if mf == nil {
		mf = families[name]
	}
	if mf == nil {
		return 0, false
	}
	var (
		v       float64
		matched bool
	)
	for _, m := range mf.Metric {
		if !modelLabelMatches(m, modelLabel) {
			continue
		}
		if c := m.GetCounter(); c != nil {
			v += c.GetValue()
			matched = true
		}
	}
	return v, matched
}

// modelLabelMatches reports whether m's `model_name` label equals want.
// want == "" disables the filter (any series qualifies). A series that does
// not carry the label at all does NOT qualify when want is non-empty — we'd
// rather under-report a single tick than risk attributing another model's
// metrics to this replica.
func modelLabelMatches(m *dto.Metric, want string) bool {
	if want == "" {
		return true
	}
	for _, lp := range m.GetLabel() {
		if lp.GetName() == modelLabelName {
			return lp.GetValue() == want
		}
	}
	return false
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

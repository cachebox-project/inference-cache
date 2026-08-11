// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package subscriber

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	icpb "github.com/cachebox-project/inference-cache/gen/inferencecache/v1alpha1"
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
	// T2 (external offload tier, e.g. LMCache) cumulative token counters.
	metricExtHits    = "vllm:external_prefix_cache_hits"    // Counter; exposition appends _total
	metricExtQueries = "vllm:external_prefix_cache_queries" // Counter; exposition appends _total
	metricRunning    = "vllm:num_requests_running"
	metricWaiting    = "vllm:num_requests_waiting"

	defaultScrapeTimeout = 5 * time.Second
)

// SGLang exposes a disjoint metric namespace (sglang:*) on :30000 under
// --enable-metrics. The names, gauge types, and the priority="" total /
// priority="<int>" breakdown convention below were verified against SGLang's
// SchedulerMetricsCollector at the image pinned in docs/reference-stack/VERSIONS.md;
// re-verify on a version bump (names have shifted across releases). See
// internal/subscriber/testdata/sglang_metrics.txt.
const (
	sglangUsage   = "sglang:token_usage"    // gauge, 0-1 KV-utilization ratio (maps to vLLM's usage_perc)
	sglangHitRate = "sglang:cache_hit_rate" // gauge, 0-1 rate — read DIRECTLY, NOT a hits/queries delta
	sglangRunning = "sglang:num_running_reqs"
	sglangWaiting = "sglang:num_queue_reqs"
	// No core sglang:* external/T2-tier hit counters exist (vLLM has
	// vllm:external_prefix_cache_*); SGLang+LMCache T2 stats live in LMCache's
	// own metrics, so the SGLang profile leaves T2 unknown rather than fabricate.
)

// hitRateMode selects how a profile derives hit_rate.
type hitRateMode int

const (
	// hitRateCounterDelta derives hit_rate as Δhits/Δqueries across two
	// cumulative counters between scrapes (vLLM). Keeps per-scraper state.
	hitRateCounterDelta hitRateMode = iota
	// hitRateDirectGauge reads hit_rate from a single 0-1 gauge the engine
	// already computes (SGLang) — no delta, no cross-scrape state.
	hitRateDirectGauge
)

// usageSpec names the 0-1 KV-utilization gauges for a profile. kv is the
// canonical/unified gauge; gpu/cpu are legacy fallbacks (vLLM only, "" when the
// engine exposes none). selectUsage(auto) prefers kv, else max(gpu, cpu).
type usageSpec struct{ kv, gpu, cpu string }

// metricProfile is the per-engine metric name set + extraction strategy, keyed
// by hash_scheme. The scraper reads exactly ONE profile per replica, so a vLLM
// scraper pointed at an SGLang endpoint (or vice-versa) recognizes nothing and
// fails loud instead of fabricating zeros.
type metricProfile struct {
	scheme      string
	usage       usageSpec
	running     string
	waiting     string
	hitRateMode hitRateMode
	hitGauge    string // hitRateDirectGauge
	hits        string // hitRateCounterDelta
	queries     string // hitRateCounterDelta
	extHits     string // T2 external-tier counters; "" = unsupported (leave T2 at 0)
	extQueries  string
}

// knownNames lists every metric family name this profile reads, for the
// fail-loud recognizer. Empty names are skipped.
func (p metricProfile) knownNames() []string {
	all := []string{p.usage.kv, p.usage.gpu, p.usage.cpu, p.running, p.waiting, p.hitGauge, p.hits, p.queries, p.extHits, p.extQueries}
	out := make([]string, 0, len(all))
	for _, n := range all {
		if n != "" {
			out = append(out, n)
		}
	}
	return out
}

var (
	vllmProfile = metricProfile{
		scheme:      "vllm",
		usage:       usageSpec{kv: metricKVUsage, gpu: metricGPUUsage, cpu: metricCPUUsage},
		running:     metricRunning,
		waiting:     metricWaiting,
		hitRateMode: hitRateCounterDelta,
		hits:        metricHits,
		queries:     metricQueries,
		extHits:     metricExtHits,
		extQueries:  metricExtQueries,
	}
	sglangProfile = metricProfile{
		scheme:      "sglang",
		usage:       usageSpec{kv: sglangUsage}, // single token_usage gauge; no gpu/cpu tiers
		running:     sglangRunning,
		waiting:     sglangWaiting,
		hitRateMode: hitRateDirectGauge,
		hitGauge:    sglangHitRate,
		// extHits/extQueries intentionally empty — no core sglang:* T2 counters.
	}
)

// profileForScheme maps a hash_scheme to its metric profile. Unknown/empty
// defaults to vLLM — every pre-existing caller is vLLM and sets no scheme, so
// their behavior is unchanged.
func profileForScheme(scheme string) metricProfile {
	switch scheme {
	case "sglang":
		return sglangProfile
	default:
		return vllmProfile
	}
}

// ScraperConfig tunes the metrics scraper. URL is required.
type ScraperConfig struct {
	// URL is the engine's Prometheus /metrics endpoint.
	URL string
	// Scheme selects the per-engine metric profile ("vllm" | "sglang"). Empty
	// defaults to vLLM. It MUST match the engine the URL points at: a mismatch
	// makes every metric family unrecognized and Scrape fails loud rather than
	// fabricate zeros. Wired from the subscriber's --hash-scheme.
	Scheme string
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

// MetricsScraper polls an engine's Prometheus /metrics endpoint and projects the
// payload into a ReplicaStats, using the per-engine metricProfile selected by
// ScraperConfig.Scheme. It is fail-soft: any HTTP/parse error returns a zero
// ReplicaStats + the error so the caller logs and retries on the next tick.
//
// For vLLM, hit rate is a sliding signal — the per-scrape delta of
// (prefix_cache_hits_total / prefix_cache_queries_total) — so the very first
// scrape returns hit_rate=0 (no delta available) and the previous values are
// cached on the scraper for the next tick. A counter reset (engine restart)
// resets the delta state too. SGLang reads a direct gauge instead (see hitRate).
type MetricsScraper struct {
	http    *http.Client
	cfg     ScraperConfig
	profile metricProfile
	logger  *slog.Logger

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
	profile := profileForScheme(cfg.Scheme)
	if cfg.Scheme != "" && cfg.Scheme != profile.scheme {
		// An unknown non-empty scheme falls back to the vLLM profile. Log it so
		// the fallback is visible: if the engine is not vLLM, the vllm:* families
		// are absent and Scrape fails loud (stale) rather than delivering wrong
		// stats — but the operator should still fix the scheme.
		logger.Warn("unknown --hash-scheme; using the vLLM metric profile (a mismatched engine fails loud on scrape)",
			"scheme", cfg.Scheme, "profile", profile.scheme)
	}
	return &MetricsScraper{http: httpClient, cfg: cfg, profile: profile, logger: logger}
}

// Scrape performs one GET against the engine /metrics endpoint and returns the
// derived ReplicaStats. On error a zero-valued *icpb.ReplicaStats is returned
// alongside the error — the caller logs and skips the tick.
//
// A 200 OK whose body contains NONE of the recognized engine metric families is
// treated as an error, not a healthy all-zero sample: it means a metric-name /
// port / engine mismatch (e.g. an SGLang endpoint exposing sglang:* names, or a
// wrong --engine-metrics-url), and emitting fabricated zeros would be marked
// delivered and silently disable load-aware routing. Likewise, a scrape
// that recognizes the engine but is missing either load gauge — including a
// model_name filter that excludes them — fails loud rather than report a
// fabricated pressure=0. Cache-side metrics may still be absent and report 0:
// usage (cache_memory_bytes) is not consumed by the ranker, and an absent
// hit_rate reports 0, which conservatively disables the replica's TENANT_HOT
// fallback (a safe, hint-only degradation — see hitRate) rather than fail the
// whole scrape and also drop the load signal.
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

	// Fail loud on a scheme/name mismatch instead of fabricating zeros;
	// see the Scrape doc and hasAnyKnownFamily.
	p := s.profile
	if !hasAnyKnownFamily(families, p) {
		return &icpb.ReplicaStats{}, fmt.Errorf(
			"scrape %s: no recognized %s metric families in response (%d parsed); wrong engine, port, or metric-name scheme", s.cfg.URL, p.scheme, len(families))
	}

	// Require BOTH load gauges (after the model_name filter) BEFORE deriving any
	// stats or mutating scraper state. Pressure drives load-aware routing, so a
	// scrape that recognizes the engine but is missing running/waiting — a partial
	// exposition, or a model_name filter that excludes them — must fail loud rather
	// than report a fabricated pressure=0. Validating first also guarantees a
	// rejected scrape never advances the hit-rate counter baseline (hitRate mutates
	// state on the vLLM path). An idle engine still exposes both gauges at value 0,
	// so this rejects absence, not zero load.
	running, haveRunning := aggregateGauge(families, p.running, s.cfg.ModelLabel, aggSumTotals)
	waiting, haveWaiting := aggregateGauge(families, p.waiting, s.cfg.ModelLabel, aggSumTotals)
	if !haveRunning || !haveWaiting {
		return &icpb.ReplicaStats{}, fmt.Errorf(
			"scrape %s: %s load gauges incomplete (running=%t, waiting=%t); partial exposition, engine missing --enable-metrics, or model_name=%q excludes them",
			s.cfg.URL, p.scheme, haveRunning, haveWaiting, s.cfg.ModelLabel)
	}
	pressure := pressureFrom(running+waiting, s.cfg.MaxConcurrencyCeiling)

	usage, _ := selectUsage(s.cfg.Tier, families, p.usage, s.cfg.ModelLabel)
	hitRate := s.hitRate(p, families)

	// T2 (external offload tier) cumulative token counters, forwarded as-is.
	// The server derives a presence-aware tier-2 hit-rate (a 0% rate is only
	// meaningful once t2_query_tokens > 0). On engines without an external
	// tier — including SGLang, whose profile leaves these names empty — the
	// metrics are absent, sumCounter returns 0, and the server reads that as
	// "tier-2 not exercised" — never a fabricated 0% hit-rate.
	extHits, _ := sumCounter(families, p.extHits, s.cfg.ModelLabel)
	extQueries, _ := sumCounter(families, p.extQueries, s.cfg.ModelLabel)

	var cacheBytes int64
	if s.cfg.CacheSizeBytes > 0 {
		cacheBytes = int64(usage * float64(s.cfg.CacheSizeBytes))
	}

	return &icpb.ReplicaStats{
		CacheMemoryBytes: cacheBytes,
		HitRate:          float32(hitRate),
		Pressure:         float32(pressure),
		T2HitTokens:      int64(extHits),
		T2QueryTokens:    int64(extQueries),
	}, nil
}

// hitRate derives hit_rate per the active profile: SGLang reads a 0-1 gauge the
// engine already computes (sglang:cache_hit_rate, max across ranks) directly — no
// delta, meaningful on the first scrape; vLLM derives Δhits/Δqueries across two
// cumulative counters (0 on the first scrape).
//
// An absent hit-rate deliberately reports 0 (not an error): hit_rate gates the
// server's TENANT_HOT fallback (a replica below the policy's minimum hit-rate is
// not offered as tenant-hot), so 0 conservatively withholds that soft hint for
// this replica while the prefix-match and load signals keep working. Reporting 0
// on absence is indistinguishable from a true 0 here, same as vLLM's first-scrape
// 0 — an acceptable degradation for an advisory hint.
func (s *MetricsScraper) hitRate(p metricProfile, families map[string]*dto.MetricFamily) float64 {
	if p.hitRateMode == hitRateDirectGauge {
		v, ok := aggregateGauge(families, p.hitGauge, s.cfg.ModelLabel, aggMax)
		if !ok {
			return 0
		}
		return clamp01(v)
	}
	hits, haveHits := sumCounter(families, p.hits, s.cfg.ModelLabel)
	queries, haveQueries := sumCounter(families, p.queries, s.cfg.ModelLabel)
	return s.consumeHitRate(hits, queries, haveHits && haveQueries)
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
// the lookup to one metric. Within a family, the max across ranks is taken (the
// most-saturated scheduler is the bottleneck).
func selectUsage(tier CacheTier, families map[string]*dto.MetricFamily, spec usageSpec, modelLabel string) (float64, bool) {
	switch tier {
	case CacheTierKV:
		return aggregateGauge(families, spec.kv, modelLabel, aggMax)
	case CacheTierGPU:
		return aggregateGauge(families, spec.gpu, modelLabel, aggMax)
	case CacheTierCPU:
		return aggregateGauge(families, spec.cpu, modelLabel, aggMax)
	}
	// CacheTierAuto: prefer the unified/canonical gauge (spec.kv), else the
	// legacy gpu/cpu tiers. A profile with no gpu/cpu (e.g. SGLang's single
	// token_usage) passes "" for those, which aggregateGauge reports absent.
	if v, ok := aggregateGauge(families, spec.kv, modelLabel, aggMax); ok {
		return v, true
	}
	gpu, haveGPU := aggregateGauge(families, spec.gpu, modelLabel, aggMax)
	cpu, haveCPU := aggregateGauge(families, spec.cpu, modelLabel, aggMax)
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

// gaugeAgg selects how multiple matching series of one gauge are combined.
// Engines emit one series per scheduler in a data/tensor-parallel deployment
// (SGLang labels them by dp_rank/tp_rank/pp_rank), so a single-series read would
// undercount node-wide load.
type gaugeAgg int

const (
	// aggSumTotals sums the per-scheduler totals across ranks, EXCLUDING SGLang's
	// per-priority breakdown series (priority="<int>"), which duplicate the total
	// the engine also records under priority="" / no priority label. Used for the
	// load gauges so a DP deployment reports total node load, not one scheduler's.
	aggSumTotals gaugeAgg = iota
	// aggMax takes the max across matching series — the most-saturated rank
	// (bottleneck). Used for utilization / hit-rate ratios, which don't sum.
	aggMax
)

// aggregateGauge combines the matching series of a gauge family per agg, after
// the model_name filter. Returns (value, present); present is false when the
// family is absent or no series matched.
func aggregateGauge(families map[string]*dto.MetricFamily, name, modelLabel string, agg gaugeAgg) (float64, bool) {
	mf := families[name]
	if mf == nil {
		return 0, false
	}
	var acc float64
	found := false
	for _, m := range mf.Metric {
		if !modelLabelMatches(m, modelLabel) {
			continue
		}
		g := m.GetGauge()
		if g == nil {
			continue
		}
		if agg == aggSumTotals && isPriorityBreakdown(m) {
			continue // skip SGLang's priority="<int>" duplicates of the total
		}
		v := g.GetValue()
		if math.IsNaN(v) || math.IsInf(v, 0) {
			// Prometheus permits NaN/Inf; a non-finite reading is unusable —
			// skip it so it neither counts as "present" nor poisons the
			// aggregate. A load gauge that is only ever non-finite then reads
			// absent and Scrape fails loud rather than delivering NaN pressure.
			continue
		}
		switch {
		case !found:
			acc, found = v, true
		case agg == aggSumTotals:
			acc += v
		case agg == aggMax:
			if v > acc {
				acc = v
			}
		}
	}
	return acc, found
}

// isPriorityBreakdown reports whether m carries a non-empty `priority` label —
// SGLang's per-priority QueueCount breakdown, which duplicates the total the
// engine records under priority="" (or no priority label at all). See SGLang's
// SchedulerMetricsCollector.
func isPriorityBreakdown(m *dto.Metric) bool {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == "priority" {
			return lp.GetValue() != ""
		}
	}
	return false
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
			cv := c.GetValue()
			if math.IsNaN(cv) || math.IsInf(cv, 0) {
				continue // non-finite counter is unusable; don't poison the delta
			}
			v += cv
			matched = true
		}
	}
	return v, matched
}

// modelLabelMatches reports whether m's `model_name` label equals want.
// want == "" disables the filter (any series qualifies). A series that does
// not carry the label at all does NOT qualify when want is non-empty — never
// attribute another model's metrics to this replica. When the filter excludes a
// load gauge, Scrape fails loud rather than report a fabricated pressure=0;
// cache-side metrics (usage, hit-rate) simply under-report.
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

// hasAnyKnownFamily reports whether the parsed exposition contains at least one
// metric family the ACTIVE profile knows how to read, INDEPENDENT of the
// model_name filter. It is the discriminator between a name/port/engine mismatch
// (no known family at all → Scrape fails loud — including a scheme set
// to the wrong engine) and a model-label filter that excluded every series
// (family present, just filtered — a filtered-out load gauge is then caught by
// the both-load-gauges requirement and fails loud too). Counters may appear with
// or without the _total suffix.
func hasAnyKnownFamily(families map[string]*dto.MetricFamily, p metricProfile) bool {
	for _, name := range p.knownNames() {
		if families[name] != nil || families[name+"_total"] != nil {
			return true
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

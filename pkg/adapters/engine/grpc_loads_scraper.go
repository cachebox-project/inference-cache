package engine

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	vpb "github.com/cachebox-project/inference-cache/pkg/adapters/engine/vllmengine"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// GrpcLoadsScraperConfig tunes the gRPC GetLoads scraper. Addr is required.
type GrpcLoadsScraperConfig struct {
	// Addr is the engine's VllmEngine gRPC address (host:port), e.g.
	// 127.0.0.1:50051.
	Addr string
	// CacheSizeBytes is the engine's total KV-cache capacity, used to map
	// token_usage (0..1) to cache_memory_bytes. When zero, cache_memory_bytes is
	// emitted as 0 (an honest "unknown" rather than a fabricated number).
	CacheSizeBytes int64
	// MaxConcurrencyCeiling is the pressure denominator:
	//   pressure = clamp01((num_running_reqs + num_waiting_reqs) / ceiling).
	// 0 disables pressure (it stays 0).
	MaxConcurrencyCeiling int
	// Timeout bounds each GetLoads call; defaults to defaultScrapeTimeout when <= 0.
	Timeout time.Duration
}

// GrpcLoadsScraper reads engine load via the SMG engine's GetLoads gRPC RPC and
// projects it into a ReplicaStats — the gRPC-native alternative to scraping the
// engine's HTTP /metrics. An SMG gRPC engine (vllm.entrypoints.grpc_server)
// exposes no HTTP metrics endpoint; live load is available only over GetLoads.
// It implements the same statsScraper interface as MetricsScraper, so the
// StatsReporter uses whichever is configured, unchanged.
//
// Note: GetLoads carries no external-tier (T2/LMCache) token counters, so
// T2HitTokens/T2QueryTokens are left 0 here — those remain HTTP-/metrics-only.
// The load signals the ranker actually uses (pressure, cache-usage, hit-rate)
// are all provided.
type GrpcLoadsScraper struct {
	client vpb.VllmEngineClient
	cfg    GrpcLoadsScraperConfig
	closer func() error
}

// NewGrpcLoadsScraper dials the engine (lazily — grpc.NewClient does not connect
// until the first RPC, so a down engine doesn't fail construction) and returns a
// scraper. Call Close to release the connection.
func NewGrpcLoadsScraper(cfg GrpcLoadsScraperConfig) (*GrpcLoadsScraper, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("grpc loads scraper: Addr is required")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultScrapeTimeout
	}
	conn, err := grpc.NewClient(cfg.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc loads scraper: dial %s: %w", cfg.Addr, err)
	}
	return &GrpcLoadsScraper{client: vpb.NewVllmEngineClient(conn), cfg: cfg, closer: conn.Close}, nil
}

// newGrpcLoadsScraperWithClient injects a client (tests) and never owns a conn.
func newGrpcLoadsScraperWithClient(c vpb.VllmEngineClient, cfg GrpcLoadsScraperConfig) *GrpcLoadsScraper {
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultScrapeTimeout
	}
	return &GrpcLoadsScraper{client: c, cfg: cfg, closer: func() error { return nil }}
}

// Close releases the underlying gRPC connection.
func (s *GrpcLoadsScraper) Close() error { return s.closer() }

// Scrape calls GetLoads once and projects the per-DP-rank SchedulerLoad into a
// ReplicaStats. On error a zero-valued *icpb.ReplicaStats is returned alongside
// the error — the StatsReporter logs and skips the tick, same as the HTTP path.
func (s *GrpcLoadsScraper) Scrape(ctx context.Context) (*icpb.ReplicaStats, error) {
	rctx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()

	resp, err := s.client.GetLoads(rctx, &vpb.GetLoadsRequest{})
	if err != nil {
		return &icpb.ReplicaStats{}, fmt.Errorf("getloads %s: %w", s.cfg.Addr, err)
	}

	// Aggregate across DP ranks: request counts SUM (total in-flight across the
	// engine), KV-cache usage is the MAX (worst-case pressure proxy), hit-rate is
	// the mean of ranks that report one.
	var running, waiting int64
	var usage, hitRateSum float64
	var hitRateN int
	for _, l := range resp.GetLoads() {
		running += int64(l.GetNumRunningReqs())
		waiting += int64(l.GetNumWaitingReqs())
		if u := l.GetTokenUsage(); u > usage {
			usage = u
		}
		hitRateSum += l.GetCacheHitRate()
		hitRateN++
	}
	hitRate := 0.0
	if hitRateN > 0 {
		hitRate = hitRateSum / float64(hitRateN)
	}

	pressure := pressureFrom(float64(running+waiting), s.cfg.MaxConcurrencyCeiling)
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

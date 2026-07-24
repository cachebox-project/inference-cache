// Command kvevent-subscriber runs as a sidecar next to a vLLM engine replica:
// it subscribes to the engine's KV-cache events over ZMQ and reports cache state
// to the inferencecache-server over gRPC. Metadata only — never tokens or prompt
// text. Fail-soft: engine/server outages are retried and never stall the engine.
//
// Two independent paths share the gRPC client:
//   - Event path: ZMQ → decoded EventBatch → ReportCacheState (prefix adds) +
//     PublishEvent (removals/clears). Debounced on a short window.
//   - Stats path: engine load → derived ReplicaStats → ReportCacheState
//     (stats-only CSU). The load source is selectable: HTTP GET against the
//     engine's Prometheus /metrics (default), or the VllmEngine GetLoads gRPC
//     RPC when --engine-loads-grpc is set (SMG gRPC engines expose no HTTP
//     /metrics). Ticks on its own cadence (~10s), so the snapshot/CR status
//     surface (cache_memory_bytes, hit_rate, pressure) lights up regardless of
//     event rate.
//
// The two paths are independent failure domains — a scrape failure never
// blocks the event stream, and an event-stream drop never delays a stats tick.
package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/cachebox-project/inference-cache/pkg/adapters/engine"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// engineScraper is the load-source contract both scrapers satisfy — a local
// mirror of the engine package's unexported statsScraper. NewStatsReporter
// accepts either concrete type through it.
type engineScraper interface {
	Scrape(context.Context) (*icpb.ReplicaStats, error)
}

// scraperParams carries the load-source selection inputs for buildStatsScraper.
type scraperParams struct {
	loadsGRPC      string
	metricsURL     string
	engineModel    string
	tier           engine.CacheTier
	cacheSizeBytes int64
	ceiling        int
}

// buildStatsScraper selects the engine load source: the GetLoads gRPC scraper
// when loadsGRPC is non-empty (preferred for SMG gRPC engines, which expose no
// HTTP /metrics), else the HTTP /metrics scraper. It returns the scraper, an
// optional closer (the gRPC scraper owns a client conn — the HTTP one owns
// nothing, so closer is nil), and an error only from gRPC dial setup.
func buildStatsScraper(p scraperParams, httpClient *http.Client, logger *slog.Logger) (engineScraper, io.Closer, error) {
	if p.loadsGRPC != "" {
		gs, err := engine.NewGRPCLoadsScraper(engine.GRPCLoadsScraperConfig{
			Addr:                  p.loadsGRPC,
			CacheSizeBytes:        p.cacheSizeBytes,
			MaxConcurrencyCeiling: p.ceiling,
		})
		if err != nil {
			return nil, nil, err
		}
		return gs, gs, nil
	}
	ms := engine.NewMetricsScraper(httpClient, engine.ScraperConfig{
		URL:                   p.metricsURL,
		Tier:                  p.tier,
		ModelLabel:            p.engineModel,
		CacheSizeBytes:        p.cacheSizeBytes,
		MaxConcurrencyCeiling: p.ceiling,
	}, logger)
	return ms, nil, nil
}

func main() {
	var (
		endpoint           = flag.String("engine-endpoint", "tcp://127.0.0.1:5557", "engine KV-event ZMQ PUB endpoint")
		topic              = flag.String("topic", "kv-events", "ZMQ topic to subscribe to (empty = all)")
		server             = flag.String("server", "127.0.0.1:9090", "inferencecache-server gRPC address")
		replica            = flag.String("replica-id", "", "engine replica id (required)")
		model              = flag.String("model-id", "", "served model id (required)")
		tenant             = flag.String("tenant-id", "", "tenant id (optional)")
		scheme             = flag.String("hash-scheme", "vllm", "engine prefix-hash scheme (required, non-empty)")
		window             = flag.Duration("window", 100*time.Millisecond, "add-batching/debounce flush window")
		metricsURL         = flag.String("engine-metrics-url", "http://127.0.0.1:8000/metrics", "engine Prometheus /metrics URL")
		loadsGRPC          = flag.String("engine-loads-grpc", "", "engine VllmEngine gRPC address (host:port) to read load via the GetLoads RPC instead of scraping --engine-metrics-url. Preferred for SMG gRPC engines, which expose no HTTP /metrics. Empty = use the HTTP scrape.")
		statsInterval      = flag.Duration("stats-interval", 10*time.Second, "ReplicaStats scrape/emit cadence")
		cacheSizeBytes     = flag.Int64("engine-cache-size-bytes", 0, "engine total KV-cache capacity in bytes (multiplied by usage_perc to derive cacheMemoryBytes; 0 emits cacheMemoryBytes=0)")
		ceiling            = flag.Int("max-concurrency-ceiling", 256, "denominator for the pressure proxy = clamp01((num_requests_running+num_requests_waiting)/ceiling)")
		cacheTier          = flag.String("cache-tier", "auto", `which vLLM cache-usage gauge to read: "auto" (kv→gpu→cpu fallback) | "kv" | "gpu" | "cpu"`)
		engineModel        = flag.String("engine-model-name", "", `value of the engine's `+"`model_name`"+` Prometheus label to filter /metrics by (e.g. "Qwen/Qwen2.5-0.5B-Instruct"). Distinct from --model-id (the cache-plane index key). Empty = no label filter (aggregates every series — fine when the engine serves one model).`)
		ignoreBlockRemoved = flag.Bool("ignore-block-removed", false, `declare that this engine is paired with an L2 offload tier (e.g. LMCache): a BlockRemoved is re-reported as a T2 (reload-from-L2) entry instead of a PREFIX_EVICTED delete, so the routing hint survives the HBM eviction but is honestly tagged colder. Default off for single-tier deployments (BlockRemoved → PREFIX_EVICTED). Name kept for compatibility. See docs/design/kvevent-subscriber-wiring.md "L2 cache tier semantics".`)
		adapterNames       = flag.String("lora-adapter-names", "", `comma-separated `+"`id=name`"+` map from the engine's internal LoRA id (BlockStored.lora_id, assigned in --lora-modules load order) to the stable adapter identity used as the index partition — the same string the gateway sends as LookupRouteRequest.adapter_id (e.g. "1=sql-lora,2=chat-lora"). An unmapped non-nil lora_id is FAIL-CLOSED: its blocks are dropped (not cached) rather than indexed under a replica-local "lora:<id>" that could alias different adapters across replicas — so LoRA caching REQUIRES this map. Omit ONLY for base-model / non-LoRA traffic (nil lora_id → the default "" partition). The identity mapped here MUST match the gateway's LookupRoute query adapter_id end-to-end or every lookup silently misses (the same agreement --hash-scheme requires).`)
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	names, err := engine.ParseAdapterNames(*adapterNames)
	if err != nil {
		logger.Error("invalid --lora-adapter-names", "value", *adapterNames, "err", err)
		os.Exit(2)
	}

	cfg := engine.Config{
		ReplicaID:    *replica,
		ModelID:      *model,
		TenantID:     *tenant,
		HashScheme:   *scheme,
		AdapterNames: names,
	}
	if err := cfg.Validate(); err != nil {
		logger.Error("invalid config", "err", err)
		os.Exit(2)
	}
	tier := engine.CacheTier(*cacheTier)
	if !tier.IsValid() {
		logger.Error("invalid --cache-tier", "value", *cacheTier, "valid", engine.ValidCacheTierNames())
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Lazy connect: gRPC dials on first RPC, so the subscriber starts even if the
	// server isn't up yet (it will connect when events begin flowing).
	conn, err := grpc.NewClient(*server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("grpc client", "server", *server, "err", err)
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	client := icpb.NewInferenceCacheClient(conn)

	reporter := engine.NewReporter(client, cfg,
		engine.WithWindow(*window),
		engine.WithLogger(logger),
		engine.WithIgnoreBlockRemoved(*ignoreBlockRemoved))
	sub := engine.NewSubscriber(*endpoint, *topic, engine.WithSubscriberLogger(logger))

	// Load source: gRPC GetLoads (preferred for SMG gRPC engines, which expose no
	// HTTP /metrics) when --engine-loads-grpc is set, else the HTTP /metrics scrape.
	// Both satisfy the same statsScraper, so the StatsReporter is identical.
	scraper, scraperCloser, serr := buildStatsScraper(scraperParams{
		loadsGRPC:      *loadsGRPC,
		metricsURL:     *metricsURL,
		engineModel:    *engineModel,
		tier:           tier,
		cacheSizeBytes: *cacheSizeBytes,
		ceiling:        *ceiling,
	}, &http.Client{Timeout: 5 * time.Second}, logger)
	if serr != nil {
		logger.Error("build stats scraper", "err", serr)
		os.Exit(1)
	}
	if scraperCloser != nil {
		defer func() {
			if err := scraperCloser.Close(); err != nil {
				logger.Warn("closing load-scraper connection", "err", err)
			}
		}()
	}

	loadSource, loadTarget := "HTTP /metrics scrape", *metricsURL
	if *loadsGRPC != "" {
		loadSource, loadTarget = "GetLoads gRPC", *loadsGRPC
	}
	logger.Info("engine load source", "source", loadSource, "target", loadTarget)

	statsReporter := engine.NewStatsReporter(client, scraper, cfg,
		engine.WithStatsInterval(*statsInterval),
		engine.WithStatsLogger(logger),
	)

	out := make(chan *engine.EventBatch, 256)

	// The reporter stops by draining a closed channel, not by signal — so on
	// shutdown the batches already buffered in `out` are flushed rather than
	// dropped. Its context is background (only the subscriber watches the signal).
	reporterDone := make(chan struct{})
	go func() {
		defer close(reporterDone)
		if err := reporter.Run(context.Background(), out); err != nil {
			logger.Error("reporter stopped", "err", err)
		}
	}()

	// The stats reporter is signal-driven (cancelled on ctx.Done) so it can
	// abandon an in-flight scrape/RPC on shutdown without stalling drain.
	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		if err := statsReporter.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("stats reporter stopped", "err", err)
		}
	}()

	logger.Info("kvevent-subscriber starting",
		"engine_endpoint", *endpoint,
		"server", *server,
		"replica_id", *replica,
		"model_id", *model,
		"load_source", loadSource,
		"load_target", loadTarget,
		"stats_interval", statsInterval.String(),
		"cache_tier", *cacheTier,
		"engine_model_name", *engineModel,
		"ignore_block_removed", *ignoreBlockRemoved,
	)

	if err := sub.Run(ctx, out); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("subscriber stopped", "err", err)
	}

	close(out) // stop the reporter and let it drain + final-flush
	select {
	case <-reporterDone:
	case <-time.After(5 * time.Second):
		logger.Warn("reporter drain timed out")
	}
	select {
	case <-statsDone:
	case <-time.After(5 * time.Second):
		logger.Warn("stats reporter drain timed out")
	}
	logger.Info("kvevent-subscriber stopped")
}

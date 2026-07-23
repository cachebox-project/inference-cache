package engine

import (
	"context"
	"errors"
	"log/slog"
	"time"

	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// statsScraper is the dependency a StatsReporter takes. *MetricsScraper
// implements it; tests pass stubs.
type statsScraper interface {
	Scrape(ctx context.Context) (*icpb.ReplicaStats, error)
}

// StatsReporter periodically scrapes engine /metrics and emits a stats-only
// CacheStateUpdate via ReportCacheState. It runs alongside the event Reporter
// (different cadence, different data source) and shares the same gRPC client.
//
// Failure independence is load-bearing: a scrape failure (engine /metrics down,
// HTTP timeout, parse error) logs and skips the tick — it never blocks the
// event path or kills the subscriber. The two paths are independent failure
// domains.
type StatsReporter struct {
	client     icpb.InferenceCacheClient
	scraper    statsScraper
	cfg        Config
	interval   time.Duration
	rpcTimeout time.Duration
	now        func() time.Time
	logger     *slog.Logger

	// staleThreshold consecutive scrape failures flips the reporter to "stale":
	// it logs once at Error on that transition and once at Info on recovery,
	// guarding against both per-tick log spam and a silently unrefreshed load
	// signal. The transition message depends on everSucceeded: after at least one
	// good scrape IC keeps ranking on that last sample until it ages out at the
	// index TTL; if every scrape since startup has failed there is no sample at
	// all and the replica is ranked on residency only from the outset.
	staleThreshold int
	consecFails    int
	everSucceeded  bool
}

// StatsReporterOption configures a StatsReporter.
type StatsReporterOption func(*StatsReporter)

// WithStatsInterval sets the scrape tick interval (default 10s).
func WithStatsInterval(d time.Duration) StatsReporterOption {
	return func(r *StatsReporter) { r.interval = d }
}

// WithStatsRPCTimeout bounds each ReportCacheState call (default 5s).
func WithStatsRPCTimeout(d time.Duration) StatsReporterOption {
	return func(r *StatsReporter) { r.rpcTimeout = d }
}

// WithStatsLogger sets the logger (default slog.Default()).
func WithStatsLogger(l *slog.Logger) StatsReporterOption {
	return func(r *StatsReporter) { r.logger = l }
}

// NewStatsReporter builds a StatsReporter that ticks against scraper and
// publishes onto client.
func NewStatsReporter(client icpb.InferenceCacheClient, scraper statsScraper, cfg Config, opts ...StatsReporterOption) *StatsReporter {
	r := &StatsReporter{
		client:     client,
		scraper:    scraper,
		cfg:        cfg,
		interval:   10 * time.Second,
		rpcTimeout: 5 * time.Second,
		now:        time.Now,
		logger:     slog.Default(),

		staleThreshold: 3,
	}
	for _, o := range opts {
		o(r)
	}
	if r.interval <= 0 {
		r.interval = 10 * time.Second // guard time.NewTicker
	}
	if r.rpcTimeout <= 0 {
		r.rpcTimeout = 5 * time.Second
	}
	return r
}

// Run ticks until ctx is cancelled, scraping and emitting on each tick. It
// returns ctx.Err() on shutdown so the caller can distinguish clean exit from
// a misconfiguration that triggers an early return.
func (r *StatsReporter) Run(ctx context.Context) error {
	r.logger.Info("stats reporter starting", "interval", r.interval)
	defer r.logger.Info("stats reporter stopped")

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			r.tick(ctx)
		}
	}
}

// tick scrapes once and sends one stats-only CacheStateUpdate. All failures are
// non-fatal — they log and return so the next tick retries.
func (r *StatsReporter) tick(ctx context.Context) {
	stats, err := r.scraper.Scrape(ctx)
	if err != nil {
		r.consecFails++
		switch {
		case r.consecFails == r.staleThreshold:
			// healthy -> stale transition: surface once, loudly. The accurate
			// consequence differs by whether a sample was ever collected: with one
			// in hand IC keeps ranking on it until the index TTL ages it out; with
			// none (all scrapes failed since startup) there is nothing to fall back
			// on and the replica is ranked on residency alone from the start.
			if r.everSucceeded {
				r.logger.Error("engine load stats stale; IC ranking this replica on its last sample until it ages out (index TTL), then residency-only",
					"consecutive_failures", r.consecFails, "err", err)
			} else {
				r.logger.Error("engine load stats unavailable since startup; IC ranking this replica on residency only (no load signal yet)",
					"consecutive_failures", r.consecFails, "err", err)
			}
		case r.consecFails > r.staleThreshold:
			// already surfaced; keep the per-tick detail out of the way.
			r.logger.Debug("engine load stats still unavailable; skipping tick", "err", err)
		default:
			// Don't spam at error level: brief scrape outages are expected during
			// engine restarts and the subscriber must survive them.
			r.logger.Warn("stats scrape failed; skipping tick", "err", err)
		}
		return
	}
	if r.consecFails >= r.staleThreshold {
		r.logger.Info("engine load stats recovered", "after_failures", r.consecFails)
	}
	r.consecFails = 0
	r.everSucceeded = true // a real sample now exists for the fall-back window
	csu := r.cfg.StatsUpdate(r.now().UnixMicro(), stats)
	if csu == nil {
		return
	}
	r.send(ctx, csu)
}

// send opens a fresh, time-bounded ReportCacheState stream and sends exactly
// one stats-only CSU before half-closing. Errors are logged and dropped — the
// next tick will re-emit a fresh sample (soft state).
func (r *StatsReporter) send(ctx context.Context, csu *icpb.CacheStateUpdate) {
	sendCtx, cancel := context.WithTimeout(ctx, r.rpcTimeout)
	defer cancel()

	stream, err := r.client.ReportCacheState(sendCtx)
	if err != nil {
		r.logger.Warn("open stats report stream failed; dropping stats", "err", err)
		return
	}
	if err := stream.Send(csu); err != nil {
		// io.EOF on Send means the server closed the stream early; CloseAndRecv
		// will surface the actual status. Fall through into it rather than
		// returning here so the recv'd reason ends up in the log.
		if !errors.Is(err, context.Canceled) {
			r.logger.Warn("stats send failed; awaiting close for reason", "err", err)
		}
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		r.logger.Warn("stats report close failed", "err", err)
	}
}

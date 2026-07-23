package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
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

	// staleThreshold consecutive ticks that fail to deliver a fresh sample to IC
	// (a failed scrape OR a failed ReportCacheState — either way IC gets nothing)
	// flip the reporter to "stale": it logs once at Error on that transition and
	// once at Info on recovery, guarding against both per-tick log spam and a
	// silently unrefreshed load signal. The transition message depends on
	// everDelivered: once a sample has actually reached IC it keeps ranking on
	// that until the index TTL ages it out; if nothing has reached IC since
	// startup the replica is ranked on residency alone from the outset.
	staleThreshold int
	consecFails    int
	everDelivered  bool
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

// Stable structured log-event values for the stale/recovery transitions. Emitted
// under the "event" key so operators can alert on the machine-readable field
// rather than the human-readable message text (which may be reworded).
const (
	loadSignalStale     = "load_signal_stale"
	loadSignalRecovered = "load_signal_recovered"
)

// errNoStats marks a tick whose scrape succeeded but produced no ReplicaStats to
// report — nothing reaches IC, so it counts as a non-delivery.
var errNoStats = errors.New("scrape produced no stats to report")

// tick scrapes once and delivers one stats-only CacheStateUpdate to IC. A tick
// "succeeds" only when a fresh sample actually reaches IC — a failed scrape and a
// failed delivery (ReportCacheState) are treated identically, since either one
// leaves IC without a fresh load signal for this replica. All failures are
// non-fatal: they log per the stale state machine and the next tick retries.
func (r *StatsReporter) tick(ctx context.Context) {
	stats, err := r.scraper.Scrape(ctx)
	if err != nil {
		r.markStale(err)
		return
	}
	csu := r.cfg.StatsUpdate(r.now().UnixMicro(), stats)
	if csu == nil {
		// The scraper returned no usable stats (the interface permits a nil
		// result). Nothing reaches IC, so this is a non-delivery like a failed
		// scrape — count it toward staleness rather than silently leaving the
		// failure streak untouched.
		r.markStale(errNoStats)
		return
	}
	if err := r.send(ctx, csu); err != nil {
		r.markStale(err)
		return
	}
	r.markDelivered()
}

// markStale records one tick that failed to deliver a fresh sample to IC and
// logs the healthy -> stale transition exactly once, loudly; it then stays quiet
// (Debug) while the outage persists and Warn below the threshold, so a sustained
// loss of load-aware routing is surfaced without per-tick spam.
func (r *StatsReporter) markStale(cause error) {
	r.consecFails++
	switch {
	case r.consecFails == r.staleThreshold:
		// The accurate consequence depends on whether a sample was ever delivered:
		// with one in IC's index it keeps ranking on that until the index TTL ages
		// it out; with none (nothing has reached IC since startup) the replica is
		// ranked on residency alone from the start.
		// Both branches carry event=load_signal_stale so alerting keys on that
		// stable structured field, not the human-readable message text.
		if r.everDelivered {
			r.logger.Error("engine load stats stale; IC ranking this replica on its last delivered sample until it ages out (index TTL), then residency-only",
				"event", loadSignalStale, "consecutive_failures", r.consecFails, "err", cause)
		} else {
			r.logger.Error("engine load stats undelivered since startup; IC has no load sample for this replica (residency-only ranking)",
				"event", loadSignalStale, "consecutive_failures", r.consecFails, "err", cause)
		}
	case r.consecFails > r.staleThreshold:
		// already surfaced; keep the per-tick detail out of the way.
		r.logger.Debug("engine load stats still not reaching IC; skipping tick", "err", cause)
	default:
		// Brief outages are expected during engine/server restarts; don't spam Error.
		r.logger.Warn("engine load stats update failed; will retry", "err", cause)
	}
}

// markDelivered records a fresh sample reaching IC: it logs recovery once if the
// reporter was stale, clears the failure counter, and notes that IC now holds a
// sample for the fall-back window.
func (r *StatsReporter) markDelivered() {
	if r.consecFails >= r.staleThreshold {
		r.logger.Info("engine load stats recovered", "event", loadSignalRecovered, "after_failures", r.consecFails)
	}
	r.consecFails = 0
	r.everDelivered = true
}

// send opens a fresh, time-bounded ReportCacheState stream, delivers exactly one
// stats-only CSU, and half-closes. It returns the delivery error (nil on
// success) so the caller's stale state machine can treat a sample that never
// reached IC the same as a failed scrape. Soft state: the next tick re-emits a
// fresh sample, so a dropped one is never fatal.
func (r *StatsReporter) send(ctx context.Context, csu *icpb.CacheStateUpdate) error {
	sendCtx, cancel := context.WithTimeout(ctx, r.rpcTimeout)
	defer cancel()

	stream, err := r.client.ReportCacheState(sendCtx)
	if err != nil {
		return fmt.Errorf("open stats stream: %w", err)
	}
	// io.EOF on Send means the server closed the stream early; CloseAndRecv
	// carries the real status, so always call it and treat its result as
	// authoritative for whether the sample landed.
	sendErr := stream.Send(csu)
	if _, closeErr := stream.CloseAndRecv(); closeErr != nil {
		return fmt.Errorf("deliver stats: %w", closeErr)
	}
	if sendErr != nil && !errors.Is(sendErr, io.EOF) && !errors.Is(sendErr, context.Canceled) {
		return fmt.Errorf("send stats: %w", sendErr)
	}
	return nil
}

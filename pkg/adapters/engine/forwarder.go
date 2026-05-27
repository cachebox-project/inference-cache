package engine

import (
	"context"
	"log/slog"
	"time"

	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// Reporter forwards decoded KV-cache events to the policy server over gRPC.
//
// Adds (BlockStored) are accumulated and flushed on a short window via a
// long-lived ReportCacheState client stream — this debounces high engine event
// rates into fewer RPCs. Removals (BlockRemoved → PREFIX_EVICTED,
// AllBlocksCleared → ALL_CLEARED) are sent immediately via the unary
// PublishEvent.
//
// It is fail-soft: transport errors are logged and the stream is reconnected
// with backoff; the cache is an optimization and must never stall or fail the
// engine. Run only returns on context cancellation or input close.
type Reporter struct {
	client  icpb.InferenceCacheClient
	cfg     Config
	window  time.Duration
	backoff time.Duration
	maxPend int
	logger  *slog.Logger
}

// ReporterOption configures a Reporter.
type ReporterOption func(*Reporter)

// WithWindow sets the add-batching/debounce flush window (default 100ms).
func WithWindow(d time.Duration) ReporterOption { return func(r *Reporter) { r.window = d } }

// WithBackoff sets the base reconnect backoff (default 1s).
func WithBackoff(d time.Duration) ReporterOption { return func(r *Reporter) { r.backoff = d } }

// WithLogger sets the logger (default slog.Default()).
func WithLogger(l *slog.Logger) ReporterOption { return func(r *Reporter) { r.logger = l } }

// NewReporter builds a Reporter for one engine replica.
func NewReporter(client icpb.InferenceCacheClient, cfg Config, opts ...ReporterOption) *Reporter {
	r := &Reporter{
		client:  client,
		cfg:     cfg,
		window:  100 * time.Millisecond,
		backoff: time.Second,
		maxPend: 4096,
		logger:  slog.Default(),
	}
	for _, o := range opts {
		o(r)
	}
	if r.window <= 0 {
		r.window = 100 * time.Millisecond // guard time.NewTicker
	}
	if r.backoff <= 0 {
		r.backoff = time.Second
	}
	return r
}

// Run consumes decoded event batches until ctx is cancelled or in is closed.
func (r *Reporter) Run(ctx context.Context, in <-chan *EventBatch) error {
	ticker := time.NewTicker(r.window)
	defer ticker.Stop()

	var (
		stream  icpb.InferenceCache_ReportCacheStateClient
		pending []*icpb.PrefixEntry
		pendTs  int64
	)
	defer func() {
		// Final flush on shutdown. The run ctx may be cancelled (so the open
		// stream, bound to it, is dead) — drop it and reopen under a short
		// detached context so the last buffered adds still land.
		r.closeStream(&stream)
		flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		r.flush(flushCtx, &stream, &pending, pendTs)
		r.closeStream(&stream)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case b, ok := <-in:
			if !ok {
				return nil
			}
			tsUs := microsFromSeconds(b.TimestampSeconds)
			for _, ev := range b.Events {
				switch e := ev.(type) {
				case BlockStored:
					pending = append(pending, StoredPrefixes(e)...)
					if tsUs > 0 {
						pendTs = tsUs
					}
					if len(pending) >= r.maxPend {
						r.flush(ctx, &stream, &pending, pendTs)
					}
				case BlockRemoved:
					for _, cev := range r.cfg.RemovedEvents(e, b.TimestampSeconds) {
						r.publish(ctx, cev)
					}
				case AllBlocksCleared:
					// A clear supersedes buffered adds for this replica.
					pending = pending[:0]
					r.publish(ctx, r.cfg.ClearedEvent(b.TimestampSeconds))
				}
			}

		case <-ticker.C:
			r.flush(ctx, &stream, &pending, pendTs)
		}
	}
}

// flush sends the accumulated prefixes as one CacheStateUpdate, (re)opening the
// stream as needed. On a send error it drops the stream so the next flush
// reconnects; the buffered prefixes are dropped (soft state — the engine will
// re-report on its next event, and a miss is never a wrong answer).
func (r *Reporter) flush(ctx context.Context, stream *icpb.InferenceCache_ReportCacheStateClient, pending *[]*icpb.PrefixEntry, tsUs int64) {
	if len(*pending) == 0 {
		return
	}
	update := r.cfg.Update(tsUs, *pending)
	if err := r.ensureStream(ctx, stream); err != nil {
		r.logger.Warn("report stream unavailable; dropping batch", "err", err, "prefixes", len(*pending))
		*pending = (*pending)[:0]
		return
	}
	if err := (*stream).Send(update); err != nil {
		r.logger.Warn("report send failed; reconnecting", "err", err)
		r.closeStream(stream)
		r.sleep(ctx)
	}
	*pending = (*pending)[:0]
}

// ensureStream opens the ReportCacheState stream if it is not already open.
func (r *Reporter) ensureStream(ctx context.Context, stream *icpb.InferenceCache_ReportCacheStateClient) error {
	if *stream != nil {
		return nil
	}
	s, err := r.client.ReportCacheState(ctx)
	if err != nil {
		return err
	}
	*stream = s
	return nil
}

// closeStream half-closes the stream and discards its ack. Errors are ignored —
// we are tearing down.
func (r *Reporter) closeStream(stream *icpb.InferenceCache_ReportCacheStateClient) {
	if *stream == nil {
		return
	}
	_, _ = (*stream).CloseAndRecv()
	*stream = nil
}

// publish sends a single CacheEvent via the unary PublishEvent. Best-effort:
// failures are logged and dropped.
func (r *Reporter) publish(ctx context.Context, ev *icpb.CacheEvent) {
	if _, err := r.client.PublishEvent(ctx, ev); err != nil {
		r.logger.Warn("publish event failed; dropping", "err", err, "type", ev.GetType().String())
	}
}

// sleep waits one backoff interval or until ctx is cancelled.
func (r *Reporter) sleep(ctx context.Context) {
	t := time.NewTimer(r.backoff)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

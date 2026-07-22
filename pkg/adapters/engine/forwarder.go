package engine

import (
	"context"
	"log/slog"
	"time"

	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// Reporter forwards decoded KV-cache events to the policy server over gRPC.
//
// Adds (BlockStored) are accumulated and flushed on a short window — this
// debounces high engine event rates. Each flush sends one CacheStateUpdate on a
// fresh, time-bounded ReportCacheState stream; removals (BlockRemoved →
// PREFIX_EVICTED, AllBlocksCleared → ALL_CLEARED) go via a time-bounded unary
// PublishEvent.
//
// Every RPC uses its own bounded context, so a stalled or unreachable server can
// never block the loop for longer than rpcTimeout — the cache is an optimization
// and must never stall the engine. Errors are logged and dropped (soft state);
// Run only returns on context cancellation or input close.
type Reporter struct {
	client             icpb.InferenceCacheClient
	cfg                Config
	window             time.Duration
	rpcTimeout         time.Duration
	maxPend            int
	logger             *slog.Logger
	ignoreBlockRemoved bool
	// pos derives our positional content fingerprint from each event's tokens and
	// chains it across events. Owned by Run (single goroutine), so unsynchronized.
	pos *positionalIndex
}

// ReporterOption configures a Reporter.
type ReporterOption func(*Reporter)

// WithWindow sets the add-batching/debounce flush window (default 100ms).
func WithWindow(d time.Duration) ReporterOption { return func(r *Reporter) { r.window = d } }

// WithRPCTimeout bounds each gRPC call/flush (default 5s).
func WithRPCTimeout(d time.Duration) ReporterOption { return func(r *Reporter) { r.rpcTimeout = d } }

// WithLogger sets the logger (default slog.Default()).
func WithLogger(l *slog.Logger) ReporterOption { return func(r *Reporter) { r.logger = l } }

// WithIgnoreBlockRemoved drops BlockRemoved events instead of forwarding them as
// PREFIX_EVICTED. Required when the engine is paired with an L2 cache tier
// (e.g. LMCache) that retains a block after the engine evicts it from GPU:
// vLLM emits BlockRemoved on every GPU eviction even when the block is still
// resident at L2, and forwarding that as PREFIX_EVICTED makes the server drop
// a routing hint the replica can still cheaply serve from the L2 tier. With
// the flag set the index keeps the entry until the freshness TTL expires; a
// stale hint is soft state (cache miss at worst, never a wrong answer), while
// a missing one routes the request elsewhere and wastes the L2 cache hit.
func WithIgnoreBlockRemoved(b bool) ReporterOption {
	return func(r *Reporter) { r.ignoreBlockRemoved = b }
}

// NewReporter builds a Reporter for one engine replica.
func NewReporter(client icpb.InferenceCacheClient, cfg Config, opts ...ReporterOption) *Reporter {
	r := &Reporter{
		client:     client,
		cfg:        cfg,
		window:     100 * time.Millisecond,
		rpcTimeout: 5 * time.Second,
		maxPend:    4096,
		logger:     slog.Default(),
		pos:        newPositionalIndex(),
	}
	for _, o := range opts {
		o(r)
	}
	if r.window <= 0 {
		r.window = 100 * time.Millisecond // guard time.NewTicker
	}
	if r.rpcTimeout <= 0 {
		r.rpcTimeout = 5 * time.Second
	}
	return r
}

// Run consumes decoded event batches until ctx is cancelled or in is closed.
// On input close it drains the final buffered adds before returning.
func (r *Reporter) Run(ctx context.Context, in <-chan *EventBatch) error {
	ticker := time.NewTicker(r.window)
	defer ticker.Stop()

	var (
		pending []*icpb.PrefixEntry
		pendTs  int64
	)
	flush := func() {
		if len(pending) == 0 {
			return
		}
		r.sendAdds(pending, pendTs)
		pending = pending[:0]
		pendTs = 0
	}
	defer flush() // final flush on shutdown (bounded inside sendAdds)

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
					// Resolve the engine's internal LoRA id to the stable adapter
					// identity that partitions the index (see Config.AdapterID).
					// nil id → "" → the default partition, i.e. exactly the
					// pre-adapter behavior for every non-LoRA deployment.
					entries := r.pos.Stored(e, r.cfg.AdapterID(e.LoRAID))
					if len(entries) == 0 && len(e.BlockHashes) > 0 {
						// A BlockStored carrying block hashes produced no index
						// entries — either token_ids are absent (engine not emitting
						// them → the fingerprint path silently regresses to all
						// NO_HINT) or the block_hashes / token-block counts disagree.
						// Either is a producer problem on the routing-hit ingest path,
						// so surface it rather than dropping silently.
						r.logger.Warn("BlockStored produced no index entries (missing or mismatched token_ids)",
							"block_hashes", len(e.BlockHashes), "token_ids", len(e.TokenIDs), "block_size", e.BlockSize)
					}
					pending = append(pending, entries...)
					if tsUs > 0 {
						pendTs = tsUs
					}
					if len(pending) >= r.maxPend {
						flush()
					}
				case BlockRemoved:
					// When the engine has an L2 cache tier behind it (e.g. LMCache),
					// BlockRemoved fires every time the engine evicts a block from
					// GPU even though the block is still cached at L2 — forwarding
					// it as PREFIX_EVICTED would drop the routing hint while the
					// replica can still cheaply serve the block. With the flag set
					// the entry stays in the index until the freshness TTL expires;
					// rely on TTL for actual staleness and leave the L2-served hint
					// intact. See WithIgnoreBlockRemoved.
					//
					// Either way the local reverse map MUST drop the evicted blocks:
					// the engine never references a GPU-evicted block as a future
					// parent, so retaining it only grows pos unbounded — and in
					// L2 mode there is no other prune point until AllBlocksCleared.
					// So prune locally even when we suppress the server-side eviction.
					if r.ignoreBlockRemoved {
						r.pos.Removed(e) // prune the reverse map; don't forward (L2 keeps the hint)
						continue
					}
					// Removals are the pruning path and adds are additive, so the
					// eviction must not be overtaken by a still-buffered add of the
					// same block (store-then-evict within one window). Flush pending
					// adds first to preserve store→evict order.
					flush()
					for _, ev := range r.pos.Removed(e) {
						r.publish(r.cfg.EvictedEvent(ev.PrefixHash, ev.AdapterID, b.TimestampSeconds))
					}
				case AllBlocksCleared:
					pending = pending[:0] // a clear supersedes buffered adds
					pendTs = 0
					r.pos.Cleared()
					r.publish(r.cfg.ClearedEvent(b.TimestampSeconds))
				}
			}

		case <-ticker.C:
			flush()
		}
	}
}

// sendAdds sends one CacheStateUpdate on a fresh, time-bounded stream. Errors are
// logged and the batch is dropped (soft state); the next flush retries.
func (r *Reporter) sendAdds(prefixes []*icpb.PrefixEntry, tsUs int64) {
	ctx, cancel := context.WithTimeout(context.Background(), r.rpcTimeout)
	defer cancel()

	stream, err := r.client.ReportCacheState(ctx)
	if err != nil {
		r.logger.Warn("open report stream failed; dropping batch", "err", err, "prefixes", len(prefixes))
		return
	}
	if err := stream.Send(r.cfg.Update(tsUs, prefixes)); err != nil {
		r.logger.Warn("report send failed; dropping batch", "err", err, "prefixes", len(prefixes))
		return
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		r.logger.Warn("report close failed", "err", err)
	}
}

// publish sends a single CacheEvent via a time-bounded unary PublishEvent.
// Best-effort: failures are logged and dropped.
func (r *Reporter) publish(ev *icpb.CacheEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), r.rpcTimeout)
	defer cancel()
	if _, err := r.client.PublishEvent(ctx, ev); err != nil {
		r.logger.Warn("publish event failed; dropping", "err", err, "type", ev.GetType().String())
	}
}

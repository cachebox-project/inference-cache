package engine

import (
	"context"
	"log/slog"
	"time"

	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// Reporter forwards decoded KV-cache events to the policy server over gRPC.
//
// Adds (BlockStored, tagged tier T1) are accumulated and flushed on a short
// window — this debounces high engine event rates. Each flush sends one
// CacheStateUpdate on a fresh, time-bounded ReportCacheState stream. A
// BlockRemoved is handled one of two ways depending on whether the engine has an
// L2 offload tier (WithIgnoreBlockRemoved): with one, the block moved tiers, so it
// is re-reported at T2 through the same ReportCacheState add path; without one it
// is gone, forwarded as PREFIX_EVICTED via a time-bounded unary PublishEvent.
// AllBlocksCleared → ALL_CLEARED, also via PublishEvent. Adds, T2 downgrades, and
// evictions all carry the entry's resolved adapter partition (see positionalIndex).
//
// Every RPC uses its own bounded context, so a stalled or unreachable server can
// never block the loop for longer than rpcTimeout — the cache is an optimization
// and must never stall the engine. Errors are logged and dropped (soft state);
// Run only returns on context cancellation or input close.
type Reporter struct {
	client     icpb.InferenceCacheClient
	cfg        Config
	window     time.Duration
	rpcTimeout time.Duration
	maxPend    int
	logger     *slog.Logger
	// l2TierPresent records whether this replica's engine is paired with an L2
	// offload tier (e.g. LMCache) that retains a block after the engine evicts it
	// from HBM. It changes what a BlockRemoved means: with an L2 tier the block
	// moved tiers (downgrade the hint to T2), without one it is gone (forward
	// PREFIX_EVICTED). Set via WithIgnoreBlockRemoved (the operator flag is
	// --ignore-block-removed; the name predates the T2 downgrade and is kept for
	// compatibility — see that option's doc).
	l2TierPresent bool
	// pos derives our positional content fingerprint from each event's tokens and
	// chains it across events. Owned by Run (single goroutine), so unsynchronized.
	pos *positionalIndex
	// warnedAdapters remembers which unmapped LoRA ids we've already logged a
	// fail-closed drop for, so a hot adapter warns once rather than per event.
	// Owned by Run (single goroutine), so unsynchronized.
	warnedAdapters map[int64]bool
}

// ReporterOption configures a Reporter.
type ReporterOption func(*Reporter)

// WithWindow sets the add-batching/debounce flush window (default 100ms).
func WithWindow(d time.Duration) ReporterOption { return func(r *Reporter) { r.window = d } }

// WithRPCTimeout bounds each gRPC call/flush (default 5s).
func WithRPCTimeout(d time.Duration) ReporterOption { return func(r *Reporter) { r.rpcTimeout = d } }

// WithLogger sets the logger (default slog.Default()).
func WithLogger(l *slog.Logger) ReporterOption { return func(r *Reporter) { r.logger = l } }

// WithIgnoreBlockRemoved declares that this replica's engine is paired with an L2
// offload tier (e.g. LMCache) that retains a block after the engine evicts it from
// HBM. vLLM emits BlockRemoved on every HBM eviction even when the block is still
// resident at L2, so a plain PREFIX_EVICTED would drop a routing hint the replica
// can still cheaply serve from the offload tier. With this set, a BlockRemoved is
// instead re-reported as a T2 entry (reload-able from L2) rather than deleted — the
// hint stays, but honestly tagged colder than HBM, and ages out on the freshness
// TTL like any other entry. Without it (single-tier), a BlockRemoved forwards
// PREFIX_EVICTED (the block is gone). A wrong T2 tag is soft state (a cache miss at
// worst, never a wrong answer); a missing hint routes the request away from its
// warm replica and wastes the L2 hit — the opposite risk, hence the split.
//
// The name (and the operator flag --ignore-block-removed) predates the T2 downgrade
// — earlier this path simply suppressed the eviction and left the entry stale at
// T1. It is kept for backward compatibility; the signal it carries ("this replica
// has an L2 tier") is unchanged.
func WithIgnoreBlockRemoved(b bool) ReporterOption {
	return func(r *Reporter) { r.l2TierPresent = b }
}

// NewReporter builds a Reporter for one engine replica.
func NewReporter(client icpb.InferenceCacheClient, cfg Config, opts ...ReporterOption) *Reporter {
	r := &Reporter{
		client:         client,
		cfg:            cfg,
		window:         100 * time.Millisecond,
		rpcTimeout:     5 * time.Second,
		maxPend:        4096,
		logger:         slog.Default(),
		pos:            newPositionalIndex(),
		warnedAdapters: make(map[int64]bool),
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
					// identity that partitions the index (see Config.AdapterID). A
					// nil id → "" → the default partition, exactly the pre-adapter
					// behavior for every non-LoRA deployment. A non-nil id with no
					// --lora-adapter-names mapping is FAIL-CLOSED: its only identity
					// is a replica-local load-order integer that could alias
					// different adapters across replicas, so drop the event rather
					// than index it under a hazardous partition (warned once per id).
					// The adapter is cached once its id is mapped.
					adapterID, ok := r.cfg.AdapterID(e.LoRAID)
					if !ok {
						r.warnUnmappedAdapter(e.LoRAID)
						continue
					}
					entries := r.pos.Stored(e, adapterID)
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
					// The local reverse map MUST always drop the evicted blocks: the
					// engine never references an HBM-evicted block as a future parent,
					// so retaining it only grows pos unbounded — and in L2 mode there
					// is no other prune point until AllBlocksCleared. Resolve the keys
					// (which also forgets them, and carries each block's adapter
					// partition) before deciding how to report.
					evicted := r.pos.Removed(e)
					if r.l2TierPresent {
						// L2 tier present: the block moved from HBM to the offload tier,
						// it is not gone. Re-report each evicted prefix at T2 (reload-able
						// from L2) in its own adapter partition instead of deleting it —
						// the index applies last-write-wins on tier, moving the entry
						// T1→T2; a later BlockStored re-store re-reports it at T1
						// (upgrade). Nothing to do when the block wasn't ours.
						if len(evicted) == 0 {
							continue
						}
						// Send the downgrade as its OWN CacheStateUpdate carrying THIS
						// eviction's timestamp. A CSU has a single timestamp_us for all
						// its prefixes, so the T2 entry must NOT ride the debounced
						// `pending` buffer: a later BlockStored in the same window would
						// overwrite the shared pendTs and refresh the T2 entry's freshness
						// away from the eviction time — but T2 freshness is meant to be
						// anchored at the eviction (when reload-ability was last
						// confirmed). Flush buffered adds first so store→evict order is
						// preserved on the wire; sendAdds is synchronous (CloseAndRecv
						// waits for the server to ingest), so the downgrade is fully
						// applied before the next event is read.
						flush()
						downgrades := make([]*icpb.PrefixEntry, 0, len(evicted))
						for _, ep := range evicted {
							downgrades = append(downgrades, &icpb.PrefixEntry{
								PrefixHash: ep.PrefixHash,
								TokenCount: ep.TokenCount,
								AdapterId:  ep.AdapterID,
								Tier:       icpb.CacheTier_CACHE_TIER_T2,
							})
						}
						r.sendAdds(downgrades, tsUs)
						continue
					}
					// Single-tier: the block is gone. Removals are a separate unary
					// RPC and adds are additive, so the eviction must not be overtaken
					// by a still-buffered add of the same block (store-then-evict
					// within one window). Flush pending adds first to preserve
					// store→evict order.
					flush()
					for _, ep := range evicted {
						r.publish(r.cfg.EvictedEvent(ep.PrefixHash, ep.AdapterID, b.TimestampSeconds))
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

// warnUnmappedAdapter logs, once per LoRA id, that a BlockStored was dropped
// because the id has no --lora-adapter-names mapping (fail-closed — see
// Config.AdapterID). Run is single-goroutine, so warnedAdapters needs no lock.
func (r *Reporter) warnUnmappedAdapter(loraID *int64) {
	if loraID == nil {
		return
	}
	id := *loraID
	if r.warnedAdapters[id] {
		return
	}
	r.warnedAdapters[id] = true
	r.logger.Warn("dropping BlockStored for an unmapped LoRA id (fail-closed; add it to --lora-adapter-names to cache this adapter)",
		"lora_id", id)
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

package server

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/cachebox-project/inference-cache/pkg/index"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// Reason codes returned on the lookup path (tech spec §4.2 / grpc-contract.md).
// String, not enum — forward-compat per the gRPC contract decision.
const (
	reasonPrefixMatch = "PREFIX_MATCH"
	reasonNoHint      = "NO_HINT"
	reasonTimeout     = "TIMEOUT"
	reasonOK          = "OK"
)

// inferenceCacheService implements the InferenceCache contract
// (docs/design/grpc-contract.md). LookupRoute / ReportCacheState / PublishEvent
// / GetCacheState are backed by the in-memory CacheIndex (B6); the remaining
// RPCs (RenderTemplate, LookupPDRoute, streams) stay fail-open stubs until their
// modules land. All lookups remain side-effect-free apart from emitting metrics
// and fail open — an empty result with NO_HINT (no match / below the configured
// minimumPrefixTokens) or with TIMEOUT (lookupTimeoutMs budget breach) so the
// gateway routes as it normally would.
type inferenceCacheService struct {
	icpb.UnimplementedInferenceCacheServer

	index    *index.Index
	metrics  *serverMetrics
	policies *PolicyStore

	// lookupFn is the index lookup function the handler runs through the
	// goroutine+select wall-time bound. Defaults to s.index.Lookup; tests
	// override it to inject slow lookups that prove the deadline path
	// actually fires.
	lookupFn func(index.LookupRequest) []index.ReplicaScore
}

func newInferenceCacheService(idx *index.Index, metrics *serverMetrics, policies *PolicyStore) *inferenceCacheService {
	return &inferenceCacheService{
		index:    idx,
		metrics:  metrics,
		policies: policies,
		lookupFn: idx.Lookup,
	}
}

// RenderTemplate: no rendering yet (M7). An empty stable_prefix_hash signals the
// caller to fall back to hashing the raw prompt itself.
func (*inferenceCacheService) RenderTemplate(context.Context, *icpb.RenderTemplateRequest) (*icpb.RenderTemplateResponse, error) {
	return &icpb.RenderTemplateResponse{ReasonCode: reasonOK}, nil
}

// LookupRoute consults the index for replicas holding the request's prefix and
// returns them ranked. The handler honors the tenant's CachePolicy:
//
//   - minimumPrefixTokens: a pre-lookup gate on the request's prefix token
//     count. If the request's prefix is shorter than the threshold the index
//     is never touched and the response is NO_HINT. Matches the CRD doc
//     ("minimum prefix token count before lookup", docs/design/policy-crds.md)
//     and avoids spending lock/lookup budget on requests that wouldn't yield
//     a useful hint anyway.
//   - lookupTimeoutMs: a deadline is applied around the lookup. If the caller's
//     ctx is already past its deadline, or if the in-memory lookup exceeds the
//     policy budget, the response is TIMEOUT (still fail-open: empty scores).
//
// A no-match still returns NO_HINT (fail open) — never an error on the hot path.
func (s *inferenceCacheService) LookupRoute(ctx context.Context, req *icpb.LookupRouteRequest) (*icpb.LookupRouteResponse, error) {
	tenant := req.GetTenantId()
	model := req.GetModelId()

	// Pre-lookup gate. Resolve the threshold once and short-circuit on a
	// request that can't clear it — no index lock, no goroutine. A chain
	// request reports its token budget via block_token_counts (the legacy
	// prefix_token_count may be 0); fall back to that sum so chain callers
	// aren't gated out by a zero legacy field.
	if minTokens := s.policyMinimumPrefixTokens(tenant); minTokens > 0 && effectivePrefixTokens(req) < minTokens {
		resp := &icpb.LookupRouteResponse{ReasonCode: reasonNoHint}
		s.metrics.observeLookup(model, resp.ReasonCode, false, 0)
		return resp, nil
	}

	// Apply the per-tenant lookup budget as a derived context deadline so we
	// honor whichever is tighter — the caller's deadline or the policy budget.
	budget := s.policyTimeout(tenant)
	if budget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, budget)
		defer cancel()
	}

	// Fast-path the timeout check: an upstream deadline already breached means
	// running the lookup will produce a stale answer for a caller that has
	// given up. Still fail open (no error).
	if err := ctx.Err(); err != nil {
		return s.timeoutResponse(model, 0), nil
	}

	lookupReq := index.LookupRequest{
		Model:            model,
		Tenant:           tenant,
		HashScheme:       req.GetHashScheme(),
		PrefixHash:       req.GetPrefixHash(),
		TokenCount:       req.GetPrefixTokenCount(),
		BlockHashes:      req.GetBlockHashes(),
		BlockTokenCounts: req.GetBlockTokenCounts(),
	}

	// Default (and dominant) path: no policy budget AND no caller deadline.
	// The in-memory lookup is normally sub-millisecond, so wrapping it in a
	// goroutine + channel every call would just churn allocations and pile
	// up runtime work behind the index lock during a sweep — measurably the
	// hot path for tenants with no CachePolicy. Run synchronously.
	_, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		start := time.Now()
		scores := s.lookupFn(lookupReq)
		return s.buildLookupResponse(model, scores, time.Since(start)), nil
	}

	// Bounded path: a deadline is active, so bound the lookup at wall-clock
	// time. The in-memory lookup takes the index's read lock, which a sweep
	// or large writer can hold — without the goroutine+select the RPC could
	// block past the policy budget and surface a client-side deadline
	// instead of a clean fail-open TIMEOUT.
	start := time.Now()
	type lookupResult struct {
		scores  []index.ReplicaScore
		elapsed time.Duration
	}
	resCh := make(chan lookupResult, 1)
	go func() {
		sc := s.lookupFn(lookupReq)
		resCh <- lookupResult{scores: sc, elapsed: time.Since(start)}
	}()

	var (
		scores  []index.ReplicaScore
		elapsed time.Duration
	)
	select {
	case res := <-resCh:
		// When both resCh AND ctx.Done() are ready, Go's select picks
		// pseudorandomly — so a lookup that overran the deadline could
		// still win and we'd surface stale scores as PREFIX_MATCH.
		// Re-check the deadline before honoring the result.
		if ctx.Err() != nil {
			return s.timeoutResponse(model, time.Since(start)), nil
		}
		scores = res.scores
		elapsed = res.elapsed
		if budget > 0 && elapsed > budget {
			return s.timeoutResponse(model, elapsed), nil
		}
	case <-ctx.Done():
		// Deadline (or upstream cancellation) hit while waiting for the
		// lookup. The goroutine will land eventually with its result
		// discarded; the RPC returns immediately.
		return s.timeoutResponse(model, time.Since(start)), nil
	}

	return s.buildLookupResponse(model, scores, elapsed), nil
}

// buildLookupResponse turns a ranked score set into the proto envelope and
// records the matching metric observation. Shared by the synchronous
// fast-path and the bounded path so the proto shape stays identical across
// both.
func (s *inferenceCacheService) buildLookupResponse(model string, scores []index.ReplicaScore, elapsed time.Duration) *icpb.LookupRouteResponse {
	resp := &icpb.LookupRouteResponse{ReasonCode: reasonNoHint}
	if len(scores) > 0 {
		resp.ReasonCode = reasonPrefixMatch
		resp.ReplicaScores = make([]*icpb.ReplicaScore, 0, len(scores))
		for _, sc := range scores {
			resp.ReplicaScores = append(resp.ReplicaScores, &icpb.ReplicaScore{
				ReplicaId:             sc.ReplicaID,
				Score:                 sc.Score,
				MatchedTokens:         sc.MatchedTokens,
				EstimatedCacheHitProb: sc.EstimatedCacheHitProb,
			})
		}
	}
	resp.LookupLatencyUs = elapsed.Microseconds()
	s.metrics.observeLookup(model, resp.ReasonCode, len(scores) > 0, elapsed)
	return resp
}

// timeoutResponse builds the fail-open TIMEOUT envelope plus its metric
// observation. Kept as a helper because both the pre-lookup deadline-breach
// branch and the post-lookup budget-breach branch share the same shape.
func (s *inferenceCacheService) timeoutResponse(model string, elapsed time.Duration) *icpb.LookupRouteResponse {
	resp := &icpb.LookupRouteResponse{
		ReasonCode:      reasonTimeout,
		LookupLatencyUs: elapsed.Microseconds(),
	}
	s.metrics.observeLookup(model, reasonTimeout, false, elapsed)
	return resp
}

// policyTimeout returns the per-tenant LookupRoute deadline, or 0 if none.
func (s *inferenceCacheService) policyTimeout(tenant string) time.Duration {
	if s.policies == nil {
		return 0
	}
	return s.policies.LookupTimeout(tenant)
}

// policyMinimumPrefixTokens returns the per-tenant threshold, or 0 if none.
func (s *inferenceCacheService) policyMinimumPrefixTokens(tenant string) int32 {
	if s.policies == nil {
		return 0
	}
	return s.policies.MinimumPrefixTokens(tenant)
}

// LookupPDRoute: prefill/decode routing is Phase 2 — fail open.
func (*inferenceCacheService) LookupPDRoute(context.Context, *icpb.LookupPDRouteRequest) (*icpb.LookupPDRouteResponse, error) {
	return &icpb.LookupPDRouteResponse{ReasonCode: reasonNoHint}, nil
}

// GetCacheState returns the aggregate held in the index for a (tenant, model).
func (s *inferenceCacheService) GetCacheState(_ context.Context, req *icpb.GetCacheStateRequest) (*icpb.GetCacheStateResponse, error) {
	replicas, totalPrefixes := s.index.CacheState(req.GetTenantId(), req.GetModelId())

	resp := &icpb.GetCacheStateResponse{
		Summary: &icpb.CacheSummary{TotalPrefixes: int64(totalPrefixes)},
	}
	for _, r := range replicas {
		resp.Replicas = append(resp.Replicas, &icpb.ReplicaStats{
			ReplicaId:        r.ReplicaID,
			CacheMemoryBytes: r.CacheMemoryBytes,
			HitRate:          r.HitRate,
			Pressure:         r.Pressure,
		})
	}
	return resp, nil
}

// ReportCacheState ingests replica update deltas (adds/refreshes; removals
// arrive via PublishEvent or expire by TTL) into the index until the client
// half-closes, then acks. A non-EOF Recv error is propagated.
func (s *inferenceCacheService) ReportCacheState(stream icpb.InferenceCache_ReportCacheStateServer) error {
	for {
		update, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return stream.SendAndClose(&icpb.Ack{Accepted: true})
			}
			return err
		}
		s.index.Ingest(updateFromProto(update))
	}
}

// PublishEvent applies a single cache-state delta to the index.
func (s *inferenceCacheService) PublishEvent(_ context.Context, ev *icpb.CacheEvent) (*icpb.Ack, error) {
	if t := eventTypeFromProto(ev.GetType()); t != 0 {
		s.index.ApplyEvent(index.Event{
			Type:       t,
			ReplicaID:  ev.GetReplicaId(),
			Model:      ev.GetModelId(),
			Tenant:     ev.GetTenantId(),
			PrefixHash: ev.GetPrefixHash(),
			Timestamp:  microsToTime(ev.GetTimestampUs()),
		})
	}
	return &icpb.Ack{Accepted: true}, nil
}

// StreamCacheEvents / StreamMetrics: outbound streaming is M10 — close cleanly.
func (*inferenceCacheService) StreamCacheEvents(*icpb.StreamEventsRequest, icpb.InferenceCache_StreamCacheEventsServer) error {
	return nil
}

func (*inferenceCacheService) StreamMetrics(*icpb.StreamMetricsRequest, icpb.InferenceCache_StreamMetricsServer) error {
	return nil
}

// updateFromProto translates a CacheStateUpdate into the index domain type.
func updateFromProto(u *icpb.CacheStateUpdate) index.Update {
	out := index.Update{
		ReplicaID:  u.GetReplicaId(),
		Model:      u.GetModelId(),
		Tenant:     u.GetTenantId(),
		HashScheme: u.GetHashScheme(),
		Timestamp:  microsToTime(u.GetTimestampUs()),
	}
	for _, p := range u.GetPrefixes() {
		out.Prefixes = append(out.Prefixes, index.PrefixRef{
			PrefixHash:       p.GetPrefixHash(),
			TokenCount:       p.GetTokenCount(),
			BlockHashes:      p.GetBlockHashes(),
			BlockTokenCounts: p.GetBlockTokenCounts(),
		})
	}
	if st := u.GetStats(); st != nil {
		out.Stats = &index.ReplicaStats{
			// Use the top-level replica id (the index key); the nested
			// stats.replica_id is redundant and not trusted for identity.
			ReplicaID:        u.GetReplicaId(),
			CacheMemoryBytes: st.GetCacheMemoryBytes(),
			HitRate:          st.GetHitRate(),
			Pressure:         st.GetPressure(),
		}
	}
	return out
}

// eventTypeFromProto maps the proto enum to the index event type; returns 0 for
// unspecified/unknown (caller skips).
func eventTypeFromProto(t icpb.CacheEvent_Type) index.EventType {
	switch t {
	case icpb.CacheEvent_PREFIX_ADDED:
		return index.EventPrefixAdded
	case icpb.CacheEvent_PREFIX_EVICTED:
		return index.EventPrefixEvicted
	case icpb.CacheEvent_REPLICA_UPDATED:
		return index.EventReplicaUpdated
	case icpb.CacheEvent_ALL_CLEARED:
		return index.EventAllCleared
	default:
		return 0
	}
}

// microsToTime converts a microsecond Unix timestamp to time.Time; 0 → zero
// time (the index treats that as "now").
func microsToTime(us int64) time.Time {
	if us == 0 {
		return time.Time{}
	}
	return time.UnixMicro(us)
}

// effectivePrefixTokens returns the token count the request asserts its
// prefix covers, picking the right field based on whether the request is in
// chain or legacy form. A chain-only request (no prefix_token_count, but
// block_token_counts populated) reports its budget as the sum of per-block
// counts — otherwise the legacy prefix_token_count would zero-out the
// CachePolicy.minimumPrefixTokens gate. Mismatched-length chains are still
// dropped downstream by the index; here we fall back to whichever is set.
func effectivePrefixTokens(req *icpb.LookupRouteRequest) int32 {
	if c := req.GetPrefixTokenCount(); c > 0 {
		return c
	}
	var sum int32
	for _, v := range req.GetBlockTokenCounts() {
		sum += v
	}
	return sum
}

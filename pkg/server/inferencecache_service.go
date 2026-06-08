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
// String, not enum — forward-compat per the gRPC contract decision (a new
// code is a server-only addition; old clients degrade to NO_HINT).
const (
	reasonPrefixMatch = "PREFIX_MATCH"
	reasonTenantHot   = "TENANT_HOT"
	reasonNoHint      = "NO_HINT"
	reasonTimeout     = "TIMEOUT"
	reasonOK          = "OK"

	// Diagnostic codes for LookupRoute contract-key mismatches. Emitted on
	// the miss path when the index can tell the caller that one of
	// (tenant_id, model_id, hash_scheme) does not match any data it holds —
	// distinguishing a misconfigured gateway from a genuine novel prefix.
	// Old clients degrade these to NO_HINT per the forward-compat rule, so
	// callers that have not been updated continue to fail open. See
	// docs/design/lookuproute-diagnostics.md.
	reasonUnknownTenant     = "UNKNOWN_TENANT"
	reasonUnknownModel      = "UNKNOWN_MODEL"
	reasonUnknownHashScheme = "UNKNOWN_HASH_SCHEME"
)

// inferenceCacheService implements the InferenceCache contract
// (docs/design/grpc-contract.md). LookupRoute / ReportCacheState / PublishEvent
// / GetCacheState are backed by the in-memory CacheIndex (B6); the remaining
// RPCs (RenderTemplate, LookupPDRoute, streams) stay fail-open stubs until their
// modules land. All lookups remain side-effect-free apart from emitting metrics
// and fail open — an empty result with NO_HINT (no match; below the configured
// minimumPrefixTokens request-side gate; or every replica's realized
// matched_tokens fell below the per-namespace minimumMatchedTokens
// result-side floor — see docs/design/lookuproute-ranking.md §2.6), with
// TIMEOUT (lookupTimeoutMs budget breach), or with
// one of the diagnostic codes UNKNOWN_TENANT / UNKNOWN_MODEL / UNKNOWN_HASH_SCHEME
// when the lookup misses AND the index can identify which contract key did not
// match anything held (see docs/design/lookuproute-diagnostics.md). Every empty-
// result path fails open the same way: the gateway routes as it normally would
// and the diagnostic codes are advisory.
type inferenceCacheService struct {
	icpb.UnimplementedInferenceCacheServer

	index    *index.Index
	metrics  *serverMetrics
	policies *PolicyStore

	// lookupFn is the index lookup orchestrator the handler runs through the
	// goroutine+select wall-time bound. Defaults to s.index.LookupRoute (which
	// runs the ranking-v2 strategies and emits a Strategy → reason_code); tests
	// override it to inject slow lookups that prove the deadline path actually
	// fires.
	lookupFn func(index.LookupRequest) index.LookupResult
}

func newInferenceCacheService(idx *index.Index, metrics *serverMetrics, policies *PolicyStore) *inferenceCacheService {
	return &inferenceCacheService{
		index:    idx,
		metrics:  metrics,
		policies: policies,
		lookupFn: idx.LookupRoute,
	}
}

// RenderTemplate: no rendering yet (M7). An empty stable_prefix_hash signals the
// caller to fall back to hashing the raw prompt itself.
func (*inferenceCacheService) RenderTemplate(context.Context, *icpb.RenderTemplateRequest) (*icpb.RenderTemplateResponse, error) {
	return &icpb.RenderTemplateResponse{ReasonCode: reasonOK}, nil
}

// LookupRoute consults the index for replicas holding the request's prefix
// and returns them ranked. The handler honors the tenant's CachePolicy and
// runs the ranking-v2 orchestrator (index.LookupRoute) which:
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
//   - Ranking-v2 strategies: the index returns StrategyPrefixMatch (exact
//     prefix hit, scored with the pressure- and SLO-aware formula),
//     StrategyTenantHot (no prefix match but the tenant has recently warm
//     replicas in the requested engine domain — a softer locality hint), or
//     a miss strategy — StrategyUnknownTenant / StrategyUnknownModel /
//     StrategyUnknownHashScheme when the index can identify which contract
//     key did not match anything held, otherwise StrategyNone (the
//     genuine-novel-prefix fail-open default). The handler maps Strategy →
//     reason_code (PREFIX_MATCH / TENANT_HOT / UNKNOWN_TENANT /
//     UNKNOWN_MODEL / UNKNOWN_HASH_SCHEME / NO_HINT) via reasonForStrategy.
//
// Every empty-result code is fail-open — never an error on the hot path.
func (s *inferenceCacheService) LookupRoute(ctx context.Context, req *icpb.LookupRouteRequest) (*icpb.LookupRouteResponse, error) {
	tenant := req.GetTenantId()
	model := req.GetModelId()

	// Reserved probe scope: never serve external LookupRoute queries against
	// the server-internal probe tenant. Without this guard, a caller that
	// knows (or guesses) a backend name could re-derive the deterministic
	// probe hash and observe the synthetic __probe-<backend> replica during
	// a Run, contradicting the "server-internal / never leaks into a real
	// LookupRoute" contract. Fail open with NO_HINT. The metric is still
	// observed (reason_code=NO_HINT, hint_used=false, latency=0) so the
	// "one increment per LookupRoute call" contract on
	// inferencecache_lookup_route_calls_total stays intact — every external
	// LookupRoute call is counted in the unified NO_HINT bucket regardless
	// of which short-circuit produced it. (The metric is labeled by
	// model / reason_code / hint_used only, not tenant_id, so the bucket
	// doesn't isolate "reserved-tenant traffic specifically" today; that
	// would require a schema change owned by the standalone F-series
	// metric work.) The legitimate probe path uses index.LookupRoute
	// directly, not the gRPC handler.
	if tenant == ProbeTenantID {
		resp := &icpb.LookupRouteResponse{ReasonCode: reasonNoHint}
		s.metrics.observeLookup(model, resp.ReasonCode, false, 0)
		return resp, nil
	}

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

	slo := req.GetSlo()
	lookupReq := index.LookupRequest{
		Model:            model,
		Tenant:           tenant,
		HashScheme:       req.GetHashScheme(),
		PrefixHash:       req.GetPrefixHash(),
		TokenCount:       req.GetPrefixTokenCount(),
		BlockHashes:      req.GetBlockHashes(),
		BlockTokenCounts: req.GetBlockTokenCounts(),
		TTFTBudgetMs:     slo.GetTtftMs(),
		TBTBudgetMs:      slo.GetTbtMs(),
	}

	// Default (and dominant) path: no policy budget AND no caller deadline.
	// The in-memory lookup is normally sub-millisecond, so wrapping it in a
	// goroutine + channel every call would just churn allocations and pile
	// up runtime work behind the index lock during a sweep — measurably the
	// hot path for tenants with no CachePolicy. Run synchronously.
	_, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		start := time.Now()
		result := s.lookupFn(lookupReq)
		return s.buildLookupResponse(model, tenant, result, time.Since(start)), nil
	}

	// Bounded path: a deadline is active, so bound the lookup at wall-clock
	// time. The in-memory lookup takes the index's read lock, which a sweep
	// or large writer can hold — without the goroutine+select the RPC could
	// block past the policy budget and surface a client-side deadline
	// instead of a clean fail-open TIMEOUT.
	start := time.Now()
	type boundedResult struct {
		result  index.LookupResult
		elapsed time.Duration
	}
	resCh := make(chan boundedResult, 1)
	go func() {
		r := s.lookupFn(lookupReq)
		resCh <- boundedResult{result: r, elapsed: time.Since(start)}
	}()

	var (
		result  index.LookupResult
		elapsed time.Duration
	)
	select {
	case b := <-resCh:
		// When both resCh AND ctx.Done() are ready, Go's select picks
		// pseudorandomly — so a lookup that overran the deadline could
		// still win and we'd surface stale scores as PREFIX_MATCH.
		// Re-check the deadline before honoring the result.
		if ctx.Err() != nil {
			return s.timeoutResponse(model, time.Since(start)), nil
		}
		result = b.result
		elapsed = b.elapsed
		if budget > 0 && elapsed > budget {
			return s.timeoutResponse(model, elapsed), nil
		}
	case <-ctx.Done():
		// Deadline (or upstream cancellation) hit while waiting for the
		// lookup. The goroutine will land eventually with its result
		// discarded; the RPC returns immediately.
		return s.timeoutResponse(model, time.Since(start)), nil
	}

	return s.buildLookupResponse(model, tenant, result, elapsed), nil
}

// buildLookupResponse turns a LookupResult into the proto envelope and records
// the matching metric observation. Shared by the synchronous fast-path and
// the bounded path so the proto shape stays identical across both. The
// reason_code comes from the index's chosen Strategy (PREFIX_MATCH /
// TENANT_HOT / NO_HINT / UNKNOWN_TENANT / UNKNOWN_MODEL / UNKNOWN_HASH_SCHEME)
// via reasonForStrategy.
func (s *inferenceCacheService) buildLookupResponse(model, tenant string, result index.LookupResult, elapsed time.Duration) *icpb.LookupRouteResponse {
	// Two-stage result-side floor on PREFIX_MATCH responses. Both happen
	// BEFORE CreditHits below so a non-delivered hint never bumps an LFU
	// counter — the no-credit-on-non-delivery invariant.
	//
	// Stage 1 — matched-tokens floor (per-replica). Filters individual
	// replicas whose realized matched_tokens count falls below the
	// per-namespace minimumMatchedTokens floor. The chat-template-only
	// 1-block match (~16 tokens) is the canonical case this catches:
	// a sibling replica that genuinely went deeper on the prefix is kept
	// while the sub-floor sibling is dropped. If no replica clears the
	// floor, the whole response downgrades to NO_HINT.
	//
	// Stage 2 — routing-floor-score (whole-response). Compares the top
	// surviving replica's *score* (matched_tokens × freshness × pressure ×
	// slo_bias × distinguishing_power) against the per-namespace
	// routingFloorScore. The canonical case this catches is the trivial-
	// overlap shape where every replica holds the prefix:
	// distinguishing_power=0 → score=0 → downgrade. Workload-agnostic
	// (works for RAG headers and custom system prompts that the fixed-
	// token-count Stage 1 cannot catch).
	//
	// Order matters: Stage 1 may itself reduce the scored set or downgrade
	// to NO_HINT, in which case Stage 2 naturally skips (StrategyPrefixMatch
	// no longer holds, OR no scores remain). When both fire on the same
	// response Stage 1 takes precedence for per-replica filtering and
	// Stage 2 then re-checks the survivor's score.
	if result.Strategy == index.StrategyPrefixMatch {
		result = s.applyMatchedTokensFloor(result, tenant)
	}
	if result.Strategy == index.StrategyPrefixMatch {
		if floor := s.policyRoutingFloorScore(tenant); floor > 0 && len(result.Scores) > 0 {
			// Scores are sorted descending by Score (see
			// sortScoresDescByScoreThenID in pkg/index), so the first
			// element is the best surviving replica.
			if result.Scores[0].Score < floor {
				// Drop the hits map by constructing a fresh result —
				// the dropped scores must not credit any LFU counter.
				result = index.LookupResult{Strategy: index.StrategyNone}
			}
		}
	}
	// Credit the LFU access counters for the entries this response actually
	// delivers. buildLookupResponse runs on every DELIVERED response (including
	// NO_HINT and the UNKNOWN_* diagnostic responses) but never on the
	// TIMEOUT/early-deadline branches, which return via timeoutResponse — so a
	// lookup the handler discarded for latency never bumps a counter. CreditHits
	// is a no-op unless result carries prefix-match hits (empty for LRU
	// namespaces and for NO_HINT/TENANT_HOT/UNKNOWN_* results).
	result.CreditHits()
	resp := &icpb.LookupRouteResponse{ReasonCode: reasonForStrategy(result.Strategy)}
	if len(result.Scores) > 0 {
		resp.ReplicaScores = make([]*icpb.ReplicaScore, 0, len(result.Scores))
		for _, sc := range result.Scores {
			resp.ReplicaScores = append(resp.ReplicaScores, &icpb.ReplicaScore{
				ReplicaId:             sc.ReplicaID,
				Score:                 sc.Score,
				MatchedTokens:         sc.MatchedTokens,
				EstimatedCacheHitProb: sc.EstimatedCacheHitProb,
			})
		}
	}
	resp.LookupLatencyUs = elapsed.Microseconds()
	s.metrics.observeLookup(model, resp.ReasonCode, len(result.Scores) > 0, elapsed)
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

// policyMinimumMatchedTokens returns the per-tenant matched-tokens floor
// applied to PREFIX_MATCH responses. A nil store skips the floor entirely
// (used by the test scaffolding that wires a service without a PolicyStore);
// otherwise the resolver returns the tenant's configured value, or the
// server-wide DefaultMinimumMatchedTokens when no CachePolicy is set.
func (s *inferenceCacheService) policyMinimumMatchedTokens(tenant string) int32 {
	if s.policies == nil {
		return 0
	}
	return s.policies.MinimumMatchedTokens(tenant)
}

// applyMatchedTokensFloor filters scores below the per-tenant floor and, when
// no replica survives, replaces the result with a fail-open NO_HINT. The
// matching LFU hits are pruned in lockstep via LookupResult.RetainReplicas
// (and dropped entirely on the all-empty downgrade), so a non-delivered
// hint never bumps an LFU counter — preserving the no-credit-on-non-delivery
// invariant even on the partial-keep path where one replica survives and a
// sibling falls below the floor. The check is a no-op when the floor is zero
// (policy opt-out) or when every score already clears the floor — the common
// case for a real long-prefix match.
func (s *inferenceCacheService) applyMatchedTokensFloor(result index.LookupResult, tenant string) index.LookupResult {
	floor := s.policyMinimumMatchedTokens(tenant)
	if floor <= 0 || len(result.Scores) == 0 {
		return result
	}
	// Walk once: classify each score and count survivors. If every score
	// clears the floor we return the original result untouched (zero
	// allocation, common case for a real long-prefix match).
	keep := make(map[string]bool, len(result.Scores))
	survivors := 0
	for _, sc := range result.Scores {
		if sc.MatchedTokens >= floor {
			keep[sc.ReplicaID] = true
			survivors++
		}
	}
	if survivors == len(result.Scores) {
		return result
	}
	if survivors == 0 {
		// No replica cleared the floor — downgrade to a fail-open NO_HINT.
		// Constructing a fresh LookupResult drops the hits map entirely so a
		// non-delivered hint cannot bump an LFU counter.
		return index.LookupResult{Strategy: index.StrategyNone}
	}
	// Partial-keep: prune Scores AND hitsByReplica together so dropped
	// replicas' LFU entries are not credited at CreditHits time.
	result.RetainReplicas(keep)
	return result
}

// policyRoutingFloorScore returns the per-tenant routing floor applied to
// PREFIX_MATCH responses. A nil store skips the floor entirely (the test
// scaffolding that wires a service without a PolicyStore); otherwise the
// resolver returns the tenant's configured value (including the explicit 0
// opt-out) or DefaultRoutingFloorScore when no CachePolicy is set.
func (s *inferenceCacheService) policyRoutingFloorScore(tenant string) float32 {
	if s.policies == nil {
		return 0
	}
	return s.policies.RoutingFloorScore(tenant)
}

// reasonForStrategy maps the index's ranking Strategy onto the gRPC contract's
// reason_code vocabulary. StrategyNone collapses to NO_HINT — the fail-open
// default; an unknown strategy is treated the same so a future Strategy
// addition (e.g. block-level matching) won't surface as a junk reason code
// before its mapping ships. The diagnostic strategies (UNKNOWN_*) surface as
// the matching contract codes — see docs/design/lookuproute-diagnostics.md.
func reasonForStrategy(s index.Strategy) string {
	switch s {
	case index.StrategyPrefixMatch:
		return reasonPrefixMatch
	case index.StrategyTenantHot:
		return reasonTenantHot
	case index.StrategyUnknownTenant:
		return reasonUnknownTenant
	case index.StrategyUnknownModel:
		return reasonUnknownModel
	case index.StrategyUnknownHashScheme:
		return reasonUnknownHashScheme
	default:
		return reasonNoHint
	}
}

// LookupPDRoute: prefill/decode routing is Phase 2 — fail open.
func (*inferenceCacheService) LookupPDRoute(context.Context, *icpb.LookupPDRouteRequest) (*icpb.LookupPDRouteResponse, error) {
	return &icpb.LookupPDRouteResponse{ReasonCode: reasonNoHint}, nil
}

// GetCacheState returns the aggregate held in the index for a (tenant, model).
// Reads against the reserved probe tenant return an empty aggregate so
// in-flight probe state (synthetic replica stats during Stage A / Stage C)
// never reaches an external caller. The legitimate consumer (the controller)
// reads the cluster-wide aggregate via /snapshot, which also filters reserved
// tenants.
func (s *inferenceCacheService) GetCacheState(_ context.Context, req *icpb.GetCacheStateRequest) (*icpb.GetCacheStateResponse, error) {
	if req.GetTenantId() == ProbeTenantID {
		return &icpb.GetCacheStateResponse{Summary: &icpb.CacheSummary{}}, nil
	}
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
//
// Updates whose tenant_id equals the server-reserved probe tenant
// (ProbeTenantID) are DROPPED on ingest. The probe scope is server-internal
// state; an external client must not be able to write to it via the public
// gRPC contract (the in-process Prober.Run writes to the index directly, so
// the legitimate probe path is unaffected). Drops are silent — the contract
// is fail-open everywhere on the hot path — and complement the CacheTenant
// admission rule that rejects a CR claiming the same id at the CRD layer.
func (s *inferenceCacheService) ReportCacheState(stream icpb.InferenceCache_ReportCacheStateServer) error {
	for {
		update, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return stream.SendAndClose(&icpb.Ack{Accepted: true})
			}
			return err
		}
		if update.GetTenantId() == ProbeTenantID {
			continue
		}
		s.index.Ingest(updateFromProto(update))
	}
}

// PublishEvent applies a single cache-state delta to the index. Events
// against the reserved probe tenant are DROPPED (acked, but not applied) so
// an external client cannot fake a PREFIX_EVICTED / ALL_CLEARED that would
// wipe the probe's mid-flight state — the probe re-synthesizes on every Run
// regardless, but the silent drop keeps the public gRPC contract from
// touching server-internal state.
func (s *inferenceCacheService) PublishEvent(_ context.Context, ev *icpb.CacheEvent) (*icpb.Ack, error) {
	if ev.GetTenantId() == ProbeTenantID {
		return &icpb.Ack{Accepted: true}, nil
	}
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
// chain or legacy form. Chain takes precedence over the legacy
// prefix_token_count to match the documented precedence rule: a chain-bearing
// request is a positive assertion of the new form, so a co-set legacy field
// must not override what the chain reports. The handler uses the result to
// gate against CachePolicy.minimumPrefixTokens before touching the index.
func effectivePrefixTokens(req *icpb.LookupRouteRequest) int32 {
	if counts := req.GetBlockTokenCounts(); len(counts) > 0 {
		var sum int32
		for _, v := range counts {
			sum += v
		}
		return sum
	}
	return req.GetPrefixTokenCount()
}

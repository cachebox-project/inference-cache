package server

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"time"

	"github.com/cachebox-project/inference-cache/pkg/fingerprint"
	"github.com/cachebox-project/inference-cache/pkg/index"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/tokenize"
)

// DefaultEngineBlockSize is the KV block size (tokens per block) the server
// assumes when it fingerprints token_ids / tokenized prompt_text on the
// dual-input LookupRoute path. It MUST match the engine's KV block size (and the
// kvevent-subscriber's, which reads it per-event) or the derived block-hash
// chain won't line up with the ingested keys. 16 is vLLM's default.
const DefaultEngineBlockSize = 16

// DefaultTokenizeTimeout bounds server-side prompt_text tokenization when no
// tighter deadline applies (no caller deadline AND no CachePolicy.lookupTimeoutMs).
// Tokenizers are loaded eagerly at startup (not on the request path), so this is
// defense-in-depth: it caps the in-memory chat-template render + encode so a
// pathological prompt can't block a no-deadline LookupRoute — it fails open
// instead. Set a CachePolicy.lookupTimeoutMs for tighter, per-tenant control.
const DefaultTokenizeTimeout = 5 * time.Second

// MaxLookupTokens caps the caller-supplied token_ids a single LookupRoute will
// fingerprint, so an oversized request can't burn CPU/memory on the hot path —
// it fails open to NO_HINT instead. Far above any real model context window
// (~1M tokens); legitimate prompts never approach it.
const MaxLookupTokens = 1 << 20

// MaxPromptTextBytes caps the raw prompt_text a single LookupRoute will tokenize.
// The cgo tokenizer call is not cancellable once it enters Rust, so a timed-out
// LookupRoute returns TIMEOUT while the encode keeps running; bounding the input
// bounds that worst-case in-flight work — an oversized prompt fails open WITHOUT
// entering the tokenizer. ~1 MiB is far above any real prompt and within the gRPC
// receive limit.
const MaxPromptTextBytes = 1 << 20

// MaxConcurrentTokenize caps concurrent prompt_text tokenizations across all
// LookupRoute calls, bounding in-flight (uncancellable) cgo encode work. Beyond
// it the prompt_text path sheds load and fails open to NO_HINT rather than
// accumulating goroutines behind a slow/wedged tokenizer.
const MaxConcurrentTokenize = 64

// Reason codes returned on the lookup path (tech spec §4.2 / grpc-contract.md).
// String, not enum — forward-compat per the gRPC contract decision (a new
// code is a server-only addition; old clients degrade to NO_HINT).
const (
	reasonPrefixMatch         = "PREFIX_MATCH"
	reasonTenantHot           = "TENANT_HOT"
	reasonAffinityHint        = "AFFINITY_HINT"
	reasonNoHint              = "NO_HINT"
	reasonTimeout             = "TIMEOUT"
	reasonOK                  = "OK"
	reasonPolicyRequiresChain = "POLICY_REQUIRES_CHAIN"

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
// minimumPrefixTokens request-side gate; every replica's realized
// matched_tokens fell below the per-namespace minimumMatchedTokens
// result-side floor — see docs/design/lookuproute-ranking.md §2.6; or the
// top per-replica score fell below the per-namespace routingFloorScore
// post-score floor on the distinguishing-power-aware ranker — see
// docs/design/lookuproute-ranking.md §2.7), with POLICY_REQUIRES_CHAIN
// (CachePolicy.spec.strategy.requireChain requires a wire block-hash chain), with
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

	// tokenizer serves the (model, prompt_text) dual-input LookupRoute path:
	// apply the model's chat template, tokenize, then fingerprint. The default
	// build wires tokenize.Unavailable (no cgo), so that path fails open to
	// NO_HINT; a cgo build (tag smgcgo) injects the SMG-backed tokenizer. The
	// pre-tokenized token_ids path needs no tokenizer.
	tokenizer tokenize.Tokenizer

	// blockSize is the KV block size used to fingerprint token_ids / tokenized
	// prompt_text into the block-hash chain. Defaults to DefaultEngineBlockSize.
	blockSize int

	// tokenizeTimeout bounds prompt_text tokenization when no caller deadline or
	// CachePolicy.lookupTimeoutMs applies. Defaults to DefaultTokenizeTimeout;
	// tests override it.
	tokenizeTimeout time.Duration

	// tokenizeSem caps concurrent prompt_text tokenizations. The cgo encode runs
	// in the deadline-bounded resolution goroutine and can't be cancelled, so a
	// slow/wedged tokenizer would otherwise accumulate unbounded in-flight work
	// under load. A non-blocking acquire sheds load: when saturated, the
	// prompt_text path fails open (NO_HINT) instead of piling up goroutines.
	// Buffered to MaxConcurrentTokenize; tests may replace it.
	tokenizeSem chan struct{}
}

func newInferenceCacheService(idx *index.Index, metrics *serverMetrics, policies *PolicyStore) *inferenceCacheService {
	return &inferenceCacheService{
		index:           idx,
		metrics:         metrics,
		policies:        policies,
		lookupFn:        idx.LookupRoute,
		tokenizeSem:     make(chan struct{}, MaxConcurrentTokenize),
		tokenizer:       tokenize.Unavailable{},
		blockSize:       DefaultEngineBlockSize,
		tokenizeTimeout: DefaultTokenizeTimeout,
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
//   - minimumPrefixTokens: a gate on the request's prefix token count. With
//     affinityRouting Disabled, a request shorter than the threshold
//     short-circuits to NO_HINT without touching the index. With
//     affinityRouting Enabled (the default), the request runs the full
//     lookup so the index can classify UNKNOWN_* diagnostics first, then a
//     sub-threshold positive hint is downgraded result-side and the affinity
//     fallback may surface AFFINITY_HINT. Matches the CRD doc ("minimum
//     prefix token count before lookup", docs/design/policy-crds.md).
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
	start := time.Now() // hot-path clock — includes tokenization, reported on every path

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

	// Apply the per-tenant lookup budget as a derived context deadline FIRST so
	// it bounds the WHOLE hot path — including tokenization — not just the index
	// lookup. We honor whichever is tighter: the caller's deadline or the policy
	// budget. (A prior version resolved the prompt_text tokenizer before the
	// budget existed, so a slow first-time tokenizer load could block past the
	// budget instead of failing open.)
	budget := s.policyTimeout(tenant)
	if budget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, budget)
		defer cancel()
	}
	// When prompt_text is the EFFECTIVE input (no higher-precedence chain /
	// prefix_hash / token_ids that would make us ignore it) and nothing else
	// bounds the call, apply a default safety timeout so tokenization can't block
	// the hot path indefinitely — it fails open instead.
	chainMatchingEnabled := s.policyChainMatchingEnabled(tenant)
	willTokenize := req.GetPromptText() != "" &&
		(!chainMatchingEnabled || (len(req.GetBlockHashes()) == 0 && len(req.GetBlockTokenCounts()) == 0)) &&
		len(req.GetPrefixHash()) == 0 && len(req.GetTokenIds()) == 0
	if _, has := ctx.Deadline(); !has && willTokenize && s.tokenizeTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.tokenizeTimeout)
		defer cancel()
	}
	// Fast-path the timeout check: an upstream deadline already breached means
	// any further work serves a caller that has given up. Still fail open.
	if err := ctx.Err(); err != nil {
		return s.timeoutResponse(model, time.Since(start), nil), nil
	}
	if s.policyChainRequired(tenant) && !requestCarriesValidBlockHashChain(req) {
		return s.policyGateResponse(model, reasonPolicyRequiresChain, time.Since(start), nil), nil
	}
	_, hasDeadline := ctx.Deadline()

	// Dual-input resolution: turn token_ids / prompt_text into the block-hash
	// chain the rest of the handler understands (an explicit prefix_hash /
	// block_hashes chain is passed through). The prompt_text path runs the
	// tokenizer encode (tokenizers are pre-loaded at startup, so no per-request
	// I/O), which is still a cgo call that can't be cancelled mid-flight, so when
	// a deadline is active we run resolution in a goroutine and fail open with
	// TIMEOUT rather than block the hot path past the budget. resolveLookupChain
	// does NOT mutate req, so a goroutine that outlives this call shares nothing
	// mutable and is race-free.
	var in lookupInputs
	if hasDeadline {
		ch := make(chan lookupInputs, 1)
		go func() { ch <- s.resolveLookupChain(ctx, req, chainMatchingEnabled) }()
		select {
		case in = <-ch:
		case <-ctx.Done():
			return s.timeoutResponse(model, time.Since(start), nil), nil
		}
	} else {
		in = s.resolveLookupChain(ctx, req, chainMatchingEnabled)
	}
	// A deadline-aware tokenizer may have returned a context error that surfaced
	// as failOpen; if the budget actually expired that is a TIMEOUT, not NO_HINT.
	if ctx.Err() != nil {
		return s.timeoutResponse(model, time.Since(start), in.echoTokens), nil
	}
	if in.failOpen {
		// in.echoTokens is set when a sub-block prompt_text still tokenized
		// successfully (the gateway needs those tokens); nil when the tokenizer
		// was unavailable or errored. Either way any response after the server
		// tokenized carries whatever tokens it produced, and reports the elapsed
		// hot-path time so an expensive tokenizer failure is visible.
		return s.noHintResponse(model, time.Since(start), in.echoTokens), nil
	}

	// Pre-lookup gate on the effective prefix token count (from the resolved
	// chain, falling back to the legacy prefix_token_count). Short-circuit a
	// request that can't clear the threshold — no index lock. Still echo the
	// canonical tokens: a prompt_text caller needs them to call the engine even
	// when there is no routing hint.
	if minTokens := s.policyMinimumPrefixTokens(tenant); minTokens > 0 &&
		effectivePrefixTokens(in.blockTokenCounts, in.exactTokenCount) < minTokens &&
		!s.affinityRoutingEnabled(tenant) {
		return s.noHintResponse(model, time.Since(start), in.echoTokens), nil
	}

	lookupBlockHashes := in.blockHashes
	lookupBlockTokenCounts := in.blockTokenCounts
	if !chainMatchingEnabled {
		lookupBlockHashes = nil
		lookupBlockTokenCounts = nil
	}

	slo := req.GetSlo()
	lookupReq := index.LookupRequest{
		Model:            model,
		Tenant:           tenant,
		HashScheme:       req.GetHashScheme(),
		PrefixHash:       in.exactPrefixHash,
		TokenCount:       in.exactTokenCount,
		BlockHashes:      lookupBlockHashes,
		BlockTokenCounts: lookupBlockTokenCounts,
		TTFTBudgetMs:     slo.GetTtftMs(),
		TBTBudgetMs:      slo.GetTbtMs(),
	}

	// Default (and dominant) path: no deadline → run the in-memory lookup
	// synchronously (it is normally sub-millisecond; a goroutine + channel every
	// call would just churn allocations behind the index lock during a sweep).
	if !hasDeadline {
		result := s.lookupFn(lookupReq)
		result = s.applyLookupStrategyGates(result, tenant)
		return s.buildLookupResponse(req, model, tenant, result, time.Since(start), in), nil
	}

	// Bounded path: a deadline is active, so bound the lookup at wall-clock
	// time. The in-memory lookup takes the index's read lock, which a sweep
	// or large writer can hold — without the goroutine+select the RPC could
	// block past the policy budget and surface a client-side deadline
	// instead of a clean fail-open TIMEOUT.
	lookupStart := time.Now()
	type boundedResult struct {
		result  index.LookupResult
		elapsed time.Duration
	}
	resCh := make(chan boundedResult, 1)
	go func() {
		r := s.lookupFn(lookupReq)
		resCh <- boundedResult{result: r, elapsed: time.Since(lookupStart)}
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
			return s.timeoutResponse(model, time.Since(start), in.echoTokens), nil
		}
		result = b.result
		elapsed = b.elapsed
		if budget > 0 && elapsed > budget {
			return s.timeoutResponse(model, time.Since(start), in.echoTokens), nil
		}
	case <-ctx.Done():
		// Deadline (or upstream cancellation) hit while waiting for the
		// lookup. The goroutine will land eventually with its result
		// discarded; the RPC returns immediately.
		return s.timeoutResponse(model, time.Since(start), in.echoTokens), nil
	}

	// Report total hot-path time (tokenization + lookup), not just the index
	// lookup, so the new prompt_text path isn't under-reported on the latency
	// metric. The budget check above still uses the lookup-only `elapsed`.
	result = s.applyLookupStrategyGates(result, tenant)
	return s.buildLookupResponse(req, model, tenant, result, time.Since(start), in), nil
}

// lookupInputs is the resolved dual-input chain the handler looks up. On the
// explicit-chain path it may alias req's block_hashes / block_token_counts
// slices; that is safe for the deadline-bounded resolution goroutine because
// resolveLookupChain only READS req (never mutates it), and a result produced
// after the handler has already timed out is discarded (never read), so there is
// no concurrent access to the aliased slices.
type lookupInputs struct {
	blockHashes      [][]byte // effective chain (empty → fall back to req.prefix_hash exact match)
	blockTokenCounts []int32  // parallel per-block token counts
	exactPrefixHash  []byte   // effective legacy exact key
	exactTokenCount  int32    // token count for the effective exact key
	echoTokens       []uint32 // canonical tokens to echo (prompt_text path only)
	failOpen         bool     // true → return NO_HINT (tokenizer unavailable/errored)
}

// resolveLookupChain implements the dual-input precedence for LookupRoute and
// returns the block-hash chain to look up — WITHOUT mutating req (so it is
// race-free when run in a deadline-bounded goroutine). Precedence:
//
//  1. An explicit prefix_hash or block_hashes chain (a gateway that already
//     fingerprinted) wins — req's chain is passed through.
//  2. token_ids (pre-tokenized): fingerprinted directly via pkg/fingerprint, no
//     tokenizer needed. Not echoed (the caller already has the tokens).
//  3. prompt_text (raw text): the model's chat template is applied and the text
//     tokenized, then fingerprinted; the canonical tokens are echoed so the
//     caller forwards exactly those to the engine (match by construction).
//
// failOpen is set for the prompt_text path when the tokenizer is unavailable or
// errors, AND for a token_ids / prompt_text input too short to fill one full KV
// block (fingerprint.Chain yields an empty chain): a dual-input caller is asking
// for a specific-prefix lookup, so a sub-block prompt must fail open with
// NO_HINT rather than fall through to the legacy empty-prefix path (which could
// otherwise surface a TENANT_HOT locality hint for a prompt that has no
// cacheable block — symmetric with the "chain misses never fall to TENANT_HOT"
// rule in grpc-contract.md).
func (s *inferenceCacheService) resolveLookupChain(ctx context.Context, req *icpb.LookupRouteRequest, chainMatchingEnabled bool) lookupInputs {
	// An explicit fingerprint attempt: the caller set a prefix_hash and/or a
	// block-hash chain. Pass req's values through to the index (which exact-
	// matches prefix_hash and walks the chain). When chain matching is enabled,
	// a one-sided / malformed chain — block_hashes and block_token_counts present
	// with mismatched lengths, or one set without the other — must fail open with
	// NO_HINT rather than fall through to a lower-precedence input (token_ids /
	// prompt_text) or a TENANT_HOT hint. When chain matching is disabled, chain
	// fields are ignored and the request uses the legacy exact path.
	bh, btc := req.GetBlockHashes(), req.GetBlockTokenCounts()
	exactPrefixHash, exactTokenCount := req.GetPrefixHash(), req.GetPrefixTokenCount()
	ignoredChain := false
	if !chainMatchingEnabled {
		if len(exactPrefixHash) > 0 {
			return lookupInputs{exactPrefixHash: exactPrefixHash, exactTokenCount: exactTokenCount}
		}
		if len(bh) > 0 || len(btc) > 0 {
			ignoredChain = true
			bh = nil
			btc = nil
		}
	}
	if len(bh) > 0 || len(btc) > 0 || len(req.GetPrefixHash()) > 0 {
		if len(bh) != len(btc) {
			return lookupInputs{failOpen: true}
		}
		return lookupInputs{
			blockHashes:      bh,
			blockTokenCounts: btc,
			exactPrefixHash:  exactPrefixHash,
			exactTokenCount:  exactTokenCount,
		}
	}
	if toks := req.GetTokenIds(); len(toks) > 0 {
		if len(toks) > MaxLookupTokens {
			return lookupInputs{failOpen: true} // oversized — don't fingerprint on the hot path
		}
		bh, btc := fingerprint.Chain(toks, s.blockSize)
		if len(bh) == 0 {
			return lookupInputs{failOpen: true} // shorter than one block — nothing the engine can prefix-cache
		}
		return lookupInputs{
			blockHashes:      chainHashesIfEnabled(bh, chainMatchingEnabled),
			blockTokenCounts: chainCountsIfEnabled(btc, chainMatchingEnabled),
			exactPrefixHash:  bh[len(bh)-1],
			exactTokenCount:  sumBlockTokenCounts(btc),
		}
	}
	if text := req.GetPromptText(); text != "" {
		if len(text) > MaxPromptTextBytes {
			return lookupInputs{failOpen: true} // oversized raw prompt — don't enter the (uncancellable) tokenizer
		}
		// Shed load: cap concurrent (uncancellable) tokenizations. A non-blocking
		// acquire means a saturated tokenizer fails open immediately rather than
		// queuing more in-flight cgo work.
		if s.tokenizeSem != nil {
			select {
			case s.tokenizeSem <- struct{}{}:
				defer func() { <-s.tokenizeSem }()
			default:
				return lookupInputs{failOpen: true}
			}
		}
		toks, err := s.tokenizer.Encode(ctx, req.GetModelId(),
			[]tokenize.Message{{Role: "user", Content: text}},
			tokenize.EncodeOptions{AddGenerationPrompt: true})
		if err != nil || len(toks) == 0 || len(toks) > MaxLookupTokens {
			return lookupInputs{failOpen: true}
		}
		bh, btc := fingerprint.Chain(toks, s.blockSize)
		if len(bh) == 0 {
			// Sub-block prompt: no cacheable chain, so fail open — but still echo
			// the tokens we produced; a tokenizer-less gateway needs them to call
			// the engine even though there is no routing hint.
			return lookupInputs{failOpen: true, echoTokens: toks}
		}
		return lookupInputs{
			blockHashes:      chainHashesIfEnabled(bh, chainMatchingEnabled),
			blockTokenCounts: chainCountsIfEnabled(btc, chainMatchingEnabled),
			exactPrefixHash:  bh[len(bh)-1],
			exactTokenCount:  sumBlockTokenCounts(btc),
			echoTokens:       toks,
		}
	}
	if ignoredChain {
		return lookupInputs{failOpen: true}
	}
	return lookupInputs{exactPrefixHash: exactPrefixHash, exactTokenCount: exactTokenCount}
}

func requestCarriesValidBlockHashChain(req *icpb.LookupRouteRequest) bool {
	return len(req.GetBlockHashes()) > 0 && len(req.GetBlockHashes()) == len(req.GetBlockTokenCounts())
}

func chainHashesIfEnabled(hashes [][]byte, enabled bool) [][]byte {
	if !enabled {
		return nil
	}
	return hashes
}

func chainCountsIfEnabled(counts []int32, enabled bool) []int32 {
	if !enabled {
		return nil
	}
	return counts
}

func sumBlockTokenCounts(counts []int32) int32 {
	var sum int32
	for _, count := range counts {
		sum += count
	}
	return sum
}

// buildLookupResponse turns a LookupResult into the proto envelope and records
// the matching metric observation. Shared by the synchronous fast-path and
// the bounded path so the proto shape stays identical across both. The
// reason_code comes from the index's chosen Strategy (PREFIX_MATCH /
// TENANT_HOT / NO_HINT / UNKNOWN_TENANT / UNKNOWN_MODEL / UNKNOWN_HASH_SCHEME)
// via reasonForStrategy.
func (s *inferenceCacheService) buildLookupResponse(req *icpb.LookupRouteRequest, model, tenant string, result index.LookupResult, elapsed time.Duration, in lookupInputs) *icpb.LookupRouteResponse {
	// Stage 0 — minimumPrefixTokens result-side downgrade (affinity path).
	// The pre-lookup short-circuit only fires for affinityRouting=Disabled
	// (see the caller); with affinityRouting=Enabled the index runs first so
	// it can classify UNKNOWN_TENANT / UNKNOWN_MODEL / UNKNOWN_HASH_SCHEME
	// before any fallback (precedence: diagnostic codes > AFFINITY_HINT). The
	// gate's intent — tiny prompts don't surface a positive hint — still must
	// hold after the lookup, so downgrade a sub-threshold PREFIX_MATCH /
	// TENANT_HOT to StrategyNone; the affinity branch below then surfaces
	// AFFINITY_HINT (or NO_HINT). MUST precede CreditHits so a discarded hint
	// never bumps an LFU counter.
	if minTokens := s.policyMinimumPrefixTokens(tenant); minTokens > 0 &&
		effectivePrefixTokens(in.blockTokenCounts, in.exactTokenCount) < minTokens {
		if result.Strategy == index.StrategyPrefixMatch || result.Strategy == index.StrategyTenantHot {
			result = index.LookupResult{Strategy: index.StrategyNone}
		}
	}
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
	// Affinity-routing fallback: a genuine no-match (StrategyNone, from the
	// ranker or downgraded by the gates above) is handed to the consistent-
	// hash picker, which returns AFFINITY_HINT with a stable replica when
	// affinityRouting is Enabled and the request is well-formed with a usable
	// seed + serving replica. Diagnostic strategies keep precedence and are
	// not rewritten. See tryAffinityResponse.
	if result.Strategy == index.StrategyNone {
		if resp := s.tryAffinityResponse(req, in, tenant, model, elapsed); resp != nil {
			return resp
		}
	}
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
				Tier:                  cacheTierToProto(sc.Tier),
			})
		}
	}
	// Echo the canonical tokens the server tokenized (prompt_text path only) so
	// the caller forwards exactly those to the engine — the engine then caches
	// the same tokens this lookup was fingerprinted over. Empty on the
	// token_ids / pre-fingerprinted paths (the caller already has the tokens).
	resp.TokenIds = in.echoTokens
	resp.LookupLatencyUs = elapsed.Microseconds()
	s.metrics.observeLookup(model, resp.ReasonCode, len(result.Scores) > 0, elapsed)
	return resp
}

// tryAffinityResponse builds the consistent-hash AFFINITY_HINT response
// when the per-namespace AffinityRouting toggle is Enabled, the request
// is structurally well-formed (non-empty hash_scheme, balanced chain
// arrays), the prompt fingerprint is non-empty, and the index knows at
// least one replica SERVING the request's (tenant, model, hash_scheme)
// engine domain. Returns nil when any of those preconditions fail, so
// the caller can fall through to its NO_HINT path. Called only from
// buildLookupResponse's post-ranker StrategyNone branch — the
// pre-lookup minimumPrefixTokens short-circuit no longer routes
// through here (it would skip the index's UNKNOWN_HASH_SCHEME
// classification; affinity must run AFTER the full lookup so
// diagnostic codes keep precedence).
//
// The synthetic ReplicaScore carries no hits map and zeroed scoring
// fields by design — there is no cache-evidence claim, only a stable
// assignment. Skipping CreditHits avoids bumping an LFU counter for an
// entry we never actually matched.
//
// Two malformed-request guards mirror the index's fail-soft input
// checks (see affinityEligible): a request with no hash_scheme has no
// engine domain to be stable in (the index also drops these), and a
// chain whose two parallel arrays disagree in length is structurally
// malformed. In both cases the request is gateway misconfiguration,
// not a genuine no-match — handing it a stable replica would paper
// over the bug and mislead operators about cache health.
func (s *inferenceCacheService) tryAffinityResponse(req *icpb.LookupRouteRequest, in lookupInputs, tenant, model string, elapsed time.Duration) *icpb.LookupRouteResponse {
	if !s.affinityRoutingEnabled(tenant) {
		return nil
	}
	if !affinityEligible(req) {
		return nil
	}
	seed := canonicalAffinitySeed(in)
	if seed == nil {
		return nil
	}
	rid, ok := s.index.AffinityHint(tenant, model, req.GetHashScheme(), seed)
	if !ok {
		return nil
	}
	resp := &icpb.LookupRouteResponse{
		ReasonCode:      reasonAffinityHint,
		ReplicaScores:   []*icpb.ReplicaScore{{ReplicaId: rid}},
		LookupLatencyUs: elapsed.Microseconds(),
		// Echo the canonical tokens the server tokenized (prompt_text path) so a
		// tokenizer-less gateway can still forward exactly those to the engine even
		// when the routing hint is an affinity pick. Empty on the block_hashes /
		// token_ids paths (the caller already holds the tokens).
		TokenIds: in.echoTokens,
	}
	s.metrics.observeLookup(model, resp.ReasonCode, true, elapsed)
	return resp
}

// affinityRoutingEnabled returns the per-tenant consistent-hash fallback
// toggle, or DefaultAffinityRoutingEnabled when no PolicyStore is wired.
// Mirrors the shape of policyTimeout / policyRoutingFloorScore so the
// caller stays oblivious to whether a store is attached. See
// CachePolicy.spec.affinityRouting + PolicyStore.AffinityRoutingEnabled.
func (s *inferenceCacheService) affinityRoutingEnabled(tenant string) bool {
	if s.policies == nil {
		return DefaultAffinityRoutingEnabled
	}
	return s.policies.AffinityRoutingEnabled(tenant)
}

// affinityEligible returns whether the LookupRoute request is structurally
// well-formed enough for the consistent-hash fallback to engage. Mirrors
// the index's fail-soft input checks so a misconfigured gateway gets
// NO_HINT (an honest "we have no signal") instead of AFFINITY_HINT
// (a stable replica pick we'd be making up out of malformed input).
//
//   - An empty hash_scheme has no engine domain to be stable within —
//     same prompt content under two different schemes (vLLM vs SGLang)
//     would collapse to one assignment and silently route across
//     incompatible engines.
//   - A chain whose block_hashes and block_token_counts arrays disagree
//     in length is structurally malformed (the same condition the
//     index's chain ingest drops silently). Returning a stable replica
//     for it would hide the gateway bug behind a green-looking metric.
func affinityEligible(req *icpb.LookupRouteRequest) bool {
	// All three contract keys must be set. Unspecified tenant_id /
	// model_id / hash_scheme is a contract violation (the index maps
	// any of them to StrategyNone), so the request has no
	// "engine domain" to be stable within — same fail-open NO_HINT
	// rule as the index applies. Without this guard, an empty-key
	// request could turn into AFFINITY_HINT if servingByScope happens
	// to have entries under that empty scope (e.g. a buggy producer).
	if req.GetTenantId() == "" || req.GetModelId() == "" || req.GetHashScheme() == "" {
		return false
	}
	// Mirror the index's chain-bearing detection: EITHER array being
	// non-empty engages chain mode and both must agree in length. A
	// counts-only request (block_hashes empty, block_token_counts set)
	// is just as malformed as the inverse and must not fall through to
	// prefix_hash + AFFINITY_HINT — that would paper over malformed
	// gateway input.
	bh, bc := req.GetBlockHashes(), req.GetBlockTokenCounts()
	if (len(bh) > 0 || len(bc) > 0) && len(bh) != len(bc) {
		return false
	}
	return true
}

// canonicalAffinitySeed builds the raw canonical fingerprint bytes that
// Index.AffinityHint then SHA-256-hashes exactly once. Returning the
// pre-hash bytes (rather than a pre-computed digest) matches the
// documented contract "SHA-256 over the canonical sequence" — the
// SHA-256 happens inside AffinityHint, not here. An operator who logs
// the seed bytes can independently compute the same SHA-256 and
// reproduce the routing decision, which is the debuggability story.
//
// Encoding rules:
//   - in.blockHashes non-empty (the resolved chain — passed through for
//     explicit-chain callers, server-derived from token_ids / prompt_text):
//     for each hash, BigEndian uint32(len) then the hash bytes —
//     length-prefixed because proto bytes have no fixed width (vLLM hashes
//     are 8B, SGLang may differ) and pure concat would let [ab,cd] collide
//     with [abcd].
//   - in.blockHashes empty AND in.exactPrefixHash non-empty: a fresh copy
//     of the resolved exact-prefix bytes (legacy single-hash callers).
//     Copying defends against any caller that mutates the inputs later.
//   - Both empty: return nil so AffinityHint returns ok=false and the
//     handler falls through to NO_HINT.
//
// (tenant, model, hash_scheme) are NOT mixed into the seed bytes
// because the replica set AffinityHint chooses from is already filtered
// by (tenant, model, hash_scheme) — those coordinates select the
// candidate set; the seed selects the entry within it.
func canonicalAffinitySeed(in lookupInputs) []byte {
	if bh := in.blockHashes; len(bh) > 0 {
		total := 0
		for _, b := range bh {
			total += 4 + len(b)
		}
		out := make([]byte, 0, total)
		var lenBuf [4]byte
		for _, b := range bh {
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(b)))
			out = append(out, lenBuf[:]...)
			out = append(out, b...)
		}
		return out
	}
	if ph := in.exactPrefixHash; len(ph) > 0 {
		out := make([]byte, len(ph))
		copy(out, ph)
		return out
	}
	return nil
}

// timeoutResponse builds the fail-open TIMEOUT envelope plus its metric
// observation. Kept as a helper because both the pre-lookup deadline-breach
// branch and the post-lookup budget-breach branch share the same shape.
func (s *inferenceCacheService) timeoutResponse(model string, elapsed time.Duration, echoTokens []uint32) *icpb.LookupRouteResponse {
	resp := &icpb.LookupRouteResponse{
		ReasonCode:      reasonTimeout,
		LookupLatencyUs: elapsed.Microseconds(),
		// Echo the canonical tokens when the server already tokenized prompt_text
		// (nil otherwise) so the caller can still forward them to the engine even
		// though the routing hint timed out.
		TokenIds: echoTokens,
	}
	s.metrics.observeLookup(model, reasonTimeout, false, elapsed)
	return resp
}

// noHintResponse builds a fail-open NO_HINT envelope that reports the real
// elapsed hot-path time (so an expensive tokenizer load/failure is visible on
// inferencecache_lookup_route_latency_seconds) and echoes the canonical tokens
// when the server tokenized prompt_text. Used by the post-tokenization fail-open
// and min-prefix-gate paths; the pre-tokenization early returns (probe, empty
// input) keep their zero-latency NO_HINT.
func (s *inferenceCacheService) noHintResponse(model string, elapsed time.Duration, echoTokens []uint32) *icpb.LookupRouteResponse {
	resp := &icpb.LookupRouteResponse{
		ReasonCode:      reasonNoHint,
		LookupLatencyUs: elapsed.Microseconds(),
		TokenIds:        echoTokens,
	}
	s.metrics.observeLookup(model, reasonNoHint, false, elapsed)
	return resp
}

// policyGateResponse builds an empty fail-open response for a CachePolicy gate
// that deliberately uses a more specific reason_code than NO_HINT. The gateway
// behavior is still the same: no replica scores, route normally.
func (s *inferenceCacheService) policyGateResponse(model, reason string, elapsed time.Duration, echoTokens []uint32) *icpb.LookupRouteResponse {
	resp := &icpb.LookupRouteResponse{
		ReasonCode:      reason,
		LookupLatencyUs: elapsed.Microseconds(),
		TokenIds:        echoTokens,
	}
	s.metrics.observeLookup(model, reason, false, elapsed)
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

func (s *inferenceCacheService) policyChainMatchingEnabled(tenant string) bool {
	if s.policies == nil {
		return DefaultEnableChainMatching
	}
	return s.policies.ChainMatchingEnabled(tenant)
}

func (s *inferenceCacheService) policyChainRequired(tenant string) bool {
	if s.policies == nil {
		return DefaultRequireChain
	}
	return s.policies.ChainRequired(tenant)
}

func (s *inferenceCacheService) policyTenantHotEnabled(tenant string) bool {
	if s.policies == nil {
		return DefaultEnableTenantHot
	}
	return s.policies.TenantHotEnabled(tenant)
}

func (s *inferenceCacheService) applyLookupStrategyGates(result index.LookupResult, tenant string) index.LookupResult {
	if result.Strategy == index.StrategyTenantHot && !s.policyTenantHotEnabled(tenant) {
		return index.LookupResult{Strategy: index.StrategyNone}
	}
	return result
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
			T2HitTokens:      r.T2HitTokens,
			T2QueryTokens:    r.T2QueryTokens,
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
			T2HitTokens:      st.GetT2HitTokens(),
			T2QueryTokens:    st.GetT2QueryTokens(),
		}
	}
	return out
}

// cacheTierToProto maps the index's cache-tier tag onto the wire enum. An
// unknown/zero tier maps to CACHE_TIER_UNSPECIFIED so a non-prefix hint
// (TENANT_HOT / AFFINITY_HINT) and any future value stay backwards-compatible.
func cacheTierToProto(t index.CacheTier) icpb.CacheTier {
	switch t {
	case index.TierT1:
		return icpb.CacheTier_CACHE_TIER_T1
	case index.TierT2:
		return icpb.CacheTier_CACHE_TIER_T2
	case index.TierT3:
		return icpb.CacheTier_CACHE_TIER_T3
	default:
		return icpb.CacheTier_CACHE_TIER_UNSPECIFIED
	}
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

// effectivePrefixTokens returns the token count the request asserts its prefix
// covers, given the resolved per-block token counts (from the dual-input chain)
// and the legacy prefix_token_count. The chain takes precedence: a chain-bearing
// request is a positive assertion of the new form, so a co-set legacy field must
// not override what the chain reports. The handler uses the result to gate
// against CachePolicy.minimumPrefixTokens before touching the index.
func effectivePrefixTokens(blockTokenCounts []int32, legacyCount int32) int32 {
	if len(blockTokenCounts) > 0 {
		var sum int32
		for _, v := range blockTokenCounts {
			sum += v
		}
		return sum
	}
	return legacyCount
}

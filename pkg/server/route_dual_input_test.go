package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cachebox-project/inference-cache/pkg/adapters/engine"
	"github.com/cachebox-project/inference-cache/pkg/fingerprint"
	"github.com/cachebox-project/inference-cache/pkg/index"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/tokenize"
)

// fakeTokenizer is a test double for the server-side tokenizer: it returns
// canned tokens (or an error) regardless of input, so handler tests can pin the
// (model, prompt_text) path without a real cgo tokenizer.
type fakeTokenizer struct {
	tokens []uint32
	err    error
}

func (f fakeTokenizer) Encode(context.Context, string, []tokenize.Message, tokenize.EncodeOptions) ([]uint32, error) {
	return f.tokens, f.err
}

func (f fakeTokenizer) EncodeText(context.Context, string, string, tokenize.EncodeOptions) ([]uint32, error) {
	return f.tokens, f.err
}

// blockingTokenizer blocks Encode until released, simulating a slow first-time
// tokenizer load that a non-cancellable cgo call can't interrupt mid-flight.
type blockingTokenizer struct{ release chan struct{} }

func (b blockingTokenizer) Encode(context.Context, string, []tokenize.Message, tokenize.EncodeOptions) ([]uint32, error) {
	<-b.release
	return nil, nil
}

func (b blockingTokenizer) EncodeText(context.Context, string, string, tokenize.EncodeOptions) ([]uint32, error) {
	<-b.release
	return nil, nil
}

// A slow tokenizer on the prompt_text path must be bounded by the tenant's
// lookupTimeoutMs and fail open with TIMEOUT — never block the hot path past the
// budget. Guards the deadline-ordering fix (tokenization happens under the
// budget context, not before it).
func TestLookupRoutePromptTextSlowTokenizerTimesOut(t *testing.T) {
	svc := newTestService()
	svc.policies.Replace([]ResolvedPolicy{{Namespace: "tenant-x", LookupTimeoutMs: 20}})
	release := make(chan struct{})
	defer close(release) // unblock the tokenizer goroutine at test end
	svc.tokenizer = blockingTokenizer{release: release}

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "tenant-x", HashScheme: "vllm", PromptText: "hello world",
	})
	if err != nil {
		t.Fatalf("LookupRoute must not error on the hot path: %v", err)
	}
	if resp.GetReasonCode() != "TIMEOUT" {
		t.Errorf("reason = %q, want TIMEOUT (slow tokenizer must be bounded by lookupTimeoutMs)", resp.GetReasonCode())
	}
}

// ingestFingerprintPrefix stores tokens as per-block positional prefix entries
// for replica on (model, tenant, scheme) — mirroring what the kvevent-subscriber
// ingests for a cached prefix, so a content-fingerprint lookup matches.
func ingestFingerprintPrefix(idx *index.Index, replica, model, tenant, scheme string, tokens []uint32, blockSize int) {
	phs := fingerprint.PrefixHashes(tokens, blockSize)
	prefixes := make([]index.PrefixRef, len(phs))
	for i, ph := range phs {
		prefixes[i] = index.PrefixRef{PrefixHash: fingerprint.Bytes(ph), TokenCount: int32((i + 1) * blockSize)}
	}
	idx.Ingest(index.Update{ReplicaID: replica, Model: model, Tenant: tenant, HashScheme: scheme, Prefixes: prefixes})
}

// A pre-tokenized LookupRoute (token_ids) must produce exactly the same hit as
// an explicit block-hash chain over the same tokens — the server fingerprints
// the tokens itself. This is the by-construction match: a gateway can send the
// tokens it will forward to the engine and the lookup keys line up with the
// engine's cached prefix.
func TestLookupRouteTokenIDsEqualsExplicitChain(t *testing.T) {
	const (
		modelID  = "vllm-model"
		tenantID = "ic-smoke"
		scheme   = "vllm"
		replica  = "vllm-engine-cs1"
		blockSz  = 16
	)
	tokens := tokenSeq(1_000, 64) // 4 blocks, matched_tokens=64 clears the default floor

	batch := &engine.EventBatch{
		TimestampSeconds: 0,
		Events: []engine.Event{engine.BlockStored{
			BlockHashes: [][]byte{be8(1), be8(2), be8(3), be8(4)},
			TokenIDs:    tokens,
			BlockSize:   blockSz,
		}},
	}
	client, stop := runEngineReporterAgainstServer(t,
		[]engine.ReporterOption{engine.WithIgnoreBlockRemoved(true)}, batch)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	byTokens, err := client.LookupRoute(ctx, &icpb.LookupRouteRequest{
		ModelId: modelID, TenantId: tenantID, HashScheme: scheme, TokenIds: tokens,
	})
	if err != nil {
		t.Fatalf("LookupRoute(token_ids): %v", err)
	}

	bh, btc := fingerprint.Chain(tokens, blockSz)
	byChain, err := client.LookupRoute(ctx, &icpb.LookupRouteRequest{
		ModelId: modelID, TenantId: tenantID, HashScheme: scheme,
		BlockHashes: bh, BlockTokenCounts: btc,
	})
	if err != nil {
		t.Fatalf("LookupRoute(chain): %v", err)
	}

	if byTokens.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("token_ids reason = %q, want PREFIX_MATCH", byTokens.GetReasonCode())
	}
	if byChain.GetReasonCode() != byTokens.GetReasonCode() {
		t.Fatalf("chain reason %q != token_ids reason %q", byChain.GetReasonCode(), byTokens.GetReasonCode())
	}
	if len(byTokens.GetReplicaScores()) != 1 || byTokens.GetReplicaScores()[0].GetReplicaId() != replica {
		t.Fatalf("token_ids scores = %+v, want single hit on %s", byTokens.GetReplicaScores(), replica)
	}
	if got, want := byTokens.GetReplicaScores()[0].GetMatchedTokens(), int32(64); got != want {
		t.Fatalf("token_ids matched_tokens = %d, want %d", got, want)
	}
	// The token_ids path does not echo tokens back — the caller already has them.
	if len(byTokens.GetTokenIds()) != 0 {
		t.Errorf("token_ids path echoed %d tokens, want 0", len(byTokens.GetTokenIds()))
	}
}

// A novel pre-tokenized prefix the server has never seen must fail open.
func TestLookupRouteTokenIDsNovelMisses(t *testing.T) {
	stored := tokenSeq(1_000, 64)
	batch := &engine.EventBatch{Events: []engine.Event{engine.BlockStored{
		BlockHashes: [][]byte{be8(1), be8(2), be8(3), be8(4)}, TokenIDs: stored, BlockSize: 16,
	}}}
	client, stop := runEngineReporterAgainstServer(t,
		[]engine.ReporterOption{engine.WithIgnoreBlockRemoved(true)}, batch)
	defer stop()

	resp, err := client.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "vllm-model", TenantId: "ic-smoke", HashScheme: "vllm",
		TokenIds: tokenSeq(90_000_000, 64), // never stored
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Errorf("novel token_ids reason = %q, want NO_HINT", resp.GetReasonCode())
	}
}

// An explicit block-hash chain (a gateway that already fingerprinted) takes
// precedence over token_ids. We supply a matching chain AND a novel token_ids;
// if precedence holds the chain wins and we PREFIX_MATCH. If token_ids wrongly
// overrode it the server would fingerprint the novel tokens and miss.
func TestLookupRouteExplicitChainBeatsTokenIDs(t *testing.T) {
	stored := tokenSeq(1_000, 64)
	batch := &engine.EventBatch{Events: []engine.Event{engine.BlockStored{
		BlockHashes: [][]byte{be8(1), be8(2), be8(3), be8(4)}, TokenIDs: stored, BlockSize: 16,
	}}}
	client, stop := runEngineReporterAgainstServer(t,
		[]engine.ReporterOption{engine.WithIgnoreBlockRemoved(true)}, batch)
	defer stop()

	bh, btc := fingerprint.Chain(stored, 16)
	resp, err := client.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "vllm-model", TenantId: "ic-smoke", HashScheme: "vllm",
		BlockHashes: bh, BlockTokenCounts: btc,
		TokenIds: tokenSeq(90_000_000, 64), // novel; must be ignored
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Errorf("reason = %q, want PREFIX_MATCH (explicit chain must win over token_ids)", resp.GetReasonCode())
	}
}

// The (model, prompt_text) path: the server applies the tokenizer, fingerprints
// the result, matches the cached prefix, AND echoes the canonical tokens back so
// the caller can forward exactly those to the engine.
func TestLookupRoutePromptTextTokenizesAndEchoes(t *testing.T) {
	const (
		modelID  = "m"
		tenantID = "tenant-x"
		scheme   = "vllm"
		replica  = "r1"
		blockSz  = 16
	)
	tokens := tokenSeq(2_000_000, 64)

	svc := newTestService()
	ingestFingerprintPrefix(svc.index, replica, modelID, tenantID, scheme, tokens, blockSz)
	svc.tokenizer = fakeTokenizer{tokens: tokens}

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: modelID, TenantId: tenantID, HashScheme: scheme, PromptText: "hello world",
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("reason = %q, want PREFIX_MATCH", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 1 || resp.GetReplicaScores()[0].GetReplicaId() != replica {
		t.Fatalf("scores = %+v, want single hit on %s", resp.GetReplicaScores(), replica)
	}
	if !equalU32(resp.GetTokenIds(), tokens) {
		t.Errorf("echoed token_ids = %v, want %v", resp.GetTokenIds(), tokens)
	}
}

// The server's configured block size (not a hardcoded 16) drives token_ids
// fingerprinting: a prefix ingested at block size 32 matches a token_ids lookup
// only when the service fingerprints at the same 32.
func TestLookupRouteTokenIDsHonorsConfiguredBlockSize(t *testing.T) {
	const blockSz = 32
	tokens := tokenSeq(3_000, 64) // 2 blocks of 32, matched_tokens=64 clears the floor

	svc := newTestService()
	svc.blockSize = blockSz
	ingestFingerprintPrefix(svc.index, "r1", "m", "tenant-x", "vllm", tokens, blockSz)

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "tenant-x", HashScheme: "vllm", TokenIds: tokens,
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("reason = %q, want PREFIX_MATCH at block size %d", resp.GetReasonCode(), blockSz)
	}

	// The default-16 service must NOT match the block-32 ingest (different chunking).
	svc16 := newTestService()
	ingestFingerprintPrefix(svc16.index, "r1", "m", "tenant-x", "vllm", tokens, blockSz)
	resp16, err := svc16.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "tenant-x", HashScheme: "vllm", TokenIds: tokens,
	})
	if err != nil {
		t.Fatalf("LookupRoute (bs16): %v", err)
	}
	if resp16.GetReasonCode() == "PREFIX_MATCH" {
		t.Errorf("block-16 service matched a block-32 ingest — block size must affect chunking")
	}
}

// No cgo tokenizer (the default build): the (model, prompt_text) path fails open
// to NO_HINT rather than erroring on the hot path.
func TestLookupRoutePromptTextUnavailableTokenizerFailsOpen(t *testing.T) {
	tokens := tokenSeq(2_000_000, 64)
	svc := newTestService() // default tokenizer is Unavailable
	ingestFingerprintPrefix(svc.index, "r1", "m", "tenant-x", "vllm", tokens, 16)

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "tenant-x", HashScheme: "vllm", PromptText: "hello world",
	})
	if err != nil {
		t.Fatalf("LookupRoute must not error on the hot path: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Errorf("reason = %q, want NO_HINT (fail-open when tokenizer unavailable)", resp.GetReasonCode())
	}
	if len(resp.GetTokenIds()) != 0 {
		t.Errorf("unavailable path echoed tokens, want none")
	}
}

// A tokenizer error (e.g. unknown model artifacts) also fails open.
func TestLookupRoutePromptTextTokenizerErrorFailsOpen(t *testing.T) {
	svc := newTestService()
	svc.tokenizer = fakeTokenizer{err: errors.New("boom")}

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "tenant-x", HashScheme: "vllm", PromptText: "hello",
	})
	if err != nil {
		t.Fatalf("LookupRoute must not error: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Errorf("reason = %q, want NO_HINT (fail-open on tokenizer error)", resp.GetReasonCode())
	}
}

func equalU32(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

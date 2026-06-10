// Package engineclient sends a PRE-TOKENIZED prompt (token IDs) to an inference
// engine. It is the "pass tokens to the engine" half of server-side tokenization:
// the engine caches exactly the tokens the router fingerprinted, so the routing
// key and the engine's cache key match by construction — no tokenizer-parity
// dependence between router and engine.
//
// It is a library, not a request proxy: the inference-cache server never calls
// it on the hot path. A gateway, benchmark, or canary drives the flow
// (tokenize → fingerprint → LookupRoute → pick replica → Complete).
package engineclient

import (
	"context"
	"errors"
)

// CompletionParams carries the sampling knobs a caller sets per request. Kept
// minimal on purpose — this is a routing/cache demonstrator, not a full
// inference SDK.
type CompletionParams struct {
	MaxTokens   int
	Temperature float32
}

// Completion is the engine's response, trimmed to what callers need to confirm
// a request landed and to read usage.
type Completion struct {
	Text             string
	FinishReason     string
	PromptTokens     int
	CompletionTokens int
}

// EngineClient sends pre-tokenized input to one engine replica.
type EngineClient interface {
	// Complete sends tokenIDs as the prompt to the engine at endpoint and returns
	// the completion. The engine MUST treat tokenIDs as the input verbatim (no
	// re-tokenization) so the cached prefix equals the fingerprinted tokens.
	Complete(ctx context.Context, endpoint, model string, tokenIDs []uint32, p CompletionParams) (Completion, error)
}

// ErrNotImplemented is returned by clients whose transport is not wired yet.
var ErrNotImplemented = errors.New("engineclient: not implemented")

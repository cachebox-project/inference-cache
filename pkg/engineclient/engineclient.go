// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

// Package engineclient sends a pre-tokenized prompt (token IDs) to an inference
// engine. It is the "pass tokens to the engine" half of server-side tokenization:
// the engine caches exactly the tokens the router fingerprinted, so the routing
// key and the engine's cache key match by construction — no tokenizer-parity
// dependence between router and engine.
//
// It is a library, not a request proxy: the inference-cache server never calls
// it on the hot path. A gateway, benchmark, or canary drives the flow
// (tokenize → fingerprint → LookupRoute → pick replica → Complete).
//
// The supported boundary is deliberately narrow: EngineClient, CompletionParams,
// Completion, OpenAIClient, NewOpenAI, and the pre-tokenized OpenAI-compatible
// /v1/completions mapping. It does not promise authentication, retries, endpoint
// discovery, load balancing, streaming, tracing, or a complete OpenAI API SDK.
package engineclient

import "context"

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

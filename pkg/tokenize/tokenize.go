// Package tokenize is the server-owned tokenization seam: it turns a
// (model, prompt) request into engine-aligned token IDs so the lookup path can
// fingerprint over the same tokens the engine caches.
//
// The interface is pure Go. The production implementation is cgo-backed by SMG's
// llm-tokenizer (build tag `smgcgo`, see cgo_smg.go) — HF/tiktoken loaders plus
// chat-template rendering, reused rather than reimplemented. Default builds ship
// no cgo dependency and hold an Unavailable tokenizer, so the binary compiles
// and runs without a Rust toolchain; the (model, prompt_text) LookupRoute path
// then fails open to NO_HINT while the pre-tokenized token_ids path keeps
// working (it needs only pkg/fingerprint, which is pure Go).
package tokenize

import (
	"context"
	"errors"
)

// Message is one chat message fed to the model's chat template before
// tokenization. The single-turn (model, prompt_text) lookup path wraps the
// prompt as a single user Message; richer multi-turn structured input is a
// future wire addition.
type Message struct {
	Role    string
	Content string
}

// EncodeOptions tunes tokenization.
type EncodeOptions struct {
	// AddGenerationPrompt appends the assistant generation prompt when applying
	// the chat template — matching what a chat-completion frontend does before
	// the engine tokenizes and caches, so our fingerprint tokens line up.
	AddGenerationPrompt bool
}

// Tokenizer renders a model's chat template and tokenizes into engine-aligned
// token IDs. Implementations MUST be safe for concurrent use.
type Tokenizer interface {
	// Encode applies model's chat template to msgs, then tokenizes the rendered
	// text. This is the path that matches a chat engine's cached tokens.
	Encode(ctx context.Context, model string, msgs []Message, opts EncodeOptions) ([]uint32, error)

	// EncodeText tokenizes already-rendered text with no chat template applied —
	// for base/completion models and the approximate text-prefix fallback.
	EncodeText(ctx context.Context, model, text string, opts EncodeOptions) ([]uint32, error)
}

// ErrUnavailable is returned when no tokenizer can serve the request — the
// default (non-cgo) build, or a model whose tokenizer artifacts are missing.
// The lookup hot path treats it as fail-open (NO_HINT), never an RPC error.
var ErrUnavailable = errors.New("tokenize: tokenizer unavailable")

// Config configures the tokenizer constructed by New.
type Config struct {
	// ModelsDir is the directory of vetted per-model tokenizer artifacts
	// (<ModelsDir>/<model_id>/tokenizer.json; model_id may contain a namespace
	// separator). The cgo (smgcgo) build eagerly loads every tokenizer under it
	// at startup and serves them from memory, confined to this directory. Empty
	// disables server-side tokenization (New returns Unavailable). The default
	// (non-cgo) build ignores this field.
	ModelsDir string
}

// Unavailable is the default Tokenizer: every call fails open with
// ErrUnavailable. It lets the server build and run without the cgo tokenizer.
type Unavailable struct{}

// Encode always returns ErrUnavailable.
func (Unavailable) Encode(context.Context, string, []Message, EncodeOptions) ([]uint32, error) {
	return nil, ErrUnavailable
}

// EncodeText always returns ErrUnavailable.
func (Unavailable) EncodeText(context.Context, string, string, EncodeOptions) ([]uint32, error) {
	return nil, ErrUnavailable
}

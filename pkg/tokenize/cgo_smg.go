//go:build smgcgo

// This file is the cgo-backed Tokenizer, compiled only with the `smgcgo` build
// tag. It links the IC-owned `ictokenizer` Rust static archive (rust/ictokenizer),
// which depends solely on SMG's llm-tokenizer crate. The default build excludes
// this file and uses Unavailable instead, so the server needs no Rust toolchain.
//
// Build prerequisite: the static archive must exist and be on the linker path,
// e.g. `cargo build --release --manifest-path rust/ictokenizer/Cargo.toml` then
// `CGO_LDFLAGS="-Lrust/ictokenizer/target/release" go build -tags smgcgo ...`.
// `make tokenize-cgo-test` wires this up.

package tokenize

/*
#cgo darwin LDFLAGS: -lictokenizer -lc++ -framework SystemConfiguration -framework CoreFoundation -liconv -lSystem -lm
#cgo linux  LDFLAGS: -lictokenizer -ldl -lm -lpthread
#include <stdint.h>
#include <stdlib.h>

// Status code mirrored from rust/ictokenizer (IC_OK == success). Rust consts
// are not C symbols, so the success code is defined here for the cgo side.
#define IC_OK 0

typedef struct Handle Handle;

Handle* ic_tokenizer_create(const char* model_or_path, char** err_out);
int ic_tokenizer_encode_text(Handle* h, const char* text, int add_special_tokens, uint32_t** ids_out, size_t* len_out, char** err_out);
int ic_tokenizer_encode_chat(Handle* h, const char* messages_json, int add_generation_prompt, uint32_t** ids_out, size_t* len_out, char** err_out);
void ic_free_ids(uint32_t* ids, size_t len);
void ic_free_string(char* s);
void ic_tokenizer_free(Handle* h);
*/
import "C"

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"unsafe"
)

// smgTokenizer is the cgo-backed Tokenizer. It lazily loads and caches one
// llm-tokenizer handle per model (loads are expensive — an HF download or a
// disk read).
//
// Concurrency: the RWMutex guards only the handle-cache map (creation). Encoding
// itself needs no lock: a handle wraps an Arc<dyn Tokenizer>, and upstream's
// Encoder/Decoder traits are `Send + Sync` with `&self` encode / chat-template
// methods, so simultaneous Encode/EncodeText calls on the same handle are safe
// by Rust's Sync guarantee. Serializing them would needlessly bottleneck the
// LookupRoute hot path.
type smgTokenizer struct {
	// resolve maps a model id to the path/HF-id the tokenizer loads from.
	resolve func(model string) string

	mu      sync.RWMutex
	handles map[string]*C.Handle
}

// newSMGTokenizer builds the cgo tokenizer. modelsDir, when non-empty, resolves
// a model id to "<modelsDir>/<model>" (a directory holding tokenizer.json);
// otherwise the model id is passed through (a local path, or an HF model id that
// llm-tokenizer downloads — gated models need HF_TOKEN).
func newSMGTokenizer(modelsDir string) *smgTokenizer {
	resolve := func(model string) string { return model }
	if modelsDir != "" {
		resolve = func(model string) string { return filepath.Join(modelsDir, model) }
	}
	return &smgTokenizer{resolve: resolve, handles: make(map[string]*C.Handle)}
}

func (t *smgTokenizer) handleFor(model string) (*C.Handle, error) {
	t.mu.RLock()
	h, ok := t.handles[model]
	t.mu.RUnlock()
	if ok {
		return h, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if h, ok := t.handles[model]; ok { // double-check after taking the write lock
		return h, nil
	}

	cPath := C.CString(t.resolve(model))
	defer C.free(unsafe.Pointer(cPath))
	var cErr *C.char
	handle := C.ic_tokenizer_create(cPath, &cErr)
	if handle == nil {
		return nil, fmt.Errorf("%w: load tokenizer for model %q: %s", ErrUnavailable, model, takeCErr(cErr))
	}
	t.handles[model] = handle
	return handle, nil
}

type wireMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Encode applies the model's chat template to msgs then tokenizes.
func (t *smgTokenizer) Encode(ctx context.Context, model string, msgs []Message, opts EncodeOptions) ([]uint32, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	h, err := t.handleFor(model)
	if err != nil {
		return nil, err
	}
	wire := make([]wireMessage, len(msgs))
	for i, m := range msgs {
		wire[i] = wireMessage{Role: m.Role, Content: m.Content}
	}
	payload, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("tokenize: marshal messages: %w", err)
	}
	cMsgs := C.CString(string(payload))
	defer C.free(unsafe.Pointer(cMsgs))

	var (
		ids  *C.uint32_t
		n    C.size_t
		cErr *C.char
	)
	rc := C.ic_tokenizer_encode_chat(h, cMsgs, cBool(opts.AddGenerationPrompt), &ids, &n, &cErr)
	if rc != C.IC_OK {
		return nil, fmt.Errorf("tokenize: encode_chat (model %q): %s", model, takeCErr(cErr))
	}
	return takeIDs(ids, n), nil
}

// EncodeText tokenizes already-rendered text with no chat template.
func (t *smgTokenizer) EncodeText(ctx context.Context, model, text string, opts EncodeOptions) ([]uint32, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	h, err := t.handleFor(model)
	if err != nil {
		return nil, err
	}
	cText := C.CString(text)
	defer C.free(unsafe.Pointer(cText))

	var (
		ids  *C.uint32_t
		n    C.size_t
		cErr *C.char
	)
	// add_special_tokens=true: raw-text path with no chat template still needs
	// the model's BOS/EOS to match how a completion engine tokenizes.
	rc := C.ic_tokenizer_encode_text(h, cText, C.int(1), &ids, &n, &cErr)
	if rc != C.IC_OK {
		return nil, fmt.Errorf("tokenize: encode_text (model %q): %s", model, takeCErr(cErr))
	}
	return takeIDs(ids, n), nil
}

// Close frees every cached tokenizer handle.
func (t *smgTokenizer) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for model, h := range t.handles {
		C.ic_tokenizer_free(h)
		delete(t.handles, model)
	}
}

// takeIDs copies a Rust-owned token-id buffer into a Go slice and frees it.
func takeIDs(ids *C.uint32_t, n C.size_t) []uint32 {
	defer C.ic_free_ids(ids, n)
	if n == 0 {
		return nil
	}
	src := unsafe.Slice((*uint32)(unsafe.Pointer(ids)), int(n))
	out := make([]uint32, int(n))
	copy(out, src)
	return out
}

// takeCErr converts a Rust-owned error string to a Go string and frees it.
func takeCErr(cErr *C.char) string {
	if cErr == nil {
		return "unknown error"
	}
	defer C.ic_free_string(cErr)
	return C.GoString(cErr)
}

func cBool(b bool) C.int {
	if b {
		return 1
	}
	return 0
}

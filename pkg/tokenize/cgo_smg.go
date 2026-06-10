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
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unsafe"
)

// smgTokenizer is the cgo-backed Tokenizer. Tokenizers are loaded EAGERLY at
// construction from a vetted models directory (one subdir per model, holding the
// tokenizer artifacts) and served from an in-memory map — so the LookupRoute hot
// path never loads, downloads, or touches a request-controlled filesystem path.
// A request model_id that is not in the pre-loaded set fails open (ErrUnavailable).
// This is what makes server-side tokenization safe to expose: an untrusted
// prompt_text caller cannot drive arbitrary tokenizer loads/downloads or escape
// the vetted directory via the model_id.
//
// Concurrency: the map is populated once at construction, then only read. Encode
// holds the RLock across the cgo call so Close cannot free a handle mid-use;
// concurrent Encode calls share the RLock — upstream's Encoder/Decoder traits are
// `Send + Sync` with `&self` methods, so simultaneous encodes on one handle are
// safe by Rust's Sync guarantee.
type smgTokenizer struct {
	mu      sync.RWMutex
	handles map[string]*C.Handle
}

// newSMGTokenizer eagerly loads every tokenizer under modelsDir. A model is any
// directory (at any depth) holding the tokenizer artifacts (tokenizer.json, plus
// tokenizer_config.json for the chat template); its model id is its path relative
// to modelsDir, so namespaced ids like "Qwen/Qwen2.5-0.5B-Instruct" arrange as
// nested directories. Loads are confined to modelsDir — a request model_id is never joined
// onto a path — and happen at startup, never on the hot path, so they trigger no
// HF download mid-request. A model that fails to load is logged and skipped
// (fail-soft startup; that model is simply unavailable).
func newSMGTokenizer(modelsDir string) *smgTokenizer {
	t := &smgTokenizer{handles: make(map[string]*C.Handle)}
	if _, err := os.Stat(modelsDir); err != nil {
		slog.Warn("tokenize: cannot read tokenizer models dir; server-side tokenization disabled",
			"dir", modelsDir, "err", err)
		return t
	}
	for _, m := range discoverModels(modelsDir) {
		h, err := loadHandle(m.path)
		if err != nil {
			slog.Warn("tokenize: failed to load tokenizer; model unavailable", "model", m.id, "err", err)
			continue
		}
		t.handles[m.id] = h
	}
	slog.Info("tokenize: loaded tokenizers", "dir", modelsDir, "count", len(t.handles))
	return t
}

type discoveredModel struct {
	id   string // engine-facing model id (slash-separated relative path)
	path string // absolute/relative filesystem path to load from
}

// discoverModels walks modelsDir and returns each model directory keyed by its
// path RELATIVE to modelsDir (slash-separated), so a model id with a namespace
// separator (e.g. "Qwen/Qwen2.5-0.5B-Instruct") resolves. The request model_id is
// only ever matched against these pre-loaded keys, never joined onto a path —
// confinement holds.
func discoverModels(modelsDir string) []discoveredModel {
	var out []discoveredModel
	_ = filepath.WalkDir(modelsDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil // skip unreadable entries, keep walking siblings
		}
		if !d.IsDir() || path == modelsDir || !isModelDir(path) {
			return nil
		}
		rel, relErr := filepath.Rel(modelsDir, path)
		if relErr != nil {
			return filepath.SkipDir
		}
		out = append(out, discoveredModel{id: filepath.ToSlash(rel), path: path})
		return filepath.SkipDir // a model directory is a leaf; don't descend into it
	})
	return out
}

// isModelDir reports whether dir holds tokenizer artifacts the loader can use —
// an HF tokenizer.json or a *.tiktoken file.
func isModelDir(dir string) bool {
	if _, err := os.Stat(filepath.Join(dir, "tokenizer.json")); err == nil {
		return true
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "*.tiktoken"))
	return len(matches) > 0
}

// loadHandle creates one tokenizer handle from a local path (a tokenizer dir) or
// — for tests only — an HF model id. Production construction (newSMGTokenizer)
// only ever passes confined local paths, so the hot path never downloads.
func loadHandle(ref string) (*C.Handle, error) {
	cRef := C.CString(ref)
	defer C.free(unsafe.Pointer(cRef))
	var cErr *C.char
	h := C.ic_tokenizer_create(cRef, &cErr)
	if h == nil {
		return nil, fmt.Errorf("%w: load tokenizer %q: %s", ErrUnavailable, ref, takeCErr(cErr))
	}
	return h, nil
}

// loadSingleModel builds a tokenizer with exactly one model loaded directly from
// ref (a local tokenizer dir or an HF model id). It keeps the cgo `C` type out
// of test files, which can't carry the cgo preamble; tests use it to exercise
// the real tokenizer without a structured models directory.
func loadSingleModel(model, ref string) (*smgTokenizer, error) {
	h, err := loadHandle(ref)
	if err != nil {
		return nil, err
	}
	return &smgTokenizer{handles: map[string]*C.Handle{model: h}}, nil
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

	t.mu.RLock()
	defer t.mu.RUnlock()
	h, ok := t.handles[model]
	if !ok {
		return nil, fmt.Errorf("%w: model %q not loaded", ErrUnavailable, model)
	}
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
	// C.CString builds a NUL-terminated string, so an embedded NUL would silently
	// truncate the text on the Rust side — reject it rather than tokenize a
	// truncated prompt. (Encode goes through JSON, which escapes NUL, so it's
	// unaffected.)
	if strings.IndexByte(text, 0) >= 0 {
		return nil, fmt.Errorf("tokenize: text contains a NUL byte")
	}
	cText := C.CString(text)
	defer C.free(unsafe.Pointer(cText))

	t.mu.RLock()
	defer t.mu.RUnlock()
	h, ok := t.handles[model]
	if !ok {
		return nil, fmt.Errorf("%w: model %q not loaded", ErrUnavailable, model)
	}
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

// Close frees every loaded tokenizer handle.
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

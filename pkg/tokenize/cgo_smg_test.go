//go:build smgcgo

package tokenize

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cachebox-project/inference-cache/pkg/fingerprint"
)

const testModel = "test-model"
const goldenTokenizerModel = "Qwen/Qwen2.5-0.5B-Instruct"

// newTestTokenizer builds a tokenizer with a single model loaded directly from
// IC_TEST_TOKENIZER (a local tokenizer dir/tokenizer.json or an HF model id,
// which downloads). It bypasses the dir-scan constructor so the real-encode
// tests don't need a structured models directory. `make tokenize-cgo-test`
// provides IC_TEST_TOKENIZER; the tests skip when it's unset, so the tag-gated
// build still links and runs without network (the link itself is a smoke test
// of the rust/ictokenizer static archive).
func newTestTokenizer(t *testing.T) *smgTokenizer {
	t.Helper()
	ref := os.Getenv("IC_TEST_TOKENIZER")
	if ref == "" {
		t.Skip("set IC_TEST_TOKENIZER to a tokenizer path or HF model id to run the real-tokenizer cgo tests")
	}
	tk, err := loadSingleModel(testModel, ref)
	if err != nil {
		t.Fatalf("loadSingleModel(%q): %v", ref, err)
	}
	t.Cleanup(tk.Close)
	return tk
}

func TestCgoTokenizerEncodeIsDeterministic(t *testing.T) {
	tk := newTestTokenizer(t)
	ctx := context.Background()

	msgs := []Message{{Role: "user", Content: "The quick brown fox jumps over the lazy dog."}}
	a, err := tk.Encode(ctx, testModel, msgs, EncodeOptions{AddGenerationPrompt: true})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(a) == 0 {
		t.Fatal("Encode returned no tokens")
	}
	b, err := tk.Encode(ctx, testModel, msgs, EncodeOptions{AddGenerationPrompt: true})
	if err != nil {
		t.Fatalf("Encode (2nd): %v", err)
	}
	if !equalU32(a, b) {
		t.Fatalf("Encode not deterministic: %v vs %v", a, b)
	}
}

func TestCgoTokenizerEncodeTextWorks(t *testing.T) {
	tk := newTestTokenizer(t)

	ids, err := tk.EncodeText(context.Background(), testModel, "hello world", EncodeOptions{})
	if err != nil {
		t.Fatalf("EncodeText: %v", err)
	}
	if len(ids) == 0 {
		t.Fatal("EncodeText returned no tokens")
	}
}

func TestCgoTokenizerGoldenTextMatchesFingerprintChain(t *testing.T) {
	if ref := os.Getenv("IC_TEST_TOKENIZER"); ref != "" && ref != goldenTokenizerModel {
		t.Skipf("golden token vector is pinned to %s; got IC_TEST_TOKENIZER=%s", goldenTokenizerModel, ref)
	}
	g := loadTokenizerGolden(t)
	if g.Model != goldenTokenizerModel {
		t.Fatalf("tokenizer golden model = %q, want %q", g.Model, goldenTokenizerModel)
	}
	tk := newTestTokenizer(t)

	ids, err := tk.EncodeText(context.Background(), testModel, g.Text, EncodeOptions{})
	if err != nil {
		t.Fatalf("EncodeText: %v", err)
	}
	if !equalU32(ids, g.Tokens) {
		t.Fatalf("EncodeText tokens = %v, want golden vector tokens %v", ids, g.Tokens)
	}

	hashes, counts := fingerprint.Chain(ids, g.BlockSize)
	if len(hashes) != len(g.PrefixHashes) {
		t.Fatalf("fingerprint.Chain produced %d hashes, want %d", len(hashes), len(g.PrefixHashes))
	}
	for i, got := range hashes {
		want := decodeGoldenHash(t, g.PrefixHashes[i].B64BE)
		if !bytes.Equal(got, want) {
			t.Fatalf("prefix_hash[%d] = %x, want %x", i, got, want)
		}
		if counts[i] != int32(g.BlockSize) {
			t.Fatalf("block_token_counts[%d] = %d, want %d", i, counts[i], g.BlockSize)
		}
	}
}

// Hermetic (no network): without a vetted models directory the cgo build loads
// no tokenizer (the prompt_text path then fails open to NO_HINT downstream).
func TestCgoNewEmptyDirIsUnavailable(t *testing.T) {
	tk := New(Config{}) // no ModelsDir
	_, err := tk.Encode(context.Background(), "anything", []Message{{Role: "user", Content: "hi"}}, EncodeOptions{})
	if !errors.Is(err, ErrUnavailable) {
		t.Errorf("Encode err = %v, want ErrUnavailable (no models dir → no tokenizer loaded; lookup fails open downstream)", err)
	}
}

// Hermetic (no network): a model not present in the vetted directory fails open
// rather than triggering a lazy load/download.
func TestCgoUnknownModelFailsOpen(t *testing.T) {
	tk := New(Config{ModelsDir: t.TempDir()}) // empty dir → no models loaded
	_, err := tk.Encode(context.Background(), "not-loaded", []Message{{Role: "user", Content: "hi"}}, EncodeOptions{})
	if !errors.Is(err, ErrUnavailable) {
		t.Errorf("Encode err = %v, want ErrUnavailable (model not in vetted dir)", err)
	}
}

func TestCgoUnsafeModelIDsFailOpenWithoutCacheMutation(t *testing.T) {
	tk, ok := New(Config{ModelsDir: t.TempDir()}).(*smgTokenizer)
	if !ok {
		t.Fatal("New(Config{ModelsDir}) did not return *smgTokenizer under smgcgo")
	}
	before := len(tk.handles)
	for _, model := range []string{
		"../secret",
		"models/../../secret",
		"/var/tmp/tokenizer",
		"Qwen//Qwen2.5-0.5B-Instruct",
		"Qwen/./Qwen2.5-0.5B-Instruct",
		`Qwen\Qwen2.5-0.5B-Instruct`,
		"C:/models/Qwen",
		`C:\models\Qwen`,
	} {
		t.Run(model, func(t *testing.T) {
			_, err := tk.Encode(context.Background(), model, []Message{{Role: "user", Content: "hi"}}, EncodeOptions{})
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("Encode err = %v, want ErrUnavailable", err)
			}
		})
	}
	if got := len(tk.handles); got != before {
		t.Fatalf("tokenizer cache grew from %d to %d after unsafe request model_ids", before, got)
	}
}

// Hermetic (no network): a model id with a namespace separator must be
// discovered keyed by its slash-separated relative path, and non-model dirs
// ignored. Exercises the dir-walk/keying without needing a real tokenizer.
func TestDiscoverModelsKeysByRelativePath(t *testing.T) {
	root := t.TempDir()
	for _, m := range []string{"Qwen/Qwen2.5-0.5B-Instruct", "gpt2"} {
		dir := filepath.Join(root, filepath.FromSlash(m))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "tokenizer.json"), []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(root, "not-a-model"), 0o755); err != nil {
		t.Fatal(err)
	}

	got := map[string]bool{}
	for _, m := range discoverModels(root) {
		got[m.id] = true
	}
	for _, want := range []string{"Qwen/Qwen2.5-0.5B-Instruct", "gpt2"} {
		if !got[want] {
			t.Errorf("discoverModels missing %q; got %v", want, got)
		}
	}
	if got["not-a-model"] {
		t.Error("discoverModels included a dir with no tokenizer artifacts")
	}
	if len(got) != 2 {
		t.Errorf("discoverModels found %d models, want 2: %v", len(got), got)
	}
}

func TestSafeModelID(t *testing.T) {
	for _, model := range []string{
		"gpt2",
		"Qwen/Qwen2.5-0.5B-Instruct",
		"meta-llama/Llama-3.1-8B-Instruct",
	} {
		if !safeModelID(model) {
			t.Errorf("safeModelID(%q) = false, want true", model)
		}
	}
	for _, model := range []string{
		"",
		".",
		"..",
		"../secret",
		"models/../../secret",
		"/absolute/model",
		"Qwen//model",
		"Qwen/./model",
		`Qwen\model`,
		"C:/models/qwen",
		`C:\models\qwen`,
	} {
		if safeModelID(model) {
			t.Errorf("safeModelID(%q) = true, want false", model)
		}
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

type goldenHash struct {
	B64BE string `json:"b64be"`
}

type tokenizerGolden struct {
	Model        string       `json:"model"`
	Text         string       `json:"text"`
	Tokens       []uint32     `json:"tokens"`
	BlockSize    int          `json:"block_size"`
	PrefixHashes []goldenHash `json:"prefix_hashes"`
}

func loadTokenizerGolden(t *testing.T) tokenizerGolden {
	t.Helper()
	raw, err := os.ReadFile("testdata/qwen_hello_world_golden.json")
	if err != nil {
		t.Fatalf("read tokenizer golden vector: %v", err)
	}
	var g tokenizerGolden
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatalf("parse tokenizer golden vector: %v", err)
	}
	return g
}

func decodeGoldenHash(t *testing.T, b64 string) []byte {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		t.Fatalf("decode golden hash %q: %v", b64, err)
	}
	if len(b) != 8 {
		t.Fatalf("decode golden hash %q: got %d bytes, want 8", b64, len(b))
	}
	return b
}

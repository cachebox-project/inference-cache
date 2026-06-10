//go:build smgcgo

package tokenize

import (
	"context"
	"errors"
	"os"
	"testing"
)

const testModel = "test-model"

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

// Hermetic (no network): server-side tokenization is fail-closed without a vetted
// models directory.
func TestCgoNewEmptyDirIsUnavailable(t *testing.T) {
	tk := New(Config{}) // no ModelsDir
	_, err := tk.Encode(context.Background(), "anything", []Message{{Role: "user", Content: "hi"}}, EncodeOptions{})
	if !errors.Is(err, ErrUnavailable) {
		t.Errorf("Encode err = %v, want ErrUnavailable (no models dir → fail closed)", err)
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

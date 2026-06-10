//go:build smgcgo

package tokenize

import (
	"context"
	"os"
	"testing"
)

// These tests exercise the real cgo tokenizer and need a tokenizer artifact.
// Set IC_TEST_TOKENIZER to a local tokenizer dir/tokenizer.json (or an HF model
// id, which downloads). `make tokenize-cgo-test` provides one. Skipped when
// unset so the tag-gated build still links and runs in CI-like environments
// without network. The build linking at all is itself a smoke test of the
// rust/ictokenizer static archive.
func testTokenizerModel(t *testing.T) string {
	t.Helper()
	m := os.Getenv("IC_TEST_TOKENIZER")
	if m == "" {
		t.Skip("set IC_TEST_TOKENIZER to a tokenizer path or HF model id to run the cgo tokenizer tests")
	}
	return m
}

func TestCgoTokenizerEncodeIsDeterministic(t *testing.T) {
	model := testTokenizerModel(t)
	tk := New(Config{})
	ctx := context.Background()

	msgs := []Message{{Role: "user", Content: "The quick brown fox jumps over the lazy dog."}}
	a, err := tk.Encode(ctx, model, msgs, EncodeOptions{AddGenerationPrompt: true})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(a) == 0 {
		t.Fatal("Encode returned no tokens")
	}
	b, err := tk.Encode(ctx, model, msgs, EncodeOptions{AddGenerationPrompt: true})
	if err != nil {
		t.Fatalf("Encode (2nd): %v", err)
	}
	if !equalU32(a, b) {
		t.Fatalf("Encode not deterministic: %v vs %v", a, b)
	}
}

func TestCgoTokenizerEncodeTextWorks(t *testing.T) {
	model := testTokenizerModel(t)
	tk := New(Config{})

	ids, err := tk.EncodeText(context.Background(), model, "hello world", EncodeOptions{})
	if err != nil {
		t.Fatalf("EncodeText: %v", err)
	}
	if len(ids) == 0 {
		t.Fatal("EncodeText returned no tokens")
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

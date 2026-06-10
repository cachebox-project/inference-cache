package engineclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The OpenAI client must POST the prompt as a token-ID array to
// /v1/completions — that's what makes the engine cache exactly the tokens the
// router fingerprinted (no re-tokenization). We assert the wire shape against a
// fake engine and parse a canned completion back.
func TestOpenAISendsTokenIDPromptAndParsesCompletion(t *testing.T) {
	tokens := []uint32{101, 202, 303, 404}

	var gotPath, gotModel string
	var gotPrompt []uint32
	var gotMaxTokens int
	var gotTemp *float32 // pointer so we can tell "sent 0" from "omitted"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		var body struct {
			Model       string   `json:"model"`
			Prompt      []uint32 `json:"prompt"`
			MaxTokens   int      `json:"max_tokens"`
			Temperature *float32 `json:"temperature"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		gotModel, gotPrompt, gotMaxTokens, gotTemp = body.Model, body.Prompt, body.MaxTokens, body.Temperature
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"text":" world","finish_reason":"length"}],"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6}}`))
	}))
	defer srv.Close()

	c := NewOpenAI(nil)
	got, err := c.Complete(context.Background(), srv.URL, "qwen", tokens, CompletionParams{MaxTokens: 2, Temperature: 0})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	if gotPath != "/v1/completions" {
		t.Errorf("path = %q, want /v1/completions", gotPath)
	}
	if gotModel != "qwen" {
		t.Errorf("model = %q, want qwen", gotModel)
	}
	if !equalU32(gotPrompt, tokens) {
		t.Errorf("prompt = %v, want token-id array %v", gotPrompt, tokens)
	}
	if gotMaxTokens != 2 {
		t.Errorf("max_tokens = %d, want 2", gotMaxTokens)
	}
	// An explicit temperature: 0 must reach the wire (not be dropped by omitempty).
	if gotTemp == nil || *gotTemp != 0 {
		t.Errorf("temperature = %v, want explicit 0 on the wire", gotTemp)
	}
	if got.Text != " world" {
		t.Errorf("text = %q, want %q", got.Text, " world")
	}
	if got.PromptTokens != 4 || got.CompletionTokens != 2 {
		t.Errorf("usage = (%d,%d), want (4,2)", got.PromptTokens, got.CompletionTokens)
	}
}

// A non-2xx engine response is an error, not a silent empty completion.
func TestOpenAIErrorsOnNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "model not found", http.StatusNotFound)
	}))
	defer srv.Close()

	c := NewOpenAI(nil)
	if _, err := c.Complete(context.Background(), srv.URL, "missing", []uint32{1}, CompletionParams{MaxTokens: 1}); err == nil {
		t.Fatal("expected an error on HTTP 404, got nil")
	}
}

// The gRPC TokenizedInput client is a stub for now — it must report that
// clearly rather than silently doing nothing.
func TestGRPCClientNotImplemented(t *testing.T) {
	var c EngineClient = GRPCTokenizedClient{}
	if _, err := c.Complete(context.Background(), "engine:8000", "m", []uint32{1}, CompletionParams{}); err == nil {
		t.Fatal("expected ErrNotImplemented, got nil")
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

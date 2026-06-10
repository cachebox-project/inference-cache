package engineclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// OpenAIClient sends token-ID prompts to an OpenAI-compatible /v1/completions
// endpoint. Stock vLLM (and SGLang) accept an integer-array `prompt` as
// pre-tokenized input and do not re-tokenize it, so the engine caches exactly
// the supplied tokens — that is what makes the routing fingerprint match the
// engine cache by construction. /v1/completions (not /v1/chat/completions) is
// deliberate: it applies no chat template, so the caller's tokens are used as-is
// (the chat template was already applied when the tokens were produced).
type OpenAIClient struct {
	http *http.Client
}

// NewOpenAI builds an OpenAI-compatible engine client. A nil httpClient gets a
// default client with a 60s timeout.
func NewOpenAI(httpClient *http.Client) *OpenAIClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 60 * time.Second}
	}
	return &OpenAIClient{http: httpClient}
}

type completionRequest struct {
	Model       string   `json:"model"`
	Prompt      []uint32 `json:"prompt"`
	MaxTokens   int      `json:"max_tokens,omitempty"`
	Temperature float32  `json:"temperature,omitempty"`
	Stream      bool     `json:"stream"`
}

type completionResponse struct {
	Choices []struct {
		Text         string `json:"text"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage"`
}

// Complete POSTs the token-ID prompt to {endpoint}/v1/completions.
func (c *OpenAIClient) Complete(ctx context.Context, endpoint, model string, tokenIDs []uint32, p CompletionParams) (Completion, error) {
	payload, err := json.Marshal(completionRequest{
		Model:       model,
		Prompt:      tokenIDs,
		MaxTokens:   p.MaxTokens,
		Temperature: p.Temperature,
		Stream:      false, // this client reads a single non-streamed completion
	})
	if err != nil {
		return Completion{}, fmt.Errorf("engineclient: marshal request: %w", err)
	}

	url := strings.TrimRight(endpoint, "/") + "/v1/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return Completion{}, fmt.Errorf("engineclient: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return Completion{}, fmt.Errorf("engineclient: POST %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return Completion{}, fmt.Errorf("engineclient: read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Completion{}, fmt.Errorf("engineclient: engine returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var parsed completionResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return Completion{}, fmt.Errorf("engineclient: decode response: %w", err)
	}
	out := Completion{
		PromptTokens:     parsed.Usage.PromptTokens,
		CompletionTokens: parsed.Usage.CompletionTokens,
	}
	if len(parsed.Choices) > 0 {
		out.Text = parsed.Choices[0].Text
		out.FinishReason = parsed.Choices[0].FinishReason
	}
	return out, nil
}

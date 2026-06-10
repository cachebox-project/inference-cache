package engineclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// mockVLLM mimics just enough of a vLLM OpenAI server for the canary: it serves
// /v1/completions and exposes vLLM-shaped prefix-cache counters on /metrics,
// incrementing hits when it sees a token-ID prompt identical to the previous one
// (block-prefix reuse, modeled coarsely at whole-prompt granularity).
type mockVLLM struct {
	mu       sync.Mutex
	queries  int
	hits     int
	lastSeen string
}

func (m *mockVLLM) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/completions", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Prompt []uint32 `json:"prompt"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		key := fmt.Sprint(body.Prompt)

		m.mu.Lock()
		m.queries++
		if key == m.lastSeen && key != "[]" {
			m.hits++
		}
		m.lastSeen = key
		m.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"text":"ok","finish_reason":"length"}],"usage":{"prompt_tokens":4,"completion_tokens":1}}`))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		m.mu.Lock()
		q, h := m.queries, m.hits
		m.mu.Unlock()
		// Include labels to exercise the label-tolerant scraper, and a decoy
		// metric whose name shares the prefix to catch sloppy matching.
		fmt.Fprintf(w, "# HELP vllm:prefix_cache_hits_total hits\n")
		fmt.Fprintf(w, "vllm:prefix_cache_hits_total{model=\"m\"} %d\n", h)
		fmt.Fprintf(w, "vllm:prefix_cache_queries_total{model=\"m\"} %d\n", q)
		fmt.Fprintf(w, "vllm:prefix_cache_hits_total_bogus 999\n")
	})
	return mux
}

// The probe must observe a positive hit delta on the warm (repeated) token-ID
// prompt — the by-construction signal that the engine cached exactly the tokens
// it was given.
func TestPrefixCacheProbeDetectsWarmHit(t *testing.T) {
	srv := httptest.NewServer((&mockVLLM{}).handler())
	defer srv.Close()

	probe := &PrefixCacheProbe{
		Client:    NewOpenAI(nil),
		EngineURL: srv.URL,
		Model:     "m",
	}
	tokens := tokenRange(0, 64)
	res, err := probe.Run(context.Background(), tokens, CompletionParams{MaxTokens: 1})
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if res.HitsDelta <= 0 {
		t.Errorf("HitsDelta = %d, want > 0 (warm token-id prompt must hit the prefix cache)", res.HitsDelta)
	}
	if res.QueriesDelta < 1 {
		t.Errorf("QueriesDelta = %d, want >= 1", res.QueriesDelta)
	}
	if res.Warm.Text == "" {
		t.Error("warm completion text empty; engine call did not round-trip")
	}
}

// The decoy-prefixed counter (vllm:prefix_cache_hits_total_bogus) must NOT be
// summed into the real counter.
func TestScrapeCounterIgnoresPrefixCollisions(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("vllm:prefix_cache_hits_total 5\nvllm:prefix_cache_hits_total_bogus 999\n"))
	}))
	defer srv.Close()

	got, err := scrapeCounter(context.Background(), http.DefaultClient, srv.URL, "vllm:prefix_cache_hits_total")
	if err != nil {
		t.Fatalf("scrapeCounter: %v", err)
	}
	if got != 5 {
		t.Errorf("scrapeCounter = %d, want 5 (must ignore the _bogus prefix collision)", got)
	}
}

// scrapeCounter must read the sample value, not a trailing timestamp, and must
// tolerate label values that contain spaces.
func TestScrapeCounterReadsValueNotTimestamp(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// First series carries a label value with a space AND a trailing timestamp.
		_, _ = w.Write([]byte("vllm:prefix_cache_hits_total{model=\"a b\"} 7 1700000000000\n" +
			"vllm:prefix_cache_hits_total{model=\"c\"} 3\n"))
	}))
	defer srv.Close()

	got, err := scrapeCounter(context.Background(), http.DefaultClient, srv.URL, "vllm:prefix_cache_hits_total")
	if err != nil {
		t.Fatalf("scrapeCounter: %v", err)
	}
	if got != 10 {
		t.Errorf("scrapeCounter = %d, want 10 (values 7+3, not the timestamp)", got)
	}
}

// TestPrefixCacheCanaryLive runs the real by-construction canary against a live
// OpenAI-compatible engine. Operator-run, not CI: set IC_ENGINE_URL (e.g.
// http://localhost:8000) and IC_ENGINE_MODEL (the served model). A long
// ascending token sequence exceeds the prefix-cache block threshold; the warm
// identical request must register a prefix-cache hit.
func TestPrefixCacheCanaryLive(t *testing.T) {
	engineURL := os.Getenv("IC_ENGINE_URL")
	model := os.Getenv("IC_ENGINE_MODEL")
	if engineURL == "" || model == "" {
		t.Skip("set IC_ENGINE_URL and IC_ENGINE_MODEL to run the live engine canary")
	}
	n := 2048
	if v := os.Getenv("IC_ENGINE_PROMPT_TOKENS"); v != "" {
		if parsed, err := strconv.Atoi(strings.TrimSpace(v)); err == nil && parsed > 0 {
			n = parsed
		}
	}

	probe := &PrefixCacheProbe{Client: NewOpenAI(nil), EngineURL: engineURL, Model: model}
	res, err := probe.Run(context.Background(), tokenRange(1, n), CompletionParams{MaxTokens: 1, Temperature: 0})
	if err != nil {
		t.Fatalf("live canary: %v", err)
	}
	t.Logf("live canary: queries delta=%d hits delta=%d (warm request)", res.QueriesDelta, res.HitsDelta)
	if res.HitsDelta <= 0 {
		t.Errorf("HitsDelta = %d, want > 0 — the engine did not prefix-cache the token-id prompt", res.HitsDelta)
	}
}

// tokenRange returns [start, start+1, ..., start+n-1] as token IDs.
func tokenRange(start, n int) []uint32 {
	out := make([]uint32, n)
	for i := range out {
		out[i] = uint32(start + i)
	}
	return out
}

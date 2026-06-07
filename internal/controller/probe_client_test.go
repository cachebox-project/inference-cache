package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cacheserver "github.com/cachebox-project/inference-cache/pkg/server"
)

// TestProbeClientRunHappyPath drives the client end-to-end against an
// httptest.Server: it must POST JSON to the configured URL, attach the
// bearer token, and decode the ProbeResult that the server returns. The
// fixture mirrors the actual server-side handler's wire contract
// (`POST /probe`, JSON request/response, fixed shape) so the client test
// shares the same definition of "well-formed" as the server tests.
func TestProbeClientRunHappyPath(t *testing.T) {
	const wantToken = "the-projected-sa-token"
	wantBody := cacheserver.ProbeRequest{
		Backend: "team-a/cb-1", Model: "functional-self-test", HashScheme: "vllm",
		BackendType: "LMCache",
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("want POST, got %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer "+wantToken {
			t.Errorf("Authorization header = %q, want %q", got, "Bearer "+wantToken)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", got)
		}
		var req cacheserver.ProbeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if req != wantBody {
			t.Errorf("request body = %+v, want %+v", req, wantBody)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cacheserver.ProbeResult{
			Backend: "team-a/cb-1",
			Ingest:  cacheserver.ProbeStageOK,
			Routing: cacheserver.ProbeStageOK,
			T2:      cacheserver.ProbeStageSkipped,
		})
	}))
	defer srv.Close()

	tokenPath := writeTokenFile(t, wantToken)
	client := &ProbeClient{
		ProbeURL:        srv.URL,
		BearerTokenPath: tokenPath,
		HTTPClient:      srv.Client(),
	}
	got, err := client.Run(context.Background(), wantBody)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !got.AllPassed() {
		t.Fatalf("AllPassed() = false, got result %+v", got)
	}
	if got.Backend != "team-a/cb-1" {
		t.Errorf("Backend = %q, want team-a/cb-1", got.Backend)
	}
}

// TestProbeClientRunNoTokenFile pins the "local dev / no token mounted"
// posture: with a missing bearer-token file the request goes out
// unauthenticated rather than returning an error. Matches CacheIndexPoller
// and ControlPlaneReconciler; the server's 401 then surfaces as the
// non-2xx error (see TestProbeClientRunRejectsNon2xx).
func TestProbeClientRunNoTokenFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("Authorization should be empty when no token file is mounted; got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cacheserver.ProbeResult{
			Backend: "x", Ingest: cacheserver.ProbeStageOK,
			Routing: cacheserver.ProbeStageOK, T2: cacheserver.ProbeStageSkipped,
		})
	}))
	defer srv.Close()

	client := &ProbeClient{
		ProbeURL:        srv.URL,
		BearerTokenPath: filepath.Join(t.TempDir(), "does-not-exist"),
		HTTPClient:      srv.Client(),
	}
	if _, err := client.Run(context.Background(), cacheserver.ProbeRequest{Backend: "x", Model: "m", HashScheme: "vllm"}); err != nil {
		t.Fatalf("Run with no token file should succeed; got: %v", err)
	}
}

// TestProbeClientRunDisabled pins the structural-off path: a client with
// no ProbeURL returns ErrProbeClientDisabled — distinct from a transport
// error so the gate can detect "no probe configured" without parsing
// error strings.
func TestProbeClientRunDisabled(t *testing.T) {
	cases := []struct {
		name   string
		client *ProbeClient
	}{
		{"nil client", nil},
		{"empty URL", &ProbeClient{ProbeURL: ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.client.Run(context.Background(), cacheserver.ProbeRequest{Backend: "x", Model: "m", HashScheme: "vllm"})
			if !errors.Is(err, ErrProbeClientDisabled) {
				t.Fatalf("err = %v, want ErrProbeClientDisabled", err)
			}
		})
	}
}

// TestProbeClientRunRejectsNon2xx pins the non-2xx error contract:
// anything outside [200, 299] is wrapped as an error with the status code
// and a snippet of the response body. The gate translates this into a
// FunctionalProbeOK=Unknown condition rather than degrading Ready.
func TestProbeClientRunRejectsNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized: bad audience", http.StatusUnauthorized)
	}))
	defer srv.Close()

	client := &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()}
	_, err := client.Run(context.Background(), cacheserver.ProbeRequest{Backend: "x", Model: "m", HashScheme: "vllm"})
	if err == nil {
		t.Fatalf("expected non-2xx to surface as error")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("error %q should include the HTTP status 401", err)
	}
	if !strings.Contains(err.Error(), "unauthorized") {
		t.Errorf("error %q should include a snippet of the response body", err)
	}
}

// TestProbeClientRunBadJSON pins decode-error handling: a 200 response with
// garbage body surfaces as an error rather than a zero ProbeResult, so the
// gate can distinguish "server returned ok" from "server returned mush".
func TestProbeClientRunBadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("not json"))
	}))
	defer srv.Close()
	client := &ProbeClient{ProbeURL: srv.URL, HTTPClient: srv.Client()}
	if _, err := client.Run(context.Background(), cacheserver.ProbeRequest{Backend: "x", Model: "m", HashScheme: "vllm"}); err == nil {
		t.Fatalf("expected decode error on invalid JSON body")
	}
}

// TestProbeClientRunTransportError pins transport failure: a closed-server
// connection surfaces as an error (wraps the underlying connection refusal).
func TestProbeClientRunTransportError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	addr := srv.URL
	srv.Close() // hang up first

	client := &ProbeClient{ProbeURL: addr, HTTPClient: &http.Client{Timeout: time.Second}}
	if _, err := client.Run(context.Background(), cacheserver.ProbeRequest{Backend: "x", Model: "m", HashScheme: "vllm"}); err == nil {
		t.Fatalf("expected transport error against a closed server")
	}
}

// TestProbeClientRunTokenFileUnreadable pins the "file present but
// unreadable" branch: a 0o000 permissions file surfaces an error rather
// than silently degrading to unauthenticated. The operator gets the real
// cause (token IO error) instead of misdiagnosing a downstream 401 as a
// server-side identity mismatch.
func TestProbeClientRunTokenFileUnreadable(t *testing.T) {
	dir := t.TempDir()
	tokenPath := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenPath, []byte("token"), 0o000); err != nil {
		t.Fatalf("write token: %v", err)
	}
	defer func() { _ = os.Chmod(tokenPath, 0o600) }()

	// Skip on root (CI/dev container often runs as uid 0; 0o000 doesn't deny
	// root). The branch we want to exercise is "permission denied for the
	// process". On root the test would falsely pass and obscure a future
	// regression in the bearer-token error wrapping.
	if os.Geteuid() == 0 {
		t.Skip("running as root, 0o000 permissions don't deny access")
	}

	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	client := &ProbeClient{ProbeURL: srv.URL, BearerTokenPath: tokenPath, HTTPClient: srv.Client()}
	_, err := client.Run(context.Background(), cacheserver.ProbeRequest{Backend: "x", Model: "m", HashScheme: "vllm"})
	if err == nil {
		t.Fatalf("expected token-IO error to surface")
	}
	if !strings.Contains(err.Error(), "bearer token") {
		t.Errorf("error %q should mention bearer token IO", err)
	}
}

// writeTokenFile writes a token into a tempdir and returns the path,
// matching the production mount-path shape.
func writeTokenFile(t *testing.T, token string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	// Sanity: the client should read the file and Trim the trailing
	// whitespace (consistent with how the snapshot poller handles it).
	b, _ := io.ReadAll(strings.NewReader(token + "\n"))
	if strings.TrimSpace(string(b)) != token {
		t.Fatalf("test setup error: token round-trip mismatch")
	}
	return path
}

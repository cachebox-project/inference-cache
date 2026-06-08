package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"time"

	cacheserver "github.com/cachebox-project/inference-cache/pkg/server"
)

// ProbeClient is the controller's POST /probe wrapper. It shares the
// bearer-token + HTTP shape used by CacheIndexPoller and ControlPlaneReconciler
// so all three controller↔server bridge calls have one identity, one token
// source, and the same auth profile on the server side. Two intentional
// differences from those two siblings:
//
//   - /probe is per-CacheBackend rather than cluster-wide, so this client is
//     invoked from inside the CacheBackend reconciler (not a separate
//     Runnable/Reconciler loop).
//   - An unreadable token file (mounted but ReadFile fails — corrupted secret,
//     permission flip) returns an error from Run instead of being logged and
//     proceeding unauthenticated. The reconciler surfaces that error as
//     FunctionalProbeOK=Unknown/ProbeError on the affected CR (operator-
//     visible), where the sibling clients' log-and-continue posture would
//     leave the bad-token symptom invisible until an unrelated 401 burst
//     appears in /metrics. An absent token file (the local-dev case) still
//     matches the siblings — empty Authorization header, server's 401
//     surfaces as the same non-2xx path.
//
// All fields are optional and have sensible defaults; the production binary
// only sets ProbeURL via cmd/controller/main.go's --server-probe-url flag —
// BearerTokenPath falls back to DefaultBearerTokenPath and HTTPClient falls
// back to a 5s-timeout default. A zero value (ProbeURL=="") is treated as
// the local-dev default — no probe call is issued and the caller skips the
// gate.
type ProbeClient struct {
	// ProbeURL is the fully-qualified URL of the server's /probe endpoint,
	// e.g. http://inference-cache-server:8081/probe. An empty value disables
	// the client; Run returns ErrProbeClientDisabled so the caller can opt
	// the gate out entirely without nil-checking everywhere.
	ProbeURL string

	// BearerTokenPath is the file the projected ServiceAccount token is
	// mounted at. "" → DefaultBearerTokenPath. Error semantics match the
	// CacheIndexPoller's bearerToken — file missing is treated as
	// "no token configured" (local-dev), file present but unreadable
	// surfaces an error.
	BearerTokenPath string

	// HTTPClient is the http.Client used for all requests. nil → a 5s-timeout
	// default. Tests inject httptest-bound clients.
	HTTPClient *http.Client
}

// ErrProbeClientDisabled signals that the client was constructed without a
// ProbeURL — the caller should skip the gate entirely (do not write a
// FunctionalProbeOK condition; the probe is intentionally not configured).
// Distinct from a transport error so the gate can choose "leave the condition
// alone" vs "surface the HTTP failure" without parsing error strings.
var ErrProbeClientDisabled = errors.New("probe client: ProbeURL not configured")

// Run issues one POST /probe with the supplied request body and decodes the
// JSON ProbeResult. It is fail-soft on auth-token IO (a missing token is a
// local-dev expectation; an unreadable file is surfaced) and surfaces any
// HTTP-level failure as a wrapped error so the caller can decide how to
// represent the gap in the FunctionalProbeOK condition.
//
// Auth posture mirrors CacheIndexPoller / ControlPlaneReconciler: the
// projected SA token at BearerTokenPath is sent as Authorization: Bearer on
// every request, re-read each call so kubelet rotations are picked up. An
// absent token file is NOT an error — the request goes out unauthenticated
// and the server's 401 surfaces as a "non-2xx" failure, matching what the
// other two controller↔server HTTP channels do.
func (c *ProbeClient) Run(ctx context.Context, req cacheserver.ProbeRequest) (cacheserver.ProbeResult, error) {
	if c == nil || c.ProbeURL == "" {
		return cacheserver.ProbeResult{}, ErrProbeClientDisabled
	}

	body, err := json.Marshal(req)
	if err != nil {
		return cacheserver.ProbeResult{}, fmt.Errorf("probe client: marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.ProbeURL, bytes.NewReader(body))
	if err != nil {
		return cacheserver.ProbeResult{}, fmt.Errorf("probe client: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	// Send the projected SA token if one is mounted. An IO error reading
	// the token is surfaced (not swallowed) so the operator sees the real
	// cause rather than misattributing the eventual 401 to a server-side
	// identity mismatch; an absent file IS expected (local-dev) and goes
	// unauthenticated. Matches CacheIndexPoller.bearerToken's posture.
	token, tokenErr := c.bearerToken()
	if tokenErr != nil {
		return cacheserver.ProbeResult{}, fmt.Errorf("probe client: bearer token: %w", tokenErr)
	}
	if token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := c.httpClient().Do(httpReq)
	if err != nil {
		return cacheserver.ProbeResult{}, fmt.Errorf("probe client: POST %s: %w", c.ProbeURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		// Drain a small slice of the body for the diagnostic — the server's
		// JSON error responses are bounded (a few hundred bytes), so 1 KiB
		// is plenty of context without risking unbounded reads.
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return cacheserver.ProbeResult{}, fmt.Errorf("probe client: %s: unexpected status %d: %s",
			c.ProbeURL, resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	var result cacheserver.ProbeResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return cacheserver.ProbeResult{}, fmt.Errorf("probe client: decode response: %w", err)
	}
	return result, nil
}

// bearerToken reads the projected ServiceAccount token. Re-read on every call
// so kubelet rotations are picked up without process restarts; the file is
// tmpfs so the read is cheap. Error semantics mirror CacheIndexPoller and
// ControlPlaneReconciler's bearerToken: file missing → ("", nil); present
// but unreadable → ("", wrappedError).
func (c *ProbeClient) bearerToken() (string, error) {
	path := c.BearerTokenPath
	if path == "" {
		path = DefaultBearerTokenPath
	}
	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "", nil
	case err != nil:
		return "", fmt.Errorf("read bearer token %q: %w", path, err)
	}
	return strings.TrimSpace(string(b)), nil
}

// httpClient returns the configured http.Client or a default with a tight
// 5s timeout — the probe is a synchronous in-cluster call on the controller's
// hot path, so a slow probe must time out rather than block the reconcile.
// Matches the timeout used by the snapshot poller.
func (c *ProbeClient) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return &http.Client{Timeout: 5 * time.Second}
}

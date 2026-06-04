package checks

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cachebox-project/inference-cache/pkg/cli/doctor"
)

const (
	checkServerReachability   = "ServerReachability"
	checkSnapshotReachability = "SnapshotReachability"
	checkPolicyReachability   = "PolicyReachability"
)

// snapshotBodyLimit caps how much of the /snapshot response doctor reads before
// validating it as JSON — the aggregate is small, and an unbounded read would
// let a misbehaving endpoint balloon doctor's memory.
const snapshotBodyLimit = 8 << 20 // 8 MiB

// ServerReachability dials the cache-plane server's gRPC health service and
// checks that the default service ("") reports SERVING. A nil health client
// (the gRPC target could not be resolved/dialed) or any Check error is a FAIL;
// a non-SERVING status is a FAIL; SERVING is an OK.
func ServerReachability(ctx context.Context, h HealthChecker, target string) []doctor.Finding {
	ref := target
	if h == nil {
		return []doctor.Finding{{
			Code:     doctor.CodeServerUnreachable,
			Status:   doctor.StatusFail,
			Check:    checkServerReachability,
			Resource: ref,
			Message:  "could not establish a gRPC connection to the cache-plane server; pass --server-endpoint (host:port) or check that the inference-cache-server Service exists and is reachable",
		}}
	}

	resp, err := h.Check(ctx, &healthpb.HealthCheckRequest{Service: ""})
	if err != nil {
		return []doctor.Finding{{
			Code:     doctor.CodeServerUnreachable,
			Status:   doctor.StatusFail,
			Check:    checkServerReachability,
			Resource: ref,
			Message:  fmt.Sprintf("gRPC health check failed: %v", err),
		}}
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return []doctor.Finding{{
			Code:     doctor.CodeServerNotServing,
			Status:   doctor.StatusFail,
			Check:    checkServerReachability,
			Resource: ref,
			Message:  fmt.Sprintf("server reported health status %s, want SERVING", resp.GetStatus()),
		}}
	}
	return []doctor.Finding{{
		Code:     doctor.CodeServerServing,
		Status:   doctor.StatusOK,
		Check:    checkServerReachability,
		Resource: ref,
		Message:  "gRPC health check reports SERVING",
	}}
}

// SnapshotReachability issues an HTTP GET to the server's /snapshot endpoint and
// confirms it returns 200 with a JSON-parseable body. When a bearer token is
// supplied it is presented; when it is empty doctor probes the unauthenticated
// path and additionally emits an INFO flagging that the snapshot answered
// without authentication (the auth hardening is not yet wired everywhere).
func SnapshotReachability(ctx context.Context, doer HTTPDoer, url, token string) []doctor.Finding {
	if doer == nil || url == "" {
		return []doctor.Finding{{
			Code:     doctor.CodeSnapshotUnreachable,
			Status:   doctor.StatusFail,
			Check:    checkSnapshotReachability,
			Resource: url,
			Message:  "could not resolve the server /snapshot endpoint; pass --server-endpoint or check the inference-cache-server Service",
		}}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return []doctor.Finding{{
			Code:     doctor.CodeSnapshotUnreachable,
			Status:   doctor.StatusFail,
			Check:    checkSnapshotReachability,
			Resource: url,
			Message:  fmt.Sprintf("could not build /snapshot request: %v", err),
		}}
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := doer.Do(req)
	if err != nil {
		return []doctor.Finding{{
			Code:     doctor.CodeSnapshotUnreachable,
			Status:   doctor.StatusFail,
			Check:    checkSnapshotReachability,
			Resource: url,
			Message:  fmt.Sprintf("GET /snapshot failed: %v", err),
		}}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return []doctor.Finding{{
			Code:     doctor.CodeSnapshotUnreachable,
			Status:   doctor.StatusFail,
			Check:    checkSnapshotReachability,
			Resource: url,
			Message:  fmt.Sprintf("GET /snapshot returned HTTP %d, want 200", resp.StatusCode),
		}}
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, snapshotBodyLimit))
	if err != nil {
		return []doctor.Finding{{
			Code:     doctor.CodeSnapshotBadBody,
			Status:   doctor.StatusWarn,
			Check:    checkSnapshotReachability,
			Resource: url,
			Message:  fmt.Sprintf("GET /snapshot returned 200 but the body could not be read: %v", err),
		}}
	}
	if !json.Valid(body) {
		return []doctor.Finding{{
			Code:     doctor.CodeSnapshotBadBody,
			Status:   doctor.StatusWarn,
			Check:    checkSnapshotReachability,
			Resource: url,
			Message:  "GET /snapshot returned 200 but the body did not parse as JSON",
		}}
	}

	findings := []doctor.Finding{{
		Code:     doctor.CodeSnapshotOK,
		Status:   doctor.StatusOK,
		Check:    checkSnapshotReachability,
		Resource: url,
		Message:  "GET /snapshot returned 200 with a JSON-parseable body",
	}}
	if token == "" {
		findings = append(findings, doctor.Finding{
			Code:     doctor.CodeSnapshotUnauthenticated,
			Status:   doctor.StatusInfo,
			Check:    checkSnapshotReachability,
			Resource: url,
			Message:  "/snapshot answered without a bearer token — no ServiceAccount token was available, or snapshot authentication is not enforced on this install",
		})
	}
	return findings
}

// PolicyReachability confirms the /policy route exists without mutating it. It
// issues a non-mutating HTTP HEAD. A mounted route answers with its own status
// (200, 401 from auth, or 405 from the GET/POST/PUT-only handler rejecting
// HEAD) — all of which prove the route is wired. A 404, by contrast, is what the
// server's ServeMux returns when /policy is NOT registered at all, so it is a
// FAIL: the route doctor claims to verify is absent. A transport-level failure
// (connection refused, DNS, timeout) is likewise a FAIL.
func PolicyReachability(ctx context.Context, doer HTTPDoer, url string) []doctor.Finding {
	if doer == nil || url == "" {
		return []doctor.Finding{{
			Code:     doctor.CodePolicyRouteMissing,
			Status:   doctor.StatusFail,
			Check:    checkPolicyReachability,
			Resource: url,
			Message:  "could not resolve the server /policy endpoint; pass --server-endpoint or check the inference-cache-server Service",
		}}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return []doctor.Finding{{
			Code:     doctor.CodePolicyRouteMissing,
			Status:   doctor.StatusFail,
			Check:    checkPolicyReachability,
			Resource: url,
			Message:  fmt.Sprintf("could not build /policy request: %v", err),
		}}
	}

	resp, err := doer.Do(req)
	if err != nil {
		return []doctor.Finding{{
			Code:     doctor.CodePolicyRouteMissing,
			Status:   doctor.StatusFail,
			Check:    checkPolicyReachability,
			Resource: url,
			Message:  fmt.Sprintf("HEAD /policy failed to connect: %v", err),
		}}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return []doctor.Finding{{
			Code:     doctor.CodePolicyRouteMissing,
			Status:   doctor.StatusFail,
			Check:    checkPolicyReachability,
			Resource: url,
			Message:  "HEAD /policy returned HTTP 404 — the /policy route is not mounted on the server (a wired route answers 200/401/405, never 404)",
		}}
	}

	return []doctor.Finding{{
		Code:     doctor.CodePolicyRouteWired,
		Status:   doctor.StatusOK,
		Check:    checkPolicyReachability,
		Resource: url,
		Message:  fmt.Sprintf("/policy route is wired (HTTP %d to a non-mutating HEAD)", resp.StatusCode),
	}}
}

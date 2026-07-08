package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	reflectpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	authnv1 "k8s.io/api/authentication/v1"

	"github.com/cachebox-project/inference-cache/pkg/fingerprint"
	"github.com/cachebox-project/inference-cache/pkg/index"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/tokenize"
)

// newTestService builds a service backed by a fresh, empty index + policy
// store for handler-level unit tests. The index uses the policy store as its
// TTL resolver so per-tenant TTL changes during a test are reflected exactly
// as they would be in the running binary, and reserves the probe tenant so
// the cap/snapshot/aggregate behavior matches the production binary's
// Service.New wiring (otherwise tests would see different read-side
// filtering than the running server).
func newTestService() *inferenceCacheService {
	policies := NewPolicyStore()
	idx := index.New(
		index.WithTTLResolver(policies),
		index.WithReservedTenants(ProbeTenantID),
	)
	return newInferenceCacheService(idx, newServerMetrics(), policies)
}

func TestHealthAndReadinessReturnOK(t *testing.T) {
	_, baseURL, stop := startInProcessServer(t)
	defer stop()

	for _, path := range []string{"/healthz", "/readyz"} {
		status, body := getString(t, baseURL+path)
		if status != http.StatusOK {
			t.Errorf("%s status = %d, want %d", path, status, http.StatusOK)
		}
		if body != "ok\n" {
			t.Errorf("%s body = %q, want %q", path, body, "ok\n")
		}
	}
}

// TestMetricsExposesServerUp checks the Prometheus endpoint is mounted and
// emits the documented inferencecache_server_up gauge (tech spec §4.3), set to
// 1 while the server is serving.
func TestMetricsExposesServerUp(t *testing.T) {
	_, baseURL, stop := startInProcessServer(t)
	defer stop()

	status, body := getString(t, baseURL+"/metrics")
	if status != http.StatusOK {
		t.Fatalf("/metrics status = %d, want %d", status, http.StatusOK)
	}
	if !strings.Contains(body, "inferencecache_server_up 1") {
		t.Fatalf("/metrics missing 'inferencecache_server_up 1'; body:\n%s", body)
	}
}

func TestLookupRouteFailsOpen(t *testing.T) {
	// Empty index → no match → fail open with NO_HINT.
	resp, err := newTestService().LookupRoute(context.Background(), &icpb.LookupRouteRequest{ModelId: "m"})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("reason = %q, want NO_HINT", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 0 {
		t.Fatalf("expected no replica scores, got %d", len(resp.GetReplicaScores()))
	}
}

// TestLookupRouteReasonCodes exercises the handler's strategy-to-reason_code
// mapping end-to-end: a primed index + a crafted request must surface as
// PREFIX_MATCH or TENANT_HOT through the gRPC envelope, not just inside the
// index. Pins the contract surface the gateway reads.
func TestLookupRouteReasonCodes(t *testing.T) {
	t.Run("PREFIX_MATCH on exact prefix hit", func(t *testing.T) {
		svc := newTestService()
		// TokenCount=128 keeps the realized match above the
		// DefaultMinimumMatchedTokens floor so this test pins the
		// PREFIX_MATCH wire code, not the floor.
		svc.index.Ingest(index.Update{
			ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
			Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 128}},
		})
		resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
			ModelId: "m", TenantId: "t", HashScheme: "vllm", PrefixHash: []byte("p"),
		})
		if err != nil {
			t.Fatalf("LookupRoute: %v", err)
		}
		if resp.GetReasonCode() != "PREFIX_MATCH" {
			t.Fatalf("reason = %q, want PREFIX_MATCH", resp.GetReasonCode())
		}
		if len(resp.GetReplicaScores()) != 1 || resp.GetReplicaScores()[0].GetReplicaId() != "r1" {
			t.Fatalf("scores = %+v, want one r1", resp.GetReplicaScores())
		}
	})

	t.Run("TENANT_HOT on prefix miss with warm replica", func(t *testing.T) {
		svc := newTestService()
		svc.index.Ingest(index.Update{
			ReplicaID: "warm", Model: "m", Tenant: "t", HashScheme: "vllm",
			Prefixes: []index.PrefixRef{{PrefixHash: []byte("other"), TokenCount: 1}},
			Stats:    &index.ReplicaStats{HitRate: 0.8},
		})
		resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
			ModelId: "m", TenantId: "t", HashScheme: "vllm", PrefixHash: []byte("novel"),
		})
		if err != nil {
			t.Fatalf("LookupRoute: %v", err)
		}
		if resp.GetReasonCode() != "TENANT_HOT" {
			t.Fatalf("reason = %q, want TENANT_HOT", resp.GetReasonCode())
		}
		if len(resp.GetReplicaScores()) != 1 || resp.GetReplicaScores()[0].GetReplicaId() != "warm" {
			t.Fatalf("scores = %+v, want one warm replica", resp.GetReplicaScores())
		}
		// MatchedTokens is 0 in the TENANT_HOT branch (no prefix overlap);
		// the gateway must rely on reason_code, not MatchedTokens, for that.
		if resp.GetReplicaScores()[0].GetMatchedTokens() != 0 {
			t.Fatalf("TENANT_HOT MatchedTokens = %d, want 0", resp.GetReplicaScores()[0].GetMatchedTokens())
		}
	})
}

// TestLookupRouteEmptyHashSchemeFailsOpenOverGRPC pins the contract through
// the handler: even if the tenant has warm replicas that would qualify for
// TENANT_HOT, an unspecified hash_scheme on the request must surface as
// NO_HINT with empty scores — never a soft hint based on tenant stats alone.
func TestLookupRouteEmptyHashSchemeFailsOpenOverGRPC(t *testing.T) {
	svc := newTestService()
	svc.index.Ingest(index.Update{
		ReplicaID: "warm", Model: "m", Tenant: "t", HashScheme: "vllm",
		Stats: &index.ReplicaStats{HitRate: 0.9},
	})

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "t", HashScheme: "", PrefixHash: []byte("novel"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" || len(resp.GetReplicaScores()) != 0 {
		t.Fatalf("empty hash_scheme must fail open; got reason=%q scores=%+v",
			resp.GetReasonCode(), resp.GetReplicaScores())
	}
}

// TestLookupRouteSLOFlowsThroughHandler proves the proto SLO field actually
// reaches the index ranker. Two replicas hold the prefix and are ingested
// at "now" (so freshness ≈ 1 for both). With a tight SLO the score gets
// the freshness boost; with no SLO it does not. Guards against a future
// refactor that drops req.GetSlo() on the floor without anyone noticing.
func TestLookupRouteSLOFlowsThroughHandler(t *testing.T) {
	svc := newTestService()

	// TokenCount=128 keeps the realized match above the
	// DefaultMinimumMatchedTokens floor so this test pins SLO
	// plumbing, not the floor.
	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 128}},
	})

	base := &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "t", HashScheme: "vllm", PrefixHash: []byte("p"),
	}
	tight := &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "t", HashScheme: "vllm", PrefixHash: []byte("p"),
		Slo: &icpb.SLO{TtftMs: 50}, // < DefaultSLOTightTTFTMs (200) → tight
	}

	baseResp, err := svc.LookupRoute(context.Background(), base)
	if err != nil {
		t.Fatalf("LookupRoute (no SLO): %v", err)
	}
	tightResp, err := svc.LookupRoute(context.Background(), tight)
	if err != nil {
		t.Fatalf("LookupRoute (tight SLO): %v", err)
	}
	if baseResp.GetReasonCode() != "PREFIX_MATCH" || tightResp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("both should be PREFIX_MATCH, got base=%q tight=%q",
			baseResp.GetReasonCode(), tightResp.GetReasonCode())
	}
	if len(baseResp.GetReplicaScores()) != 1 || len(tightResp.GetReplicaScores()) != 1 {
		t.Fatalf("expected one score each, got base=%d tight=%d",
			len(baseResp.GetReplicaScores()), len(tightResp.GetReplicaScores()))
	}
	// The freshness boost (1 + freshness × DefaultSLOTightBias) is strictly
	// > 1 for any positive freshness, so the tight-SLO score must exceed
	// the no-SLO baseline. If the handler drops req.GetSlo() the index sees
	// TTFTBudgetMs=0, no bias is applied, and the two scores match.
	if tightResp.GetReplicaScores()[0].GetScore() <= baseResp.GetReplicaScores()[0].GetScore() {
		t.Fatalf("tight SLO did not reach the ranker — score not boosted (base=%v tight=%v)",
			baseResp.GetReplicaScores()[0].GetScore(), tightResp.GetReplicaScores()[0].GetScore())
	}
}

func TestRenderTemplateFailsOpen(t *testing.T) {
	resp, err := newTestService().RenderTemplate(context.Background(), &icpb.RenderTemplateRequest{TemplateRef: "t"})
	if err != nil {
		t.Fatalf("RenderTemplate: %v", err)
	}
	if resp.GetReasonCode() != "OK" {
		t.Fatalf("reason = %q, want OK", resp.GetReasonCode())
	}
	if len(resp.GetStablePrefixHash()) != 0 {
		t.Fatalf("expected empty stable_prefix_hash in stub, got %d bytes", len(resp.GetStablePrefixHash()))
	}
}

// startInProcessServer starts the service on an in-memory gRPC listener
// (bufconn) plus loopback HTTP listeners. It returns a connected gRPC client,
// the public HTTP base URL ("http://host:port") for the health/metrics
// endpoints, and a stop func that tears the server down and fails the test if
// Serve reported an error.
func startInProcessServer(t *testing.T) (client icpb.InferenceCacheClient, httpBaseURL string, stop func()) {
	t.Helper()
	conn, httpBaseURL, _, stop := startInProcessServerConnFull(t)
	return icpb.NewInferenceCacheClient(conn), httpBaseURL, stop
}

// startInProcessServerConn is the underlying helper that returns the raw gRPC
// client connection so tests using non-InferenceCache services (e.g. server
// reflection, grpc.health.v1) can attach their own clients to the same wire.
func startInProcessServerConn(t *testing.T) (*grpc.ClientConn, string, func()) {
	t.Helper()
	conn, httpBaseURL, _, stop := startInProcessServerConnFull(t)
	return conn, httpBaseURL, stop
}

// startInProcessServerConnFull additionally returns the snapshot listener's
// base URL so tests that exercise /snapshot can target the dedicated port.
// bufSize is the bufconn in-memory pipe buffer — it bounds bytes in flight on
// the fake socket, unrelated to the size of any individual CacheStateUpdate;
// 1 MiB is the bufconn default and is ample for these metadata-only messages.
func startInProcessServerConnFull(t *testing.T) (*grpc.ClientConn, string, string, func()) {
	t.Helper()

	const bufSize = 1024 * 1024
	grpcListener := bufconn.Listen(bufSize)

	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen http: %v", err)
	}
	snapshotListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = httpListener.Close()
		t.Fatalf("listen snapshot http: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- New().Serve(ctx, grpcListener, httpListener, snapshotListener)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return grpcListener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		cancel()
		t.Fatalf("dial bufnet: %v", err)
	}

	stop := func() {
		_ = conn.Close()
		cancel()
		if err := <-errCh; err != nil {
			t.Errorf("serve shutdown: %v", err)
		}
	}
	return conn, "http://" + httpListener.Addr().String(), "http://" + snapshotListener.Addr().String(), stop
}

// getString issues a GET against the server's HTTP endpoint and returns the
// status code and body.
func getString(t *testing.T, url string) (int, string) {
	t.Helper()
	httpClient := http.Client{Timeout: 2 * time.Second}
	resp, err := httpClient.Get(url)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body from %s: %v", url, err)
	}
	return resp.StatusCode, string(body)
}

// TestSnapshotEndpointReflectsIngest ingests state over gRPC and confirms the
// internal /snapshot HTTP endpoint reflects it as JSON (the controller scrapes
// this to populate the CacheIndex status).
func TestSnapshotEndpointReflectsIngest(t *testing.T) {
	conn, _, snapshotURL, stop := startInProcessServerConnFull(t)
	grpcClient := icpb.NewInferenceCacheClient(conn)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := grpcClient.ReportCacheState(ctx)
	if err != nil {
		t.Fatalf("open ReportCacheState: %v", err)
	}
	if err := stream.Send(&icpb.CacheStateUpdate{
		ReplicaId:  "replica-a",
		ModelId:    "llama-3-8b",
		TenantId:   "tenant-a",
		HashScheme: "vllm",
		Prefixes:   []*icpb.PrefixEntry{{PrefixHash: []byte("p"), TokenCount: 64}},
		Stats:      &icpb.ReplicaStats{ReplicaId: "replica-a", CacheMemoryBytes: 1234, HitRate: 0.75},
	}); err != nil {
		t.Fatalf("send update: %v", err)
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}

	code, body := getString(t, snapshotURL+"/snapshot")
	if code != http.StatusOK {
		t.Fatalf("/snapshot status = %d, want 200", code)
	}
	var snap index.Snapshot
	if err := json.Unmarshal([]byte(body), &snap); err != nil {
		t.Fatalf("decode snapshot JSON: %v (body=%s)", err, body)
	}
	if snap.TotalPrefixes != 1 {
		t.Fatalf("totalPrefixes = %d, want 1", snap.TotalPrefixes)
	}
	if len(snap.Replicas) != 1 || snap.Replicas[0].ReplicaID != "replica-a" || snap.Replicas[0].CacheMemoryBytes != 1234 {
		t.Fatalf("replicas = %+v, want replica-a with 1234 bytes", snap.Replicas)
	}
	if len(snap.Tenants) != 1 || snap.Tenants[0].TenantID != "tenant-a" {
		t.Fatalf("tenants = %+v, want tenant-a", snap.Tenants)
	}
}

// TestReportCacheState_AcceptsClientVersionOnReplicaStats is a wire-level smoke
// for the ReplicaStats.client_version (field 5) addition: a producer that sets
// client_version on its CacheStateUpdate.Stats must be accepted by the real
// server stream end-to-end (Ack returned, no error, the replica still lands in
// the snapshot), so older producers that don't fill the field AND newer ones
// that do both keep working today. The server doesn't yet surface
// client_version on /snapshot — that wiring lands in a follow-up PR; this test
// is the guard that the new field has reached the server's regenerated proto
// descriptor and is silently preserved on the ingest path.
func TestReportCacheState_AcceptsClientVersionOnReplicaStats(t *testing.T) {
	conn, _, snapshotURL, stop := startInProcessServerConnFull(t)
	grpcClient := icpb.NewInferenceCacheClient(conn)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := grpcClient.ReportCacheState(ctx)
	if err != nil {
		t.Fatalf("open ReportCacheState: %v", err)
	}
	if err := stream.Send(&icpb.CacheStateUpdate{
		ReplicaId:  "replica-cv",
		ModelId:    "llama-3-8b",
		TenantId:   "tenant-cv",
		HashScheme: "vllm",
		Prefixes:   []*icpb.PrefixEntry{{PrefixHash: []byte("p"), TokenCount: 64}},
		Stats: &icpb.ReplicaStats{
			ReplicaId:        "replica-cv",
			CacheMemoryBytes: 9999,
			HitRate:          0.5,
			ClientVersion:    "lmcache==0.4.2",
		},
	}); err != nil {
		t.Fatalf("send update with client_version: %v", err)
	}
	ack, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	if !ack.GetAccepted() {
		t.Fatalf("Ack.accepted = false, reason=%q — server rejected an update carrying client_version", ack.GetReasonCode())
	}

	code, body := getString(t, snapshotURL+"/snapshot")
	if code != http.StatusOK {
		t.Fatalf("/snapshot status = %d, want 200", code)
	}
	var snap index.Snapshot
	if err := json.Unmarshal([]byte(body), &snap); err != nil {
		t.Fatalf("decode snapshot JSON: %v (body=%s)", err, body)
	}
	if snap.TotalPrefixes != 1 {
		t.Fatalf("totalPrefixes = %d, want 1 — ingestion path dropped the update", snap.TotalPrefixes)
	}
	var got *index.ReplicaSnapshot
	for i := range snap.Replicas {
		if snap.Replicas[i].ReplicaID == "replica-cv" {
			got = &snap.Replicas[i]
			break
		}
	}
	if got == nil {
		t.Fatalf("replica-cv not present in /snapshot.replicas; client_version on the wire broke ingest: %+v", snap.Replicas)
	}
	if got.CacheMemoryBytes != 9999 {
		t.Errorf("replica-cv.cacheMemoryBytes = %d, want 9999 — other ReplicaStats fields stopped flowing alongside client_version", got.CacheMemoryBytes)
	}
}

// TestSnapshotNotServedOnPublicListener confirms /snapshot is *only* exposed
// on the dedicated listener — the same listener the NetworkPolicy restricts.
// A regression here would silently re-open the endpoint to every pod that can
// reach :8080 (the kubelet- and Prometheus-facing port).
func TestSnapshotNotServedOnPublicListener(t *testing.T) {
	_, publicURL, _, stop := startInProcessServerConnFull(t)
	defer stop()

	code, _ := getString(t, publicURL+"/snapshot")
	if code != http.StatusNotFound {
		t.Fatalf("/snapshot on public listener returned %d, want 404", code)
	}
}

// TestPolicyNotServedOnPublicListener is the symmetric guard for /policy:
// the endpoint must NEVER be exposed on the kubelet/Prometheus :8080
// listener — that's precisely the regression this hardening exists to
// close (any pod that could reach :8080 could replace cluster-wide
// CachePolicy state under replace-on-write). A POST returning 404 here
// proves the handler is only registered on the auth+NetworkPolicy gated
// listener.
func TestPolicyNotServedOnPublicListener(t *testing.T) {
	_, publicURL, _, stop := startInProcessServerConnFull(t)
	defer stop()

	code := postPolicy(t, publicURL+"/policy", "", emptyPolicySnapshotBody(t))
	if code != http.StatusNotFound {
		t.Fatalf("POST /policy on public listener returned %d, want 404", code)
	}
}

// TestPolicyServedOnSnapshotListener confirms the positive path: /policy is
// reachable on the dedicated controller-facing listener (unauth, since this
// test boots the server without WithControllerAuth) and the handler accepts
// a valid snapshot. Pairs with TestPolicyNotServedOnPublicListener — the two
// together pin the exact split-listener wiring this hardening introduces.
func TestPolicyServedOnSnapshotListener(t *testing.T) {
	_, _, snapshotURL, stop := startInProcessServerConnFull(t)
	defer stop()

	code := postPolicy(t, snapshotURL+"/policy", "", emptyPolicySnapshotBody(t))
	if code != http.StatusNoContent {
		t.Fatalf("POST /policy on snapshot listener returned %d, want 204", code)
	}
}

// emptyPolicySnapshotBody builds a JSON body whose Version matches the
// current PolicyPropagationVersion, so the auth/listener tests below don't
// silently turn into "the version field has drifted" failures whenever the
// schema is bumped. Reflects no policies and no tenants — the empty
// replace-on-write payload.
func emptyPolicySnapshotBody(t *testing.T) string {
	t.Helper()
	b, err := json.Marshal(PolicySnapshot{Version: PolicyPropagationVersion})
	if err != nil {
		t.Fatalf("marshal empty PolicySnapshot: %v", err)
	}
	return string(b)
}

// TestSnapshotRejectsNonGet confirms the /snapshot handler is GET-only and
// returns 405 (with an Allow: GET header) for POST / PUT / DELETE. The
// endpoint is read-only by design — accepting other methods would silently
// serve the same JSON back, which masks a misconfigured client and weakens
// the contract the design doc declares. This guard sits on the handler
// itself (no listener round-trip needed), so it runs without auth wiring.
func TestSnapshotRejectsNonGet(t *testing.T) {
	_, _, snapshotURL, stop := startInProcessServerConnFull(t)
	defer stop()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req, err := http.NewRequest(method, snapshotURL+"/snapshot", nil)
		if err != nil {
			t.Fatalf("new %s request: %v", method, err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s /snapshot: %v", method, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s /snapshot returned %d, want 405", method, resp.StatusCode)
		}
		if got := resp.Header.Get("Allow"); got != http.MethodGet {
			t.Errorf("%s /snapshot Allow header = %q, want %q", method, got, http.MethodGet)
		}
	}
}

// TestControllerAuth_RejectsUnauthenticated runs the full Service with the
// auth middleware wired in (using a fake TokenReviewer) and confirms BOTH
// the snapshot listener AND /policy on the same listener respond 401 without
// a bearer and 2xx with the controller SA. Pins the post-hardening shape:
// /snapshot + /policy share one ServiceAccount identity and one listener but
// emit per-endpoint metrics, so a dashboard can distinguish read-side (info
// leak) from write-side (active tampering) auth failures.
func TestControllerAuth_RejectsUnauthenticated(t *testing.T) {
	const sa = "system:serviceaccount:inference-cache-system:inference-cache-controller-manager"

	reviewer := fakeReviewerFunc(func(_ context.Context, tr *authnv1.TokenReview) (*authnv1.TokenReview, error) {
		if tr.Spec.Token == "good" {
			return &authnv1.TokenReview{Status: authnv1.TokenReviewStatus{
				Authenticated: true,
				User:          authnv1.UserInfo{Username: sa},
			}}, nil
		}
		return &authnv1.TokenReview{Status: authnv1.TokenReviewStatus{Authenticated: false}}, nil
	})

	const bufSize = 1024 * 1024
	grpcListener := bufconn.Listen(bufSize)
	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen public: %v", err)
	}
	snapshotListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen snapshot: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	// Audience left empty here — this test exercises the auth middleware's
	// expectedSA / cache / 401-on-missing-bearer matrix end-to-end against
	// real listeners for BOTH /snapshot AND /policy. The audience-binding
	// path is covered by the envtest integration in pkg/server/auth (real
	// apiserver mints audience-bound tokens); a fake reviewer can't
	// faithfully model audience enforcement.
	svc := New(WithControllerAuth(reviewer, sa, ""))
	go func() {
		errCh <- svc.Serve(ctx, grpcListener, httpListener, snapshotListener)
	}()
	defer func() {
		cancel()
		if err := <-errCh; err != nil {
			t.Errorf("serve shutdown: %v", err)
		}
	}()

	snapshotURL := "http://" + snapshotListener.Addr().String() + "/snapshot"
	policyURL := "http://" + snapshotListener.Addr().String() + "/policy"

	// /snapshot — unauth 401, then valid SA 200.
	if code, _ := getString(t, snapshotURL); code != http.StatusUnauthorized {
		t.Fatalf("unauth /snapshot status = %d, want 401", code)
	}
	if code, _ := getStringWithBearer(t, snapshotURL, "good"); code != http.StatusOK {
		t.Fatalf("authed /snapshot status = %d, want 200", code)
	}

	// /policy — unauth 401, then valid SA POST 204. The body is a minimal
	// valid PolicySnapshot so we exercise the auth gate AND the handler.
	policyBody := emptyPolicySnapshotBody(t)
	if code := postPolicy(t, policyURL, "", policyBody); code != http.StatusUnauthorized {
		t.Fatalf("unauth POST /policy status = %d, want 401", code)
	}
	if code := postPolicy(t, policyURL, "good", policyBody); code != http.StatusNoContent {
		t.Fatalf("authed POST /policy status = %d, want 204", code)
	}

	// Both per-endpoint auth metrics must surface both outcomes.
	publicURL := "http://" + httpListener.Addr().String()
	mcode, body := getString(t, publicURL+"/metrics")
	if mcode != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", mcode)
	}
	for _, want := range []string{
		`inferencecache_snapshot_auth_total{result="unauth"} 1`,
		`inferencecache_snapshot_auth_total{result="ok"} 1`,
		`inferencecache_policy_auth_total{result="unauth"} 1`,
		`inferencecache_policy_auth_total{result="ok"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q; body:\n%s", want, body)
		}
	}
}

func TestControllerAuth_PolicyUsesPolicyAudience(t *testing.T) {
	const wantSA = "system:serviceaccount:inference-cache-system:inference-cache-controller-manager"

	reviewer := fakeReviewerFunc(func(_ context.Context, tr *authnv1.TokenReview) (*authnv1.TokenReview, error) {
		wantAudience := map[string]string{
			"snapshot-token": "inferencecache.io/controller",
			"policy-token":   "inferencecache.io/policy",
			"probe-token":    "inferencecache.io/controller",
		}
		want, ok := wantAudience[tr.Spec.Token]
		if !ok || len(tr.Spec.Audiences) != 1 || tr.Spec.Audiences[0] != want {
			return &authnv1.TokenReview{Status: authnv1.TokenReviewStatus{Authenticated: false}}, nil
		}
		return &authnv1.TokenReview{Status: authnv1.TokenReviewStatus{
			Authenticated: true,
			User:          authnv1.UserInfo{Username: wantSA},
		}}, nil
	})

	const bufSize = 1024 * 1024
	grpcListener := bufconn.Listen(bufSize)
	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen public: %v", err)
	}
	snapshotListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen snapshot: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	svc := New(
		WithControllerAuth(reviewer, wantSA, "inferencecache.io/controller"),
		WithPolicyAudience("inferencecache.io/policy"),
	)
	go func() {
		errCh <- svc.Serve(ctx, grpcListener, httpListener, snapshotListener)
	}()
	defer func() {
		cancel()
		if err := <-errCh; err != nil {
			t.Errorf("serve shutdown: %v", err)
		}
	}()

	base := "http://" + snapshotListener.Addr().String()
	if code, _ := getStringWithBearer(t, base+"/snapshot", "snapshot-token"); code != http.StatusOK {
		t.Fatalf("GET /snapshot with controller-audience token status = %d, want 200", code)
	}
	if code := postPolicy(t, base+"/policy", "policy-token", emptyPolicySnapshotBody(t)); code != http.StatusNoContent {
		t.Fatalf("POST /policy with policy-audience token status = %d, want 204", code)
	}
	if code := postPolicy(t, base+"/policy", "snapshot-token", emptyPolicySnapshotBody(t)); code != http.StatusUnauthorized {
		t.Fatalf("POST /policy with controller-audience token status = %d, want 401", code)
	}
	if code := postProbe(t, base+"/probe", "probe-token", `{"backend":"smoke","model":"smoke-model","hashScheme":"vllm"}`); code != http.StatusOK {
		t.Fatalf("POST /probe with controller-audience token status = %d, want 200", code)
	}
}

// TestControllerAuth_PolicyRejectsForbidden confirms a valid but
// non-controller SA token gets 403 on /policy and increments the
// `forbidden` bucket of the policy counter — the alarming signal the
// per-endpoint metric is meant to surface (someone authenticated, but as
// the wrong identity).
func TestControllerAuth_PolicyRejectsForbidden(t *testing.T) {
	const wantSA = "system:serviceaccount:inference-cache-system:inference-cache-controller-manager"
	const wrongSA = "system:serviceaccount:other:somebody"

	reviewer := fakeReviewerFunc(func(_ context.Context, tr *authnv1.TokenReview) (*authnv1.TokenReview, error) {
		switch tr.Spec.Token {
		case "good":
			return &authnv1.TokenReview{Status: authnv1.TokenReviewStatus{
				Authenticated: true,
				User:          authnv1.UserInfo{Username: wantSA},
			}}, nil
		case "wrong-sa":
			return &authnv1.TokenReview{Status: authnv1.TokenReviewStatus{
				Authenticated: true,
				User:          authnv1.UserInfo{Username: wrongSA},
			}}, nil
		}
		return &authnv1.TokenReview{Status: authnv1.TokenReviewStatus{Authenticated: false}}, nil
	})

	const bufSize = 1024 * 1024
	grpcListener := bufconn.Listen(bufSize)
	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen public: %v", err)
	}
	snapshotListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen snapshot: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- New(WithControllerAuth(reviewer, wantSA, "")).Serve(ctx, grpcListener, httpListener, snapshotListener)
	}()
	defer func() {
		cancel()
		if err := <-errCh; err != nil {
			t.Errorf("serve shutdown: %v", err)
		}
	}()

	policyURL := "http://" + snapshotListener.Addr().String() + "/policy"
	if code := postPolicy(t, policyURL, "wrong-sa", emptyPolicySnapshotBody(t)); code != http.StatusForbidden {
		t.Fatalf("wrong-SA POST /policy status = %d, want 403", code)
	}

	_, body := getString(t, "http://"+httpListener.Addr().String()+"/metrics")
	if !strings.Contains(body, `inferencecache_policy_auth_total{result="forbidden"} 1`) {
		t.Fatalf("metrics missing policy forbidden counter; body:\n%s", body)
	}
}

// postPolicy issues a POST to the /policy endpoint with the given body and
// optional bearer token, returning the HTTP status. Distinct from
// getStringWithBearer because /policy is POST-only.
func postPolicy(t *testing.T, url, token, body string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	httpClient := http.Client{Timeout: 2 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	return resp.StatusCode
}

func postProbe(t *testing.T, url, token, body string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	httpClient := http.Client{Timeout: 2 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	return resp.StatusCode
}

// fakeReviewerFunc adapts a plain func to auth.TokenReviewer for server-side
// tests that don't need the auth package's test-only fakes.
type fakeReviewerFunc func(context.Context, *authnv1.TokenReview) (*authnv1.TokenReview, error)

func (f fakeReviewerFunc) CreateTokenReview(ctx context.Context, tr *authnv1.TokenReview) (*authnv1.TokenReview, error) {
	return f(ctx, tr)
}

func getStringWithBearer(t *testing.T, url, token string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	httpClient := http.Client{Timeout: 2 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, string(body)
}

// TestInferenceCacheServiceOverGRPC exercises the service over a real gRPC
// connection (in-memory bufconn). Unlike the handler-level tests above, this
// proves the service is actually registered on the server and that the
// ReportCacheState client-stream — the one handler with non-trivial control
// flow — drains updates and returns an Ack over the wire.
func TestInferenceCacheServiceOverGRPC(t *testing.T) {
	client, _, stop := startInProcessServer(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Unary RPC over the wire confirms the service is registered.
	lookup, err := client.LookupRoute(ctx, &icpb.LookupRouteRequest{ModelId: "m"})
	if err != nil {
		t.Fatalf("LookupRoute over grpc: %v", err)
	}
	if lookup.GetReasonCode() != "NO_HINT" {
		t.Fatalf("reason = %q, want NO_HINT", lookup.GetReasonCode())
	}

	// Client-stream happy path: send metadata-only updates, half-close, expect Ack.
	stream, err := client.ReportCacheState(ctx)
	if err != nil {
		t.Fatalf("open ReportCacheState: %v", err)
	}
	for _, replicaID := range []string{"replica-a", "replica-b"} {
		if err := stream.Send(&icpb.CacheStateUpdate{ReplicaId: replicaID}); err != nil {
			t.Fatalf("send update %q: %v", replicaID, err)
		}
	}
	ack, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	if !ack.GetAccepted() {
		t.Fatalf("ack.Accepted = false, want true")
	}
}

// TestGRPCServerReflectionEnumeratesRegisteredServices exercises gRPC server
// reflection over the wire and asserts the server enumerates both registered
// services (InferenceCache + grpc.health.v1.Health) without the client having
// to ship the .proto. This is the test gate for the operator-tooling promise:
// `grpcurl list` (and the file-descriptor lookups that back `grpcurl describe`
// and generic calls, all routed through the same reflection service)
// must work against a running policy server without `-proto` flags. The
// assertions here cover the list path; the describe/generic-call paths share
// the same registration and need no separate wire test.
func TestGRPCServerReflectionEnumeratesRegisteredServices(t *testing.T) {
	conn, _, stop := startInProcessServerConn(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := reflectpb.NewServerReflectionClient(conn).ServerReflectionInfo(ctx)
	if err != nil {
		t.Fatalf("open ServerReflectionInfo: %v", err)
	}
	if err := stream.Send(&reflectpb.ServerReflectionRequest{
		MessageRequest: &reflectpb.ServerReflectionRequest_ListServices{ListServices: ""},
	}); err != nil {
		t.Fatalf("send ListServices: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("recv reflection response: %v", err)
	}
	listResp := resp.GetListServicesResponse()
	if listResp == nil {
		t.Fatalf("expected ListServicesResponse, got %T (%v)", resp.GetMessageResponse(), resp.GetMessageResponse())
	}

	got := make([]string, 0, len(listResp.GetService()))
	for _, svc := range listResp.GetService() {
		got = append(got, svc.GetName())
	}
	for _, want := range []string{
		"grpc.health.v1.Health",
		"inferencecache.v1alpha1.InferenceCache",
	} {
		if !slices.Contains(got, want) {
			t.Errorf("reflection ListServices missing %q; got %v", want, got)
		}
	}
}

// TestReportCacheStateClientCancel covers the non-EOF error branch of the
// ReportCacheState handler. When the client cancels mid-stream (e.g. a crashed
// or disconnected engine adapter) instead of half-closing, the server's Recv
// returns a cancellation error — not io.EOF — and the handler propagates it
// rather than acking. Use a handler-level stream here so the test does not race
// real gRPC cancellation propagation against the server's success Ack path.
func TestReportCacheStateClientCancel(t *testing.T) {
	svc := newTestService()
	stream := &fakeReportStream{
		updates: []*icpb.CacheStateUpdate{{ReplicaId: "replica-a"}},
		recvErr: status.Error(codes.Canceled, "client canceled"),
	}

	err := svc.ReportCacheState(stream)
	if status.Code(err) != codes.Canceled {
		t.Fatalf("ReportCacheState error code = %v, want Canceled", status.Code(err))
	}
	if stream.acked {
		t.Fatal("ReportCacheState sent a success Ack after client cancellation")
	}
}

// TestReportThenLookupReturnsPrefixMatch is the B6 end-to-end path: a replica
// reports a prefix via ReportCacheState, then a LookupRoute for the same
// (model, tenant, hash_scheme, prefix_hash) returns that replica ranked with a
// PREFIX_MATCH — proving ingestion populates the index and lookups read it back
// over the wire.
func TestReportThenLookupReturnsPrefixMatch(t *testing.T) {
	client, _, stop := startInProcessServer(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	prefix := []byte("prefix-hash-bytes")

	stream, err := client.ReportCacheState(ctx)
	if err != nil {
		t.Fatalf("open ReportCacheState: %v", err)
	}
	if err := stream.Send(&icpb.CacheStateUpdate{
		ReplicaId:  "replica-a",
		ModelId:    "llama-3-8b",
		TenantId:   "tenant-a",
		HashScheme: "vllm",
		Prefixes:   []*icpb.PrefixEntry{{PrefixHash: prefix, TokenCount: 128}},
	}); err != nil {
		t.Fatalf("send update: %v", err)
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}

	resp, err := client.LookupRoute(ctx, &icpb.LookupRouteRequest{
		ModelId:    "llama-3-8b",
		TenantId:   "tenant-a",
		HashScheme: "vllm",
		PrefixHash: prefix,
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("reason = %q, want PREFIX_MATCH", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 1 || resp.GetReplicaScores()[0].GetReplicaId() != "replica-a" {
		t.Fatalf("expected single hit for replica-a, got %+v", resp.GetReplicaScores())
	}
	if got := resp.GetReplicaScores()[0].GetMatchedTokens(); got != 128 {
		t.Fatalf("matched_tokens = %d, want 128", got)
	}

	// A different hash_scheme must not match the same bytes (engine
	// isolation). Post-diagnostics the classifier surfaces the more specific
	// code — (tenant, model) is populated under vllm; the lookup asks under
	// sglang — so the reason narrows from NO_HINT to UNKNOWN_HASH_SCHEME.
	// The cross-scheme no-leak property is unchanged: no replica score
	// appears.
	other, err := client.LookupRoute(ctx, &icpb.LookupRouteRequest{
		ModelId: "llama-3-8b", TenantId: "tenant-a", HashScheme: "sglang", PrefixHash: prefix,
	})
	if err != nil {
		t.Fatalf("LookupRoute (other scheme): %v", err)
	}
	if other.GetReasonCode() != "UNKNOWN_HASH_SCHEME" {
		t.Fatalf("cross-scheme reason = %q, want UNKNOWN_HASH_SCHEME (scheme-mismatch diagnostic)", other.GetReasonCode())
	}
	if len(other.GetReplicaScores()) != 0 {
		t.Fatalf("cross-scheme lookup must yield no scores; got %+v", other.GetReplicaScores())
	}
}

func TestLookupPDRouteFailsOpen(t *testing.T) {
	resp, err := newTestService().LookupPDRoute(context.Background(), &icpb.LookupPDRouteRequest{ModelId: "m"})
	if err != nil {
		t.Fatalf("LookupPDRoute: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("reason = %q, want NO_HINT", resp.GetReasonCode())
	}
}

func TestGetCacheStateReturnsAggregate(t *testing.T) {
	svc := newTestService()
	svc.index.Ingest(index.Update{
		ReplicaID: "replica-a", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 10}},
		Stats:    &index.ReplicaStats{ReplicaID: "replica-a", CacheMemoryBytes: 2048, HitRate: 0.5, T2HitTokens: 600, T2QueryTokens: 1000},
	})

	resp, err := svc.GetCacheState(context.Background(), &icpb.GetCacheStateRequest{ModelId: "m", TenantId: "t"})
	if err != nil {
		t.Fatalf("GetCacheState: %v", err)
	}
	if resp.GetSummary().GetTotalPrefixes() != 1 {
		t.Fatalf("total_prefixes = %d, want 1", resp.GetSummary().GetTotalPrefixes())
	}
	if len(resp.GetReplicas()) != 1 || resp.GetReplicas()[0].GetReplicaId() != "replica-a" {
		t.Fatalf("expected replica-a stats, got %+v", resp.GetReplicas())
	}
	if resp.GetReplicas()[0].GetCacheMemoryBytes() != 2048 {
		t.Fatalf("cache_memory_bytes = %d, want 2048", resp.GetReplicas()[0].GetCacheMemoryBytes())
	}
	if r := resp.GetReplicas()[0]; r.GetT2HitTokens() != 600 || r.GetT2QueryTokens() != 1000 {
		t.Fatalf("t2 counters = (%d, %d), want (600, 1000)", r.GetT2HitTokens(), r.GetT2QueryTokens())
	}
}

// TestLookupRouteFailsOpenForReservedProbeTenant pins the symmetric read-side
// guard: an external gRPC caller that knows (or guesses) a backend name and
// re-derives the deterministic probe hash MUST NOT observe the synthetic
// __probe-<backend> replica when querying with tenant_id == ProbeTenantID.
// Fail-open with NO_HINT — the contract says "never error on the hot path",
// and the legitimate probe path uses index.LookupRoute directly (not the
// gRPC handler), so the in-process Run is unaffected.
func TestLookupRouteFailsOpenForReservedProbeTenant(t *testing.T) {
	svc := newTestService()
	// Seed something in the probe scope so a leak would actually surface.
	svc.index.Ingest(index.Update{
		ReplicaID: ProbeReplicaID("cb-1"), Model: "m", Tenant: ProbeTenantID, HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 32}},
	})

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: ProbeTenantID, HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("reason = %q, want NO_HINT — external lookup against reserved tenant must fail-open", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 0 {
		t.Fatalf("external lookup against reserved tenant returned scores: %+v", resp.GetReplicaScores())
	}
}

// TestLookupRouteEmitsMetricForReservedProbeTenantNoHint pins the metric
// contract on the reserved-tenant short-circuit: even though the call is
// fail-open without touching the index, observeLookup still fires with
// reason_code=NO_HINT + hint_used=false, so the "one increment per
// LookupRoute call" contract on inferencecache_lookup_route_calls_total
// holds. A future dashboard slicing the metric on tenant_id can surface
// external traffic against the reserved tenant as an attempted scope probe.
func TestLookupRouteEmitsMetricForReservedProbeTenantNoHint(t *testing.T) {
	svc := newTestService()
	if _, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: ProbeTenantID, HashScheme: "vllm", PrefixHash: []byte("p"),
	}); err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	got := lookupCallsValueFromService(t, svc, "m", "NO_HINT", "false")
	if got != 1 {
		t.Errorf("lookup_route_calls_total{model=m,reason=NO_HINT,hint_used=false} = %v, want 1 — reserved-tenant short-circuit must still emit the metric", got)
	}
}

// lookupCallsValueFromService reads the in-memory counter value for one
// label set off the service's per-Service Prometheus registry. Avoids
// pulling in github.com/prometheus/client_golang/prometheus/testutil
// (and its transitive kylelemons/godebug dep) for one assertion.
func lookupCallsValueFromService(t *testing.T, svc *inferenceCacheService, model, reason, hintUsed string) float64 {
	t.Helper()
	mfs, err := svc.metrics.registry.Gather()
	if err != nil {
		t.Fatalf("registry.Gather: %v", err)
	}
	const name = "inferencecache_lookup_route_calls_total"
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			matched := 0
			for _, l := range m.GetLabel() {
				switch l.GetName() {
				case "model":
					if l.GetValue() == model {
						matched++
					}
				case "reason_code":
					if l.GetValue() == reason {
						matched++
					}
				case "hint_used":
					if l.GetValue() == hintUsed {
						matched++
					}
				}
			}
			if matched == 3 {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// TestGetCacheStateReturnsEmptyForReservedProbeTenant is the same guard on
// the per-(tenant, model) aggregate RPC: an external caller MUST NOT see
// the probe's synthetic replica stats or prefix count via GetCacheState.
func TestGetCacheStateReturnsEmptyForReservedProbeTenant(t *testing.T) {
	svc := newTestService()
	svc.index.Ingest(index.Update{
		ReplicaID: ProbeReplicaID("cb-1"), Model: "m", Tenant: ProbeTenantID, HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 32}},
		Stats:    &index.ReplicaStats{ReplicaID: ProbeReplicaID("cb-1"), CacheMemoryBytes: 1234, HitRate: 1.0},
	})

	resp, err := svc.GetCacheState(context.Background(), &icpb.GetCacheStateRequest{
		ModelId: "m", TenantId: ProbeTenantID,
	})
	if err != nil {
		t.Fatalf("GetCacheState: %v", err)
	}
	if resp.GetSummary().GetTotalPrefixes() != 0 {
		t.Fatalf("GetCacheState exposed %d probe prefixes; want 0", resp.GetSummary().GetTotalPrefixes())
	}
	if len(resp.GetReplicas()) != 0 {
		t.Fatalf("GetCacheState exposed probe replicas: %+v", resp.GetReplicas())
	}
}

// TestSnapshotFiltersReservedProbeTenant pins the cluster-aggregate
// (/snapshot) read-side guard. The CacheIndex poller polls /snapshot every
// ~25s; a probe in flight during that poll must not be temporarily reflected
// into operator-visible CacheIndex CR status.
func TestSnapshotFiltersReservedProbeTenant(t *testing.T) {
	svc := newTestService()
	// Seed both a real tenant (visible) and the probe tenant (hidden).
	svc.index.Ingest(index.Update{
		ReplicaID: "real-r", Model: "m", Tenant: "real-tenant", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("rp"), TokenCount: 64}},
		Stats:    &index.ReplicaStats{ReplicaID: "real-r", CacheMemoryBytes: 5000, HitRate: 0.5},
	})
	svc.index.Ingest(index.Update{
		ReplicaID: ProbeReplicaID("cb-1"), Model: "m", Tenant: ProbeTenantID, HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("pp"), TokenCount: 16}},
		Stats:    &index.ReplicaStats{ReplicaID: ProbeReplicaID("cb-1"), CacheMemoryBytes: 1234, HitRate: 1.0},
	})

	snap := svc.index.Snapshot()

	if snap.TotalPrefixes != 1 {
		t.Errorf("TotalPrefixes = %d, want 1 — reserved tenant must not contribute to the cluster total", snap.TotalPrefixes)
	}
	for _, r := range snap.Replicas {
		if r.Tenant == ProbeTenantID || strings.HasPrefix(r.ReplicaID, ProbeReplicaPrefix) {
			t.Errorf("Snapshot exposed reserved replica row: %+v", r)
		}
	}
	for _, tn := range snap.Tenants {
		if tn.TenantID == ProbeTenantID {
			t.Errorf("Snapshot exposed reserved tenant row: %+v", tn)
		}
	}
}

// TestReportCacheStateDropsReservedProbeTenant pins the gRPC defense against
// an external client writing to the server-reserved probe scope: an ingest
// carrying tenant_id == ProbeTenantID is silently dropped. The complementary
// CacheTenant admission rule rejects a CR claiming the same id at the CRD
// layer; together they keep the probe scope server-internal across all
// reservation paths.
func TestReportCacheStateDropsReservedProbeTenant(t *testing.T) {
	svc := newTestService()
	stream := &fakeReportStream{updates: []*icpb.CacheStateUpdate{{
		ReplicaId:  "spoofed",
		ModelId:    "m",
		TenantId:   ProbeTenantID,
		HashScheme: "vllm",
		Prefixes:   []*icpb.PrefixEntry{{PrefixHash: []byte("p"), TokenCount: 32}},
	}}}
	if err := svc.ReportCacheState(stream); err != nil {
		t.Fatalf("ReportCacheState: %v", err)
	}
	scores := svc.index.Lookup(index.LookupRequest{
		Tenant: ProbeTenantID, Model: "m", HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	if len(scores) != 0 {
		t.Fatalf("external ingest under reserved probe tenant landed in the index: %+v", scores)
	}
}

// TestPublishEventDropsReservedProbeTenant pins the symmetric guard for
// CacheEvent: an external PREFIX_EVICTED / ALL_CLEARED targeting the probe
// tenant must not reach the index. The probe re-synthesizes on every Run so
// the impact would be limited, but the contract is "external clients can't
// touch server-internal state" — silent drop, no error on the hot path.
func TestPublishEventDropsReservedProbeTenant(t *testing.T) {
	svc := newTestService()
	svc.index.Ingest(index.Update{
		ReplicaID: "real", Model: "m", Tenant: ProbeTenantID, HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 32}},
	})
	ack, err := svc.PublishEvent(context.Background(), &icpb.CacheEvent{
		Type: icpb.CacheEvent_ALL_CLEARED, ReplicaId: "real",
		ModelId: "m", TenantId: ProbeTenantID,
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if !ack.GetAccepted() {
		t.Fatalf("Ack should be true even on a silent drop")
	}
	// The seeded entry must survive — the ALL_CLEARED was dropped before the index saw it.
	scores := svc.index.Lookup(index.LookupRequest{
		Tenant: ProbeTenantID, Model: "m", HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	if len(scores) != 1 || scores[0].ReplicaID != "real" {
		t.Fatalf("external CacheEvent against probe tenant disturbed reserved state: %+v", scores)
	}
}

// fakeReportStream replays a fixed slice of updates and signals EOF.
type fakeReportStream struct {
	icpb.InferenceCache_ReportCacheStateServer
	updates []*icpb.CacheStateUpdate
	recvErr error
	idx     int
	acked   bool
}

func (f *fakeReportStream) Recv() (*icpb.CacheStateUpdate, error) {
	if f.idx >= len(f.updates) {
		if f.recvErr != nil {
			return nil, f.recvErr
		}
		return nil, io.EOF
	}
	u := f.updates[f.idx]
	f.idx++
	return u, nil
}

func (f *fakeReportStream) SendAndClose(*icpb.Ack) error { f.acked = true; return nil }

func TestPublishEventAppliesToIndex(t *testing.T) {
	svc := newTestService()
	// Seed two replicas holding the same prefix, then evict one via PublishEvent.
	for _, r := range []string{"replica-a", "replica-b"} {
		svc.index.Ingest(index.Update{
			ReplicaID: r, Model: "m", Tenant: "t", HashScheme: "vllm",
			Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 10}},
		})
	}
	ack, err := svc.PublishEvent(context.Background(), &icpb.CacheEvent{
		Type: icpb.CacheEvent_PREFIX_EVICTED, ReplicaId: "replica-a",
		ModelId: "m", TenantId: "t", PrefixHash: []byte("p"),
	})
	if err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	if !ack.GetAccepted() {
		t.Fatalf("ack.Accepted = false, want true")
	}
	got := svc.index.Lookup(index.LookupRequest{Model: "m", Tenant: "t", HashScheme: "vllm", PrefixHash: []byte("p")})
	if len(got) != 1 || got[0].ReplicaID != "replica-b" {
		t.Fatalf("after PREFIX_EVICTED of replica-a, expected only replica-b; got %+v", got)
	}
}

func TestEventTypeFromProto(t *testing.T) {
	cases := map[icpb.CacheEvent_Type]index.EventType{
		icpb.CacheEvent_PREFIX_ADDED:     index.EventPrefixAdded,
		icpb.CacheEvent_PREFIX_EVICTED:   index.EventPrefixEvicted,
		icpb.CacheEvent_REPLICA_UPDATED:  index.EventReplicaUpdated,
		icpb.CacheEvent_ALL_CLEARED:      index.EventAllCleared,
		icpb.CacheEvent_TYPE_UNSPECIFIED: 0,
	}
	for in, want := range cases {
		if got := eventTypeFromProto(in); got != want {
			t.Errorf("eventTypeFromProto(%v) = %v, want %v", in, got, want)
		}
	}
}

func TestMicrosToTime(t *testing.T) {
	if got := microsToTime(0); !got.IsZero() {
		t.Fatalf("microsToTime(0) = %v, want zero time", got)
	}
	if got := microsToTime(1_000_000); got.IsZero() {
		t.Fatalf("microsToTime(1e6) should be non-zero")
	}
}

// TestLookupRouteAboveMinimumPrefixTokensProceedsToLookup verifies that a
// request whose prefix_token_count meets the policy threshold proceeds to
// the index lookup and returns the normal PREFIX_MATCH response.
func TestLookupRouteAboveMinimumPrefixTokensProceedsToLookup(t *testing.T) {
	svc := newTestService()
	svc.policies.Replace([]ResolvedPolicy{
		{Namespace: "team-a", MinimumPrefixTokens: 50},
	})
	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "team-a", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 100}},
	})

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "team-a", HashScheme: "vllm",
		PrefixHash: []byte("p"), PrefixTokenCount: 100, // clears the 50 threshold
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("reason = %q, want PREFIX_MATCH (request clears the threshold)", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 1 || resp.GetReplicaScores()[0].GetReplicaId() != "r" {
		t.Fatalf("expected hit on replica r, got %+v", resp.GetReplicaScores())
	}
}

// TestLookupRouteBelowMinimumPrefixTokensReturnsNoHintWithoutTouchingIndex
// pins the documented semantics: the threshold gates the request BEFORE
// the lookup. To prove the index is not touched even when a match exists,
// we inject a lookupFn that fails the test if ever called.
//
// The "skip the index lookup" optimization only applies when affinity
// routing is Disabled — with affinity Enabled (the kubebuilder default)
// the request goes through the full lookup so the index can classify
// UNKNOWN_HASH_SCHEME / UNKNOWN_MODEL diagnostics (which keep precedence
// over AFFINITY_HINT) and only then fall through to the affinity
// fallback. This test pins the optimization shape that still applies
// with affinity Disabled.
func TestLookupRouteBelowMinimumPrefixTokensReturnsNoHintWithoutTouchingIndex(t *testing.T) {
	svc := newTestService()
	fal := false
	svc.policies.Replace([]ResolvedPolicy{
		{Namespace: "team-a", MinimumPrefixTokens: 200, AffinityRouting: &fal},
	})
	svc.lookupFn = func(index.LookupRequest) index.LookupResult {
		t.Fatal("index lookup should not run when the request is below the policy threshold")
		return index.LookupResult{}
	}

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "team-a", HashScheme: "vllm",
		PrefixHash: []byte("p"), PrefixTokenCount: 10, // below the 200 threshold
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("reason = %q, want NO_HINT when request is below the threshold", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 0 {
		t.Fatalf("expected empty scores below the threshold, got %+v", resp.GetReplicaScores())
	}
}

func TestLookupRouteReturnsTimeoutWhenCallerDeadlineBreached(t *testing.T) {
	svc := newTestService()
	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "team-a", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 100}},
	})

	// Build a context whose deadline has already passed so the handler's
	// pre-lookup ctx.Err() check fires deterministically.
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancel()

	resp, err := svc.LookupRoute(ctx, &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "team-a", HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "TIMEOUT" {
		t.Fatalf("reason = %q, want TIMEOUT when caller deadline has passed", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 0 {
		t.Fatalf("expected empty scores on TIMEOUT, got %+v", resp.GetReplicaScores())
	}
}

// TestLookupRouteReturnsTimeoutEvenIfLookupRacesPastDeadline injects a
// lookup that returns immediately but waits until *after* the deadline
// has elapsed so the select arms with both channels ready. Re-checking
// ctx.Err() after resCh wins is what catches this — without it, Go's
// select pseudorandom choice could leak stale scores as PREFIX_MATCH.
func TestLookupRouteReturnsTimeoutEvenIfLookupRacesPastDeadline(t *testing.T) {
	svc := newTestService()
	svc.policies.Replace([]ResolvedPolicy{
		{Namespace: "team-a", LookupTimeoutMs: 5},
	})
	// Lookup deliberately exceeds the budget before returning a hit.
	svc.lookupFn = func(index.LookupRequest) index.LookupResult {
		time.Sleep(50 * time.Millisecond)
		return index.LookupResult{
			Strategy: index.StrategyPrefixMatch,
			Scores:   []index.ReplicaScore{{ReplicaID: "would-have-been-stale", MatchedTokens: 100}},
		}
	}

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "team-a", HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "TIMEOUT" {
		t.Fatalf("reason = %q, want TIMEOUT — lookup overran the budget", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 0 {
		t.Fatalf("TIMEOUT must not leak stale scores; got %+v", resp.GetReplicaScores())
	}
}

// TestLookupRouteBoundsWallTimeWhenLookupBlocks injects a lookup that
// blocks indefinitely; the handler must still return promptly under the
// policy budget rather than wait for the lookup. This guards the case
// where a writer holding the index lock past the budget would otherwise
// let the gRPC RPC exceed its deadline instead of fail-opening with
// reason_code:TIMEOUT.
func TestLookupRouteBoundsWallTimeWhenLookupBlocks(t *testing.T) {
	svc := newTestService()
	svc.policies.Replace([]ResolvedPolicy{
		{Namespace: "team-a", LookupTimeoutMs: 20},
	})

	// Replace the lookup with one that blocks until the test ends.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	svc.lookupFn = func(index.LookupRequest) index.LookupResult {
		<-release
		return index.LookupResult{}
	}

	start := time.Now()
	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "team-a", HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "TIMEOUT" {
		t.Fatalf("reason = %q, want TIMEOUT when the lookup hangs past the policy budget", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 0 {
		t.Fatalf("expected empty scores on TIMEOUT, got %+v", resp.GetReplicaScores())
	}
	// Allow generous slack to keep the test non-flaky on a loaded CI host,
	// but assert we returned in well under a second — the point is that the
	// handler didn't wait for the blocking lookup.
	if elapsed > time.Second {
		t.Fatalf("handler should have returned promptly under the budget; elapsed = %v", elapsed)
	}
}

func TestLookupRouteAppliesPolicyTimeoutBudget(t *testing.T) {
	svc := newTestService()
	// Generous budget (1s) so the assertion is about the *budget mechanism*,
	// not wall-clock racing. The in-memory lookup is ~tens of µs, but it runs
	// on the bounded path (goroutine + select) once a policy budget is set; a
	// sub-millisecond budget would race goroutine-scheduling jitter against the
	// ctx deadline and intermittently TIMEOUT on a loaded CI runner even though
	// the lookup did no slow work. A 1s budget can't be tripped by scheduling
	// jitter, so the "fast lookup stays under budget" invariant is deterministic.
	// The negative direction (a too-small budget DOES TIMEOUT) is covered
	// deterministically by TestLookupRouteReturnsTimeoutEvenIfLookupRacesPastDeadline
	// and TestLookupRouteBoundsWallTimeWhenLookupBlocks, which inject a lookup
	// that overruns the budget by a large, jitter-proof margin.
	svc.policies.Replace([]ResolvedPolicy{
		{Namespace: "team-a", LookupTimeoutMs: 1000},
	})
	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "team-a", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 100}},
	})

	// Within budget — TIMEOUT must NOT fire on a fast path.
	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "team-a", HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() == "TIMEOUT" {
		t.Fatalf("a sub-millisecond in-memory lookup should not breach a 1s budget; got %+v", resp)
	}
}

func TestLookupRouteUnaffectedByPolicyForUnknownTenant(t *testing.T) {
	svc := newTestService()
	svc.policies.Replace([]ResolvedPolicy{
		{Namespace: "team-a", MinimumPrefixTokens: 200, LookupTimeoutMs: 1},
	})
	// TokenCount=128 keeps the realized match above the server-wide
	// DefaultMinimumMatchedTokens floor, which fires for any
	// tenant with no CachePolicy. The test pins "team-a's policy doesn't
	// leak into team-b", not the floor.
	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "team-b", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 128}},
	})

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "team-b", HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" || len(resp.GetReplicaScores()) != 1 {
		t.Fatalf("team-b has no policy; lookup should pass through unfiltered. Got %+v", resp)
	}
}

// TestLookupRouteChainReturnsPartialPrefixMatch is the longest-prefix e2e: a replica
// reports a chain via ReportCacheState, then a LookupRoute carrying a longer
// chain that shares the first K blocks returns PREFIX_MATCH with
// matched_tokens reflecting the partial run (not the full request chain).
func TestLookupRouteChainReturnsPartialPrefixMatch(t *testing.T) {
	client, _, stop := startInProcessServer(t)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ingestHashes := [][]byte{[]byte("b1"), []byte("b2"), []byte("b3")}
	// Block tokens=64 each keeps the matched run (3 blocks × 64 = 192) above
	// the DefaultMinimumMatchedTokens floor, so the test pins
	// partial-run matched_tokens accounting, not the floor.
	ingestCounts := []int32{64, 64, 64}

	stream, err := client.ReportCacheState(ctx)
	if err != nil {
		t.Fatalf("open ReportCacheState: %v", err)
	}
	if err := stream.Send(&icpb.CacheStateUpdate{
		ReplicaId: "replica-a", ModelId: "llama-3-8b", TenantId: "tenant-a", HashScheme: "vllm",
		Prefixes: []*icpb.PrefixEntry{{BlockHashes: ingestHashes, BlockTokenCounts: ingestCounts}},
	}); err != nil {
		t.Fatalf("send update: %v", err)
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}

	lookupHashes := [][]byte{[]byte("b1"), []byte("b2"), []byte("b3"), []byte("x4"), []byte("x5")}
	lookupCounts := []int32{64, 64, 64, 64, 64}
	resp, err := client.LookupRoute(ctx, &icpb.LookupRouteRequest{
		ModelId: "llama-3-8b", TenantId: "tenant-a", HashScheme: "vllm",
		BlockHashes: lookupHashes, BlockTokenCounts: lookupCounts,
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("reason = %q, want PREFIX_MATCH (partial chain still counts)", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 1 || resp.GetReplicaScores()[0].GetReplicaId() != "replica-a" {
		t.Fatalf("expected single hit for replica-a, got %+v", resp.GetReplicaScores())
	}
	if got := resp.GetReplicaScores()[0].GetMatchedTokens(); got != 192 {
		t.Fatalf("matched_tokens = %d, want 192 (3 blocks × 64 — the partial run, not the full request chain)", got)
	}
}

// TestLookupRouteAboveMinimumPrefixTokensViaChainCounts verifies the
// minimumPrefixTokens gate uses the sum of block_token_counts when a chain
// request omits the legacy prefix_token_count (a chain-only caller). Without
// this fallback the policy threshold would erroneously short-circuit every
// chain request to NO_HINT regardless of its actual token budget.
//
// The policy explicitly sets MinimumMatchedTokens=0 so the §2.6 result-side
// floor is disabled for this namespace. Otherwise the 48-token realized
// match would clear the request-side gate (32) but fail the
// DefaultMinimumMatchedTokens (64) the apiserver materializes on a bare CR,
// and the test would assert PREFIX_MATCH for a configuration that production
// would actually downgrade to NO_HINT. Making the opt-out explicit pins the
// test scope to the request-side gate alone, matching its name.
func TestLookupRouteAboveMinimumPrefixTokensViaChainCounts(t *testing.T) {
	svc := newTestService()
	svc.policies.Replace([]ResolvedPolicy{
		{Namespace: "team-a", MinimumPrefixTokens: 32, MinimumMatchedTokens: 0},
	})
	hashes := [][]byte{[]byte("b1"), []byte("b2"), []byte("b3")}
	counts := []int32{16, 16, 16}
	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "team-a", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{BlockHashes: hashes, BlockTokenCounts: counts}},
	})

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "team-a", HashScheme: "vllm",
		BlockHashes: hashes, BlockTokenCounts: counts, // sum=48, clears 32
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("reason = %q, want PREFIX_MATCH — chain budget (48) clears threshold (32)", resp.GetReasonCode())
	}
}

// TestLookupRouteBelowMinimumPrefixTokensViaChainCounts confirms the gate
// still fires when the chain's summed token budget is below the threshold.
// See the comment on the sibling
// TestLookupRouteBelowMinimumPrefixTokensReturnsNoHintWithoutTouchingIndex
// for why this test scopes itself to affinityRouting=Disabled.
func TestLookupRouteBelowMinimumPrefixTokensViaChainCounts(t *testing.T) {
	svc := newTestService()
	fal := false
	svc.policies.Replace([]ResolvedPolicy{
		{Namespace: "team-a", MinimumPrefixTokens: 200, AffinityRouting: &fal},
	})
	svc.lookupFn = func(index.LookupRequest) index.LookupResult {
		t.Fatal("index lookup should not run when chain budget is below the threshold")
		return index.LookupResult{}
	}

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "team-a", HashScheme: "vllm",
		BlockHashes:      [][]byte{[]byte("b1"), []byte("b2")},
		BlockTokenCounts: []int32{16, 16}, // sum=32, below 200
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("reason = %q, want NO_HINT when chain budget is below the threshold", resp.GetReasonCode())
	}
}

func TestLookupRouteRequireChainGateReturnsPolicyReason(t *testing.T) {
	tests := []struct {
		name string
		req  *icpb.LookupRouteRequest
	}{
		{
			name: "legacy exact prefix hash",
			req:  &icpb.LookupRouteRequest{PrefixHash: []byte("p")},
		},
		{
			name: "malformed chain",
			req: &icpb.LookupRouteRequest{
				BlockHashes:      [][]byte{[]byte("b1"), []byte("b2")},
				BlockTokenCounts: []int32{16},
			},
		},
		{
			name: "token ids synthesize chain but do not carry one",
			req:  &icpb.LookupRouteRequest{TokenIds: make([]uint32, 32)},
		},
		{
			name: "prompt text could synthesize chain but does not carry one",
			req:  &icpb.LookupRouteRequest{PromptText: "hello"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := newTestService()
			reqChain := true
			svc.policies.Replace([]ResolvedPolicy{{
				Namespace: "team-a",
				Strategy:  &ResolvedLookupStrategy{RequireChain: &reqChain},
			}})
			svc.lookupFn = func(index.LookupRequest) index.LookupResult {
				t.Fatal("index lookup should not run when policy requires a carried chain and request lacks one")
				return index.LookupResult{}
			}

			tc.req.ModelId = "m"
			tc.req.TenantId = "team-a"
			tc.req.HashScheme = "vllm"
			resp, err := svc.LookupRoute(context.Background(), tc.req)
			if err != nil {
				t.Fatalf("LookupRoute: %v", err)
			}
			if resp.GetReasonCode() != reasonPolicyRequiresChain {
				t.Fatalf("reason = %q, want %s", resp.GetReasonCode(), reasonPolicyRequiresChain)
			}
			if len(resp.GetReplicaScores()) != 0 {
				t.Fatalf("policy gate must not return scores, got %+v", resp.GetReplicaScores())
			}
		})
	}
}

func TestLookupRouteDisableChainMatchingUsesExactPrefixHash(t *testing.T) {
	tokenHashes, tokenCounts := fingerprint.Chain(tokenSeq(1_000, 32), 16)
	promptTokens := tokenSeq(2_000, 32)
	promptHashes, promptCounts := fingerprint.Chain(promptTokens, 16)

	tests := []struct {
		name            string
		req             *icpb.LookupRouteRequest
		tokenizer       tokenize.Tokenizer
		wantPrefixHash  []byte
		wantTokenCount  int32
		wantEchoedToken []uint32
		wantNoLookup    bool
	}{
		{
			name: "legacy prefix wins over well formed chain",
			req: &icpb.LookupRouteRequest{
				PrefixHash:       []byte("legacy-exact"),
				PrefixTokenCount: 99,
				BlockHashes: [][]byte{
					[]byte("b1"),
					[]byte("b2"),
				},
				BlockTokenCounts: []int32{64, 64},
			},
			wantPrefixHash: []byte("legacy-exact"),
			wantTokenCount: 99,
		},
		{
			name: "malformed chain is ignored when legacy prefix exists",
			req: &icpb.LookupRouteRequest{
				PrefixHash:       []byte("legacy-exact"),
				PrefixTokenCount: 99,
				BlockHashes: [][]byte{
					[]byte("b1"),
					[]byte("b2"),
				},
				BlockTokenCounts: []int32{64},
			},
			wantPrefixHash: []byte("legacy-exact"),
			wantTokenCount: 99,
		},
		{
			name: "chain only fails open",
			req: &icpb.LookupRouteRequest{
				BlockHashes: [][]byte{
					[]byte("b1"),
					[]byte("b2"),
				},
				BlockTokenCounts: []int32{64, 64},
			},
			wantNoLookup: true,
		},
		{
			name:           "token ids use derived final block as exact prefix",
			req:            &icpb.LookupRouteRequest{TokenIds: tokenSeq(1_000, 32)},
			wantPrefixHash: tokenHashes[len(tokenHashes)-1],
			wantTokenCount: sumBlockTokenCounts(tokenCounts),
		},
		{
			name: "malformed chain is ignored before token ids",
			req: &icpb.LookupRouteRequest{
				BlockHashes:      [][]byte{[]byte("b1"), []byte("b2")},
				BlockTokenCounts: []int32{64},
				TokenIds:         tokenSeq(1_000, 32),
			},
			wantPrefixHash: tokenHashes[len(tokenHashes)-1],
			wantTokenCount: sumBlockTokenCounts(tokenCounts),
		},
		{
			name:            "prompt text uses derived final block as exact prefix",
			req:             &icpb.LookupRouteRequest{PromptText: "hello"},
			tokenizer:       fakeTokenizer{tokens: promptTokens},
			wantPrefixHash:  promptHashes[len(promptHashes)-1],
			wantTokenCount:  sumBlockTokenCounts(promptCounts),
			wantEchoedToken: promptTokens,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc := newTestService()
			if tc.tokenizer != nil {
				svc.tokenizer = tc.tokenizer
			}
			enableChain := false
			svc.policies.Replace([]ResolvedPolicy{{
				Namespace: "team-a",
				Strategy:  &ResolvedLookupStrategy{EnableChainMatching: &enableChain},
			}})
			var got index.LookupRequest
			var called bool
			svc.lookupFn = func(req index.LookupRequest) index.LookupResult {
				got = req
				called = true
				return index.LookupResult{Strategy: index.StrategyNone}
			}

			tc.req.ModelId = "m"
			tc.req.TenantId = "team-a"
			tc.req.HashScheme = "vllm"
			resp, err := svc.LookupRoute(context.Background(), tc.req)
			if err != nil {
				t.Fatalf("LookupRoute: %v", err)
			}
			if tc.wantNoLookup {
				if called {
					t.Fatal("index lookup was called")
				}
				if resp.GetReasonCode() != reasonNoHint {
					t.Fatalf("reason = %q, want %s", resp.GetReasonCode(), reasonNoHint)
				}
				return
			}
			if !called {
				t.Fatal("index lookup was not called")
			}
			if len(got.BlockHashes) != 0 || len(got.BlockTokenCounts) != 0 {
				t.Fatalf("chain fields reached index despite enableChainMatching=false: hashes=%d counts=%d", len(got.BlockHashes), len(got.BlockTokenCounts))
			}
			if !bytes.Equal(got.PrefixHash, tc.wantPrefixHash) {
				t.Fatalf("PrefixHash = %x, want %x", got.PrefixHash, tc.wantPrefixHash)
			}
			if got.TokenCount != tc.wantTokenCount {
				t.Fatalf("TokenCount = %d, want %d", got.TokenCount, tc.wantTokenCount)
			}
			if !equalU32(resp.GetTokenIds(), tc.wantEchoedToken) {
				t.Fatalf("echoed token_ids = %v, want %v", resp.GetTokenIds(), tc.wantEchoedToken)
			}
		})
	}
}

func TestLookupRouteDisableChainMatchingMinPrefixIgnoresChainCounts(t *testing.T) {
	svc := newTestService()
	enableChain := false
	affDisabled := false
	svc.policies.Replace([]ResolvedPolicy{{
		Namespace:           "team-a",
		MinimumPrefixTokens: 100,
		Strategy:            &ResolvedLookupStrategy{EnableChainMatching: &enableChain},
		// affinity Disabled so the below-threshold request still short-circuits
		// to NO_HINT without an index lookup (affinity Enabled would run the
		// full lookup to classify diagnostics before any fallback).
		AffinityRouting: &affDisabled,
	}})
	svc.lookupFn = func(index.LookupRequest) index.LookupResult {
		t.Fatal("index lookup should not run when the effective exact prefix is below the threshold")
		return index.LookupResult{}
	}

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId:          "m",
		TenantId:         "team-a",
		HashScheme:       "vllm",
		PrefixHash:       []byte("legacy-exact"),
		PrefixTokenCount: 10,
		BlockHashes: [][]byte{
			[]byte("b1"),
			[]byte("b2"),
		},
		BlockTokenCounts: []int32{100, 100},
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != reasonNoHint {
		t.Fatalf("reason = %q, want %s", resp.GetReasonCode(), reasonNoHint)
	}
}

func TestLookupRouteDisableTenantHotDowngradesToNoHint(t *testing.T) {
	svc := newTestService()
	enableTenantHot := false
	affDisabled := false
	svc.policies.Replace([]ResolvedPolicy{{
		Namespace: "t",
		Strategy:  &ResolvedLookupStrategy{EnableTenantHot: &enableTenantHot},
		// affinity Disabled so the tenant-hot downgrade surfaces as NO_HINT in
		// isolation (affinity Enabled would pick up the StrategyNone result and
		// return AFFINITY_HINT — covered by the affinity tests).
		AffinityRouting: &affDisabled,
	}})
	svc.index.Ingest(index.Update{
		ReplicaID: "warm-r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("unrelated"), TokenCount: 64}},
		Stats:    &index.ReplicaStats{HitRate: 0.9, Pressure: 0.0},
	})

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "t", HashScheme: "vllm", PrefixHash: []byte("miss"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != reasonNoHint {
		t.Fatalf("reason = %q, want NO_HINT when tenant-hot is disabled", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 0 {
		t.Fatalf("tenant-hot disabled must not return scores, got %+v", resp.GetReplicaScores())
	}
}

// TestLookupRouteMalformedChainNeverFallsThroughToTenantHot guards the
// orchestrator: a chain-bearing request with mismatched parallel-array
// lengths must hard-fail to NO_HINT, NOT fall through to TENANT_HOT — a
// soft locality hint against an unrelated warm replica would surface a
// wrong answer for what the producer asserted as a chain.
func TestLookupRouteMalformedChainNeverFallsThroughToTenantHot(t *testing.T) {
	svc := newTestService()
	// Seed a TENANT_HOT-eligible replica: warm stats AND a prefix entry in
	// the requested engine domain (the engine-domain guard requires that).
	svc.index.Ingest(index.Update{
		ReplicaID: "warm-r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("unrelated"), TokenCount: 64}},
		Stats:    &index.ReplicaStats{HitRate: 0.9, Pressure: 0.0},
	})

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "t", HashScheme: "vllm",
		BlockHashes:      [][]byte{[]byte("b1"), []byte("b2")},
		BlockTokenCounts: []int32{16}, // mismatched: 2 hashes, 1 count
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("reason = %q, want NO_HINT — malformed chain must not be downgraded to TENANT_HOT (would surface %d unrelated warm-tenant hint(s))", resp.GetReasonCode(), len(resp.GetReplicaScores()))
	}
	if len(resp.GetReplicaScores()) != 0 {
		t.Fatalf("malformed chain must not return any scores, got %+v", resp.GetReplicaScores())
	}
}

// TestEffectivePrefixTokensChainTakesPrecedence verifies the precedence
// rule the doc claims: when both the chain and the legacy
// prefix_token_count are set, the chain's sum wins so the
// CachePolicy.minimumPrefixTokens gate uses what the chain producer
// actually reported, not whatever stale legacy count was tagged along.
func TestEffectivePrefixTokensChainTakesPrecedence(t *testing.T) {
	cases := []struct {
		name string
		req  *icpb.LookupRouteRequest
		want int32
	}{
		{
			name: "chain wins over legacy",
			req: &icpb.LookupRouteRequest{
				PrefixTokenCount: 999,
				BlockTokenCounts: []int32{16, 16, 16}, // sum = 48
			},
			want: 48,
		},
		{
			name: "legacy used when chain empty",
			req:  &icpb.LookupRouteRequest{PrefixTokenCount: 128},
			want: 128,
		},
		{
			name: "chain-only request reports chain sum",
			req: &icpb.LookupRouteRequest{
				BlockTokenCounts: []int32{32, 32, 32},
			},
			want: 96,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectivePrefixTokens(tc.req.GetBlockTokenCounts(), tc.req.GetPrefixTokenCount()); got != tc.want {
				t.Fatalf("effectivePrefixTokens = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestLookupRouteChainNoOverlapNeverFallsThroughToTenantHot is the symmetric
// guard to TestLookupRouteMalformedChainNeverFallsThroughToTenantHot: a
// chain-bearing request with no first-block match (under matching contract
// keys — same (tenant, model, hash_scheme) as the warm replica) must
// hard-stop at NO_HINT, not surface a TENANT_HOT hint against an unrelated
// warm replica. The chain caller asked specifically for longest-prefix
// matching; a soft locality nudge is not what they requested. (Chain
// misses with a MISMATCHED contract key surface as the matching UNKNOWN_*
// code instead — that path is covered by the diagnostics tests; this test
// is about the same-key novel-chain case.)
func TestLookupRouteChainNoOverlapNeverFallsThroughToTenantHot(t *testing.T) {
	svc := newTestService()
	// Isolate this test to its named concern (chain miss must not surface
	// TENANT_HOT). The affinity-routing fallback would otherwise
	// turn this exact chain-miss-with-replicas-known scenario into
	// AFFINITY_HINT — that's the right behavior for the affinity path
	// (covered in affinity_routing_test.go) but orthogonal to the chain
	// vs TENANT_HOT invariant this test pins.
	fal := false
	svc.policies.Replace([]ResolvedPolicy{{Namespace: "t", AffinityRouting: &fal}})

	svc.index.Ingest(index.Update{
		ReplicaID: "warm-r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("unrelated"), TokenCount: 64}},
		Stats:    &index.ReplicaStats{HitRate: 0.9, Pressure: 0.0},
	})

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "t", HashScheme: "vllm",
		BlockHashes:      [][]byte{[]byte("q1"), []byte("q2")},
		BlockTokenCounts: []int32{16, 16}, // well-formed, but nobody holds q1
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("reason = %q, want NO_HINT — chain miss must not be downgraded to TENANT_HOT (got %d unrelated warm-tenant hint(s))", resp.GetReasonCode(), len(resp.GetReplicaScores()))
	}
	if len(resp.GetReplicaScores()) != 0 {
		t.Fatalf("chain miss must not return any scores, got %+v", resp.GetReplicaScores())
	}
}

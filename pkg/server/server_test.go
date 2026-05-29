package server

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/cachebox-project/inference-cache/pkg/index"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// newTestService builds a service backed by a fresh, empty index + policy
// store for handler-level unit tests. The index uses the policy store as its
// TTL resolver so per-tenant TTL changes during a test are reflected exactly
// as they would be in the running binary.
func newTestService() *inferenceCacheService {
	policies := NewPolicyStore()
	idx := index.New(index.WithTTLResolver(policies))
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
// (bufconn) plus a loopback HTTP listener. It returns a connected gRPC client,
// the HTTP base URL ("http://host:port") for the health/metrics endpoints, and
// a stop func that tears the server down and fails the test if Serve reported
// an error. bufSize is the size of bufconn's in-memory pipe buffer — it bounds
// bytes in flight on the fake socket, unrelated to the size of any individual
// CacheStateUpdate; 1 MiB is the standard bufconn default and is ample for
// these metadata-only messages.
func startInProcessServer(t *testing.T) (client icpb.InferenceCacheClient, httpBaseURL string, stop func()) {
	t.Helper()

	const bufSize = 1024 * 1024
	grpcListener := bufconn.Listen(bufSize)

	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen http: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- New().Serve(ctx, grpcListener, httpListener)
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

	stop = func() {
		_ = conn.Close()
		cancel()
		if err := <-errCh; err != nil {
			t.Errorf("serve shutdown: %v", err)
		}
	}
	return icpb.NewInferenceCacheClient(conn), "http://" + httpListener.Addr().String(), stop
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
	grpcClient, baseURL, stop := startInProcessServer(t)
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

	code, body := getString(t, baseURL+"/snapshot")
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

// TestReportCacheStateClientCancel covers the non-EOF error branch of the
// ReportCacheState handler. When the client cancels mid-stream (e.g. a crashed
// or disconnected engine adapter) instead of half-closing, the server's Recv
// returns a cancellation error — not io.EOF — and the handler propagates it
// rather than acking. The happy-path test only exercises the EOF→Ack branch.
func TestReportCacheStateClientCancel(t *testing.T) {
	client, _, stop := startInProcessServer(t)
	defer stop()

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.ReportCacheState(ctx)
	if err != nil {
		cancel()
		t.Fatalf("open ReportCacheState: %v", err)
	}
	if err := stream.Send(&icpb.CacheStateUpdate{ReplicaId: "replica-a"}); err != nil {
		cancel()
		t.Fatalf("send update: %v", err)
	}

	// Abort mid-stream without half-closing; the server must not return a success Ack.
	cancel()

	if ack, err := stream.CloseAndRecv(); err == nil {
		t.Fatalf("CloseAndRecv after cancel: got ack %+v, want error", ack)
	} else if status.Code(err) != codes.Canceled {
		t.Fatalf("error code = %v, want Canceled", status.Code(err))
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

	// A different hash_scheme must not match the same bytes (engine isolation).
	other, err := client.LookupRoute(ctx, &icpb.LookupRouteRequest{
		ModelId: "llama-3-8b", TenantId: "tenant-a", HashScheme: "sglang", PrefixHash: prefix,
	})
	if err != nil {
		t.Fatalf("LookupRoute (other scheme): %v", err)
	}
	if other.GetReasonCode() != "NO_HINT" {
		t.Fatalf("cross-scheme reason = %q, want NO_HINT", other.GetReasonCode())
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
		Stats:    &index.ReplicaStats{ReplicaID: "replica-a", CacheMemoryBytes: 2048, HitRate: 0.5},
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
}

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

// TestLookupRouteRespectsMinimumPrefixTokens seeds two replicas with different
// matched-token counts under one tenant's CachePolicy threshold; only the one
// at or above the threshold should be returned, and the response shape stays
// fail-open when none clear the bar.
func TestLookupRouteRespectsMinimumPrefixTokens(t *testing.T) {
	svc := newTestService()
	svc.policies.Replace([]ResolvedPolicy{
		{Namespace: "team-a", MinimumPrefixTokens: 50},
	})

	// replica-low has 10 tokens; replica-high has 100. Threshold is 50.
	for r, tok := range map[string]int32{"replica-low": 10, "replica-high": 100} {
		svc.index.Ingest(index.Update{
			ReplicaID: r, Model: "m", Tenant: "team-a", HashScheme: "vllm",
			Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: tok}},
		})
	}

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "team-a", HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("reason = %q, want PREFIX_MATCH (replica-high clears the threshold)", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 1 || resp.GetReplicaScores()[0].GetReplicaId() != "replica-high" {
		t.Fatalf("expected only replica-high after filter, got %+v", resp.GetReplicaScores())
	}
}

func TestLookupRouteReturnsNoHintWhenAllBelowThreshold(t *testing.T) {
	svc := newTestService()
	svc.policies.Replace([]ResolvedPolicy{
		{Namespace: "team-a", MinimumPrefixTokens: 200},
	})
	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "team-a", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 10}},
	})

	resp, err := svc.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "m", TenantId: "team-a", HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("reason = %q, want NO_HINT when nothing clears the threshold", resp.GetReasonCode())
	}
	if len(resp.GetReplicaScores()) != 0 {
		t.Fatalf("expected empty scores when filtered out, got %+v", resp.GetReplicaScores())
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
	svc.lookupFn = func(index.LookupRequest) []index.ReplicaScore {
		time.Sleep(50 * time.Millisecond)
		return []index.ReplicaScore{{ReplicaID: "would-have-been-stale", MatchedTokens: 100}}
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
// policy budget rather than wait for the lookup. This is the case
// Codex's review flagged: a writer holding the index lock past the
// budget would otherwise let the gRPC RPC exceed its deadline instead
// of fail-opening with reason_code:TIMEOUT.
func TestLookupRouteBoundsWallTimeWhenLookupBlocks(t *testing.T) {
	svc := newTestService()
	svc.policies.Replace([]ResolvedPolicy{
		{Namespace: "team-a", LookupTimeoutMs: 20},
	})

	// Replace the lookup with one that blocks until the test ends.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	svc.lookupFn = func(index.LookupRequest) []index.ReplicaScore {
		<-release
		return nil
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
	// Tiny budget; lookup itself is sub-µs on an empty index but the
	// pre-lookup ctx.Err() check is what we exercise here.
	svc.policies.Replace([]ResolvedPolicy{
		{Namespace: "team-a", LookupTimeoutMs: 1},
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
		t.Fatalf("a sub-millisecond in-memory lookup should not breach a 1ms budget; got %+v", resp)
	}
}

func TestLookupRouteUnaffectedByPolicyForUnknownTenant(t *testing.T) {
	svc := newTestService()
	svc.policies.Replace([]ResolvedPolicy{
		{Namespace: "team-a", MinimumPrefixTokens: 200, LookupTimeoutMs: 1},
	})
	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "team-b", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 10}},
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

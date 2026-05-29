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

// newTestService builds a service backed by a fresh, empty index for
// handler-level unit tests.
func newTestService() *inferenceCacheService {
	return newInferenceCacheService(index.New(), newServerMetrics())
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
		svc.index.Ingest(index.Update{
			ReplicaID: "r1", Model: "m", Tenant: "t", HashScheme: "vllm",
			Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 32}},
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

// TestLookupRouteSLOFlowsThroughHandler proves the proto SLO field actually
// reaches the index ranker. Two replicas hold the prefix and are ingested
// at "now" (so freshness ≈ 1 for both). With a tight SLO the score gets
// the freshness boost; with no SLO it does not. Guards against a future
// refactor that drops req.GetSlo() on the floor without anyone noticing.
func TestLookupRouteSLOFlowsThroughHandler(t *testing.T) {
	svc := newTestService()

	svc.index.Ingest(index.Update{
		ReplicaID: "r", Model: "m", Tenant: "t", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 50}},
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

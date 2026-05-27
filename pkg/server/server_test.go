package server

import (
	"context"
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

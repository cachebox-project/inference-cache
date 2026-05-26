package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

func TestHealthzReturnsOK(t *testing.T) {
	grpcListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen http: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- New().Serve(ctx, grpcListener, httpListener)
	}()

	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + httpListener.Addr().String() + "/healthz")
	if err != nil {
		cancel()
		t.Fatalf("get /healthz: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		cancel()
		t.Fatalf("read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		cancel()
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if string(body) != "ok\n" {
		cancel()
		t.Fatalf("body = %q, want ok", string(body))
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("serve shutdown: %v", err)
	}
}

func TestLookupRouteFailsOpen(t *testing.T) {
	resp, err := newInferenceCacheService().LookupRoute(context.Background(), &icpb.LookupRouteRequest{ModelId: "m"})
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
	resp, err := newInferenceCacheService().RenderTemplate(context.Background(), &icpb.RenderTemplateRequest{TemplateRef: "t"})
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

// TestInferenceCacheServiceOverGRPC exercises the service over a real gRPC
// connection (in-memory bufconn). Unlike the handler-level tests above, this
// proves the service is actually registered on the server and that the
// ReportCacheState client-stream — the one handler with non-trivial control
// flow — drains updates and returns an Ack over the wire.
func TestInferenceCacheServiceOverGRPC(t *testing.T) {
	const bufSize = 1024 * 1024
	grpcListener := bufconn.Listen(bufSize)

	// HTTP server still needs a real listener; bind an ephemeral local port.
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
	defer conn.Close()

	client := icpb.NewInferenceCacheClient(conn)
	callCtx, callCancel := context.WithTimeout(ctx, 5*time.Second)
	defer callCancel()

	// Unary RPC over the wire confirms the service is registered.
	lookup, err := client.LookupRoute(callCtx, &icpb.LookupRouteRequest{ModelId: "m"})
	if err != nil {
		cancel()
		t.Fatalf("LookupRoute over grpc: %v", err)
	}
	if lookup.GetReasonCode() != "NO_HINT" {
		cancel()
		t.Fatalf("reason = %q, want NO_HINT", lookup.GetReasonCode())
	}

	// Client-stream: send a couple of metadata-only updates, expect an Ack.
	stream, err := client.ReportCacheState(callCtx)
	if err != nil {
		cancel()
		t.Fatalf("open ReportCacheState: %v", err)
	}
	for _, replicaID := range []string{"replica-a", "replica-b"} {
		if err := stream.Send(&icpb.CacheStateUpdate{ReplicaId: replicaID}); err != nil {
			cancel()
			t.Fatalf("send update %q: %v", replicaID, err)
		}
	}
	ack, err := stream.CloseAndRecv()
	if err != nil {
		cancel()
		t.Fatalf("CloseAndRecv: %v", err)
	}
	if !ack.GetAccepted() {
		cancel()
		t.Fatalf("ack.Accepted = false, want true")
	}

	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("serve shutdown: %v", err)
	}
}

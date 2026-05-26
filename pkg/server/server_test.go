package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

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

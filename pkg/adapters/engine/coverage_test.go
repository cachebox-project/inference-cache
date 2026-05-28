package engine

import (
	"context"
	"encoding/binary"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

func TestReporterOptionsApplied(t *testing.T) {
	r := NewReporter(nil, testConfig(),
		WithWindow(7*time.Millisecond), WithRPCTimeout(2*time.Second), WithLogger(slog.Default()))
	if r.window != 7*time.Millisecond || r.rpcTimeout != 2*time.Second {
		t.Errorf("options not applied: window=%v rpcTimeout=%v", r.window, r.rpcTimeout)
	}
	// Non-positive values clamp to defaults.
	d := NewReporter(nil, testConfig(), WithWindow(0), WithRPCTimeout(-1))
	if d.window <= 0 || d.rpcTimeout <= 0 {
		t.Errorf("non-positive options not clamped: window=%v rpcTimeout=%v", d.window, d.rpcTimeout)
	}
}

func TestSubscriberOptionsApplied(t *testing.T) {
	s := NewSubscriber("tcp://x", "topic",
		WithSubscriberBackoff(9*time.Millisecond), WithSubscriberLogger(slog.Default()))
	if s.backoff != 9*time.Millisecond {
		t.Errorf("backoff not applied: %v", s.backoff)
	}
	if NewSubscriber("tcp://x", "t", WithSubscriberBackoff(0)).backoff <= 0 {
		t.Error("non-positive backoff not clamped")
	}
}

func TestMicrosFromSeconds(t *testing.T) {
	if microsFromSeconds(0) != 0 || microsFromSeconds(-3) != 0 {
		t.Error("non-positive seconds should map to 0 (server treats as now)")
	}
	if microsFromSeconds(2.5) != 2_500_000 {
		t.Errorf("got %d, want 2500000", microsFromSeconds(2.5))
	}
}

// hashToBytes must accept both a string (msgpack str) and a signed integer hash,
// not just []byte / unsigned.
func TestDecodeHashVariants(t *testing.T) {
	payload := encodeVLLMBatch(t, 1,
		[]interface{}{"BlockStored", []interface{}{"raw-bytes", int8(5)}, nil, []int64{}, int32(16), nil})
	batch, err := DecodeEventBatch(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	stored := batch.Events[0].(BlockStored)
	if string(stored.BlockHashes[0]) != "raw-bytes" {
		t.Errorf("string hash = %q, want raw-bytes", stored.BlockHashes[0])
	}
	if binary.BigEndian.Uint64(stored.BlockHashes[1]) != 5 {
		t.Errorf("signed-int hash decoded to %d, want 5", binary.BigEndian.Uint64(stored.BlockHashes[1]))
	}
}

// errorServer fails every RPC, exercising the Reporter's fail-soft error paths.
type errorServer struct {
	icpb.UnimplementedInferenceCacheServer
}

func (errorServer) ReportCacheState(stream icpb.InferenceCache_ReportCacheStateServer) error {
	return status.Error(codes.Internal, "boom")
}
func (errorServer) PublishEvent(context.Context, *icpb.CacheEvent) (*icpb.Ack, error) {
	return nil, status.Error(codes.Internal, "boom")
}

// A server that errors on every call must not crash or hang the Reporter — adds
// and removals are logged and dropped (soft state).
func TestReporterSurvivesServerErrors(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	icpb.RegisterInferenceCacheServer(srv, errorServer{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	r := NewReporter(icpb.NewInferenceCacheClient(conn), testConfig(),
		WithWindow(10*time.Millisecond), WithRPCTimeout(time.Second))
	in := make(chan *EventBatch, 2)
	in <- &EventBatch{TimestampSeconds: 1, Events: []Event{BlockStored{BlockHashes: [][]byte{{1}}, BlockSize: 16}}}
	in <- &EventBatch{TimestampSeconds: 2, Events: []Event{BlockRemoved{BlockHashes: [][]byte{{1}}}}}
	close(in)

	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background(), in) }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned error on server failure (should be fail-soft): %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run hung on a failing server")
	}
}

package engine

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// recordingServer captures what the Reporter sends, over a real gRPC connection.
type recordingServer struct {
	icpb.UnimplementedInferenceCacheServer
	mu      sync.Mutex
	updates []*icpb.CacheStateUpdate
	events  []*icpb.CacheEvent
}

func (s *recordingServer) ReportCacheState(stream icpb.InferenceCache_ReportCacheStateServer) error {
	for {
		u, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return stream.SendAndClose(&icpb.Ack{Accepted: true})
			}
			return err
		}
		s.mu.Lock()
		s.updates = append(s.updates, u)
		s.mu.Unlock()
	}
}

func (s *recordingServer) PublishEvent(_ context.Context, ev *icpb.CacheEvent) (*icpb.Ack, error) {
	s.mu.Lock()
	s.events = append(s.events, ev)
	s.mu.Unlock()
	return &icpb.Ack{Accepted: true}, nil
}

func runReporter(t *testing.T, batches ...*EventBatch) *recordingServer {
	return runReporterWindow(t, 20*time.Millisecond, batches...)
}

// runReporterWindow starts a recording server + Reporter over bufconn, feeds the
// batches, closes the input, and returns the server after Run has drained.
func runReporterWindow(t *testing.T, window time.Duration, batches ...*EventBatch) *recordingServer {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	rec := &recordingServer{}
	icpb.RegisterInferenceCacheServer(srv, rec)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	r := NewReporter(icpb.NewInferenceCacheClient(conn), testConfig(), WithWindow(window))
	in := make(chan *EventBatch, len(batches))
	for _, b := range batches {
		in <- b
	}
	close(in)

	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background(), in) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reporter did not drain in time")
	}
	return rec
}

func TestReporterForwardsBlockStored(t *testing.T) {
	h0, h1 := []byte{0x0a}, []byte{0x0b}
	rec := runReporter(t, &EventBatch{
		TimestampSeconds: 2.0,
		Events:           []Event{BlockStored{BlockHashes: [][]byte{h0, h1}, BlockSize: 128}},
	})

	rec.mu.Lock()
	defer rec.mu.Unlock()
	var hashes [][]byte
	for _, u := range rec.updates {
		if u.GetHashScheme() != "vllm" || u.GetReplicaId() != "vllm-0" {
			t.Errorf("update identity = %s/%s, want vllm-0/vllm", u.GetReplicaId(), u.GetHashScheme())
		}
		for _, p := range u.GetPrefixes() {
			if p.GetTokenCount() <= 0 {
				t.Errorf("token_count = %d, want > 0", p.GetTokenCount())
			}
			hashes = append(hashes, p.GetPrefixHash())
		}
	}
	if len(hashes) != 2 || !bytes.Equal(hashes[0], h0) || !bytes.Equal(hashes[1], h1) {
		t.Errorf("forwarded hashes = %x, want [%x %x]", hashes, h0, h1)
	}
}

// With a window long enough that the ticker never fires, the only path that can
// deliver buffered adds is the shutdown flush — which must reopen the stream.
func TestReporterFlushesPendingOnShutdown(t *testing.T) {
	h := []byte{0x42}
	rec := runReporterWindow(t, time.Hour, &EventBatch{
		TimestampSeconds: 1,
		Events:           []Event{BlockStored{BlockHashes: [][]byte{h}, BlockSize: 16}},
	})
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var found bool
	for _, u := range rec.updates {
		for _, p := range u.GetPrefixes() {
			if bytes.Equal(p.GetPrefixHash(), h) {
				found = true
			}
		}
	}
	if !found {
		t.Error("shutdown flush did not deliver the buffered add")
	}
}

// A zero/negative window must not panic time.NewTicker.
func TestNewReporterClampsWindow(t *testing.T) {
	r := NewReporter(nil, testConfig(), WithWindow(0))
	if r.window <= 0 {
		t.Errorf("window not clamped: %v", r.window)
	}
}

func TestReporterForwardsRemovalsAndClear(t *testing.T) {
	rec := runReporter(t,
		&EventBatch{TimestampSeconds: 1, Events: []Event{BlockRemoved{BlockHashes: [][]byte{{7}, {8}}}}},
		&EventBatch{TimestampSeconds: 2, Events: []Event{AllBlocksCleared{}}},
	)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	var evicted, cleared int
	for _, e := range rec.events {
		switch e.GetType() {
		case icpb.CacheEvent_PREFIX_EVICTED:
			evicted++
		case icpb.CacheEvent_ALL_CLEARED:
			cleared++
		}
	}
	if evicted != 2 {
		t.Errorf("PREFIX_EVICTED count = %d, want 2", evicted)
	}
	if cleared != 1 {
		t.Errorf("ALL_CLEARED count = %d, want 1", cleared)
	}
}

// A clear in the same batch supersedes buffered adds — nothing should be reported
// as added.
func TestReporterClearSupersedesBufferedAdds(t *testing.T) {
	rec := runReporter(t, &EventBatch{
		TimestampSeconds: 1,
		Events: []Event{
			BlockStored{BlockHashes: [][]byte{{1}, {2}}, BlockSize: 16},
			AllBlocksCleared{},
		},
	})
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, u := range rec.updates {
		if len(u.GetPrefixes()) != 0 {
			t.Errorf("expected no prefixes after clear, got %d", len(u.GetPrefixes()))
		}
	}
}

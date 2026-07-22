package engine

import (
	"context"
	"errors"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	vpb "github.com/cachebox-project/inference-cache/pkg/adapters/engine/vllmengine"
)

// fakeLoadsClient injects a canned GetLoads response/error for mapping tests.
type fakeLoadsClient struct {
	resp *vpb.GetLoadsResponse
	err  error
}

func (f *fakeLoadsClient) GetLoads(_ context.Context, _ *vpb.GetLoadsRequest, _ ...grpc.CallOption) (*vpb.GetLoadsResponse, error) {
	return f.resp, f.err
}

func TestGrpcLoadsScraperMapsSingleRank(t *testing.T) {
	resp := &vpb.GetLoadsResponse{Loads: []*vpb.SchedulerLoad{
		{NumRunningReqs: 6, NumWaitingReqs: 2, TokenUsage: 0.5, CacheHitRate: 0.8},
	}}
	s := newGrpcLoadsScraperWithClient(&fakeLoadsClient{resp: resp},
		GrpcLoadsScraperConfig{CacheSizeBytes: 1 << 30, MaxConcurrencyCeiling: 16})

	st, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	if got, want := st.GetPressure(), float32(0.5); got != want { // (6+2)/16
		t.Errorf("pressure = %v, want %v", got, want)
	}
	if got, want := st.GetCacheMemoryBytes(), int64(0.5*float64(1<<30)); got != want {
		t.Errorf("cache_memory_bytes = %d, want %d", got, want)
	}
	if got, want := st.GetHitRate(), float32(0.8); got != want {
		t.Errorf("hit_rate = %v, want %v", got, want)
	}
}

func TestGrpcLoadsScraperAggregatesRanks(t *testing.T) {
	// two DP ranks: running/waiting SUM, usage MAX, hit-rate MEAN
	resp := &vpb.GetLoadsResponse{Loads: []*vpb.SchedulerLoad{
		{NumRunningReqs: 2, NumWaitingReqs: 1, TokenUsage: 0.3, CacheHitRate: 0.5},
		{NumRunningReqs: 5, NumWaitingReqs: 4, TokenUsage: 0.7, CacheHitRate: 0.9},
	}}
	s := newGrpcLoadsScraperWithClient(&fakeLoadsClient{resp: resp},
		GrpcLoadsScraperConfig{CacheSizeBytes: 1000, MaxConcurrencyCeiling: 24})

	st, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	if got, want := st.GetPressure(), float32(12.0/24.0); got != want { // (7+5)/24
		t.Errorf("pressure = %v, want %v", got, want)
	}
	if got, want := st.GetCacheMemoryBytes(), int64(0.7*1000); got != want { // max usage
		t.Errorf("cache_memory_bytes = %d, want %d", got, want)
	}
	if got, want := st.GetHitRate(), float32(0.7); got != want { // mean(0.5,0.9)
		t.Errorf("hit_rate = %v, want %v", got, want)
	}
}

func TestGrpcLoadsScraperZeroConfigIsHonest(t *testing.T) {
	// CacheSizeBytes=0 -> 0 bytes (not fabricated); ceiling=0 -> pressure 0
	resp := &vpb.GetLoadsResponse{Loads: []*vpb.SchedulerLoad{
		{NumRunningReqs: 9, NumWaitingReqs: 9, TokenUsage: 0.9},
	}}
	s := newGrpcLoadsScraperWithClient(&fakeLoadsClient{resp: resp}, GrpcLoadsScraperConfig{})
	st, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	if st.GetCacheMemoryBytes() != 0 || st.GetPressure() != 0 {
		t.Errorf("want zeroed cache_memory_bytes/pressure, got bytes=%d pressure=%v",
			st.GetCacheMemoryBytes(), st.GetPressure())
	}
}

func TestGrpcLoadsScraperErrorIsSurfaced(t *testing.T) {
	s := newGrpcLoadsScraperWithClient(&fakeLoadsClient{err: errors.New("unavailable")},
		GrpcLoadsScraperConfig{Addr: "x:1"})
	st, err := s.Scrape(context.Background())
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if st == nil {
		t.Fatal("want zero ReplicaStats on error, got nil")
	}
}

// mockEngineServer implements the generated VllmEngineServer for a bufconn
// end-to-end test through the real generated stubs.
type mockEngineServer struct {
	vpb.UnimplementedVllmEngineServer
	resp *vpb.GetLoadsResponse
}

func (m *mockEngineServer) GetLoads(context.Context, *vpb.GetLoadsRequest) (*vpb.GetLoadsResponse, error) {
	return m.resp, nil
}

func TestGrpcLoadsScraperEndToEndOverBufconn(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	vpb.RegisterVllmEngineServer(srv, &mockEngineServer{
		resp: &vpb.GetLoadsResponse{Loads: []*vpb.SchedulerLoad{
			{NumRunningReqs: 3, NumWaitingReqs: 1, TokenUsage: 0.25, CacheHitRate: 0.6},
		}},
	})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	s := newGrpcLoadsScraperWithClient(vpb.NewVllmEngineClient(conn),
		GrpcLoadsScraperConfig{CacheSizeBytes: 1 << 30, MaxConcurrencyCeiling: 8})
	st, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	if got, want := st.GetPressure(), float32(0.5); got != want { // (3+1)/8
		t.Errorf("pressure = %v, want %v", got, want)
	}
	if st.GetCacheMemoryBytes() != int64(0.25*float64(1<<30)) {
		t.Errorf("cache_memory_bytes = %d", st.GetCacheMemoryBytes())
	}
}

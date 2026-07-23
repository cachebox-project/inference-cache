package engine

import (
	"context"
	"errors"
	"math"
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

func TestGRPCLoadsScraperMapsSingleRank(t *testing.T) {
	resp := &vpb.GetLoadsResponse{Loads: []*vpb.SchedulerLoad{
		{NumRunningReqs: 6, NumWaitingReqs: 2, TokenUsage: 0.5, CacheHitRate: 0.8},
	}}
	s := newGRPCLoadsScraperWithClient(&fakeLoadsClient{resp: resp},
		GRPCLoadsScraperConfig{CacheSizeBytes: 1 << 30, MaxConcurrencyCeiling: 16})

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

func TestGRPCLoadsScraperAggregatesRanks(t *testing.T) {
	// two DP ranks: running/waiting SUM, usage MAX, hit-rate MEAN
	resp := &vpb.GetLoadsResponse{Loads: []*vpb.SchedulerLoad{
		{NumRunningReqs: 2, NumWaitingReqs: 1, TokenUsage: 0.3, CacheHitRate: 0.5},
		{NumRunningReqs: 5, NumWaitingReqs: 4, TokenUsage: 0.7, CacheHitRate: 0.9},
	}}
	s := newGRPCLoadsScraperWithClient(&fakeLoadsClient{resp: resp},
		GRPCLoadsScraperConfig{CacheSizeBytes: 1000, MaxConcurrencyCeiling: 24})

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

func TestGRPCLoadsScraperZeroConfigIsHonest(t *testing.T) {
	// CacheSizeBytes=0 -> 0 bytes (not fabricated); ceiling=0 -> pressure 0
	resp := &vpb.GetLoadsResponse{Loads: []*vpb.SchedulerLoad{
		{NumRunningReqs: 9, NumWaitingReqs: 9, TokenUsage: 0.9},
	}}
	s := newGRPCLoadsScraperWithClient(&fakeLoadsClient{resp: resp}, GRPCLoadsScraperConfig{})
	st, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	if st.GetCacheMemoryBytes() != 0 || st.GetPressure() != 0 {
		t.Errorf("want zeroed cache_memory_bytes/pressure, got bytes=%d pressure=%v",
			st.GetCacheMemoryBytes(), st.GetPressure())
	}
}

func TestGRPCLoadsScraperSanitizesOutOfRange(t *testing.T) {
	// token_usage and cache_hit_rate are contractually [0,1] ratios, but come
	// from an external engine process. Feed garbage across ranks: over 1, NaN,
	// and negative. Sanitization must clamp/reject each so cache_memory_bytes
	// never overflows past CacheSizeBytes and hit_rate stays finite in [0,1].
	resp := &vpb.GetLoadsResponse{Loads: []*vpb.SchedulerLoad{
		{TokenUsage: 5.0, CacheHitRate: 9.0},               // both way over 1
		{TokenUsage: math.NaN(), CacheHitRate: math.NaN()}, // non-finite
		{TokenUsage: -1.0, CacheHitRate: -0.5},             // negative
	}}
	s := newGRPCLoadsScraperWithClient(&fakeLoadsClient{resp: resp},
		GRPCLoadsScraperConfig{CacheSizeBytes: 1000, MaxConcurrencyCeiling: 8})

	st, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	// usage clamps per-rank to [0,1]; MAX(1,0,0)=1 → bytes = 1*1000, never 5000/negative.
	if got := st.GetCacheMemoryBytes(); got != 1000 {
		t.Errorf("cache_memory_bytes = %d, want 1000 (usage clamped to 1, not overflowed)", got)
	}
	// hit_rate: the NaN rank is EXCLUDED (unavailable), the over-1 rank clamps to 1
	// and the negative to 0 → MEAN(1,0)=0.5, finite (no NaN poison).
	hr := float64(st.GetHitRate())
	if math.IsNaN(hr) || math.IsInf(hr, 0) {
		t.Fatalf("hit_rate = %v, want a finite value (NaN input must not propagate)", hr)
	}
	if hr < 0 || hr > 1 {
		t.Errorf("hit_rate = %v, want within [0,1]", hr)
	}
	if math.Abs(hr-0.5) > 1e-6 {
		t.Errorf("hit_rate = %v, want 0.5 (mean of clamped 1 and 0; NaN rank excluded)", hr)
	}
}

func TestGRPCLoadsScraperCacheBytesNoOverflow(t *testing.T) {
	// At an extreme capacity, float64(CacheSizeBytes) rounds above the int64 range,
	// so a naive int64(usage*float64(cap)) overflows to a negative. cache_memory_bytes
	// must stay in [0, CacheSizeBytes].
	resp := &vpb.GetLoadsResponse{Loads: []*vpb.SchedulerLoad{
		{TokenUsage: 1.0}, // full cache
	}}
	s := newGRPCLoadsScraperWithClient(&fakeLoadsClient{resp: resp},
		GRPCLoadsScraperConfig{CacheSizeBytes: math.MaxInt64})

	st, err := s.Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape: %v", err)
	}
	if got := st.GetCacheMemoryBytes(); got < 0 || got > math.MaxInt64 {
		t.Fatalf("cache_memory_bytes = %d, want within [0, MaxInt64] (no overflow)", got)
	}
	if got := st.GetCacheMemoryBytes(); got != math.MaxInt64 {
		t.Errorf("cache_memory_bytes = %d, want MaxInt64 at usage=1.0 full capacity", got)
	}
}

func TestGRPCLoadsScraperErrorIsSurfaced(t *testing.T) {
	s := newGRPCLoadsScraperWithClient(&fakeLoadsClient{err: errors.New("unavailable")},
		GRPCLoadsScraperConfig{Addr: "x:1"})
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

func TestGRPCLoadsScraperEndToEndOverBufconn(t *testing.T) {
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	vpb.RegisterVllmEngineServer(srv, &mockEngineServer{
		resp: &vpb.GetLoadsResponse{Loads: []*vpb.SchedulerLoad{
			{NumRunningReqs: 3, NumWaitingReqs: 1, TokenUsage: 0.25, CacheHitRate: 0.6},
		}},
	})
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		// Serve returns ErrServerStopped on Stop(); anything else is a real fault.
		if err := <-serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("bufconn Serve terminated unexpectedly: %v", err)
		}
	})

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	s := newGRPCLoadsScraperWithClient(vpb.NewVllmEngineClient(conn),
		GRPCLoadsScraperConfig{CacheSizeBytes: 1 << 30, MaxConcurrencyCeiling: 8})
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

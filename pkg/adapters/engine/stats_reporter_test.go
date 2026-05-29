package engine

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// stubScraper returns the configured stats (or error) and counts calls.
type stubScraper struct {
	stats *icpb.ReplicaStats
	err   error
	calls atomic.Int32
}

func (s *stubScraper) Scrape(_ context.Context) (*icpb.ReplicaStats, error) {
	s.calls.Add(1)
	return s.stats, s.err
}

// startRecordingReporter wires a recordingServer + StatsReporter over bufconn
// and returns both with a cleanup hook.
func startRecordingReporter(t *testing.T, scraper statsScraper, interval time.Duration) (*recordingServer, *StatsReporter, func()) {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	rec := &recordingServer{}
	icpb.RegisterInferenceCacheServer(srv, rec)
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	r := NewStatsReporter(icpb.NewInferenceCacheClient(conn), scraper, testConfig(),
		WithStatsInterval(interval),
		WithStatsRPCTimeout(time.Second),
	)
	stop := func() {
		_ = conn.Close()
		srv.Stop()
	}
	return rec, r, stop
}

func TestStatsReporterEmitsStatsOnlyCSU(t *testing.T) {
	scraper := &stubScraper{stats: &icpb.ReplicaStats{CacheMemoryBytes: 4096, HitRate: 0.5, Pressure: 0.25}}
	rec, r, stop := startRecordingReporter(t, scraper, 10*time.Millisecond)
	defer stop()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// Wait for at least one update to land.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rec.mu.Lock()
		n := len(rec.updates)
		rec.mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned: %v", err)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.updates) == 0 {
		t.Fatal("no CacheStateUpdates reached the server")
	}
	for _, u := range rec.updates {
		if len(u.GetPrefixes()) != 0 {
			t.Errorf("stats CSU carried %d prefixes, want 0 (stats-only)", len(u.GetPrefixes()))
		}
		st := u.GetStats()
		if st == nil {
			t.Fatal("CSU.stats was nil — stats path is dark")
		}
		if u.GetReplicaId() != "vllm-0" || u.GetModelId() != "llama" || u.GetTenantId() != "tenant-a" || u.GetHashScheme() != "vllm" {
			t.Errorf("identity = %s/%s/%s/%s, want vllm-0/llama/tenant-a/vllm",
				u.GetReplicaId(), u.GetModelId(), u.GetTenantId(), u.GetHashScheme())
		}
		if st.GetCacheMemoryBytes() != 4096 || st.GetHitRate() != 0.5 || st.GetPressure() != 0.25 {
			t.Errorf("stats payload = %+v, want bytes=4096 hit=0.5 pres=0.25", st)
		}
		if st.GetReplicaId() != "vllm-0" {
			t.Errorf("nested stats.replica_id = %q, want %q (mirrored from CSU)", st.GetReplicaId(), "vllm-0")
		}
	}
}

func TestStatsReporterScrapeErrorSurvivesAndRetries(t *testing.T) {
	scraper := &stubScraper{err: errors.New("dial tcp: connection refused")}
	rec, r, stop := startRecordingReporter(t, scraper, 5*time.Millisecond)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	if err := <-done; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run returned: %v", err)
	}

	if got := scraper.calls.Load(); got < 2 {
		t.Errorf("scraper called %d times, want ≥2 (retry behavior)", got)
	}
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.updates) != 0 {
		t.Errorf("scrape errors must not emit CSUs, got %d", len(rec.updates))
	}
}

func TestStatsReporterStopsOnContextCancel(t *testing.T) {
	scraper := &stubScraper{stats: &icpb.ReplicaStats{}}
	_, r, stop := startRecordingReporter(t, scraper, time.Hour) // ticker won't fire
	defer stop()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestStatsReporterClampsZeroInterval(t *testing.T) {
	r := NewStatsReporter(nil, &stubScraper{}, testConfig(), WithStatsInterval(0), WithStatsRPCTimeout(-1))
	if r.interval <= 0 {
		t.Errorf("interval not clamped: %v", r.interval)
	}
	if r.rpcTimeout <= 0 {
		t.Errorf("rpcTimeout not clamped: %v", r.rpcTimeout)
	}
}

func TestConfigStatsUpdate(t *testing.T) {
	c := testConfig()
	if u := c.StatsUpdate(123, nil); u != nil {
		t.Errorf("nil stats produced CSU = %+v, want nil", u)
	}
	u := c.StatsUpdate(123, &icpb.ReplicaStats{CacheMemoryBytes: 9, HitRate: 0.1, Pressure: 0.2})
	if u == nil {
		t.Fatal("StatsUpdate returned nil for non-nil stats")
	}
	if u.GetTimestampUs() != 123 {
		t.Errorf("timestamp = %d, want 123", u.GetTimestampUs())
	}
	if len(u.GetPrefixes()) != 0 {
		t.Errorf("StatsUpdate carried %d prefixes, want 0", len(u.GetPrefixes()))
	}
	if u.GetStats().GetReplicaId() != c.ReplicaID {
		t.Errorf("nested stats.replica_id = %q, want %q", u.GetStats().GetReplicaId(), c.ReplicaID)
	}
}

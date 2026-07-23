package engine

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
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

// scrapeFunc adapts a func to statsScraper for per-call control.
type scrapeFunc func() (*icpb.ReplicaStats, error)

func (f scrapeFunc) Scrape(context.Context) (*icpb.ReplicaStats, error) { return f() }

// TestStatsReporterStaleEscalation verifies the fail-silent -> loud transition in
// the STEADY-STATE regime: one good scrape lands first, then the outage. After
// staleThreshold consecutive failures the reporter logs exactly one Error naming
// the retained last sample, then stays quiet at Debug while the outage PERSISTS
// beyond the threshold, and logs one Info on recovery — instead of a per-tick
// Warn/Error that buries the loss of load-aware routing. The outage runs two
// ticks past the threshold so the consecFails > staleThreshold suppression path
// is actually exercised.
func TestStatsReporterStaleEscalation(t *testing.T) {
	fail := errors.New("dial tcp: connection refused")
	var n int
	scraper := scrapeFunc(func() (*icpb.ReplicaStats, error) {
		n++
		// one good sample (n=1), then a 5-tick outage (n=2..6), then recover.
		if n == 1 || n > 6 {
			return &icpb.ReplicaStats{}, nil
		}
		return nil, fail
	})

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// bufconn client so the recovery tick's send() has a live server.
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	icpb.RegisterInferenceCacheServer(srv, &recordingServer{})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	r := NewStatsReporter(icpb.NewInferenceCacheClient(conn), scraper, testConfig(),
		WithStatsLogger(logger), WithStatsRPCTimeout(time.Second))

	ctx := context.Background()
	// ok, fail, fail, fail(->Error), fail(->Debug), fail(->Debug), success(->Info)
	for i := 0; i < 7; i++ {
		r.tick(ctx)
	}

	out := buf.String()
	if got := strings.Count(out, `"level":"ERROR"`); got != 1 {
		t.Fatalf("want exactly 1 ERROR on stale transition, got %d\n%s", got, out)
	}
	// A sample was delivered first, so the Error must name the retained last
	// delivered sample — not the cold-start "nothing reached IC" wording.
	if !strings.Contains(out, "engine load stats stale") || !strings.Contains(out, "last delivered sample") {
		t.Fatalf("steady-state stale ERROR missing last-delivered-sample message:\n%s", out)
	}
	if strings.Contains(out, "undelivered since startup") {
		t.Fatalf("steady-state ERROR wrongly used the cold-start wording:\n%s", out)
	}
	if got := strings.Count(out, `"level":"WARN"`); got != 2 {
		t.Fatalf("want 2 WARN before threshold (not Error spam), got %d\n%s", got, out)
	}
	// The two ticks PAST the threshold must be suppressed to Debug, not repeat
	// the Error or fall back to Warn — this is the persistent-outage path.
	if got := strings.Count(out, `"level":"DEBUG"`); got != 2 {
		t.Fatalf("want 2 DEBUG while the outage persists past threshold, got %d\n%s", got, out)
	}
	// Recovery must be exactly one record, emitted at INFO (behavior contract).
	if got := strings.Count(out, "engine load stats recovered"); got != 1 {
		t.Fatalf("want exactly 1 recovery record, got %d\n%s", got, out)
	}
	if !recoveryAtInfo(out) {
		t.Fatalf("recovery record must be emitted at INFO level\n%s", out)
	}
	if !strings.Contains(out, `"event":"load_signal_recovered"`) {
		t.Fatalf("recovery record must carry the stable event field for alerting:\n%s", out)
	}
}

// recoveryAtInfo confirms the "engine load stats recovered" record is the one
// carrying "level":"INFO" — asserting the level of the specific line, not just
// that some INFO exists somewhere in the buffer.
func recoveryAtInfo(logOutput string) bool {
	for _, line := range strings.Split(strings.TrimSpace(logOutput), "\n") {
		if strings.Contains(line, "engine load stats recovered") {
			return strings.Contains(line, `"level":"INFO"`)
		}
	}
	return false
}

// TestStatsReporterColdStartNeverSucceeded covers the other regime: every scrape
// since startup fails, so no sample was ever collected. The stale-transition
// Error must say the replica is ranked on residency only (no signal yet) rather
// than claiming a retained "last sample" that never existed. A nil client is
// safe here — a failed scrape returns before any ReportCacheState send.
func TestStatsReporterColdStartNeverSucceeded(t *testing.T) {
	fail := errors.New("dial tcp: connection refused")
	scraper := scrapeFunc(func() (*icpb.ReplicaStats, error) { return nil, fail })

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	r := NewStatsReporter(nil, scraper, testConfig(), WithStatsLogger(logger))

	ctx := context.Background()
	for i := 0; i < 3; i++ { // fail, fail, fail(->Error at threshold)
		r.tick(ctx)
	}

	out := buf.String()
	if got := strings.Count(out, `"level":"ERROR"`); got != 1 {
		t.Fatalf("want exactly 1 ERROR at the cold-start stale transition, got %d\n%s", got, out)
	}
	if !strings.Contains(out, "undelivered since startup") {
		t.Fatalf("cold-start ERROR must state nothing reached IC yet:\n%s", out)
	}
	if strings.Contains(out, "last delivered sample") {
		t.Fatalf("cold-start ERROR must not claim a delivered sample:\n%s", out)
	}
}

// failingReportServer accepts connections but rejects every ReportCacheState —
// IC is reachable yet the stats stream never lands. A delivery failure, distinct
// from a scrape failure.
type failingReportServer struct {
	icpb.UnimplementedInferenceCacheServer
}

func (failingReportServer) ReportCacheState(icpb.InferenceCache_ReportCacheStateServer) error {
	return errors.New("ic rejected the stats stream")
}

// TestStatsReporterStaleOnDeliveryFailure proves the stale state machine keys on
// DELIVERY, not just scrape: scrapes succeed but every ReportCacheState fails, so
// no sample ever reaches IC. After the threshold the reporter must escalate with
// the undelivered-since-startup wording (nothing landed) — not treat the healthy
// scrapes as a delivered load signal — and must never claim recovery.
func TestStatsReporterStaleOnDeliveryFailure(t *testing.T) {
	scraper := scrapeFunc(func() (*icpb.ReplicaStats, error) {
		return &icpb.ReplicaStats{CacheMemoryBytes: 1}, nil // scrape always succeeds
	})

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	icpb.RegisterInferenceCacheServer(srv, failingReportServer{})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	r := NewStatsReporter(icpb.NewInferenceCacheClient(conn), scraper, testConfig(),
		WithStatsLogger(logger), WithStatsRPCTimeout(time.Second))

	ctx := context.Background()
	for i := 0; i < 3; i++ { // deliver fails 3x → stale transition on the 3rd
		r.tick(ctx)
	}

	out := buf.String()
	if got := strings.Count(out, `"level":"ERROR"`); got != 1 {
		t.Fatalf("want 1 ERROR when delivery keeps failing, got %d\n%s", got, out)
	}
	if !strings.Contains(out, "undelivered since startup") {
		t.Fatalf("delivery-failure ERROR must report nothing reached IC:\n%s", out)
	}
	if strings.Contains(out, "recovered") {
		t.Fatalf("must not claim recovery — no sample ever reached IC:\n%s", out)
	}
}

// TestStatsReporterNilStatsCountsAsStale covers the interface-permitted case where
// a scrape succeeds but yields no ReplicaStats (StatsUpdate returns nil). Nothing
// reaches IC, so it must count toward staleness — not leave the failure streak in
// limbo — and the transition must carry the stable event=load_signal_stale field.
func TestStatsReporterNilStatsCountsAsStale(t *testing.T) {
	scraper := scrapeFunc(func() (*icpb.ReplicaStats, error) { return nil, nil }) // scrape ok, no stats

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	r := NewStatsReporter(nil, scraper, testConfig(), WithStatsLogger(logger))

	ctx := context.Background()
	for i := 0; i < 3; i++ { // nil, nil, nil(->Error at threshold)
		r.tick(ctx)
	}

	out := buf.String()
	if got := strings.Count(out, `"level":"ERROR"`); got != 1 {
		t.Fatalf("nil-stats ticks must count toward staleness: want 1 ERROR at threshold, got %d\n%s", got, out)
	}
	if !strings.Contains(out, `"event":"load_signal_stale"`) {
		t.Fatalf("stale ERROR must carry the stable event field for alerting:\n%s", out)
	}
}

// rejectingReportServer accepts the ReportCacheState stream cleanly but replies
// Ack{accepted:false} — IC received the RPC yet did NOT index the update.
type rejectingReportServer struct {
	icpb.UnimplementedInferenceCacheServer
}

func (rejectingReportServer) ReportCacheState(stream icpb.InferenceCache_ReportCacheStateServer) error {
	for {
		if _, err := stream.Recv(); err != nil { // EOF on client half-close
			return stream.SendAndClose(&icpb.Ack{Accepted: false})
		}
	}
}

// TestStatsReporterRejectedAckCountsAsStale proves a clean CloseAndRecv carrying
// accepted=false is a non-delivery: the RPC succeeded but IC rejected the sample,
// so the reporter must escalate and never claim recovery.
func TestStatsReporterRejectedAckCountsAsStale(t *testing.T) {
	scraper := scrapeFunc(func() (*icpb.ReplicaStats, error) {
		return &icpb.ReplicaStats{CacheMemoryBytes: 1}, nil // scrape + send both "work"
	})

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	icpb.RegisterInferenceCacheServer(srv, rejectingReportServer{})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	r := NewStatsReporter(icpb.NewInferenceCacheClient(conn), scraper, testConfig(),
		WithStatsLogger(logger), WithStatsRPCTimeout(time.Second))

	ctx := context.Background()
	for i := 0; i < 3; i++ { // rejected 3x → stale transition on the 3rd
		r.tick(ctx)
	}

	out := buf.String()
	if got := strings.Count(out, `"level":"ERROR"`); got != 1 {
		t.Fatalf("Ack{accepted:false} must count as non-delivery: want 1 ERROR, got %d\n%s", got, out)
	}
	if strings.Contains(out, "recovered") {
		t.Fatalf("must not claim recovery — IC rejected every update:\n%s", out)
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

package engine_test

// End-to-end wire test: the StatsReporter scrapes a synthetic /metrics endpoint,
// emits stats-only CacheStateUpdates against the real policy server, and the
// server's /snapshot endpoint reports those stats on the replicas[] surface
// (the same surface the CacheIndex controller scrapes). Confirms the stats
// path is wired all the way through — engine /metrics → ReplicaStats → index
// stats map → snapshot replicas[].

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/cachebox-project/inference-cache/pkg/adapters/engine"
	"github.com/cachebox-project/inference-cache/pkg/index"
	"github.com/cachebox-project/inference-cache/pkg/server"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

func TestStatsReporterPopulatesSnapshotReplicas(t *testing.T) {
	// 1. Synthetic vLLM /metrics endpoint backed by the captured fixture.
	body, err := os.ReadFile(filepath.Join("testdata", "vllm_metrics_cpu.txt"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	metrics := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write(body)
	}))
	defer metrics.Close()

	// 2. Real policy server on a bufconn + loopback HTTP listeners (public + snapshot).
	grpcLis := bufconn.Listen(1 << 20)
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen http: %v", err)
	}
	snapLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen snapshot: %v", err)
	}
	serveCtx, serveCancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.New().Serve(serveCtx, grpcLis, httpLis, snapLis) }()
	t.Cleanup(func() {
		serveCancel()
		if err := <-serveDone; err != nil {
			t.Errorf("server.Serve: %v", err)
		}
	})

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return grpcLis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufnet: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// 3. Drive the StatsReporter against the live server.
	const oneGiB = 1 << 30
	scraper := engine.NewMetricsScraper(metrics.Client(), engine.ScraperConfig{
		URL:                   metrics.URL,
		Tier:                  engine.CacheTierAuto,
		CacheSizeBytes:        oneGiB,
		MaxConcurrencyCeiling: 256,
	}, nil)
	reporter := engine.NewStatsReporter(icpb.NewInferenceCacheClient(conn), scraper,
		engine.Config{ReplicaID: "vllm-0", ModelID: "Qwen/Qwen2.5-0.5B-Instruct", TenantID: "tenant-a", HashScheme: "vllm"},
		engine.WithStatsInterval(20*time.Millisecond),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = reporter.Run(ctx) }()

	// 4. Poll /snapshot until replicas[] reflects the scraped stats.
	// /snapshot is served on its own listener (split for NetworkPolicy /
	// auth scoping); the test reads it directly since no auth is configured.
	baseURL := "http://" + snapLis.Addr().String()
	deadline := time.Now().Add(5 * time.Second)
	var snap index.Snapshot
	for time.Now().Before(deadline) {
		code, body := getSnapshot(t, baseURL)
		if code == http.StatusOK && json.Unmarshal([]byte(body), &snap) == nil && len(snap.Replicas) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(snap.Replicas) == 0 {
		t.Fatalf("/snapshot.replicas stayed empty — stats path is still dark")
	}

	r := snap.Replicas[0]
	if r.ReplicaID != "vllm-0" {
		t.Errorf("replica id = %q, want vllm-0", r.ReplicaID)
	}
	// usage_perc 0.42 * 1 GiB → ~450 MiB. First-tick hit_rate = 0; subsequent
	// ticks would still be 0 here since the fixture is constant. Pressure must
	// be (3+5)/256 = 0.03125.
	if r.CacheMemoryBytes <= 0 {
		t.Errorf("cacheMemoryBytes = %d, want > 0 (usage_perc × size mapping is dark)", r.CacheMemoryBytes)
	}
	if r.Pressure < 0.03 || r.Pressure > 0.04 {
		t.Errorf("pressure = %v, want ~0.03125", r.Pressure)
	}
}

func getSnapshot(t *testing.T, baseURL string) (int, string) {
	t.Helper()
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Get(baseURL + "/snapshot")
	if err != nil {
		return 0, ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

package main

import (
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/cachebox-project/inference-cache/pkg/adapters/engine"
)

// TestBuildStatsScraperSelectsSource pins the load-source control flow: a
// non-empty --engine-loads-grpc must select the GetLoads gRPC scraper (and hand
// back a closer for its conn), while an empty flag must preserve the HTTP
// /metrics scraper (which owns no conn, so no closer). This is the crux of
// Option B — the wrong branch silently leaves an SMG gRPC engine reporting no
// load — so it gets an explicit test.
func TestBuildStatsScraperSelectsSource(t *testing.T) {
	hc := &http.Client{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("nonempty flag selects gRPC", func(t *testing.T) {
		s, closer, err := buildStatsScraper(scraperParams{loadsGRPC: "127.0.0.1:50051"}, hc, logger)
		if err != nil {
			t.Fatalf("buildStatsScraper: %v", err)
		}
		if _, ok := s.(*engine.GRPCLoadsScraper); !ok {
			t.Fatalf("selected %T, want *engine.GRPCLoadsScraper", s)
		}
		if closer == nil {
			t.Fatal("gRPC scraper must return a closer to release its client conn")
		}
		if err := closer.Close(); err != nil {
			t.Errorf("closer.Close: %v", err)
		}
	})

	t.Run("empty flag preserves HTTP", func(t *testing.T) {
		s, closer, err := buildStatsScraper(scraperParams{metricsURL: "http://127.0.0.1:8000/metrics"}, hc, logger)
		if err != nil {
			t.Fatalf("buildStatsScraper: %v", err)
		}
		if _, ok := s.(*engine.MetricsScraper); !ok {
			t.Fatalf("selected %T, want *engine.MetricsScraper", s)
		}
		if closer != nil {
			t.Fatal("HTTP scraper owns no conn; closer must be nil")
		}
	})
}

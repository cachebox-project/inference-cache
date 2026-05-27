package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/cachebox-project/inference-cache/pkg/index"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// Config controls the cache server listeners.
type Config struct {
	GRPCAddr string
	HTTPAddr string
}

// DefaultConfig returns the local development server configuration.
func DefaultConfig() Config {
	return Config{
		GRPCAddr: ":9090",
		HTTPAddr: ":8080",
	}
}

// Service hosts the gRPC API and the HTTP health/metrics endpoints.
type Service struct {
	grpcServer *grpc.Server
	httpServer *http.Server
	metrics    *serverMetrics
	index      *index.Index
}

// New constructs a cache service.
func New() *Service {
	metrics := newServerMetrics()
	idx := index.New(index.WithMetrics(metrics))

	grpcServer := grpc.NewServer()
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	icpb.RegisterInferenceCacheServer(grpcServer, newInferenceCacheService(idx, metrics))

	mux := http.NewServeMux()
	// /healthz — liveness: the process is up.
	mux.HandleFunc("/healthz", writeOK)
	// /readyz — readiness: gated on the cache index being started/ingesting.
	// (Engine-warm gating — waiting for the initial KV-event sync — arrives with
	// the C1 hook; today the index becomes ready as soon as Serve starts it.)
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !idx.Ready() {
			http.Error(w, "not ready\n", http.StatusServiceUnavailable)
			return
		}
		writeOK(w, nil)
	})
	// /metrics — Prometheus surface (inferencecache_*), tech spec §4.3.
	mux.Handle("/metrics", promhttp.HandlerFor(metrics.registry, promhttp.HandlerOpts{}))

	return &Service{
		grpcServer: grpcServer,
		httpServer: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		metrics: metrics,
		index:   idx,
	}
}

// writeOK is the handler for the plain-text health/readiness probes.
func writeOK(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// ListenAndServe starts both listeners from the supplied config.
func ListenAndServe(ctx context.Context, cfg Config) error {
	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen for grpc: %w", err)
	}
	httpListener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		_ = grpcListener.Close()
		return fmt.Errorf("listen for http: %w", err)
	}
	return New().Serve(ctx, grpcListener, httpListener)
}

// Serve starts gRPC and HTTP servers on existing listeners.
func (s *Service) Serve(ctx context.Context, grpcListener, httpListener net.Listener) error {
	// Start the cache index (marks it ready and runs TTL eviction until ctx is
	// done) before accepting traffic, so /readyz and lookups reflect a live index.
	s.index.Start(ctx)

	// Mark the server up before launching the listeners so a scrape can never
	// observe inferencecache_server_up=0 while Serve is already running.
	s.metrics.up.Set(1)
	defer s.metrics.up.Set(0)

	errCh := make(chan error, 2)
	go func() {
		if err := s.grpcServer.Serve(grpcListener); err != nil {
			errCh <- fmt.Errorf("serve grpc: %w", err)
		}
	}()
	go func() {
		if err := s.httpServer.Serve(httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("serve http: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.grpcServer.GracefulStop()
		_ = s.httpServer.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		s.grpcServer.GracefulStop()
		_ = s.httpServer.Shutdown(context.Background())
		return err
	}
}

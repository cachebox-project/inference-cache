package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

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
	policies   *PolicyStore
}

// New constructs a cache service.
func New() *Service {
	metrics := newServerMetrics()
	policies := NewPolicyStore()
	idx := index.New(
		index.WithMetrics(metrics),
		index.WithTTLResolver(policies),
	)

	grpcServer := grpc.NewServer()
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	icpb.RegisterInferenceCacheServer(grpcServer, newInferenceCacheService(idx, metrics, policies))
	// Register gRPC server reflection so operators can use grpcurl,
	// grpc_health_probe, and similar debug tooling without shipping the .proto.
	// Reflection exposes only the service schema, never KV or prompt data.
	reflection.Register(grpcServer)

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
	// /snapshot — internal JSON of the cluster-wide cache aggregate, scraped by
	// the controller to populate the CacheIndex CR status (B6 status surface).
	// Metadata only (replica/tenant stats + prefix counts), never KV/prompt data.
	mux.HandleFunc("/snapshot", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(idx.Snapshot()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	// /policy — internal endpoint the controller PUSHES resolved CachePolicy
	// snapshots to. Replace-on-write; the controller owns the source of truth
	// and re-pushes on its tick, so a server restart self-heals.
	mux.HandleFunc("/policy", policyHandler(policies))

	return &Service{
		grpcServer: grpcServer,
		httpServer: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		metrics:  metrics,
		index:    idx,
		policies: policies,
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
	// Derive a context that is cancelled on ANY return from Serve (caller's ctx
	// done OR the internal error branch), so the index's eviction goroutine and
	// Ready() state are torn down with the service rather than leaking.
	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start the cache index (marks it ready and runs TTL eviction until serveCtx
	// is done) before accepting traffic, so /readyz and lookups reflect a live index.
	s.index.Start(serveCtx)

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
		slog.InfoContext(ctx, "graceful_shutdown_started")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.grpcServer.GracefulStop()
		_ = s.httpServer.Shutdown(shutdownCtx)
		slog.InfoContext(ctx, "graceful_shutdown_done")
		return nil
	case err := <-errCh:
		// One terminal error per Serve return: cmd/server logs serve_error
		// on the non-nil return. The second listener's flap (if both fail)
		// stays in the buffered channel and is discarded by design, so this
		// branch produces no log line itself.
		s.grpcServer.GracefulStop()
		_ = s.httpServer.Shutdown(context.Background())
		return err
	}
}

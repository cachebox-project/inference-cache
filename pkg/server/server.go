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
	"github.com/cachebox-project/inference-cache/pkg/server/auth"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// Config controls the cache server listeners.
//
// The public HTTP listener (HTTPAddr) carries kubelet- and Prometheus-facing
// endpoints (/healthz, /readyz, /metrics) plus the controller-only /policy
// push. The snapshot listener (SnapshotAddr) carries only /snapshot, gated
// by ServiceAccount bearer auth — a NetworkPolicy further restricts L3/L4
// access to the controller's pod selector (see config/server).
type Config struct {
	GRPCAddr     string
	HTTPAddr     string
	SnapshotAddr string
}

// DefaultConfig returns the local development server configuration.
func DefaultConfig() Config {
	return Config{
		GRPCAddr:     ":9090",
		HTTPAddr:     ":8080",
		SnapshotAddr: ":8081",
	}
}

// Option mutates a Service after New constructs the base wiring.
type Option func(*Service)

// snapshotAuthConfig holds the parts of auth.Options that callers can
// configure from outside the package. Service.New constructs the actual
// Authenticator so the recorder stays an internal detail.
type snapshotAuthConfig struct {
	reviewer auth.TokenReviewer
	saName   string
}

// WithSnapshotAuth wires bearer-token authentication onto the /snapshot
// listener. When unset, /snapshot is served without authentication; the
// production binary always sets it. expectedSA is the canonical
// ServiceAccount username, e.g.
// "system:serviceaccount:inference-cache-system:inference-cache-controller-manager".
func WithSnapshotAuth(reviewer auth.TokenReviewer, expectedSA string) Option {
	return func(s *Service) {
		s.snapshotAuthCfg = &snapshotAuthConfig{reviewer: reviewer, saName: expectedSA}
	}
}

// Service hosts the gRPC API and the HTTP health/metrics endpoints.
type Service struct {
	grpcServer       *grpc.Server
	publicHTTPServer *http.Server
	snapshotServer   *http.Server
	snapshotHandler  http.Handler
	snapshotAuthCfg  *snapshotAuthConfig
	metrics          *serverMetrics
	index            *index.Index
	policies         *PolicyStore
}

// New constructs a cache service.
func New(opts ...Option) *Service {
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
	// Register gRPC server reflection so operators can use grpcurl
	// (list / describe / generic call) and similar schema-aware debug tooling
	// without shipping the .proto. Reflection exposes only the service schema,
	// never KV or prompt data. grpc_health_probe is unrelated — it speaks the
	// standard grpc.health.v1 service and does not need reflection.
	reflection.Register(grpcServer)

	publicMux := http.NewServeMux()
	// /healthz — liveness: the process is up.
	publicMux.HandleFunc("/healthz", writeOK)
	// /readyz — readiness: gated on the cache index being started/ingesting.
	// (Engine-warm gating — waiting for the initial KV-event sync — arrives with
	// the C1 hook; today the index becomes ready as soon as Serve starts it.)
	publicMux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !idx.Ready() {
			http.Error(w, "not ready\n", http.StatusServiceUnavailable)
			return
		}
		writeOK(w, nil)
	})
	// /metrics — Prometheus surface (inferencecache_*), tech spec §4.3.
	publicMux.Handle("/metrics", promhttp.HandlerFor(metrics.registry, promhttp.HandlerOpts{}))
	// /policy — internal endpoint the controller PUSHES resolved CachePolicy
	// snapshots to. Replace-on-write; the controller owns the source of truth
	// and re-pushes on its tick, so a server restart self-heals.
	publicMux.HandleFunc("/policy", policyHandler(policies))

	// /snapshot is served from its own listener so a NetworkPolicy can restrict
	// it to the controller's pod selector without breaking kubelet probes or
	// Prometheus scrapes on the public listener.
	snapshotMux := http.NewServeMux()
	snapshotHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(idx.Snapshot()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	snapshotMux.Handle("/snapshot", snapshotHandler)

	s := &Service{
		grpcServer: grpcServer,
		publicHTTPServer: &http.Server{
			Handler:           publicMux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		snapshotServer: &http.Server{
			Handler:           snapshotMux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		snapshotHandler: snapshotHandler,
		metrics:         metrics,
		index:           idx,
		policies:        policies,
	}
	for _, opt := range opts {
		opt(s)
	}
	// If auth is configured, wrap /snapshot in the TokenReview middleware.
	// The middleware reports per-result outcomes to serverMetrics so a
	// scrape of /metrics shows inferencecache_snapshot_auth_total{result=…}.
	if s.snapshotAuthCfg != nil {
		authenticator, err := auth.NewAuthenticator(auth.Options{
			Reviewer:               s.snapshotAuthCfg.reviewer,
			ExpectedServiceAccount: s.snapshotAuthCfg.saName,
			Recorder:               s.metrics,
		})
		if err != nil {
			// New is used by both binaries and tests; a misconfigured auth
			// here is a programming error (empty SA or nil reviewer), not a
			// runtime condition the caller can recover from.
			panic(fmt.Sprintf("server: snapshot auth: %v", err))
		}
		gated := authenticator.Middleware(snapshotHandler)
		gatedMux := http.NewServeMux()
		gatedMux.Handle("/snapshot", gated)
		s.snapshotServer.Handler = gatedMux
	}
	return s
}

// writeOK is the handler for the plain-text health/readiness probes.
func writeOK(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// ListenAndServe starts all listeners from the supplied config.
func ListenAndServe(ctx context.Context, cfg Config, opts ...Option) error {
	grpcListener, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("listen for grpc: %w", err)
	}
	httpListener, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		_ = grpcListener.Close()
		return fmt.Errorf("listen for http: %w", err)
	}
	snapshotListener, err := net.Listen("tcp", cfg.SnapshotAddr)
	if err != nil {
		_ = grpcListener.Close()
		_ = httpListener.Close()
		return fmt.Errorf("listen for snapshot: %w", err)
	}
	return New(opts...).Serve(ctx, grpcListener, httpListener, snapshotListener)
}

// Serve starts gRPC, public HTTP, and snapshot HTTP servers on existing
// listeners.
func (s *Service) Serve(ctx context.Context, grpcListener, httpListener, snapshotListener net.Listener) error {
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

	errCh := make(chan error, 3)
	go func() {
		if err := s.grpcServer.Serve(grpcListener); err != nil {
			errCh <- fmt.Errorf("serve grpc: %w", err)
		}
	}()
	go func() {
		if err := s.publicHTTPServer.Serve(httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("serve http: %w", err)
		}
	}()
	go func() {
		if err := s.snapshotServer.Serve(snapshotListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("serve snapshot http: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		slog.InfoContext(ctx, "graceful_shutdown_started")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.grpcServer.GracefulStop()
		_ = s.publicHTTPServer.Shutdown(shutdownCtx)
		_ = s.snapshotServer.Shutdown(shutdownCtx)
		slog.InfoContext(ctx, "graceful_shutdown_done")
		return nil
	case err := <-errCh:
		// One terminal error per Serve return: cmd/server logs serve_error
		// on the non-nil return. Subsequent listener flaps stay buffered in
		// the channel and are discarded by design.
		s.grpcServer.GracefulStop()
		_ = s.publicHTTPServer.Shutdown(context.Background())
		_ = s.snapshotServer.Shutdown(context.Background())
		return err
	}
}

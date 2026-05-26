package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

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

// Service hosts the empty gRPC API and HTTP health endpoints.
type Service struct {
	grpcServer *grpc.Server
	httpServer *http.Server
}

// New constructs a cache service.
func New() *Service {
	grpcServer := grpc.NewServer()
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	icpb.RegisterInferenceCacheServer(grpcServer, newInferenceCacheService())

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	return &Service{
		grpcServer: grpcServer,
		httpServer: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}
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

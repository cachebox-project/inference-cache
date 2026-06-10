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
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/cachebox-project/inference-cache/pkg/index"
	"github.com/cachebox-project/inference-cache/pkg/server/auth"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/tokenize"
)

// Config controls the cache server listeners.
//
// The public HTTP listener (HTTPAddr) carries kubelet- and Prometheus-facing
// endpoints (/healthz, /readyz, /metrics). The snapshot listener
// (SnapshotAddr) carries /snapshot (controller-read), /policy
// (controller-write), and /probe (controller-driven functional self-test),
// each gated by ServiceAccount bearer auth — a NetworkPolicy further restricts
// L3/L4 access to the controller's pod selector (see config/server). The split
// exists so kubelet probes and Prometheus scrapes — which can't carry a
// bearer — stay on an open port while every controller↔server mutation/
// observation surface lives behind the same auth profile.
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

// controllerAuthConfig holds the parts of auth.Options that callers can
// configure from outside the package. Service.New constructs the actual
// Authenticator instances so the per-endpoint recorders stay an internal
// detail.
type controllerAuthConfig struct {
	reviewer auth.TokenReviewer
	saName   string
	audience string
}

// WithControllerAuth wires bearer-token authentication onto the controller-
// facing endpoints (/snapshot, /policy, and /probe), which share the snapshot
// listener. When unset all three endpoints are served without authentication;
// the production binary always sets it. expectedSA is the canonical
// ServiceAccount username, e.g.
// "system:serviceaccount:inference-cache-system:inference-cache-controller-manager".
// audience is the value passed to TokenReviewSpec.Audiences (audience
// binding); pass an empty string to disable audience binding (legacy
// posture). The production binary defaults audience to
// auth.ControllerAudience and must keep it aligned with the controller's
// projected SA token volume. The same audience gates all three endpoints
// because they share one middleware identity (the controller SA).
func WithControllerAuth(reviewer auth.TokenReviewer, expectedSA, audience string) Option {
	return func(s *Service) {
		s.controllerAuthCfg = &controllerAuthConfig{reviewer: reviewer, saName: expectedSA, audience: audience}
	}
}

// WithTokenizer wires the server-owned tokenizer onto the (model, prompt_text)
// LookupRoute path. When unset (the default), the handler keeps tokenize.Unavailable
// and that path fails open to NO_HINT; the token_ids path is unaffected. The
// production binary passes tokenize.New(...) — the real tokenizer under the
// smgcgo build, Unavailable otherwise.
func WithTokenizer(tk tokenize.Tokenizer) Option {
	return func(s *Service) { s.tokenizer = tk }
}

// WithEngineBlockSize sets the KV block size (tokens per block) used to
// fingerprint token_ids / tokenized prompt_text on the dual-input LookupRoute
// path. It MUST match the engine's KV block size (and the kvevent-subscriber's,
// which reads it per-event) or the derived block-hash chain won't line up with
// the ingested keys. Non-positive values are ignored, leaving the
// DefaultEngineBlockSize (16, vLLM's default).
func WithEngineBlockSize(n int) Option {
	return func(s *Service) { s.blockSize = n }
}

// Service hosts the gRPC API and the HTTP health/metrics endpoints.
type Service struct {
	grpcServer        *grpc.Server
	grpcCreds         credentials.TransportCredentials
	publicHTTPServer  *http.Server
	snapshotServer    *http.Server
	snapshotHandler   http.Handler
	policyHandler     http.Handler
	controllerAuthCfg *controllerAuthConfig
	metrics           *serverMetrics
	index             *index.Index
	policies          *PolicyStore
	tokenizer         tokenize.Tokenizer
	blockSize         int
}

// New constructs a cache service.
func New(opts ...Option) *Service {
	metrics := newServerMetrics()
	policies := NewPolicyStore()
	idx := index.New(
		index.WithMetrics(metrics),
		index.WithTTLResolver(policies),
		index.WithTenantQuotaResolver(policies),
		index.WithEvictionResolver(policies),
		// Reserve the probe tenant from the global cap so a concurrent real-
		// workload Ingest can never pick a real-workload entry as a victim to
		// make room for the probe's transient ingest. Probe-tenant entries are
		// excluded from both the cap accounting (effectiveTotal = totalEntries
		// - reservedEntries) and the victim candidate set, so the probe path's
		// "never mutates real workload state" invariant holds even under
		// concurrent real-workload writes on a saturated index.
		index.WithReservedTenants(ProbeTenantID),
	)

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

	// /snapshot, /policy, and /probe are served from a dedicated listener
	// so a NetworkPolicy can restrict ingress to the controller's pod
	// selector without breaking kubelet probes or Prometheus scrapes on
	// the public listener. All three endpoints are controller-to-server:
	// /snapshot is a read (controller polls cache aggregate), /policy is
	// a write (controller pushes the resolved CachePolicy snapshot,
	// replace-on-write), and /probe is a controller-driven functional
	// self-test (POST returns per-stage outcomes). They share one auth
	// profile because they share one caller identity.
	snapshotMux := http.NewServeMux()
	snapshotHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Tighten the contract to GET-only: the controller poller and any
		// future scrapers only ever issue GETs, so anything else is either
		// a bug or a probe — return 405 with an Allow header instead of
		// silently serving the same body for POST/PUT/DELETE.
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed\n", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(idx.Snapshot()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	// /policy — internal endpoint the controller PUSHES resolved CachePolicy
	// snapshots to. Replace-on-write; the controller owns the source of truth
	// and re-pushes on its tick, so a server restart self-heals.
	policyHTTPHandler := http.Handler(policyHandler(policies))
	// /probe — functional self-test. The controller POSTs a probe request
	// per CacheBackend at reconcile time (controller-wiring follow-up); the
	// server synthesizes a deterministic round-trip and reports per-stage
	// outcomes. Shares the controller-auth profile (same listener, same SA
	// identity); this revision ships with no T2Prober wired, so the T2 stage
	// always reports skipped until the controller-wiring follow-up plumbs a
	// real one through.
	prober := NewProber(idx, nil)
	probeHTTPHandler := http.Handler(probeHandler(prober))
	snapshotMux.Handle("/snapshot", snapshotHandler)
	snapshotMux.Handle("/policy", policyHTTPHandler)
	snapshotMux.Handle("/probe", probeHTTPHandler)

	s := &Service{
		publicHTTPServer: &http.Server{
			Handler:           publicMux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		snapshotServer: &http.Server{
			Handler:           snapshotMux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		snapshotHandler: snapshotHandler,
		policyHandler:   policyHTTPHandler,
		metrics:         metrics,
		index:           idx,
		policies:        policies,
	}
	// probeHTTPHandler and prober are NOT stored on Service: the handler is
	// already mounted on snapshotMux above (and re-referenced by the auth-
	// wrapping branch below), and the prober is held alive by the handler's
	// closure for as long as the Service is. Carrying them as struct fields
	// that nothing reads would just be inert state.
	for _, opt := range opts {
		opt(s)
	}

	// Build the gRPC server AFTER options so the transport posture (set by
	// WithGRPCTLS) is known. credentials.NewTLS-backed creds terminate TLS
	// in-process on :9090; with no creds the listener is plaintext (the
	// default — TLS is the opt-in config/overlays/server-tls overlay). The
	// grpc.health.v1 service and reflection are registered on the same server,
	// so the kubelet/grpcurl health surface speaks whatever transport the
	// listener does. See docs/design/grpc-tls.md.
	var grpcOpts []grpc.ServerOption
	tlsEnabled := s.grpcCreds != nil
	if tlsEnabled {
		grpcOpts = append(grpcOpts, grpc.Creds(s.grpcCreds))
	}
	s.grpcServer = grpc.NewServer(grpcOpts...)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(s.grpcServer, healthServer)
	svc := newInferenceCacheService(idx, metrics, policies)
	if s.tokenizer != nil {
		svc.tokenizer = s.tokenizer
	}
	if s.blockSize > 0 {
		svc.blockSize = s.blockSize
	}
	icpb.RegisterInferenceCacheServer(s.grpcServer, svc)
	// Register gRPC server reflection so operators can use grpcurl
	// (list / describe / generic call) and similar schema-aware debug tooling
	// without shipping the .proto. Reflection exposes only the service schema,
	// never KV or prompt data. grpc_health_probe is unrelated — it speaks the
	// standard grpc.health.v1 service and does not need reflection.
	reflection.Register(s.grpcServer)
	// Publish the wire posture so operators can confirm it from Prometheus. The
	// human-facing startup log lives in cmd/server (the "startup" line carries
	// grpc_tls) rather than here, so constructing a Service in tests stays quiet.
	if tlsEnabled {
		s.metrics.grpcTLSEnabled.Set(1)
	} else {
		s.metrics.grpcTLSEnabled.Set(0)
	}

	// If auth is configured, wrap /snapshot, /policy, and /probe in the
	// TokenReview middleware. Three Authenticator instances share the same
	// Reviewer, ExpectedSA, and Audience but emit per-endpoint metrics
	// (inferencecache_snapshot_auth_total + inferencecache_policy_auth_total
	// + inferencecache_probe_auth_total) so a dashboard can distinguish a
	// read-side auth failure (info leak attempt) from a write-side one
	// (active tampering attempt) from a probe-side one (silent Ready-gate
	// degradation once the controller-wiring follow-up lands). The three
	// caches are independent — one extra TokenReview per endpoint per TTL
	// window in the steady state, negligible vs the apiserver's own auth
	// cost. The shared Audience means the projected SA token at the
	// controller's volume mount admits to all three endpoints uniformly;
	// there's one trust boundary, not three.
	if s.controllerAuthCfg != nil {
		snapshotAuthn, err := auth.NewAuthenticator(auth.Options{
			Reviewer:               s.controllerAuthCfg.reviewer,
			ExpectedServiceAccount: s.controllerAuthCfg.saName,
			Audience:               s.controllerAuthCfg.audience,
			Recorder:               s.metrics.SnapshotAuthRecorder(),
		})
		if err != nil {
			// New is used by both binaries and tests; a misconfigured auth
			// here is a programming error (empty SA or nil reviewer), not a
			// runtime condition the caller can recover from.
			panic(fmt.Sprintf("server: snapshot auth: %v", err))
		}
		policyAuthn, err := auth.NewAuthenticator(auth.Options{
			Reviewer:               s.controllerAuthCfg.reviewer,
			ExpectedServiceAccount: s.controllerAuthCfg.saName,
			Audience:               s.controllerAuthCfg.audience,
			Recorder:               s.metrics.PolicyAuthRecorder(),
		})
		if err != nil {
			panic(fmt.Sprintf("server: policy auth: %v", err))
		}
		probeAuthn, err := auth.NewAuthenticator(auth.Options{
			Reviewer:               s.controllerAuthCfg.reviewer,
			ExpectedServiceAccount: s.controllerAuthCfg.saName,
			Audience:               s.controllerAuthCfg.audience,
			Recorder:               s.metrics.ProbeAuthRecorder(),
		})
		if err != nil {
			panic(fmt.Sprintf("server: probe auth: %v", err))
		}
		gatedMux := http.NewServeMux()
		gatedMux.Handle("/snapshot", snapshotAuthn.Middleware(snapshotHandler))
		gatedMux.Handle("/policy", policyAuthn.Middleware(policyHTTPHandler))
		gatedMux.Handle("/probe", probeAuthn.Middleware(probeHTTPHandler))
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

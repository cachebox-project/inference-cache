package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/cachebox-project/inference-cache/pkg/server"
	"github.com/cachebox-project/inference-cache/pkg/server/auth"
	"github.com/cachebox-project/inference-cache/pkg/version"
)

func main() {
	cfg := server.DefaultConfig()
	logFormat := flag.String("log-format", string(server.LogFormatJSON), "Log output format (json|text). JSON is the production default; text is for local development.")
	logLevel := flag.String("log-level", "info", "Log level (debug|info|warn|error).")
	flag.StringVar(&cfg.GRPCAddr, "grpc-bind-address", cfg.GRPCAddr, "The address the gRPC server binds to.")
	flag.StringVar(&cfg.HTTPAddr, "http-bind-address", cfg.HTTPAddr, "The address the public HTTP server binds to (serves /healthz, /readyz, /metrics, /policy).")
	flag.StringVar(&cfg.SnapshotAddr, "snapshot-bind-address", cfg.SnapshotAddr, "The address the internal snapshot HTTP server binds to (serves /snapshot, gated by ServiceAccount bearer auth).")
	expectedSA := flag.String("snapshot-allowed-sa", "", "Fully-qualified ServiceAccount username allowed to scrape /snapshot, e.g. system:serviceaccount:inference-cache-system:inference-cache-controller-manager. REQUIRED in production. Without it the server refuses to start; passing --insecure-disable-snapshot-auth is the explicit, named escape hatch for local development.")
	insecureNoAuth := flag.Bool("insecure-disable-snapshot-auth", false, "Local-development only: serve /snapshot without authentication. The flag is named to make any operator who runs it on a real cluster notice. Mutually exclusive with --snapshot-allowed-sa.")
	flag.Parse()

	format, err := server.ParseLogFormat(*logFormat)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	level, err := server.ParseLogLevel(*logLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	handler, err := server.NewLogHandler(format, level, os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	slog.SetDefault(slog.New(handler))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Fail closed by default: refuse to start unless either a real allowed SA
	// is configured or the operator explicitly opted into the unauthenticated
	// local-dev path. The previous shape (empty flag → silent unauth) made it
	// trivial for a real cluster deployment to accidentally ship a wide-open
	// /snapshot endpoint, which defeats the point of the hardening.
	switch {
	case *expectedSA != "" && *insecureNoAuth:
		fmt.Fprintln(os.Stderr, "--snapshot-allowed-sa and --insecure-disable-snapshot-auth are mutually exclusive")
		os.Exit(2)
	case *expectedSA == "" && !*insecureNoAuth:
		fmt.Fprintln(os.Stderr, "missing --snapshot-allowed-sa; pass --insecure-disable-snapshot-auth to run /snapshot without authentication (local development only)")
		os.Exit(2)
	}

	var opts []server.Option
	if *expectedSA != "" {
		restCfg, err := rest.InClusterConfig()
		if err != nil {
			slog.ErrorContext(ctx, "in_cluster_config", "err", err)
			os.Exit(1)
		}
		clientset, err := kubernetes.NewForConfig(restCfg)
		if err != nil {
			slog.ErrorContext(ctx, "kube_client", "err", err)
			os.Exit(1)
		}
		opts = append(opts, server.WithSnapshotAuth(auth.FromClientset(clientset), *expectedSA))
		slog.InfoContext(ctx, "snapshot_auth_enabled", "expected_sa", *expectedSA)
	} else {
		// *insecureNoAuth == true, verified above.
		slog.WarnContext(ctx, "snapshot_auth_disabled",
			"reason", "--insecure-disable-snapshot-auth was set; /snapshot is unauthenticated. This must NEVER be used in production.")
	}

	slog.InfoContext(ctx, "startup",
		"version", version.GitVersion,
		"commit", version.GitCommit,
		"grpc_addr", cfg.GRPCAddr,
		"http_addr", cfg.HTTPAddr,
		"snapshot_addr", cfg.SnapshotAddr,
	)
	if err := server.ListenAndServe(ctx, cfg, opts...); err != nil {
		// Terminal error — log once here. pkg/server.Serve does NOT log on
		// the errCh branch so we don't double-emit when a listener fails.
		slog.ErrorContext(ctx, "serve_error", "err", err)
		os.Exit(1)
	}
}

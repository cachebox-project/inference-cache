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
	expectedSA := flag.String("snapshot-allowed-sa", "", "Fully-qualified ServiceAccount username allowed to scrape /snapshot, e.g. system:serviceaccount:inference-cache-system:inference-cache-controller-manager. When empty, /snapshot is served without authentication (local development only).")
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
		slog.WarnContext(ctx, "snapshot_auth_disabled", "reason", "no --snapshot-allowed-sa flag")
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

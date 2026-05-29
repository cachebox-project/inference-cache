package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/cachebox-project/inference-cache/pkg/server"
	"github.com/cachebox-project/inference-cache/pkg/version"
)

func main() {
	cfg := server.DefaultConfig()
	logFormat := flag.String("log-format", string(server.LogFormatJSON), "Log output format (json|text). JSON is the production default; text is for local development.")
	logLevel := flag.String("log-level", "info", "Log level (debug|info|warn|error).")
	flag.StringVar(&cfg.GRPCAddr, "grpc-bind-address", cfg.GRPCAddr, "The address the gRPC server binds to.")
	flag.StringVar(&cfg.HTTPAddr, "http-bind-address", cfg.HTTPAddr, "The address the HTTP server binds to (serves /healthz, /readyz, /metrics, /snapshot).")
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

	slog.InfoContext(ctx, "startup",
		"version", version.GitVersion,
		"commit", version.GitCommit,
		"grpc_addr", cfg.GRPCAddr,
		"http_addr", cfg.HTTPAddr,
	)
	if err := server.ListenAndServe(ctx, cfg); err != nil {
		// Terminal error — log once here. pkg/server.Serve does NOT log on
		// the errCh branch so we don't double-emit when a listener fails.
		slog.ErrorContext(ctx, "serve_error", "err", err)
		os.Exit(1)
	}
}

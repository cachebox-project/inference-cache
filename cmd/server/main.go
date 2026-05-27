package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/cachebox-project/inference-cache/pkg/server"
	"github.com/cachebox-project/inference-cache/pkg/version"
)

func main() {
	cfg := server.DefaultConfig()
	flag.StringVar(&cfg.GRPCAddr, "grpc-bind-address", cfg.GRPCAddr, "The address the gRPC server binds to.")
	flag.StringVar(&cfg.HTTPAddr, "http-bind-address", cfg.HTTPAddr, "The address the HTTP server binds to (serves /healthz, /readyz, /metrics).")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("starting inference-cache server version=%s commit=%s grpc=%s http=%s", version.GitVersion, version.GitCommit, cfg.GRPCAddr, cfg.HTTPAddr)
	if err := server.ListenAndServe(ctx, cfg); err != nil {
		log.Printf("server stopped with error: %v", err)
		os.Exit(1)
	}
}

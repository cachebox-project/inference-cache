// Command kvevent-subscriber runs as a sidecar next to a vLLM engine replica:
// it subscribes to the engine's KV-cache events over ZMQ and reports cache state
// to the inferencecache-server over gRPC. Metadata only — never tokens or prompt
// text. Fail-soft: engine/server outages are retried and never stall the engine.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/cachebox-project/inference-cache/pkg/adapters/engine"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

func main() {
	var (
		endpoint = flag.String("engine-endpoint", "tcp://127.0.0.1:5557", "engine KV-event ZMQ PUB endpoint")
		topic    = flag.String("topic", "kv-events", "ZMQ topic to subscribe to (empty = all)")
		server   = flag.String("server", "127.0.0.1:9090", "inferencecache-server gRPC address")
		replica  = flag.String("replica-id", "", "engine replica id (required)")
		model    = flag.String("model-id", "", "served model id (required)")
		tenant   = flag.String("tenant-id", "", "tenant id (optional)")
		scheme   = flag.String("hash-scheme", "vllm", "engine prefix-hash scheme (required, non-empty)")
		window   = flag.Duration("window", 100*time.Millisecond, "add-batching/debounce flush window")
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cfg := engine.Config{
		ReplicaID:  *replica,
		ModelID:    *model,
		TenantID:   *tenant,
		HashScheme: *scheme,
	}
	if err := cfg.Validate(); err != nil {
		logger.Error("invalid config", "err", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Lazy connect: gRPC dials on first RPC, so the subscriber starts even if the
	// server isn't up yet (it will connect when events begin flowing).
	conn, err := grpc.NewClient(*server, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Error("grpc client", "server", *server, "err", err)
		os.Exit(1)
	}
	defer func() { _ = conn.Close() }()

	reporter := engine.NewReporter(icpb.NewInferenceCacheClient(conn), cfg,
		engine.WithWindow(*window), engine.WithLogger(logger))
	sub := engine.NewSubscriber(*endpoint, *topic, engine.WithSubscriberLogger(logger))

	out := make(chan *engine.EventBatch, 256)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := reporter.Run(ctx, out); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("reporter stopped", "err", err)
		}
	}()

	logger.Info("kvevent-subscriber starting",
		"engine_endpoint", *endpoint, "server", *server, "replica_id", *replica, "model_id", *model)

	if err := sub.Run(ctx, out); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("subscriber stopped", "err", err)
	}

	stop()    // ensure the reporter sees cancellation and flushes
	wg.Wait() // wait for the final flush before exit
	logger.Info("kvevent-subscriber stopped")
}

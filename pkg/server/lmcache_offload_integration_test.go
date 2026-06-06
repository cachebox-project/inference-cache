package server

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/cachebox-project/inference-cache/pkg/adapters/engine"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// L2 offload regression: the cache-stress benchmark returned NO_HINT on every
// LookupRoute because the engine + LMCache pair churns each block through
// BlockStored → (offload) → BlockRemoved, the subscriber forwarded the
// BlockRemoved as PREFIX_EVICTED, and the index dropped the routing hint while
// the block was still cached at the L2 tier. The fix adds
// WithIgnoreBlockRemoved on the reporter — set in LMCache-integrated
// deployments — which drops the per-block evictions so the index keeps the
// hint until the freshness TTL expires.
//
// These tests pin the behavior on both branches: the default still forwards
// BlockRemoved (single-tier deployments rely on prompt eviction), and the
// L2-tier mode keeps the hint so LookupRoute matches.

// runEngineReporterAgainstServer runs the engine Reporter against an
// in-process server, feeds the batches, drains, and returns the client so
// the test can issue LookupRoute calls against the same server. The block
// hashes are 8-byte big-endian to match the on-the-wire shape the
// subscriber's hashToBytes produces from vLLM's integer-hash variant — the
// canonical L2 offload shape.
func runEngineReporterAgainstServer(t *testing.T, opts []engine.ReporterOption, batches ...*engine.EventBatch) (client icpb.InferenceCacheClient, stop func()) {
	t.Helper()
	conn, _, stopServer := startInProcessServerConn(t)
	client = icpb.NewInferenceCacheClient(conn)

	cfg := engine.Config{
		ReplicaID:  "vllm-engine-cs1",
		ModelID:    "vllm-model",
		TenantID:   "ic-smoke",
		HashScheme: "vllm",
	}
	// Short flush window so the Run loop drains promptly when the input closes.
	opts = append([]engine.ReporterOption{engine.WithWindow(10 * time.Millisecond)}, opts...)
	reporter := engine.NewReporter(client, cfg, opts...)

	in := make(chan *engine.EventBatch, len(batches))
	for _, b := range batches {
		in <- b
	}
	close(in)

	done := make(chan error, 1)
	go func() { done <- reporter.Run(context.Background(), in) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reporter did not drain in time")
	}
	// A tiny dwell to let the server finish processing the last gRPC call before
	// the test issues LookupRoute — Reporter returns once the stream is closed,
	// but the server's ingest handler may still be running for a microsecond.
	time.Sleep(50 * time.Millisecond)
	return client, stopServer
}

// be8 mirrors the subscriber's hashToBytes for an int hash: 8-byte big-endian
// of the unsigned-cast value. Same encoding the gateway proxy must use when it
// serializes vLLM's int-form block hashes for LookupRoute.
func be8(h uint64) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, h)
	return out
}

// L2-tier mode: with WithIgnoreBlockRemoved set, BlockRemoved is dropped at
// the reporter, so the index retains the routing hint and LookupRoute returns
// PREFIX_MATCH for a block the engine has since evicted from GPU. This is the
// scenario L2 offload regresses on (LMCache-integrated cache-stress runs).
func TestLMCacheOffloadKeepsRoutingHintWithIgnoreBlockRemoved(t *testing.T) {
	// One stored block then an immediate remove for the same hash — the
	// shape vLLM+LMCache produces every time the engine evicts a GPU block.
	h := be8(0xD2CD1BA8E13D7DD6)
	stored := engine.BlockStored{BlockHashes: [][]byte{h}, BlockSize: 16}
	removed := engine.BlockRemoved{BlockHashes: [][]byte{h}}

	client, stop := runEngineReporterAgainstServer(t,
		[]engine.ReporterOption{engine.WithIgnoreBlockRemoved(true)},
		&engine.EventBatch{TimestampSeconds: 0, Events: []engine.Event{stored}},
		&engine.EventBatch{TimestampSeconds: 2.0, Events: []engine.Event{removed}},
	)
	defer stop()

	resp, err := client.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "vllm-model", TenantId: "ic-smoke", HashScheme: "vllm",
		BlockHashes: [][]byte{h}, BlockTokenCounts: []int32{16},
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("reason = %q, want PREFIX_MATCH — WithIgnoreBlockRemoved must keep the routing hint after the L2-tier offload eviction. scores=%+v", resp.GetReasonCode(), resp.GetReplicaScores())
	}
	if len(resp.GetReplicaScores()) == 0 || resp.GetReplicaScores()[0].GetReplicaId() != "vllm-engine-cs1" {
		t.Fatalf("expected single hit for vllm-engine-cs1, got %+v", resp.GetReplicaScores())
	}
}

// Default (single-tier) mode: BlockRemoved still forwards as PREFIX_EVICTED,
// so the index drops the entry — pins the existing contract guard against an
// accidental flip of the flag default. The same wire shape as the L2 test
// (one store + one remove of the same hash) must yield NO_HINT here.
func TestDefaultForwardsBlockRemovedAndIndexLosesHint(t *testing.T) {
	h := be8(0xC0FFEE0011223344)
	stored := engine.BlockStored{BlockHashes: [][]byte{h}, BlockSize: 16}
	removed := engine.BlockRemoved{BlockHashes: [][]byte{h}}

	client, stop := runEngineReporterAgainstServer(t, nil, // default reporter
		&engine.EventBatch{TimestampSeconds: 0, Events: []engine.Event{stored}},
		&engine.EventBatch{TimestampSeconds: 2.0, Events: []engine.Event{removed}},
	)
	defer stop()

	resp, err := client.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "vllm-model", TenantId: "ic-smoke", HashScheme: "vllm",
		BlockHashes: [][]byte{h}, BlockTokenCounts: []int32{16},
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("reason = %q, want NO_HINT — default reporter forwards BlockRemoved as PREFIX_EVICTED, so the index must have lost the entry. scores=%+v", resp.GetReasonCode(), resp.GetReplicaScores())
	}
}

// Round-trip the on-the-wire byte shape an integer engine hash takes: subscriber
// hashToBytes produces 8-byte BE; the gateway proxy must encode the same way
// for LookupRoute. The single-block lookup must match by exact byte equality.
// Without that invariant the whole pipeline (L2 offload included) fails silently.
func TestEngineIntegerHashWireFormatRoundTripsViaReporter(t *testing.T) {
	// The exact bytes captured live from one vLLM BlockStored event during
	// the L2 offload diagnosis — `_int_to_be8(15189827530337910230) ==
	// 0xD2CD1BA8E13D7DD6` per the proxy's wire encoding.
	const hashInt uint64 = 0xD2CD1BA8E13D7DD6
	h := be8(hashInt)
	stored := engine.BlockStored{BlockHashes: [][]byte{h}, BlockSize: 16}

	client, stop := runEngineReporterAgainstServer(t,
		[]engine.ReporterOption{engine.WithIgnoreBlockRemoved(true)},
		&engine.EventBatch{TimestampSeconds: 0, Events: []engine.Event{stored}},
	)
	defer stop()

	resp, err := client.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "vllm-model", TenantId: "ic-smoke", HashScheme: "vllm",
		BlockHashes: [][]byte{h}, BlockTokenCounts: []int32{16},
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("reason = %q, want PREFIX_MATCH — subscriber-produced and proxy-produced 8-byte-BE encodings must round-trip byte-identical for the same int hash. scores=%+v", resp.GetReasonCode(), resp.GetReplicaScores())
	}
}

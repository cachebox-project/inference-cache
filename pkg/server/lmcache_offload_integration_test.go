package server

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/cachebox-project/inference-cache/pkg/adapters/engine"
	"github.com/cachebox-project/inference-cache/pkg/fingerprint"
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
	return runEngineReporterCfgAgainstServer(t, engine.Config{
		ReplicaID:  "vllm-engine-cs1",
		ModelID:    "vllm-model",
		TenantID:   "ic-smoke",
		HashScheme: "vllm",
	}, opts, batches...)
}

// runEngineReporterCfgAgainstServer is runEngineReporterAgainstServer with an
// explicit subscriber Config, so a test can vary the engine-side identity (e.g.
// supply Config.AdapterNames to exercise LoRA index partitioning end to end).
func runEngineReporterCfgAgainstServer(t *testing.T, cfg engine.Config, opts []engine.ReporterOption, batches ...*engine.EventBatch) (client icpb.InferenceCacheClient, stop func()) {
	t.Helper()
	conn, _, stopServer := startInProcessServerConn(t)
	client = icpb.NewInferenceCacheClient(conn)

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
	// Reporter.Run returning means every flush completed: sendAdds calls
	// CloseAndRecv (the server ingests synchronously before SendAndClose
	// returns the Ack), publish() is unary (the server ApplyEvent runs
	// before returning the Ack), and Run's `defer flush()` covers the
	// shutdown drain. Once `done` fires the server has fully processed
	// every event, so no extra dwell is needed before LookupRoute.
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

// tokenSeq returns [start, start+1, ..., start+n-1] as token IDs — a stable block
// of content for the Reporter to fingerprint.
func tokenSeq(start, n int) []uint32 {
	out := make([]uint32, n)
	for i := range out {
		out[i] = uint32(start + i)
	}
	return out
}

// L2-tier mode: with WithIgnoreBlockRemoved set, BlockRemoved is dropped at
// the reporter, so the index retains the routing hint and LookupRoute returns
// PREFIX_MATCH for a block the engine has since evicted from GPU. This is the
// scenario L2 offload regresses on (LMCache-integrated cache-stress runs).
func TestLMCacheOffloadKeepsRoutingHintWithIgnoreBlockRemoved(t *testing.T) {
	// One stored block then an immediate remove for the same hash — the
	// shape vLLM+LMCache produces every time the engine evicts a GPU block.
	// BlockSize=128 keeps the realized matched_tokens above the
	// DefaultMinimumMatchedTokens floor so the assertion is about
	// the offload-pinning behavior, not the floor; smaller block sizes would
	// downgrade this hint to NO_HINT before the L2-tier guard mattered.
	h := be8(0xD2CD1BA8E13D7DD6) // engine block hash — reverse-map identity only
	toks := tokenSeq(1000, 128)  // one 128-token block
	// The index keys on our content fingerprint; the gateway queries with the same.
	our := fingerprint.Bytes(fingerprint.PrefixHashes(toks, 128)[0])
	stored := engine.BlockStored{BlockHashes: [][]byte{h}, TokenIDs: toks, BlockSize: 128}
	removed := engine.BlockRemoved{BlockHashes: [][]byte{h}}

	client, stop := runEngineReporterAgainstServer(t,
		[]engine.ReporterOption{engine.WithIgnoreBlockRemoved(true)},
		&engine.EventBatch{TimestampSeconds: 0, Events: []engine.Event{stored}},
		&engine.EventBatch{TimestampSeconds: 2.0, Events: []engine.Event{removed}},
	)
	defer stop()

	resp, err := client.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "vllm-model", TenantId: "ic-smoke", HashScheme: "vllm",
		BlockHashes: [][]byte{our}, BlockTokenCounts: []int32{128},
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
	toks := tokenSeq(2000, 128)
	our := fingerprint.Bytes(fingerprint.PrefixHashes(toks, 128)[0])
	stored := engine.BlockStored{BlockHashes: [][]byte{h}, TokenIDs: toks, BlockSize: 128}
	removed := engine.BlockRemoved{BlockHashes: [][]byte{h}}

	client, stop := runEngineReporterAgainstServer(t, nil, // default reporter
		&engine.EventBatch{TimestampSeconds: 0, Events: []engine.Event{stored}},
		&engine.EventBatch{TimestampSeconds: 2.0, Events: []engine.Event{removed}},
	)
	defer stop()

	resp, err := client.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "vllm-model", TenantId: "ic-smoke", HashScheme: "vllm",
		BlockHashes: [][]byte{our}, BlockTokenCounts: []int32{128},
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "NO_HINT" {
		t.Fatalf("reason = %q, want NO_HINT — default reporter forwards BlockRemoved as PREFIX_EVICTED, so the index must have lost the entry. scores=%+v", resp.GetReasonCode(), resp.GetReplicaScores())
	}
}

// The content fingerprint the Reporter forwards on ingest (derived in-pod from
// the event's token_ids) MUST be byte-identical to what a gateway computes from
// the same tokens and sends in LookupRoute, or the server's prefixKey map lookup
// misses. This pins that round-trip end-to-end: feed a block's tokens through the
// Reporter's ingest path, then query LookupRoute with the content hash computed
// from the same tokens — expect PREFIX_MATCH. A regression in the fingerprint
// construction, the prefixKey shape, or the proto wire bytes would fail here.
func TestContentHashRoundTripViaReporterAndLookupRoute(t *testing.T) {
	h := be8(0xD2CD1BA8E13D7DD6) // engine block hash — reverse-map identity only
	// BlockSize=128 keeps matched_tokens above the DefaultMinimumMatchedTokens
	// floor, so the assertion is about the hash round-trip, not the floor.
	toks := tokenSeq(3000, 128)
	our := fingerprint.Bytes(fingerprint.PrefixHashes(toks, 128)[0])
	stored := engine.BlockStored{BlockHashes: [][]byte{h}, TokenIDs: toks, BlockSize: 128}

	client, stop := runEngineReporterAgainstServer(t,
		[]engine.ReporterOption{engine.WithIgnoreBlockRemoved(true)},
		&engine.EventBatch{TimestampSeconds: 0, Events: []engine.Event{stored}},
	)
	defer stop()

	resp, err := client.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: "vllm-model", TenantId: "ic-smoke", HashScheme: "vllm",
		BlockHashes: [][]byte{our}, BlockTokenCounts: []int32{128},
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("reason = %q, want PREFIX_MATCH — the content fingerprint the Reporter forwarded did not match the gateway-side fingerprint for the same tokens. scores=%+v", resp.GetReasonCode(), resp.GetReplicaScores())
	}
}

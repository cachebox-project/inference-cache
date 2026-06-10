package main

// GPU-free, per-PR end-to-end gate for the content-fingerprint routing path:
//
//	fake engine (this package, real ZMQ PUB socket)
//	  → kvevent-subscriber pipeline (engine.Subscriber → engine.Reporter —
//	    the same components cmd/kvevent-subscriber wires)
//	  → inference-cache server (pkg/server, real gRPC over loopback TCP)
//	  → LookupRoute
//
// This is the regression lock for the all-NO_HINT bug: the engine's own KV
// block hashes are seeded per-process (vLLM's NONE_HASH = os.urandom), so an
// index keyed on them can never match a gateway's query and every lookup
// degraded to NO_HINT. Routing instead keys on the deterministic content
// fingerprint (pkg/fingerprint) derived from the event's token_ids. If any
// link regresses — the msgpack decode, the fingerprint construction, the
// cross-event chaining, the proto wire bytes, the index keying — the
// PREFIX_MATCH assertion here fails. The negative test locks the producer
// contract from the other side: an engine that stops emitting token_ids must
// be surfaced loudly (warn log) and must index nothing, not silently regress
// the whole path back to NO_HINT.
//
// No GPU, no engine image, no cluster — everything runs in-process except the
// ZMQ hop, which crosses a real loopback TCP socket.

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/cachebox-project/inference-cache/pkg/adapters/engine"
	"github.com/cachebox-project/inference-cache/pkg/fingerprint"
	"github.com/cachebox-project/inference-cache/pkg/server"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

const (
	e2eTopic     = "kv-events"
	e2eModel     = "fake-model"
	e2eReplica   = "fake-engine-0"
	e2eScheme    = "vllm"
	e2eBlockTok  = 128 // comfortably above the default matched-tokens floor
	e2ePubEvery  = 25 * time.Millisecond
	e2eDeadline  = 20 * time.Second
	e2ePollEvery = 50 * time.Millisecond
)

// syncWriter is a goroutine-safe log sink: the Reporter warns from its own
// goroutine while the test polls String() for the expected message.
type syncWriter struct {
	mu sync.Mutex
	b  strings.Builder
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *syncWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

// startServerTCP starts the real server on loopback TCP listeners (gRPC +
// HTTP + snapshot) and returns the gRPC address — the same wire a production
// subscriber and gateway dial.
func startServerTCP(t *testing.T) (grpcAddr string, stop func()) {
	t.Helper()
	grpcL, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen grpc: %v", err)
	}
	httpL, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = grpcL.Close()
		t.Fatalf("listen http: %v", err)
	}
	snapL, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = grpcL.Close()
		_ = httpL.Close()
		t.Fatalf("listen snapshot: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- server.New().Serve(ctx, grpcL, httpL, snapL) }()

	stop = func() {
		cancel()
		if err := <-errCh; err != nil {
			t.Errorf("server shutdown: %v", err)
		}
	}
	return grpcL.Addr().String(), stop
}

// startFakeEngine binds the publisher on an ephemeral loopback port and runs
// the binary's own publishLoop (the exact code path the smoke image runs)
// until stop is called.
func startFakeEngine(t *testing.T, payloads [][]byte) (endpoint string, stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	pub := zmq4.NewPub(ctx)
	if err := pub.Listen("tcp://127.0.0.1:0"); err != nil {
		_ = pub.Close()
		cancel()
		t.Fatalf("zmq listen: %v", err)
	}
	endpoint = "tcp://" + pub.Addr().String()

	done := make(chan struct{})
	go func() {
		defer close(done)
		publishLoop(ctx, pub, e2eTopic, payloads, e2ePubEvery)
	}()
	stop = func() {
		cancel()
		<-done
		_ = pub.Close()
	}
	return endpoint, stop
}

// startSubscriberPipeline wires the real Subscriber → Reporter pair exactly
// as cmd/kvevent-subscriber/main.go does (same components, same shutdown
// ordering: cancel the subscriber, then close the channel so the reporter
// drains its final flush).
func startSubscriberPipeline(t *testing.T, grpcAddr, endpoint, tenant string, logs *syncWriter) (stop func()) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(logs, nil))

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc client: %v", err)
	}
	client := icpb.NewInferenceCacheClient(conn)

	cfg := engine.Config{
		ReplicaID:  e2eReplica,
		ModelID:    e2eModel,
		TenantID:   tenant,
		HashScheme: e2eScheme,
	}
	reporter := engine.NewReporter(client, cfg,
		engine.WithWindow(10*time.Millisecond),
		engine.WithLogger(logger))
	sub := engine.NewSubscriber(endpoint, e2eTopic,
		engine.WithSubscriberLogger(logger),
		engine.WithSubscriberBackoff(50*time.Millisecond))

	out := make(chan *engine.EventBatch, 256)
	subCtx, cancelSub := context.WithCancel(context.Background())

	// Run only exits via context cancellation (it reconnects forever, fail-soft)
	// — capture the error so an unexpected exit reason surfaces as a test
	// failure instead of a silent timeout.
	var subErr error
	subDone := make(chan struct{})
	go func() {
		defer close(subDone)
		subErr = sub.Run(subCtx, out)
	}()
	repDone := make(chan struct{})
	go func() {
		defer close(repDone)
		_ = reporter.Run(context.Background(), out)
	}()

	stop = func() {
		cancelSub()
		<-subDone
		if !errors.Is(subErr, context.Canceled) {
			t.Errorf("subscriber exited with %v, want context.Canceled", subErr)
		}
		close(out)
		<-repDone
		_ = conn.Close()
	}
	return stop
}

// dialQueryClient opens a second gRPC client to the server — the gateway's
// seat at the table, separate from the subscriber's connection.
func dialQueryClient(t *testing.T, grpcAddr string) icpb.InferenceCacheClient {
	t.Helper()
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc query client: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return icpb.NewInferenceCacheClient(conn)
}

// lookupChain issues a chain-form LookupRoute (one entry per block, the shape
// a gateway sends after rolling the prompt's fingerprint chain).
func lookupChain(t *testing.T, client icpb.InferenceCacheClient, tenant string, hashes []uint64) *icpb.LookupRouteResponse {
	t.Helper()
	req := &icpb.LookupRouteRequest{
		ModelId: e2eModel, TenantId: tenant, HashScheme: e2eScheme,
	}
	for _, h := range hashes {
		req.BlockHashes = append(req.BlockHashes, fingerprint.Bytes(h))
		req.BlockTokenCounts = append(req.BlockTokenCounts, e2eBlockTok)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.LookupRoute(ctx, req)
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	return resp
}

// waitFor polls cond until it holds or the deadline passes (PUB/SUB has a
// slow-joiner race and ingest is debounced, so assertions converge rather
// than fire instantly; the publisher republishes until then).
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(e2eDeadline)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(e2ePollEvery)
	}
	t.Fatalf("timed out after %s waiting for %s", e2eDeadline, what)
}

// TestE2EFingerprintRoutingPrefixMatchAndMiss publishes a known prompt's token
// blocks through the full pipeline, then asserts the routing contract from the
// gateway's perspective:
//
//   - the prompt's content fingerprint (recomputed independently with
//     pkg/fingerprint, as a gateway would) returns PREFIX_MATCH with the
//     publishing replica as the hint, with the full two-block chain matched —
//     including the block delivered in a second, parent-chained event;
//   - a novel prompt's fingerprint returns NO_HINT with no replica scores.
func TestE2EFingerprintRoutingPrefixMatchAndMiss(t *testing.T) {
	const tenant = "e2e-route"
	tokens := tokenSeq(5_000, 2*e2eBlockTok) // two blocks
	// Two blocks split across two parent-chained events (in one atomic batch
	// message) — exercises the subscriber's cross-event chaining, not just
	// single-event decode.
	payload, err := buildBatchPayload(tokens, e2eBlockTok, 2, false)
	if err != nil {
		t.Fatalf("buildBatchPayload: %v", err)
	}
	payloads := [][]byte{payload}

	grpcAddr, stopServer := startServerTCP(t)
	defer stopServer()
	endpoint, stopEngine := startFakeEngine(t, payloads)
	defer stopEngine()
	var logs syncWriter
	stopPipeline := startSubscriberPipeline(t, grpcAddr, endpoint, tenant, &logs)
	defer stopPipeline()

	client := dialQueryClient(t, grpcAddr)

	// The gateway-side fingerprint: same package, same scheme, computed from
	// the prompt's tokens alone — independent of anything the engine reported.
	want := fingerprint.PrefixHashes(tokens, e2eBlockTok)
	if len(want) != 2 {
		t.Fatalf("expected 2 prefix hashes, got %d", len(want))
	}

	// Hit: poll until the full two-block chain matches. MatchedTokens must
	// cover both blocks, proving the second (parent-chained, separately
	// published) event landed under the right rolling fingerprint.
	var last *icpb.LookupRouteResponse
	waitFor(t, "PREFIX_MATCH on the published prefix (full 2-block chain)", func() bool {
		last = lookupChain(t, client, tenant, want)
		return last.GetReasonCode() == "PREFIX_MATCH" &&
			len(last.GetReplicaScores()) > 0 &&
			last.GetReplicaScores()[0].GetMatchedTokens() == 2*e2eBlockTok
	})
	if got := last.GetReplicaScores()[0].GetReplicaId(); got != e2eReplica {
		t.Errorf("replica hint = %q, want %q", got, e2eReplica)
	}

	// The one-block prefix of the same prompt must also hit (a shorter prompt
	// sharing the leading block), with the same replica and exactly one
	// block's worth of matched tokens.
	resp := lookupChain(t, client, tenant, want[:1])
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Errorf("one-block prefix: reason = %q, want PREFIX_MATCH; scores=%+v",
			resp.GetReasonCode(), resp.GetReplicaScores())
	} else if scores := resp.GetReplicaScores(); len(scores) == 0 {
		t.Error("one-block prefix: PREFIX_MATCH with zero replica scores violates the contract")
	} else if s := scores[0]; s.GetReplicaId() != e2eReplica || s.GetMatchedTokens() != e2eBlockTok {
		t.Errorf("one-block prefix: got replica %q matched_tokens %d, want %q with %d",
			s.GetReplicaId(), s.GetMatchedTokens(), e2eReplica, e2eBlockTok)
	}

	// Miss: a novel prompt — same scheme, same fingerprint construction,
	// content never published. Must fail open as NO_HINT with no scores
	// (model/tenant/scheme all exist in the index, so any other reason code
	// means the miss path regressed).
	novel := fingerprint.PrefixHashes(tokenSeq(50_000_000, 2*e2eBlockTok), e2eBlockTok)
	resp = lookupChain(t, client, tenant, novel)
	if resp.GetReasonCode() != "NO_HINT" {
		t.Errorf("novel prefix: reason = %q, want NO_HINT; scores=%+v",
			resp.GetReasonCode(), resp.GetReplicaScores())
	}
	if n := len(resp.GetReplicaScores()); n != 0 {
		t.Errorf("novel prefix: %d replica scores, want 0", n)
	}

	// The engine's OWN block hashes (what the index was keyed on before the
	// fingerprint change, normalized to 8-byte big-endian like the decoder
	// does) must NOT be index keys anymore. If this ever matches, the index
	// has regressed to engine-hash keying — the exact shape of the original
	// bug, just with the miss showing up on the gateway side instead.
	resp = lookupChain(t, client, tenant, []uint64{uint64(fakeBlockHash(0)), uint64(fakeBlockHash(1))})
	if resp.GetReasonCode() == "PREFIX_MATCH" {
		t.Errorf("engine block hashes matched the index — routing must key on the content fingerprint, not the engine's hashes; scores=%+v",
			resp.GetReplicaScores())
	}
}

// truncatedBlockStoredPayload encodes a BlockStored whose tuple stops before
// token_ids/block_size — the shape a vLLM version would produce if the
// token_ids field were REMOVED from the msgspec struct (later fields shift
// position, the tuple shrinks). The subscriber's decoder must reject the whole
// batch ("dropping undecodable event batch") rather than misread block_size
// as token_ids; either way nothing may be indexed.
func truncatedBlockStoredPayload(t *testing.T) []byte {
	t.Helper()
	event := []interface{}{"BlockStored", []interface{}{fakeBlockHash(0)}, nil}
	payload, err := msgpack.Marshal([]interface{}{float64(0), []interface{}{event}})
	if err != nil {
		t.Fatalf("marshal truncated payload: %v", err)
	}
	return payload
}

// TestE2EMissingTokenIDsIndexesNothing locks the producer contract from both
// failure shapes of "the engine stops emitting token_ids" (the silent
// all-NO_HINT regression):
//
//   - field present but empty/nil (engine keeps the msgspec field, stops
//     filling it) → the Reporter logs "BlockStored produced no index entries";
//   - field removed from the struct (tuple shrinks / positions shift) → the
//     decoder rejects the batch and the Subscriber logs "dropping undecodable
//     event batch".
//
// The warn lines are the breadcrumbs an operator greps for; the empty index is
// the safety property (no wrong keys, no phantom hints) asserted for both.
func TestE2EMissingTokenIDsIndexesNothing(t *testing.T) {
	const tenant = "e2e-no-tokens"
	tokens := tokenSeq(9_000, e2eBlockTok)
	payload, err := buildBatchPayload(tokens, e2eBlockTok, 1, true /* omit token_ids */)
	if err != nil {
		t.Fatalf("buildBatchPayload: %v", err)
	}
	// The truncated shape must be its own message: a decode error drops the
	// whole batch, so bundling it with the omit-tokens event would mask the
	// forwarder-warning path.
	payloads := [][]byte{payload, truncatedBlockStoredPayload(t)}

	grpcAddr, stopServer := startServerTCP(t)
	defer stopServer()
	endpoint, stopEngine := startFakeEngine(t, payloads)
	defer stopEngine()
	var logs syncWriter
	stopPipeline := startSubscriberPipeline(t, grpcAddr, endpoint, tenant, &logs)
	defer stopPipeline()

	client := dialQueryClient(t, grpcAddr)

	// The warns are the sync points: once both fire, both events have been
	// fully processed (Stored() ran and produced nothing; the truncated batch
	// was rejected at decode), so the assertions below observe the post-ingest
	// state, not a not-yet-delivered one.
	waitFor(t, `the "BlockStored produced no index entries" warning`, func() bool {
		return strings.Contains(logs.String(), "BlockStored produced no index entries")
	})
	waitFor(t, `the "dropping undecodable event batch" warning (truncated tuple)`, func() bool {
		return strings.Contains(logs.String(), "dropping undecodable event batch")
	})

	// Nothing indexed: the aggregate for this tenant/model must be empty.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state, err := client.GetCacheState(ctx, &icpb.GetCacheStateRequest{TenantId: tenant, ModelId: e2eModel})
	if err != nil {
		t.Fatalf("GetCacheState: %v", err)
	}
	if n := state.GetSummary().GetTotalPrefixes(); n != 0 {
		t.Errorf("total prefixes = %d, want 0 — a no-token_ids event must not be indexed", n)
	}

	// And the would-be fingerprint of those tokens must miss: NO_HINT on an
	// empty index, never a phantom match.
	wouldBe := fingerprint.PrefixHashes(tokens, e2eBlockTok)
	resp := lookupChain(t, client, tenant, wouldBe)
	if resp.GetReasonCode() != "NO_HINT" {
		t.Errorf("would-be prefix: reason = %q, want NO_HINT; scores=%+v",
			resp.GetReasonCode(), resp.GetReplicaScores())
	}
	if n := len(resp.GetReplicaScores()); n != 0 {
		t.Errorf("would-be prefix: %d replica scores, want 0", n)
	}
}

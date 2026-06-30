package engine

import (
	"bytes"
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/cachebox-project/inference-cache/pkg/fingerprint"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// recordingServer captures what the Reporter sends, over a real gRPC connection.
// ops is an ordered receipt log ("add:<hex>" / "evict:<hex>" / "clear") used to
// assert cross-RPC ordering.
type recordingServer struct {
	icpb.UnimplementedInferenceCacheServer
	mu       sync.Mutex
	updates  []*icpb.CacheStateUpdate
	events   []*icpb.CacheEvent
	ops      []string
	reporter *Reporter // set by runReporterWithOpts so tests can inspect internal state
}

func (s *recordingServer) ReportCacheState(stream icpb.InferenceCache_ReportCacheStateServer) error {
	for {
		u, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return stream.SendAndClose(&icpb.Ack{Accepted: true})
			}
			return err
		}
		s.mu.Lock()
		s.updates = append(s.updates, u)
		for _, p := range u.GetPrefixes() {
			s.ops = append(s.ops, "add:"+string(p.GetPrefixHash()))
		}
		s.mu.Unlock()
	}
}

func (s *recordingServer) PublishEvent(_ context.Context, ev *icpb.CacheEvent) (*icpb.Ack, error) {
	s.mu.Lock()
	s.events = append(s.events, ev)
	switch ev.GetType() {
	case icpb.CacheEvent_PREFIX_EVICTED:
		s.ops = append(s.ops, "evict:"+string(ev.GetPrefixHash()))
	case icpb.CacheEvent_ALL_CLEARED:
		s.ops = append(s.ops, "clear")
	}
	s.mu.Unlock()
	return &icpb.Ack{Accepted: true}, nil
}

func runReporter(t *testing.T, batches ...*EventBatch) *recordingServer {
	return runReporterWindow(t, 20*time.Millisecond, batches...)
}

// runReporterWindow starts a recording server + Reporter over bufconn, feeds the
// batches, closes the input, and returns the server after Run has drained.
func runReporterWindow(t *testing.T, window time.Duration, batches ...*EventBatch) *recordingServer {
	return runReporterWithOpts(t, []ReporterOption{WithWindow(window)}, batches...)
}

// runReporterWithOpts is the most general harness; tests that need to set
// extra ReporterOptions (e.g. WithIgnoreBlockRemoved) pass them through.
// It uses the default vLLM testConfig(); runReporterCfg lets a test pick a
// different identity (e.g. a SGLang config with HashScheme="sglang").
func runReporterWithOpts(t *testing.T, opts []ReporterOption, batches ...*EventBatch) *recordingServer {
	return runReporterCfg(t, testConfig(), opts, batches...)
}

// runReporterCfg is runReporterWithOpts with an explicit Config so a test can
// exercise a non-vLLM engine identity end-to-end through the Reporter.
func runReporterCfg(t *testing.T, cfg Config, opts []ReporterOption, batches ...*EventBatch) *recordingServer {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	rec := &recordingServer{}
	icpb.RegisterInferenceCacheServer(srv, rec)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	r := NewReporter(icpb.NewInferenceCacheClient(conn), cfg, opts...)
	rec.reporter = r
	in := make(chan *EventBatch, len(batches))
	for _, b := range batches {
		in <- b
	}
	close(in)

	done := make(chan error, 1)
	go func() { done <- r.Run(context.Background(), in) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reporter did not drain in time")
	}
	return rec
}

func TestReporterForwardsBlockStored(t *testing.T) {
	const bs = 16
	toks := tokSeq(100, 2*bs) // 2 full blocks
	want := fingerprint.PrefixHashes(toks, bs)
	rec := runReporter(t, &EventBatch{
		TimestampSeconds: 2.0,
		Events: []Event{BlockStored{
			BlockHashes: [][]byte{{0x0a}, {0x0b}},
			TokenIDs:    toks,
			BlockSize:   bs,
		}},
	})

	rec.mu.Lock()
	defer rec.mu.Unlock()
	var hashes [][]byte
	for _, u := range rec.updates {
		if u.GetHashScheme() != "vllm" || u.GetReplicaId() != "vllm-0" {
			t.Errorf("update identity = %s/%s, want vllm-0/vllm", u.GetReplicaId(), u.GetHashScheme())
		}
		for _, p := range u.GetPrefixes() {
			if p.GetTokenCount() <= 0 {
				t.Errorf("token_count = %d, want > 0", p.GetTokenCount())
			}
			hashes = append(hashes, p.GetPrefixHash())
		}
	}
	// The forwarded keys are our content fingerprints, not the engine block hashes.
	if len(hashes) != 2 ||
		!bytes.Equal(hashes[0], fingerprint.Bytes(want[0])) ||
		!bytes.Equal(hashes[1], fingerprint.Bytes(want[1])) {
		t.Errorf("forwarded hashes = %x, want content hashes [%x %x]",
			hashes, fingerprint.Bytes(want[0]), fingerprint.Bytes(want[1]))
	}
}

// With a window long enough that the ticker never fires, the only path that can
// deliver buffered adds is the shutdown flush — which must reopen the stream.
func TestReporterFlushesPendingOnShutdown(t *testing.T) {
	const bs = 16
	toks := tokSeq(42, bs)
	want := fingerprint.Bytes(fingerprint.PrefixHashes(toks, bs)[0])
	rec := runReporterWindow(t, time.Hour, &EventBatch{
		TimestampSeconds: 1,
		Events:           []Event{BlockStored{BlockHashes: [][]byte{{0x42}}, TokenIDs: toks, BlockSize: bs}},
	})
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var found bool
	for _, u := range rec.updates {
		for _, p := range u.GetPrefixes() {
			if bytes.Equal(p.GetPrefixHash(), want) {
				found = true
			}
		}
	}
	if !found {
		t.Error("shutdown flush did not deliver the buffered add")
	}
}

// Store-then-evict within one debounce window must reach the server in order
// (add before evict), or the additive add would re-create the evicted prefix.
func TestReporterFlushesAddsBeforeRemoval(t *testing.T) {
	const bs = 16
	toks := tokSeq(55, bs)
	// The evict key is our content hash for the stored block (the engine hash 0x55
	// is mapped to it via the reverse map on removal).
	our := string(fingerprint.Bytes(fingerprint.PrefixHashes(toks, bs)[0]))
	// Large window so only the BlockRemoved-triggered flush (not the ticker) can
	// deliver the buffered add — exercising the ordering guarantee.
	rec := runReporterWindow(t, time.Hour,
		&EventBatch{TimestampSeconds: 1, Events: []Event{BlockStored{BlockHashes: [][]byte{{0x55}}, TokenIDs: toks, BlockSize: bs}}},
		&EventBatch{TimestampSeconds: 2, Events: []Event{BlockRemoved{BlockHashes: [][]byte{{0x55}}}}},
	)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var addAt, evictAt = -1, -1
	for i, op := range rec.ops {
		switch op {
		case "add:" + our:
			addAt = i
		case "evict:" + our:
			evictAt = i
		}
	}
	if addAt < 0 || evictAt < 0 {
		t.Fatalf("missing ops, got %v", rec.ops)
	}
	if addAt > evictAt {
		t.Errorf("add (%d) must precede evict (%d); ops=%v", addAt, evictAt, rec.ops)
	}
}

// A zero/negative window must not panic time.NewTicker.
func TestNewReporterClampsWindow(t *testing.T) {
	r := NewReporter(nil, testConfig(), WithWindow(0))
	if r.window <= 0 {
		t.Errorf("window not clamped: %v", r.window)
	}
}

func TestReporterForwardsRemovalsAndClear(t *testing.T) {
	const bs = 16
	// Store the two blocks first so removal can map their engine hashes (7, 8) to
	// our prefix hashes via the reverse map.
	toks := tokSeq(700, 2*bs)
	rec := runReporter(t,
		&EventBatch{TimestampSeconds: 1, Events: []Event{BlockStored{BlockHashes: [][]byte{{7}, {8}}, TokenIDs: toks, BlockSize: bs}}},
		&EventBatch{TimestampSeconds: 2, Events: []Event{BlockRemoved{BlockHashes: [][]byte{{7}, {8}}}}},
		&EventBatch{TimestampSeconds: 3, Events: []Event{AllBlocksCleared{}}},
	)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	var evicted, cleared int
	for _, e := range rec.events {
		switch e.GetType() {
		case icpb.CacheEvent_PREFIX_EVICTED:
			evicted++
		case icpb.CacheEvent_ALL_CLEARED:
			cleared++
		}
	}
	if evicted != 2 {
		t.Errorf("PREFIX_EVICTED count = %d, want 2", evicted)
	}
	if cleared != 1 {
		t.Errorf("ALL_CLEARED count = %d, want 1", cleared)
	}
}

// With WithIgnoreBlockRemoved the reporter must NOT forward BlockRemoved
// events as PREFIX_EVICTED. This is the L2-tier mode (e.g. LMCache): when
// the engine evicts a block from GPU the block is still resident at L2 and
// the cache plane should keep the routing hint until its freshness TTL
// expires. Forwarding the eviction would erase the hint while the replica
// can still serve the block from L2 — that is the cache-stress
// 0-PREFIX_MATCH regression. We also assert that a BlockStored add still
// reaches the server (via the shutdown flush) — silently dropping adds
// alongside evictions would defeat the whole reporter.
func TestReporterIgnoreBlockRemovedDropsEvictionsButForwardsAdds(t *testing.T) {
	const bs = 16
	toks := tokSeq(1, bs)
	wantAdd := fingerprint.Bytes(fingerprint.PrefixHashes(toks, bs)[0])
	rec := runReporterWithOpts(t,
		// time.Hour so the only path that can deliver the add is the shutdown
		// flush — exactly as in TestReporterFlushesPendingOnShutdown. That
		// also confirms BlockRemoved doesn't accidentally trigger the
		// pre-removal flush when ignore is set.
		[]ReporterOption{WithWindow(time.Hour), WithIgnoreBlockRemoved(true)},
		&EventBatch{TimestampSeconds: 1, Events: []Event{BlockStored{BlockHashes: [][]byte{{0x01}}, TokenIDs: toks, BlockSize: bs}}},
		&EventBatch{TimestampSeconds: 2, Events: []Event{BlockRemoved{BlockHashes: [][]byte{{0x02}}}}},
	)

	rec.mu.Lock()
	defer rec.mu.Unlock()

	// Adds still flow (via the shutdown flush).
	var addFound bool
	for _, u := range rec.updates {
		for _, p := range u.GetPrefixes() {
			if bytes.Equal(p.GetPrefixHash(), wantAdd) {
				addFound = true
			}
		}
	}
	if !addFound {
		t.Errorf("BlockStored add was not forwarded with ignore_block_removed=true; ops=%v", rec.ops)
	}

	// Evictions are dropped — no PREFIX_EVICTED reaches the server.
	for _, e := range rec.events {
		if e.GetType() == icpb.CacheEvent_PREFIX_EVICTED {
			t.Errorf("ignore_block_removed=true must drop PREFIX_EVICTED, got eviction for hash=%x", e.GetPrefixHash())
		}
	}
}

// In L2 mode (ignore_block_removed), a BlockRemoved must still prune the
// subscriber's reverse map even though the eviction is NOT forwarded — otherwise
// the map grows unbounded until AllBlocksCleared (the L2 memory-leak path: the
// engine evicts from GPU on every store, but the block stays at L2 so we keep the
// server hint, yet we must still forget the engine-hash → our-hash mapping).
func TestReporterIgnoreBlockRemovedStillPrunesReverseMap(t *testing.T) {
	const bs = 16
	toks := tokSeq(70, bs)
	rec := runReporterWithOpts(t,
		[]ReporterOption{WithWindow(time.Hour), WithIgnoreBlockRemoved(true)},
		&EventBatch{TimestampSeconds: 0, Events: []Event{BlockStored{BlockHashes: [][]byte{{0x70}}, TokenIDs: toks, BlockSize: bs}}},
		&EventBatch{TimestampSeconds: 0, Events: []Event{BlockRemoved{BlockHashes: [][]byte{{0x70}}}}},
	)
	// Run has returned (input drained), so the single-goroutine index is quiescent.
	if n := len(rec.reporter.pos.blocks); n != 0 {
		t.Errorf("reverse map has %d entries after store+remove in L2 mode, want 0 (unbounded-growth leak)", n)
	}
}

// AllBlocksCleared must still flow even with ignore_block_removed=true — it
// is an engine-wide reset, not a per-block GPU eviction, and an L2 tier
// cannot mask a clear-all (the engine forgot the prefixes entirely). Pinning
// this separately keeps the L2 behavior and the engine-wide reset behavior
// independently visible in the test signal.
func TestReporterIgnoreBlockRemovedStillForwardsAllCleared(t *testing.T) {
	rec := runReporterWithOpts(t,
		[]ReporterOption{WithIgnoreBlockRemoved(true)},
		&EventBatch{TimestampSeconds: 1, Events: []Event{AllBlocksCleared{}}},
	)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var cleared int
	for _, e := range rec.events {
		if e.GetType() == icpb.CacheEvent_ALL_CLEARED {
			cleared++
		}
	}
	if cleared != 1 {
		t.Errorf("ALL_CLEARED forwarded count = %d, want 1 (engine-wide resets must still flow); events=%v", cleared, rec.events)
	}
}

// Default (ignore_block_removed=false) MUST preserve the existing eviction
// contract — single-tier deployments rely on PREFIX_EVICTED to drop stale
// hints promptly. Guards against accidental flag-default flips.
func TestReporterDefaultStillForwardsBlockRemoved(t *testing.T) {
	const bs = 16
	toks := tokSeq(33, bs)
	want := fingerprint.Bytes(fingerprint.PrefixHashes(toks, bs)[0])
	// Store the block first so its engine hash (0x33) maps to our prefix hash.
	rec := runReporter(t,
		&EventBatch{TimestampSeconds: 1, Events: []Event{BlockStored{BlockHashes: [][]byte{{0x33}}, TokenIDs: toks, BlockSize: bs}}},
		&EventBatch{TimestampSeconds: 2, Events: []Event{BlockRemoved{BlockHashes: [][]byte{{0x33}}}}},
	)
	rec.mu.Lock()
	defer rec.mu.Unlock()
	var found bool
	for _, e := range rec.events {
		if e.GetType() == icpb.CacheEvent_PREFIX_EVICTED && bytes.Equal(e.GetPrefixHash(), want) {
			found = true
		}
	}
	if !found {
		t.Errorf("default Reporter must forward PREFIX_EVICTED; events=%v", rec.events)
	}
}

// A clear in the same batch supersedes buffered adds — nothing should be reported
// as added.
func TestReporterClearSupersedesBufferedAdds(t *testing.T) {
	rec := runReporter(t, &EventBatch{
		TimestampSeconds: 0,
		Events: []Event{
			// Real tokens so the store actually buffers prefix entries; the in-batch
			// clear must then discard them — otherwise this test is vacuous.
			BlockStored{BlockHashes: [][]byte{{1}, {2}}, TokenIDs: tokSeq(0, 2*16), BlockSize: 16},
			AllBlocksCleared{},
		},
	})
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, u := range rec.updates {
		if len(u.GetPrefixes()) != 0 {
			t.Errorf("expected no prefixes after clear, got %d", len(u.GetPrefixes()))
		}
	}
}

// Command kvevent-fake-engine is a TEST-ONLY stand-in for a vLLM engine's KV-event
// ZMQ publisher, used by the fingerprint-routing e2e and smokes. It binds a ZMQ PUB
// socket and repeatedly publishes a deterministic sequence of synthetic BlockStored
// events (a fixed token sequence chunked into blocks) so a kvevent-subscriber
// ingests them into the inference-cache index. It also logs the content fingerprint
// the subscriber is expected to derive (EXPECT prefix_hash[...]), so a smoke can
// assert that LookupRoute returns PREFIX_MATCH for that exact key.
//
// It is built only for tests and smoke images and is never shipped in a production
// bundle. PUB/SUB has a slow-joiner race (a subscriber that connects after a
// message is sent misses it), so the sequence is republished on an interval —
// ingest is idempotent server-side, so replaying is safe.
package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/cachebox-project/inference-cache/pkg/fingerprint"
)

func main() {
	var (
		bind       = flag.String("bind", "tcp://0.0.0.0:5557", "ZMQ PUB bind endpoint")
		topic      = flag.String("topic", "kv-events", "ZMQ topic")
		blockSize  = flag.Int("block-size", 128, "tokens per block")
		numBlocks  = flag.Int("num-blocks", 1, "number of full blocks to publish")
		numEvents  = flag.Int("events", 1, "number of BlockStored events the blocks are split across; each event after the first chains to the previous one via parent_block_hash")
		startTok   = flag.Int("start-token", 0, "first token id (sequence is start..start+n-1)")
		interval   = flag.Duration("interval", 500*time.Millisecond, "republish interval (covers the PUB/SUB slow-joiner race)")
		omitTokens = flag.Bool("omit-token-ids", false, "publish BlockStored without token_ids — reproduces an engine that stops emitting them; the subscriber must warn and index nothing")
	)
	flag.Parse()

	// Validate before allocating: a negative block-size/num-blocks would panic
	// in tokenSeq below, an oversized product would overflow it, a negative
	// start token would wrap to a huge uint32 ID, and a non-positive interval
	// would spin the publish loop hot instead of pacing it.
	const maxTotalTokens = 1 << 24 // sanity bound for a test publisher
	if *blockSize <= 0 || *numBlocks <= 0 {
		log.Fatalf("-block-size and -num-blocks must be positive, got %d and %d", *blockSize, *numBlocks)
	}
	if *numBlocks > maxTotalTokens / *blockSize {
		log.Fatalf("-block-size × -num-blocks must not exceed %d tokens, got %d × %d", maxTotalTokens, *blockSize, *numBlocks)
	}
	if *startTok < 0 {
		log.Fatalf("-start-token must be non-negative, got %d", *startTok)
	}
	if *interval <= 0 {
		log.Fatalf("-interval must be positive, got %s", *interval)
	}

	tokens := tokenSeq(*startTok, *blockSize**numBlocks)
	payloads, err := buildBatchPayloads(tokens, *blockSize, *numEvents, *omitTokens)
	if err != nil {
		log.Fatalf("build payloads: %v", err)
	}

	// Log what the subscriber should derive — smokes grep these EXPECT lines.
	if *omitTokens {
		log.Printf("EXPECT no index entries (token_ids omitted; the subscriber must log a warning and index nothing)")
	} else {
		for i, h := range fingerprint.PrefixHashes(tokens, *blockSize) {
			log.Printf("EXPECT prefix_hash[%d]=%s token_count=%d",
				i, hex.EncodeToString(fingerprint.Bytes(h)), (i+1)*(*blockSize))
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pub := zmq4.NewPub(ctx)
	if err := pub.Listen(*bind); err != nil {
		log.Fatalf("zmq listen %s: %v", *bind, err)
	}
	defer func() { _ = pub.Close() }()
	log.Printf("publishing %d BlockStored event(s) on %s topic=%q (%d block(s), %d tokens) every %s",
		len(payloads), *bind, *topic, *numBlocks, len(tokens), *interval)

	publishLoop(ctx, pub, *topic, payloads, *interval)
}

// publishLoop re-sends the payload sequence in order until ctx is cancelled.
// Each ZMQ message is [topic, seq(u64 BE), payload] — the frame shape vLLM's
// EventPublisher emits. The whole sequence is replayed every interval so a
// late-joining subscriber still sees every event.
func publishLoop(ctx context.Context, pub zmq4.Socket, topic string, payloads [][]byte, interval time.Duration) {
	var seq uint64
	for {
		for _, p := range payloads {
			sb := make([]byte, 8)
			binary.BigEndian.PutUint64(sb, seq)
			seq++
			if err := pub.Send(zmq4.NewMsgFrom([]byte(topic), sb, p)); err != nil {
				log.Printf("send err: %v", err)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// tokenSeq returns [start, start+1, ..., start+n-1] as token IDs — a stable,
// content-distinct block sequence for the subscriber to fingerprint.
func tokenSeq(start, n int) []uint32 {
	out := make([]uint32, n)
	for i := range out {
		out[i] = uint32(start + i)
	}
	return out
}

// fakeBlockHash is the deterministic engine-side hash for global block index i.
// The value is arbitrary — the index keys on the content fingerprint, not on
// these — but it must be stable across events so a later event can name an
// earlier block as its parent.
func fakeBlockHash(i int) int64 { return 0xE000 + int64(i) }

// buildBatchPayloads encodes the token sequence as a deterministic sequence of
// vLLM-shaped EventBatch payloads (one BlockStored per batch), in the msgpack
// array-tagged form the kvevent-subscriber decodes:
//
//	[ts, [["BlockStored", block_hashes, parent_block_hash, token_ids, block_size, lora_id]]]
//
// The full blocks are split across nEvents batches; every batch after the first
// carries parent_block_hash = the previous batch's last block hash — the shape
// vLLM uses when an already-cached prefix is extended, which is what exercises
// the subscriber's cross-event prefix-hash chaining. ts is 0 (the subscriber
// maps 0 to "now" server-side, so the entry is always fresh); lora_id is nil.
// With omitTokens the token_ids field is nil — the shape of an engine that
// stops emitting token_ids, which the subscriber must reject (warn + index
// nothing) rather than ingest.
func buildBatchPayloads(tokens []uint32, blockSize, nEvents int, omitTokens bool) ([][]byte, error) {
	if blockSize <= 0 {
		return nil, fmt.Errorf("block size must be positive, got %d", blockSize)
	}
	nBlocks := len(tokens) / blockSize
	if nBlocks == 0 {
		return nil, fmt.Errorf("no full block: %d tokens at block size %d", len(tokens), blockSize)
	}
	if nEvents < 1 || nEvents > nBlocks {
		return nil, fmt.Errorf("events must be in [1, %d] (the full-block count), got %d", nBlocks, nEvents)
	}

	payloads := make([][]byte, 0, nEvents)
	base, rem := nBlocks/nEvents, nBlocks%nEvents
	lo := 0
	for e := 0; e < nEvents; e++ {
		n := base
		if e < rem {
			n++
		}
		hi := lo + n

		hashes := make([]interface{}, 0, n)
		for b := lo; b < hi; b++ {
			hashes = append(hashes, fakeBlockHash(b))
		}
		var parent interface{}
		if lo > 0 {
			parent = fakeBlockHash(lo - 1)
		}
		var tokenIDs interface{}
		if !omitTokens {
			ids := make([]interface{}, 0, n*blockSize)
			for _, t := range tokens[lo*blockSize : hi*blockSize] {
				ids = append(ids, int64(t))
			}
			tokenIDs = ids
		}

		event := []interface{}{"BlockStored", hashes, parent, tokenIDs, int32(blockSize), nil}
		payload, err := msgpack.Marshal([]interface{}{float64(0), []interface{}{event}})
		if err != nil {
			return nil, fmt.Errorf("marshal event %d (blocks %d-%d): %w", e, lo, hi-1, err)
		}
		payloads = append(payloads, payload)
		lo = hi
	}
	return payloads, nil
}

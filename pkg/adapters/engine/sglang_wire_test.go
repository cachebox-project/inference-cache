package engine

import (
	"bytes"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/cachebox-project/inference-cache/pkg/fingerprint"
)

// SGLang adopted vLLM's KV-event wire wholesale: --kv-events-config drives a
// ZmqEventPublisher that emits msgspec array-like tagged structs identical to
// vLLM's, so the shipped subscriber decodes SGLang's stream unchanged. These
// tests pin that contract from the SGLang side so a future divergence in
// either engine's wire is caught here rather than silently producing zero
// index entries (all NO_HINT) in production.
//
// The two wire details that distinguish SGLang's encoder from a naive vLLM
// fixture, and that the decoder must tolerate, are exercised explicitly:
//   - the EventBatch is a 3-tuple [ts, events, attn_dp_rank] (SGLang annotates
//     every batch with the data-parallel rank), and
//   - BlockStored is the 6-field tuple [tag, block_hashes, parent_block_hash,
//     token_ids, block_size, lora_id] — the decoder reads through block_size
//     and ignores the trailing lora_id.

// encodeSGLangBatch builds a msgpack payload in SGLang's exact wire shape:
// [ts, [ [tag, ...fields], ... ], attn_dp_rank]. Mirrors SGLang's
// disaggregation/kv_events.py EventBatch (array_like, with the trailing
// attn_dp_rank field its ZmqEventPublisher always sets).
func encodeSGLangBatch(t *testing.T, ts float64, attnDPRank int, events ...[]interface{}) []byte {
	t.Helper()
	evs := make([]interface{}, len(events))
	for i, e := range events {
		evs[i] = e
	}
	b, err := msgpack.Marshal([]interface{}{ts, evs, attnDPRank})
	if err != nil {
		t.Fatalf("encode sglang fixture: %v", err)
	}
	return b
}

func TestDecodeSGLangEventBatch(t *testing.T) {
	// SGLang's BlockStored 6-tuple + a trailing attn_dp_rank on the batch.
	payload := encodeSGLangBatch(t, 1779901681.5, 3,
		[]interface{}{"BlockStored", []uint64{10, 11}, nil, []int64{0, 1, 2, 3}, int32(64), nil},
		[]interface{}{"BlockRemoved", []uint64{4}},
		[]interface{}{"AllBlocksCleared"},
	)

	batch, err := DecodeEventBatch(payload)
	if err != nil {
		t.Fatalf("DecodeEventBatch on SGLang wire: %v", err)
	}
	if batch.TimestampSeconds != 1779901681.5 {
		t.Errorf("ts = %v, want 1779901681.5 (trailing attn_dp_rank must not shift field parsing)", batch.TimestampSeconds)
	}
	if len(batch.Events) != 3 {
		t.Fatalf("got %d events, want 3", len(batch.Events))
	}
	stored, ok := batch.Events[0].(BlockStored)
	if !ok {
		t.Fatalf("event[0] = %T, want BlockStored", batch.Events[0])
	}
	if stored.BlockSize != 64 { // SGLang's default page_size with hierarchical cache
		t.Errorf("BlockSize = %d, want 64", stored.BlockSize)
	}
	if len(stored.TokenIDs) != 4 || stored.TokenIDs[0] != 0 || stored.TokenIDs[3] != 3 {
		t.Errorf("TokenIDs = %v, want [0 1 2 3] preserved (the in-pod content fingerprint needs them)", stored.TokenIDs)
	}
	if _, ok := batch.Events[1].(BlockRemoved); !ok {
		t.Errorf("event[1] = %T, want BlockRemoved", batch.Events[1])
	}
	if _, ok := batch.Events[2].(AllBlocksCleared); !ok {
		t.Errorf("event[2] = %T, want AllBlocksCleared", batch.Events[2])
	}
}

func TestReporterTagsSGLangScheme(t *testing.T) {
	// End-to-end through the Reporter with a SGLang identity: a decoded
	// BlockStored must produce a ReportCacheState CacheStateUpdate tagged
	// hash_scheme="sglang" — the load-bearing tag the index keys on so SGLang
	// prefixes never collide with vLLM's. This is the event-source half of the
	// second-engine goal: the SGLang KV-event source emits ReportCacheState
	// updates tagged hash_scheme: sglang.
	const bs = 64
	toks := tokSeq(200, 2*bs) // two full SGLang pages
	wantHashes := fingerprint.PrefixHashes(toks, bs)

	cfg := Config{ReplicaID: "sglang-0", ModelID: "llama", TenantID: "tenant-a", HashScheme: "sglang"}
	rec := runReporterCfg(t, cfg, []ReporterOption{WithWindow(20 * time.Millisecond)},
		&EventBatch{
			TimestampSeconds: 2.0,
			Events: []Event{BlockStored{
				BlockHashes: [][]byte{{0xaa}, {0xbb}},
				TokenIDs:    toks,
				BlockSize:   bs,
			}},
		})

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.updates) == 0 {
		t.Fatalf("no CacheStateUpdate recorded; SGLang BlockStored produced no index entries")
	}
	var gotHashes [][]byte
	for _, u := range rec.updates {
		if u.GetHashScheme() != "sglang" {
			t.Errorf("update hash_scheme = %q, want sglang", u.GetHashScheme())
		}
		if u.GetReplicaId() != "sglang-0" {
			t.Errorf("update replica_id = %q, want sglang-0", u.GetReplicaId())
		}
		for _, p := range u.GetPrefixes() {
			gotHashes = append(gotHashes, p.GetPrefixHash())
		}
	}
	// The reported prefix hashes are the in-pod content fingerprint of the
	// tokens (same scheme-independent algorithm vLLM uses) — proving the
	// subscriber derives + forwards routing keys from SGLang's token_ids.
	// Assert the BYTES, not just the count: a regression that emitted the
	// right number of wrong hashes would otherwise pass.
	if len(gotHashes) != len(wantHashes) {
		t.Fatalf("forwarded %d prefix hashes, want %d", len(gotHashes), len(wantHashes))
	}
	for i := range wantHashes {
		if !bytes.Equal(gotHashes[i], fingerprint.Bytes(wantHashes[i])) {
			t.Errorf("forwarded hash[%d] = %x, want content fingerprint %x", i, gotHashes[i], fingerprint.Bytes(wantHashes[i]))
		}
	}
}

package engine

import (
	"testing"

	"github.com/cachebox-project/inference-cache/pkg/fingerprint"
)

// vLLM's BlockStored tuple has always carried lora_id as its trailing field
// ([tag, block_hashes, parent_block_hash, token_ids, block_size, lora_id]); the
// decoder used to read through block_size and drop it. It is now decoded,
// because the content fingerprint is token-only: identical tokens under two
// adapters hash identically, so the id is what tells the index which partition
// the blocks belong in.

func TestDecodeBlockStoredCarriesLoRAID(t *testing.T) {
	payload := encodeVLLMBatch(t, 1.0,
		[]interface{}{"BlockStored", []uint64{10}, nil, []int64{1, 2}, int32(2), int64(7)},
	)
	batch, err := DecodeEventBatch(payload)
	if err != nil {
		t.Fatalf("DecodeEventBatch: %v", err)
	}
	bs, ok := batch.Events[0].(BlockStored)
	if !ok {
		t.Fatalf("event 0 = %T, want BlockStored", batch.Events[0])
	}
	if bs.LoRAID == nil {
		t.Fatal("LoRAID is nil, want 7 — the adapter id is on the wire and must not be dropped")
	}
	if *bs.LoRAID != 7 {
		t.Errorf("LoRAID = %d, want 7", *bs.LoRAID)
	}
}

// A nil lora_id (base-model request) and a truncated 5-field tuple both mean
// "no adapter" — neither is an error, and both land in the default partition.
func TestDecodeBlockStoredWithoutLoRAID(t *testing.T) {
	for name, ev := range map[string][]interface{}{
		"explicit nil":       {"BlockStored", []uint64{10}, nil, []int64{1, 2}, int32(2), nil},
		"field absent (5/6)": {"BlockStored", []uint64{10}, nil, []int64{1, 2}, int32(2)},
	} {
		t.Run(name, func(t *testing.T) {
			batch, err := DecodeEventBatch(encodeVLLMBatch(t, 1.0, ev))
			if err != nil {
				t.Fatalf("DecodeEventBatch: %v", err)
			}
			if bs := batch.Events[0].(BlockStored); bs.LoRAID != nil {
				t.Errorf("LoRAID = %d, want nil", *bs.LoRAID)
			}
		})
	}
}

// A present-but-unrecognized lora_id must fail the batch rather than decode to
// "no adapter": mislabelling an adapter's blocks as base-model puts them in the
// default partition, where they alias against every other adapter's identical
// token content — the exact bug the partition exists to prevent. Losing the
// batch costs a routing hint (soft state); aliasing returns a wrong hint.
func TestDecodeBlockStoredRejectsUndecodableLoRAID(t *testing.T) {
	payload := encodeVLLMBatch(t, 1.0,
		[]interface{}{"BlockStored", []uint64{10}, nil, []int64{1, 2}, int32(2), []int64{1, 2}},
	)
	if _, err := DecodeEventBatch(payload); err == nil {
		t.Fatal("DecodeEventBatch accepted a non-integer lora_id; want an error so the batch is dropped, not silently un-partitioned")
	}
}

func TestConfigAdapterID(t *testing.T) {
	cfg := testConfig()
	cfg.AdapterNames = map[int64]string{1: "sql-lora"}

	if got, ok := cfg.AdapterID(nil); got != "" || !ok {
		t.Errorf("AdapterID(nil) = (%q, %v), want (\"\", true) — base model → default partition", got, ok)
	}
	one, two := int64(1), int64(2)
	if got, ok := cfg.AdapterID(&one); got != "sql-lora" || !ok {
		t.Errorf("AdapterID(1) = (%q, %v), want (sql-lora, true)", got, ok)
	}
	// An unmapped non-nil id is fail-closed: ok=false so the caller drops the
	// event rather than indexing it under an alias-prone "lora:<id>".
	if got, ok := cfg.AdapterID(&two); got != "" || ok {
		t.Errorf("AdapterID(2) = (%q, %v), want (\"\", false) — unmapped → fail closed", got, ok)
	}
	if got, ok := (Config{}).AdapterID(&one); got != "" || ok {
		t.Errorf("AdapterID with no map = (%q, %v), want (\"\", false) — unmapped → fail closed", got, ok)
	}
}

func TestParseAdapterNames(t *testing.T) {
	got, err := ParseAdapterNames(" 1=sql-lora, 2 = chat-lora ,")
	if err != nil {
		t.Fatalf("ParseAdapterNames: %v", err)
	}
	if len(got) != 2 || got[1] != "sql-lora" || got[2] != "chat-lora" {
		t.Errorf("parsed = %v, want {1: sql-lora, 2: chat-lora}", got)
	}
	if empty, err := ParseAdapterNames(""); err != nil || empty != nil {
		t.Errorf("ParseAdapterNames(\"\") = %v, %v; want nil, nil", empty, err)
	}
	for _, bad := range []string{"sql-lora", "x=sql-lora", "1=", "1=a,1=b"} {
		if _, err := ParseAdapterNames(bad); err == nil {
			t.Errorf("ParseAdapterNames(%q) accepted a malformed mapping; a dropped entry would silently mis-partition that adapter", bad)
		}
	}
}

// Stored stamps the resolved adapter on every emitted entry and remembers it per
// block, so a later eviction can name the right partition.
func TestStoredStampsAdapterAndRemovedReportsIt(t *testing.T) {
	const bs = 16
	toks := tokSeq(1, bs)
	p := newPositionalIndex()

	entries := p.Stored(BlockStored{BlockHashes: [][]byte{engHash(5)}, TokenIDs: toks, BlockSize: bs}, "sql-lora")
	if len(entries) != 1 || entries[0].GetAdapterId() != "sql-lora" {
		t.Fatalf("entries = %+v, want one entry stamped sql-lora", entries)
	}
	// The fingerprint itself must be untouched by the adapter — adapter identity
	// lives in the partition, never in the hash.
	if beU64(entries[0].PrefixHash) != fingerprint.ContentHash(toks) {
		t.Error("prefix hash changed under an adapter; the fingerprint must stay a pure function of token content")
	}

	rm := p.Removed(BlockRemoved{BlockHashes: [][]byte{engHash(5)}})
	if len(rm) != 1 || rm[0].AdapterID != "sql-lora" {
		t.Fatalf("Removed = %+v, want the sql-lora partition named", rm)
	}
}

// Two adapters caching the SAME tokens produce the same prefix hash but must be
// reported under different partitions — this is the aliasing case, checked at
// the subscriber boundary.
func TestStoredSameTokensDifferentAdaptersShareHashNotPartition(t *testing.T) {
	const bs = 16
	toks := tokSeq(1, bs)

	sql := newPositionalIndex().Stored(BlockStored{BlockHashes: [][]byte{engHash(5)}, TokenIDs: toks, BlockSize: bs}, "sql-lora")
	chat := newPositionalIndex().Stored(BlockStored{BlockHashes: [][]byte{engHash(6)}, TokenIDs: toks, BlockSize: bs}, "chat-lora")

	if beU64(sql[0].PrefixHash) != beU64(chat[0].PrefixHash) {
		t.Fatal("precondition failed: the token-only fingerprint should be identical for both adapters")
	}
	if sql[0].GetAdapterId() == chat[0].GetAdapterId() {
		t.Errorf("both entries reported adapter %q — identical hashes with an identical partition is the alias", sql[0].GetAdapterId())
	}
}

// End-to-end through the Reporter: one batch carrying a mapped adapter's blocks,
// an UNMAPPED adapter's blocks, and base-model blocks. The mapped adapter and the
// base model are forwarded stamped with their resolved identity; the unmapped
// adapter is fail-closed (dropped, never indexed under a hazardous "lora:<id>").
// The eviction that follows names the partition it came from.
func TestReporterPartitionsEntriesByLoRAID(t *testing.T) {
	const bs = 16
	one, two := int64(1), int64(2)
	toks := tokSeq(100, bs)

	cfg := testConfig()
	cfg.AdapterNames = map[int64]string{1: "sql-lora"}

	rec := runReporterCfg(t, cfg, nil,
		&EventBatch{TimestampSeconds: 2.0, Events: []Event{
			BlockStored{BlockHashes: [][]byte{{0x0a}}, TokenIDs: toks, BlockSize: bs, LoRAID: &one},
			BlockStored{BlockHashes: [][]byte{{0x0b}}, TokenIDs: toks, BlockSize: bs, LoRAID: &two},
			BlockStored{BlockHashes: [][]byte{{0x0c}}, TokenIDs: toks, BlockSize: bs}, // base model
		}},
		&EventBatch{TimestampSeconds: 3.0, Events: []Event{
			BlockRemoved{BlockHashes: [][]byte{{0x0a}}},
		}},
	)

	rec.mu.Lock()
	defer rec.mu.Unlock()

	var adapters []string
	for _, u := range rec.updates {
		if u.GetAdapterId() != "" {
			t.Errorf("update-level adapter_id = %q, want empty — a multi-adapter batch must stamp entries, not the update", u.GetAdapterId())
		}
		for _, p := range u.GetPrefixes() {
			adapters = append(adapters, p.GetAdapterId())
		}
	}
	// lora_id=2 is unmapped → fail-closed → dropped, so it must NOT appear (only
	// the mapped sql-lora and the base "" partition are forwarded).
	want := map[string]bool{"sql-lora": false, "": false}
	for _, a := range adapters {
		if _, known := want[a]; !known {
			t.Errorf("unexpected adapter_id %q on a forwarded entry (unmapped lora_id=2 must be dropped, not partitioned)", a)
			continue
		}
		want[a] = true
	}
	for a, seen := range want {
		if !seen {
			t.Errorf("no entry forwarded for adapter %q; got %v", a, adapters)
		}
	}

	var evictions int
	for _, ev := range rec.events {
		if ev.GetPrefixHash() == nil {
			continue
		}
		evictions++
		if ev.GetAdapterId() != "sql-lora" {
			t.Errorf("eviction adapter_id = %q, want sql-lora — evicting block 0x0a must not drop the other adapters' identical hash", ev.GetAdapterId())
		}
	}
	if evictions != 1 {
		t.Errorf("got %d evictions, want 1", evictions)
	}
}

// An unmapped non-nil lora_id is fail-closed: with no --lora-adapter-names entry
// its blocks are dropped rather than indexed under a replica-local "lora:<id>"
// that could alias adapters across replicas. Nothing is forwarded — not the add,
// and not an eviction for a block that was never indexed.
func TestReporterFailsClosedOnUnmappedAdapter(t *testing.T) {
	const bs = 16
	seven := int64(7)
	cfg := testConfig() // no AdapterNames → every non-nil lora_id is unmapped

	rec := runReporterCfg(t, cfg, nil,
		&EventBatch{TimestampSeconds: 2.0, Events: []Event{
			BlockStored{BlockHashes: [][]byte{{0x0a}}, TokenIDs: tokSeq(1, bs), BlockSize: bs, LoRAID: &seven},
		}},
		&EventBatch{TimestampSeconds: 3.0, Events: []Event{
			BlockRemoved{BlockHashes: [][]byte{{0x0a}}},
		}},
	)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, u := range rec.updates {
		if n := len(u.GetPrefixes()); n != 0 {
			t.Errorf("forwarded %d prefixes for an unmapped adapter; want 0 (fail-closed)", n)
		}
	}
	for _, ev := range rec.events {
		if ev.GetPrefixHash() != nil {
			t.Errorf("forwarded a PREFIX_EVICTED (%x) for an unmapped adapter; want none — its block was never indexed", ev.GetPrefixHash())
		}
	}
}

// A deployment with no LoRA at all is byte-for-byte what it was before: every
// event has a nil lora_id, so every entry and eviction stays in the default
// partition.
func TestReporterWithoutLoRAKeepsDefaultPartition(t *testing.T) {
	const bs = 16
	rec := runReporter(t,
		&EventBatch{TimestampSeconds: 2.0, Events: []Event{
			BlockStored{BlockHashes: [][]byte{{0x0a}}, TokenIDs: tokSeq(1, bs), BlockSize: bs},
		}},
		&EventBatch{TimestampSeconds: 3.0, Events: []Event{
			BlockRemoved{BlockHashes: [][]byte{{0x0a}}},
		}},
	)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	for _, u := range rec.updates {
		for _, p := range u.GetPrefixes() {
			if p.GetAdapterId() != "" {
				t.Errorf("adapter_id = %q on a non-LoRA deployment, want empty", p.GetAdapterId())
			}
		}
	}
	for _, ev := range rec.events {
		if ev.GetAdapterId() != "" {
			t.Errorf("eviction adapter_id = %q on a non-LoRA deployment, want empty", ev.GetAdapterId())
		}
	}
}

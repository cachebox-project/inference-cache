package index

import "testing"

// The content fingerprint is derived from token IDs ALONE, so two prompts with
// identical tokens under different LoRA adapters produce the SAME prefix hash.
// The index partition — (tenant, model, hash_scheme, adapter) — is what keeps
// them from aliasing onto one entry and handing a caller a replica that only
// holds the OTHER adapter's KV.
//
// The tests below pin the four properties that define the partition:
//
//	1. different adapters, same tokens  -> no alias (the bug being fixed)
//	2. same adapter, same tokens        -> still hits (the partition is not a hash)
//	3. absent adapter                   -> byte-identical to pre-adapter behavior
//	4. per-entry adapter beats the update-level default (multi-adapter producers)

const (
	adapterTenant = "t"
	adapterModel  = "m"
	adapterScheme = "vllm"
)

// ingestUnder ingests one legacy single-blob prefix for a replica in an adapter
// partition. The prefix hash is deliberately shared across calls: identical
// tokens under different adapters is exactly the colliding input.
func ingestUnder(idx *Index, replica, adapter, prefix string) {
	idx.Ingest(Update{
		ReplicaID:  replica,
		Model:      adapterModel,
		Tenant:     adapterTenant,
		HashScheme: adapterScheme,
		Prefixes: []PrefixRef{{
			PrefixHash: hash(prefix),
			TokenCount: 128,
			Adapter:    adapter,
		}},
	})
}

func lookupUnder(idx *Index, adapter, prefix string) []ReplicaScore {
	return idx.Lookup(LookupRequest{
		Tenant:     adapterTenant,
		Model:      adapterModel,
		HashScheme: adapterScheme,
		Adapter:    adapter,
		PrefixHash: hash(prefix),
	})
}

// Property 1 — the aliasing bug. Two replicas cache the same token content under
// different adapters. A lookup for one adapter must never surface the replica
// holding the other's KV, even though both entries carry the identical
// (token-derived) prefix hash.
func TestAdapterPartitionDoesNotAliasAcrossAdapters(t *testing.T) {
	idx := New()
	ingestUnder(idx, "replica-sql", "sql-lora", "same-tokens")
	ingestUnder(idx, "replica-chat", "chat-lora", "same-tokens")

	sql := lookupUnder(idx, "sql-lora", "same-tokens")
	if len(sql) != 1 || sql[0].ReplicaID != "replica-sql" {
		t.Fatalf("sql-lora lookup = %+v, want exactly replica-sql — a hint for the chat-lora replica would route to a replica holding a DIFFERENT adapter's KV", sql)
	}
	chat := lookupUnder(idx, "chat-lora", "same-tokens")
	if len(chat) != 1 || chat[0].ReplicaID != "replica-chat" {
		t.Fatalf("chat-lora lookup = %+v, want exactly replica-chat", chat)
	}
}

// Property 2 — the partition is a partition, not a hash change: within one
// adapter the identical prefix hash still matches, and every replica holding it
// is still returned.
func TestAdapterPartitionSameAdapterStillHits(t *testing.T) {
	idx := New()
	ingestUnder(idx, "replica-a", "sql-lora", "same-tokens")
	ingestUnder(idx, "replica-b", "sql-lora", "same-tokens")

	got := lookupUnder(idx, "sql-lora", "same-tokens")
	if len(got) != 2 {
		t.Fatalf("got %d scores, want both replicas in the sql-lora partition: %+v", len(got), got)
	}
	seen := map[string]bool{}
	for _, sc := range got {
		seen[sc.ReplicaID] = true
		if sc.MatchedTokens != 128 {
			t.Errorf("%s matched_tokens = %d, want 128 — partitioning must not change matching", sc.ReplicaID, sc.MatchedTokens)
		}
	}
	if !seen["replica-a"] || !seen["replica-b"] {
		t.Errorf("scores = %+v, want replica-a and replica-b", got)
	}
}

// Property 3 — backwards compatibility. An older producer and an older gateway
// both omit the adapter, land in the default ("") partition, and behave exactly
// as before. A partition-aware lookup must NOT reach into that default
// partition, and vice versa: producer and consumer have to agree on the
// identifier, the same rule hash_scheme already imposes.
func TestAdapterPartitionAbsentAdapterIsLegacyBehavior(t *testing.T) {
	idx := New()
	ingestUnder(idx, "legacy-replica", "", "same-tokens")

	if got := lookupUnder(idx, "", "same-tokens"); len(got) != 1 || got[0].ReplicaID != "legacy-replica" {
		t.Fatalf("legacy (no-adapter) lookup = %+v, want legacy-replica — non-LoRA deployments must be unaffected", got)
	}
	if got := lookupUnder(idx, "sql-lora", "same-tokens"); len(got) != 0 {
		t.Errorf("adapter lookup against legacy ingest = %+v, want no hint", got)
	}

	// And the mirror image: adapter-scoped ingest is invisible to a legacy lookup.
	idx2 := New()
	ingestUnder(idx2, "lora-replica", "sql-lora", "same-tokens")
	if got := lookupUnder(idx2, "", "same-tokens"); len(got) != 0 {
		t.Errorf("legacy lookup against adapter ingest = %+v, want no hint", got)
	}
}

// Property 4 — a producer whose replica serves one adapter can stamp the update
// once; a multi-adapter producer stamps each entry and the per-entry value wins.
func TestAdapterPartitionUpdateLevelDefaultAndPerEntryOverride(t *testing.T) {
	idx := New()
	idx.Ingest(Update{
		ReplicaID:  "replica-0",
		Model:      adapterModel,
		Tenant:     adapterTenant,
		HashScheme: adapterScheme,
		Adapter:    "default-lora",
		Prefixes: []PrefixRef{
			{PrefixHash: hash("inherits"), TokenCount: 64},                         // no Adapter → update default
			{PrefixHash: hash("overrides"), TokenCount: 64, Adapter: "other-lora"}, // per-entry wins
		},
	})

	if got := lookupUnder(idx, "default-lora", "inherits"); len(got) != 1 {
		t.Errorf("entry without its own adapter did not inherit the update default: %+v", got)
	}
	if got := lookupUnder(idx, "other-lora", "overrides"); len(got) != 1 {
		t.Errorf("per-entry adapter did not override the update default: %+v", got)
	}
	if got := lookupUnder(idx, "default-lora", "overrides"); len(got) != 0 {
		t.Errorf("per-entry override leaked into the update-default partition: %+v", got)
	}
}

// The chain (block-hash) ingest/lookup path partitions identically — a chain
// expands into per-block entries and every one of them must land in the entry's
// adapter partition, or a longest-prefix match could still walk across adapters.
func TestAdapterPartitionChainPathDoesNotAlias(t *testing.T) {
	idx := New()
	blocks := [][]byte{hash("b0"), hash("b1")}
	counts := []int32{64, 64}

	for _, tc := range []struct{ replica, adapter string }{
		{"replica-sql", "sql-lora"},
		{"replica-chat", "chat-lora"},
	} {
		idx.Ingest(Update{
			ReplicaID: tc.replica, Model: adapterModel, Tenant: adapterTenant, HashScheme: adapterScheme,
			Prefixes: []PrefixRef{{
				BlockHashes: blocks, BlockTokenCounts: counts, Adapter: tc.adapter,
			}},
		})
	}

	res := idx.LookupRoute(LookupRequest{
		Tenant: adapterTenant, Model: adapterModel, HashScheme: adapterScheme,
		Adapter: "sql-lora", BlockHashes: blocks, BlockTokenCounts: counts,
	})
	if res.Strategy != StrategyPrefixMatch {
		t.Fatalf("strategy = %v, want PrefixMatch within the sql-lora partition", res.Strategy)
	}
	if len(res.Scores) != 1 || res.Scores[0].ReplicaID != "replica-sql" {
		t.Fatalf("chain scores = %+v, want only replica-sql — the chain walk must stay inside one adapter partition", res.Scores)
	}
	if res.Scores[0].MatchedTokens != 128 {
		t.Errorf("matched_tokens = %d, want 128 (both blocks)", res.Scores[0].MatchedTokens)
	}
}

// A PREFIX_EVICTED that names its adapter drops the prefix ONLY there. Without
// the narrowing, one adapter's GPU eviction would wipe another adapter's still
// valid hint for the same (token-derived) hash.
func TestAdapterPartitionEvictionIsScopedToItsAdapter(t *testing.T) {
	idx := New()
	ingestUnder(idx, "replica-0", "sql-lora", "same-tokens")
	ingestUnder(idx, "replica-0", "chat-lora", "same-tokens")

	idx.ApplyEvent(Event{
		Type: EventPrefixEvicted, ReplicaID: "replica-0",
		Model: adapterModel, Tenant: adapterTenant,
		PrefixHash: hash("same-tokens"), Adapter: "sql-lora", AdapterSet: true,
	})

	if got := lookupUnder(idx, "sql-lora", "same-tokens"); len(got) != 0 {
		t.Errorf("sql-lora entry survived its own eviction: %+v", got)
	}
	if got := lookupUnder(idx, "chat-lora", "same-tokens"); len(got) != 1 {
		t.Errorf("chat-lora entry = %+v, want it untouched by the sql-lora eviction", got)
	}
}

// A base-model eviction (Adapter "" WITH presence) drops ONLY the base partition
// and must not sweep a live LoRA hint for the same token hash — the over-sweep
// that adapter_id presence exists to fix.
func TestAdapterPartitionBaseEvictionWithPresenceSpareLoRA(t *testing.T) {
	idx := New()
	ingestUnder(idx, "replica-0", "", "same-tokens")
	ingestUnder(idx, "replica-0", "sql-lora", "same-tokens")

	idx.ApplyEvent(Event{
		Type: EventPrefixEvicted, ReplicaID: "replica-0",
		Model: adapterModel, Tenant: adapterTenant,
		PrefixHash: hash("same-tokens"), Adapter: "", AdapterSet: true,
	})

	if got := lookupUnder(idx, "", "same-tokens"); len(got) != 0 {
		t.Errorf("base entry survived its own eviction: %+v", got)
	}
	if got := lookupUnder(idx, "sql-lora", "same-tokens"); len(got) != 1 {
		t.Errorf("sql-lora entry = %+v, want it untouched by the base-model eviction", got)
	}
}

// An eviction with NO adapter keeps the original conservative behavior: it
// sweeps every partition. That is what a pre-adapter producer emits, and for
// such a producer all entries live in the "" partition anyway — so the
// cross-partition sweep is indistinguishable from the old code there.
func TestAdapterPartitionEvictionWithoutAdapterSweepsEveryPartition(t *testing.T) {
	idx := New()
	ingestUnder(idx, "replica-0", "sql-lora", "same-tokens")
	ingestUnder(idx, "replica-0", "", "same-tokens")

	idx.ApplyEvent(Event{
		Type: EventPrefixEvicted, ReplicaID: "replica-0",
		Model: adapterModel, Tenant: adapterTenant,
		PrefixHash: hash("same-tokens"),
	})

	if got := lookupUnder(idx, "", "same-tokens"); len(got) != 0 {
		t.Errorf("default-partition entry survived an unscoped eviction: %+v", got)
	}
	if got := lookupUnder(idx, "sql-lora", "same-tokens"); len(got) != 0 {
		t.Errorf("unscoped eviction must stay conservative and sweep every partition, got %+v", got)
	}
}

// ALL_CLEARED is a whole-replica cache flush, so it stays adapter-agnostic: the
// engine dropped every adapter's blocks at once.
func TestAdapterPartitionAllClearedIgnoresAdapter(t *testing.T) {
	idx := New()
	ingestUnder(idx, "replica-0", "sql-lora", "p")
	ingestUnder(idx, "replica-0", "chat-lora", "p")

	idx.ApplyEvent(Event{
		Type: EventAllCleared, ReplicaID: "replica-0",
		Model: adapterModel, Tenant: adapterTenant,
	})

	if got := lookupUnder(idx, "sql-lora", "p"); len(got) != 0 {
		t.Errorf("sql-lora entry survived ALL_CLEARED: %+v", got)
	}
	if got := lookupUnder(idx, "chat-lora", "p"); len(got) != 0 {
		t.Errorf("chat-lora entry survived ALL_CLEARED: %+v", got)
	}
}

// The diagnostic miss classifier is deliberately adapter-blind (scopeKey has no
// adapter — see its doc comment): an unseen adapter under a KNOWN
// (tenant, model, hash_scheme) is a novel-prefix miss (NO_HINT), not a
// contract-key mismatch. Pinning this keeps UNKNOWN_HASH_SCHEME meaning
// "wrong hash_scheme" rather than silently becoming "unseen adapter".
func TestAdapterPartitionMissClassifierUnchangedByAdapter(t *testing.T) {
	idx := New()
	ingestUnder(idx, "replica-0", "sql-lora", "p")

	res := idx.LookupRoute(LookupRequest{
		Tenant: adapterTenant, Model: adapterModel, HashScheme: adapterScheme,
		Adapter: "never-seen-lora", PrefixHash: hash("p"),
	})
	if res.Strategy != StrategyNone {
		t.Errorf("strategy = %v, want StrategyNone (NO_HINT) for an unseen adapter in a known engine domain", res.Strategy)
	}

	res = idx.LookupRoute(LookupRequest{
		Tenant: adapterTenant, Model: adapterModel, HashScheme: "sglang",
		Adapter: "sql-lora", PrefixHash: hash("p"),
	})
	if res.Strategy != StrategyUnknownHashScheme {
		t.Errorf("strategy = %v, want StrategyUnknownHashScheme — the scheme diagnostic must be unaffected by adapters", res.Strategy)
	}
}

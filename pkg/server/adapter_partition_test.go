package server

import (
	"context"
	"testing"

	"github.com/cachebox-project/inference-cache/pkg/adapters/engine"
	"github.com/cachebox-project/inference-cache/pkg/fingerprint"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// End-to-end round trip of adapter identity: engine KV event (carrying vLLM's
// lora_id) -> subscriber -> ReportCacheState -> index partition -> LookupRoute.
//
// The scenario is the one SMG v1.8.0's runtime LoRA load/unload makes ordinary:
// one replica caches the SAME token content under two adapters. The content
// fingerprint is token-only, so both blocks carry the identical prefix hash —
// only the partition keeps a lookup for one adapter from being handed the
// other's KV.
func TestLookupRouteAdapterPartitionRoundTrip(t *testing.T) {
	const (
		blockTok = 128
		modelID  = "vllm-model"
		tenantID = "ic-smoke"
		scheme   = "vllm"
	)
	sqlID, chatID := int64(1), int64(2)

	// Identical tokens under two different adapters — the aliasing input.
	toks := tokenSeq(1_000, blockTok)
	key := fingerprint.Bytes(fingerprint.PrefixHashes(toks, blockTok)[0])

	cfg := engine.Config{
		ReplicaID:  "vllm-engine-cs1",
		ModelID:    modelID,
		TenantID:   tenantID,
		HashScheme: scheme,
		// Map the engine's load-order integer ids onto the stable adapter names
		// the gateway sends as adapter_id.
		AdapterNames: map[int64]string{sqlID: "sql-lora", chatID: "chat-lora"},
	}
	client, stop := runEngineReporterCfgAgainstServer(t, cfg,
		[]engine.ReporterOption{engine.WithIgnoreBlockRemoved(true)},
		&engine.EventBatch{Events: []engine.Event{
			engine.BlockStored{BlockHashes: [][]byte{be8(1)}, TokenIDs: toks, BlockSize: blockTok, LoRAID: &sqlID},
			engine.BlockStored{BlockHashes: [][]byte{be8(2)}, TokenIDs: toks, BlockSize: blockTok, LoRAID: &chatID},
		}},
	)
	defer stop()

	lookup := func(adapter string) *icpb.LookupRouteResponse {
		t.Helper()
		resp, err := client.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
			ModelId: modelID, TenantId: tenantID, HashScheme: scheme,
			AdapterId:        adapter,
			BlockHashes:      [][]byte{key},
			BlockTokenCounts: []int32{blockTok},
		})
		if err != nil {
			t.Fatalf("LookupRoute(adapter=%q): %v", adapter, err)
		}
		return resp
	}

	// Same tokens + same adapter still hits — partitioning is not a hash change.
	sql := lookup("sql-lora")
	if sql.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("sql-lora reason_code = %s, want PREFIX_MATCH", sql.GetReasonCode())
	}
	if sql.GetAdapterId() != "sql-lora" {
		t.Errorf("response adapter_id = %q, want sql-lora (echo of the partition consulted)", sql.GetAdapterId())
	}
	if chat := lookup("chat-lora"); chat.GetReasonCode() != "PREFIX_MATCH" {
		t.Errorf("chat-lora reason_code = %s, want PREFIX_MATCH", chat.GetReasonCode())
	}

	// An adapter that never cached this content must NOT inherit the hint from
	// the adapters that did — this is the alias the partition prevents.
	if other := lookup("never-loaded-lora"); other.GetReasonCode() == "PREFIX_MATCH" {
		t.Errorf("unrelated adapter got %s with scores %+v; identical tokens under a different adapter must not alias",
			other.GetReasonCode(), other.GetReplicaScores())
	}

	// A gateway that sends no adapter_id looks in the default partition, where
	// this LoRA-scoped content was never ingested.
	if legacy := lookup(""); legacy.GetReasonCode() == "PREFIX_MATCH" {
		t.Errorf("no-adapter lookup got %s; adapter-scoped ingest must not surface in the default partition", legacy.GetReasonCode())
	}
}

// The non-LoRA deployment — the overwhelming majority — must be untouched: the
// engine emits no lora_id, the subscriber reports the default partition, and a
// gateway that has never heard of adapter_id still matches exactly as before.
func TestLookupRouteWithoutAdapterIsUnchanged(t *testing.T) {
	const (
		blockTok = 128
		modelID  = "vllm-model"
		tenantID = "ic-smoke"
		scheme   = "vllm"
	)
	toks := tokenSeq(2_000, blockTok)
	key := fingerprint.Bytes(fingerprint.PrefixHashes(toks, blockTok)[0])

	client, stop := runEngineReporterAgainstServer(t,
		[]engine.ReporterOption{engine.WithIgnoreBlockRemoved(true)},
		&engine.EventBatch{Events: []engine.Event{
			engine.BlockStored{BlockHashes: [][]byte{be8(1)}, TokenIDs: toks, BlockSize: blockTok},
		}},
	)
	defer stop()

	resp, err := client.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
		ModelId: modelID, TenantId: tenantID, HashScheme: scheme,
		BlockHashes: [][]byte{key}, BlockTokenCounts: []int32{blockTok},
	})
	if err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if resp.GetReasonCode() != "PREFIX_MATCH" {
		t.Fatalf("reason_code = %s, want PREFIX_MATCH — adding the adapter partition must not regress non-LoRA deployments", resp.GetReasonCode())
	}
	if resp.GetAdapterId() != "" {
		t.Errorf("response adapter_id = %q, want empty for a request that set none", resp.GetAdapterId())
	}
}

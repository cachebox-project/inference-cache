package server

import (
	"context"
	"testing"

	"github.com/cachebox-project/inference-cache/pkg/adapters/engine"
	"github.com/cachebox-project/inference-cache/pkg/fingerprint"
	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

// A healthy routing index must yield a *mix* of hits and misses on a designed
// workload. This guards against two degenerate failures that both look "fine" at
// a glance but are wrong:
//
//   - 100% miss — the NONE_HASH regression this change fixes: engine block hashes
//     aren't reproducible, so the gateway can never match the index. Every
//     reused prefix would fail to hit.
//   - 100% match — a degenerate or colliding fingerprint that matches everything,
//     mis-routing novel prompts to a replica that never cached them. Every novel
//     prefix would falsely hit.
//
// We store K distinct prefixes via the Reporter ingest path, then issue 2K
// LookupRoute queries: K that reuse a stored prefix (must PREFIX_MATCH) and K
// with novel content (must NO_HINT). The realized match rate must be strictly
// between 0 and 1.
func TestRouteLookupMixedHitMiss(t *testing.T) {
	const (
		k        = 8
		blockTok = 128 // one block per prefix, comfortably above the matched-tokens floor
		modelID  = "vllm-model"
		tenantID = "ic-smoke"
		scheme   = "vllm"
	)

	// Store K distinct single-block prefixes and remember each one's content key.
	var batches []*engine.EventBatch
	keys := make([][]byte, k)
	for i := 0; i < k; i++ {
		toks := tokenSeq(1_000+i*10_000, blockTok) // far-apart ranges → distinct content
		keys[i] = fingerprint.Bytes(fingerprint.PrefixHashes(toks, blockTok)[0])
		batches = append(batches, &engine.EventBatch{
			TimestampSeconds: 0, // 0 = "now" server-side; a real epoch ts would be past the freshness TTL
			Events: []engine.Event{engine.BlockStored{
				BlockHashes: [][]byte{be8(uint64(i) + 1)},
				TokenIDs:    toks,
				BlockSize:   blockTok,
			}},
		})
	}

	client, stop := runEngineReporterAgainstServer(t,
		[]engine.ReporterOption{engine.WithIgnoreBlockRemoved(true)}, batches...)
	defer stop()

	match := func(key []byte) bool {
		resp, err := client.LookupRoute(context.Background(), &icpb.LookupRouteRequest{
			ModelId: modelID, TenantId: tenantID, HashScheme: scheme,
			BlockHashes: [][]byte{key}, BlockTokenCounts: []int32{blockTok},
		})
		if err != nil {
			t.Fatalf("LookupRoute: %v", err)
		}
		return resp.GetReasonCode() == "PREFIX_MATCH"
	}

	// Reused prefixes must hit.
	hits := 0
	for i := 0; i < k; i++ {
		if match(keys[i]) {
			hits++
		}
	}
	// Novel prefixes (never stored) must miss.
	falseMatches := 0
	for i := 0; i < k; i++ {
		toks := tokenSeq(50_000_000+i*10_000, blockTok)
		if match(fingerprint.Bytes(fingerprint.PrefixHashes(toks, blockTok)[0])) {
			falseMatches++
		}
	}

	if hits != k {
		t.Errorf("reused prefixes: %d/%d hit — want all; 0 is the NONE_HASH all-miss regression", hits, k)
	}
	if falseMatches != 0 {
		t.Errorf("novel prefixes: %d/%d matched — want none; a degenerate/colliding fingerprint mis-routes novel prompts", falseMatches, k)
	}
	rate := float64(hits) / float64(2*k)
	if rate <= 0 || rate >= 1 {
		t.Errorf("match rate %.2f is degenerate (0%% or 100%%); a healthy index must mix hits and misses", rate)
	}
}

// Package main is the index-sizing measurement helper used to characterize the
// inferencecache-server in-memory index footprint at various entry counts. It
// ingests N synthetic prefix entries, forces GC + returns memory to the OS, and
// prints heap + peak RSS so operators can pick CacheTenant.spec.quota.maxIndexEntries,
// the per-namespace CachePolicy.spec.evictionTTL, and pod memory limits with
// real numbers instead of a guess. The global server cap is the compile-time
// constant pkg/index.DefaultMaxEntries.
//
// Not a shipping binary; not built by `make build`. Run with:
//
//	go run ./hack/index-sizing -keys=1500000 -replicas=1
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/cachebox-project/inference-cache/pkg/index"
)

func main() {
	keys := flag.Int("keys", 1_000_000, "distinct prefix keys to ingest")
	replicas := flag.Int("replicas", 1, "replicas reporting each prefix (entries = keys × replicas)")
	hashSize := flag.Int("hash-bytes", 32, "prefix-hash bytes per entry. Conservative default representing LMCache-style SHA hashes; the in-tree vLLM adapter normalizes integer block hashes to 8 bytes big-endian (see pkg/adapters/engine/events.go uint64BE). Minimum 8 to guarantee uniqueness across the keys range.")
	tenants := flag.Int("tenants", 1, "distinct tenant IDs (keys are split evenly across tenants×models)")
	models := flag.Int("models", 1, "distinct model IDs")
	batchSize := flag.Int("batch", 1_000, "prefixes per Ingest call")
	flag.Parse()

	// Reject inputs that would divide-by-zero (`tenants=0`/`models=0`), corrupt
	// the per-entry denominator (`keys`/`replicas` ≤ 0), or panic deep in the
	// loop (`batch` ≤ 0). hash-bytes < 8 would let encodeHash silently produce
	// duplicate hashes (it packs the key index into the first 8 bytes), so the
	// floor is 8 — enough to encode any practical -keys value uniquely.
	if *keys <= 0 || *replicas <= 0 || *tenants <= 0 || *models <= 0 || *batchSize <= 0 {
		fmt.Fprintf(os.Stderr, "keys, replicas, tenants, models, batch must be strictly positive: keys=%d replicas=%d tenants=%d models=%d batch=%d\n",
			*keys, *replicas, *tenants, *models, *batchSize)
		os.Exit(2)
	}
	if *hashSize < 8 {
		fmt.Fprintf(os.Stderr, "hash-bytes must be >= 8 (encodeHash packs the key index into the first 8 bytes; narrower widths collide). got %d\n", *hashSize)
		os.Exit(2)
	}

	// keysPerBucket rounds DOWN, so the requested -keys may not all be ingested
	// when (tenants × models) doesn't divide. Compute the actually-ingested
	// total here and use that as the denominator for every bytes-per-entry
	// number below — otherwise the report would inflate the denominator and
	// under-state per-entry cost. If the rounding leaves a zero per-bucket
	// count, the run would ingest nothing — fail rather than divide by zero
	// in the report.
	keysPerBucket := *keys / ((*tenants) * (*models))
	if keysPerBucket == 0 {
		fmt.Fprintf(os.Stderr, "keys=%d < tenants×models=%d; need at least one key per bucket. raise -keys or lower -tenants/-models.\n",
			*keys, (*tenants)*(*models))
		os.Exit(2)
	}
	ingestedKeys := keysPerBucket * (*tenants) * (*models)
	totalEntries := ingestedKeys * (*replicas)
	if ingestedKeys != *keys {
		fmt.Fprintf(os.Stderr, "warning: keys=%d not divisible by tenants×models=%d; ingesting %d keys (per-bucket=%d)\n",
			*keys, (*tenants)*(*models), ingestedKeys, keysPerBucket)
	}

	// No eviction during the run: we want steady-state population, not a
	// post-sweep slice of it. TTL + sweep are pushed past any reasonable
	// ingest duration; the cap is set above totalEntries so cap-eviction
	// never fires either.
	idx := index.New(
		index.WithTTL(24*time.Hour),
		index.WithSweepInterval(time.Hour),
		index.WithMaxEntries(totalEntries+1),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	idx.Start(ctx)

	fmt.Printf("Ingesting %d keys × %d replicas = %d entries (%d tenants, %d models, %d-byte hash, batch=%d)\n",
		ingestedKeys, *replicas, totalEntries, *tenants, *models, *hashSize, *batchSize)

	start := time.Now()
	const hashScheme = "vllm-block-v1"

	for replicaIdx := 0; replicaIdx < *replicas; replicaIdx++ {
		replicaID := fmt.Sprintf("vllm-cache-backend-pod-%d", replicaIdx)
		for tenantIdx := 0; tenantIdx < *tenants; tenantIdx++ {
			tenant := fmt.Sprintf("tenant-%d", tenantIdx)
			for modelIdx := 0; modelIdx < *models; modelIdx++ {
				model := fmt.Sprintf("meta-llama/Llama-3-70B-Instruct-m%d", modelIdx)
				bucketStart := (tenantIdx*(*models) + modelIdx) * keysPerBucket
				bucketEnd := bucketStart + keysPerBucket

				batch := make([]index.PrefixRef, 0, *batchSize)
				for prefixIdx := bucketStart; prefixIdx < bucketEnd; prefixIdx++ {
					batch = append(batch, index.PrefixRef{
						PrefixHash: encodeHash(prefixIdx, *hashSize),
						TokenCount: 256,
					})
					if len(batch) == *batchSize {
						idx.Ingest(index.Update{
							ReplicaID:  replicaID,
							Model:      model,
							Tenant:     tenant,
							HashScheme: hashScheme,
							Prefixes:   batch,
						})
						batch = batch[:0]
					}
				}
				if len(batch) > 0 {
					idx.Ingest(index.Update{
						ReplicaID:  replicaID,
						Model:      model,
						Tenant:     tenant,
						HashScheme: hashScheme,
						Prefixes:   batch,
					})
				}

				// One stats payload per (replica, tenant, model). The index also
				// holds these; they contribute to the steady-state footprint.
				idx.Ingest(index.Update{
					ReplicaID:  replicaID,
					Model:      model,
					Tenant:     tenant,
					HashScheme: hashScheme,
					Stats: &index.ReplicaStats{
						CacheMemoryBytes: 1 << 32,
						HitRate:          0.85,
						Pressure:         0.30,
					},
				})
			}
		}
	}

	ingestElapsed := time.Since(start)

	// Two GCs + FreeOSMemory: the first GC marks dead bytes, the second
	// reclaims any finalizer-deferred objects, FreeOSMemory hints to madvise
	// the released pages so RSS tracks heap_inuse instead of heap_sys.
	runtime.GC()
	runtime.GC()
	debug.FreeOSMemory()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	// Maxrss is a high-water mark, NOT current RSS — it records the largest
	// resident-set size the process ever reached, even if pages have since
	// been returned to the OS by debug.FreeOSMemory. For a one-shot bulk
	// ingest like this harness the peak is dominated by the steady-state
	// index footprint plus transient batch/PrefixRef allocations, so it
	// over-states the post-GC working set. We report it as peak_rss and
	// the doc treats it as a conservative pod-budget number, not a
	// steady-state RSS reading.
	peakRSS := uint64(ru.Maxrss)
	// macOS Maxrss is bytes; Linux is KiB. The harness is documented to run
	// on either, so normalize before reporting.
	if runtime.GOOS == "linux" {
		peakRSS *= 1024
	}

	snap := idx.Snapshot()
	var sumTenantEntries int64
	for _, t := range snap.Tenants {
		sumTenantEntries += t.IndexEntries
	}

	fmt.Printf("\n=== Memory profile (after GC + FreeOSMemory) ===\n")
	fmt.Printf("ingest_duration         %s (%.0f entries/sec)\n", ingestElapsed, float64(totalEntries)/ingestElapsed.Seconds())
	fmt.Printf("snapshot.totalPrefixes  %d (Σ tenants[].indexEntries = %d)\n", snap.TotalPrefixes, sumTenantEntries)
	fmt.Printf("snapshot.replicas       %d\n", len(snap.Replicas))
	fmt.Printf("heap_alloc              %s  (%.0f bytes/entry)\n", humanBytes(ms.HeapAlloc), float64(ms.HeapAlloc)/float64(totalEntries))
	fmt.Printf("heap_inuse              %s\n", humanBytes(ms.HeapInuse))
	fmt.Printf("heap_sys                %s\n", humanBytes(ms.HeapSys))
	fmt.Printf("sys (Go total)          %s\n", humanBytes(ms.Sys))
	fmt.Printf("peak_rss                %s  (%.0f bytes/entry; high-water mark, not current)\n", humanBytes(peakRSS), float64(peakRSS)/float64(totalEntries))
	fmt.Printf("num_gc                  %d\n", ms.NumGC)
}

// encodeHash produces a deterministic, distinct hash for index n by packing n
// into the first 8 bytes (little-endian) and zero-padding the rest. The first
// 2^63 indices are unique, which exceeds anything the harness could ingest.
func encodeHash(n, size int) []byte {
	h := make([]byte, size)
	for i := 0; i < 8 && i < size; i++ {
		h[i] = byte(uint64(n) >> (8 * i))
	}
	return h
}

func humanBytes(b uint64) string {
	const (
		KiB = 1024
		MiB = KiB * 1024
		GiB = MiB * 1024
	)
	switch {
	case b >= GiB:
		return fmt.Sprintf("%.2f GiB", float64(b)/GiB)
	case b >= MiB:
		return fmt.Sprintf("%.2f MiB", float64(b)/MiB)
	case b >= KiB:
		return fmt.Sprintf("%.2f KiB", float64(b)/KiB)
	default:
		return fmt.Sprintf("%d B", b)
	}
}

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

// runPlan is the derived per-run shape: per-bucket key count, the actually-
// ingested total (which may be less than the requested -keys if the divisor
// rounds down), and the total storage entries. Centralized in one struct so
// the helper can return it under test instead of hand-threading naked ints.
type runPlan struct {
	KeysPerBucket int
	IngestedKeys  int
	TotalEntries  int
	Truncated     bool // ingestedKeys < requested
}

// planRun translates the flag values into a runPlan, applying every rule the
// harness depends on: strict-positive flags, hash-bytes >= 8 (so encodeHash
// can't collide), no divide-by-zero from a tenants×models that exceeds keys,
// and platform-aware overflow guards. Extracted from main() so the rules are
// reachable from tests — when the rules drift, the bytes-per-entry numbers in
// the operator guide silently drift with them.
//
// Returns an error string suitable for stderr; main() prefixes it and exits.
func planRun(keys, replicas, hashSize, tenants, models, batchSize int) (runPlan, error) {
	if keys <= 0 || replicas <= 0 || tenants <= 0 || models <= 0 || batchSize <= 0 {
		return runPlan{}, fmt.Errorf("keys, replicas, tenants, models, batch must be strictly positive: keys=%d replicas=%d tenants=%d models=%d batch=%d",
			keys, replicas, tenants, models, batchSize)
	}
	if hashSize < 8 {
		return runPlan{}, fmt.Errorf("hash-bytes must be >= 8 (encodeHash packs the key index into the first 8 bytes; narrower widths collide). got %d", hashSize)
	}

	// Bound-check EVERY downstream multiply step-by-step in int64 against
	// int's actual range on this platform; a bad flag combo would otherwise
	// wrap and the harness would use the wrap as both the cap input and
	// the bytes-per-entry denominator. We also reject prod == intMax so
	// the later `WithMaxEntries(totalEntries+1)` cannot wrap to a
	// non-positive (unbounded) cap.
	const intMax = int64(^uint(0) >> 1)
	tm := int64(tenants)
	if tm > intMax/int64(models) {
		return runPlan{}, fmt.Errorf("tenants×models=%d × %d would overflow int on this platform; lower a flag", tenants, models)
	}
	tm *= int64(models)
	keysPerBucket64 := int64(keys) / tm
	if keysPerBucket64 == 0 {
		return runPlan{}, fmt.Errorf("keys=%d < tenants×models=%d; need at least one key per bucket. raise -keys or lower -tenants/-models", keys, tm)
	}
	prod := keysPerBucket64
	for _, m := range []int{tenants, models, replicas} {
		if prod > intMax/int64(m) {
			return runPlan{}, fmt.Errorf("totalEntries would overflow int on this platform; lower a flag (keysPerBucket=%d tenants=%d models=%d replicas=%d)",
				keysPerBucket64, tenants, models, replicas)
		}
		prod *= int64(m)
	}
	if prod >= intMax {
		return runPlan{}, fmt.Errorf("totalEntries=%d hits MaxInt; lower a flag so the cap input (totalEntries+1) stays representable", prod)
	}
	keysPerBucket := int(keysPerBucket64)
	ingestedKeys := keysPerBucket * tenants * models
	return runPlan{
		KeysPerBucket: keysPerBucket,
		IngestedKeys:  ingestedKeys,
		TotalEntries:  int(prod),
		Truncated:     ingestedKeys != keys,
	}, nil
}

func main() {
	keys := flag.Int("keys", 1_000_000, "distinct prefix keys to ingest")
	replicas := flag.Int("replicas", 1, "replicas reporting each prefix (entries = keys × replicas)")
	hashSize := flag.Int("hash-bytes", 32, "prefix-hash bytes per entry. Conservative default representing LMCache-style SHA hashes; the in-tree vLLM adapter normalizes integer block hashes to 8 bytes big-endian (see pkg/adapters/engine/events.go uint64BE). Minimum 8 to guarantee uniqueness across the keys range.")
	tenants := flag.Int("tenants", 1, "distinct tenant IDs (keys are split evenly across tenants×models)")
	models := flag.Int("models", 1, "distinct model IDs")
	batchSize := flag.Int("batch", 1_000, "prefixes per Ingest call")
	flag.Parse()

	plan, err := planRun(*keys, *replicas, *hashSize, *tenants, *models, *batchSize)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if plan.Truncated {
		fmt.Fprintf(os.Stderr, "warning: keys=%d not divisible by tenants×models=%d; ingesting %d keys (per-bucket=%d)\n",
			*keys, (*tenants)*(*models), plan.IngestedKeys, plan.KeysPerBucket)
	}
	keysPerBucketInt := plan.KeysPerBucket
	ingestedKeys := plan.IngestedKeys
	totalEntries := plan.TotalEntries

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
				bucketStart := (tenantIdx*(*models) + modelIdx) * keysPerBucketInt
				bucketEnd := bucketStart + keysPerBucketInt

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
	rusageErr := syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	// Maxrss is a high-water mark, NOT current RSS — it records the largest
	// resident-set size the process ever reached, even if pages have since
	// been returned to the OS by debug.FreeOSMemory. For a one-shot bulk
	// ingest like this harness the peak is dominated by the steady-state
	// index footprint plus transient batch/PrefixRef allocations, so it
	// over-states the post-GC working set. We report it as peak_rss and
	// the doc treats it as a conservative pod-budget number, not a
	// steady-state RSS reading.
	//
	// rusageErr unavailable → don't print a synthetic 0 (which a reader
	// would mistake for "peak RSS measured as zero", an impossible result
	// that would fold straight into bytes/entry as zero). Report the gap
	// explicitly so the operator knows that field is missing.
	var peakRSS uint64
	if rusageErr == nil {
		peakRSS = uint64(ru.Maxrss)
		// macOS Maxrss is bytes; Linux is KiB. The harness is documented to
		// run on either, so normalize before reporting.
		if runtime.GOOS == "linux" {
			peakRSS *= 1024
		}
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
	if rusageErr != nil {
		fmt.Printf("peak_rss                unavailable (getrusage: %v)\n", rusageErr)
	} else {
		fmt.Printf("peak_rss                %s  (%.0f bytes/entry; high-water mark, not current)\n", humanBytes(peakRSS), float64(peakRSS)/float64(totalEntries))
	}
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

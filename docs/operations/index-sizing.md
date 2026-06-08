# Index sizing & memory tuning

The inferencecache-server holds a soft-state cache-state index in process memory:
distinct prefix entries per `(tenant, model, hash_scheme, prefix_hash)`, replica
stats per `(tenant, model, replica_id)`, plus a few secondary indices. This guide
gives operators the numbers and dials to size the server pod and the per-namespace /
per-tenant knobs that bound the index footprint.

**Audience.** Cluster operators picking pod resource limits, `CachePolicy.spec.evictionTTL`,
and `CacheTenant.spec.quota.maxIndexEntries` for their workload.

**Source of measurements.** All numbers below come from the in-tree sizing harness
[`hack/index-sizing`](../../hack/index-sizing) on `go1.26.4` / `darwin/arm64`, ingested
through the real `pkg/index` code path. Re-run the harness on your own platform if you
need same-arch numbers — the harness is documented at the top of `main.go`.

---

## TL;DR

| Workload shape | Peak RSS (process high-water mark) | Pod memory budget |
|---|---|---|
| 100K distinct prefix entries | ~110 MiB | 256 MiB |
| 500K entries | ~300 MiB | 512 MiB |
| 1M entries (`DefaultMaxEntries`) | ~540 MiB | **1 GiB (recommended floor)** |
| 1.5M entries | ~700 MiB | 1.5 GiB |

The "peak RSS" column is `Maxrss` from the harness run — the high-water mark the process ever reached, not current RSS. For a one-shot bulk ingest the peak ≈ steady-state + transient ingest allocations; in production the steady-state is somewhat lower. Treat the column as a **conservative pod-budget figure**: if you provision for the peak, the steady-state has headroom built in.

The default global entry cap of `DefaultMaxEntries = 1,000,000` (see [`pkg/index/index.go`](../../pkg/index/index.go))
is sized for a **1 GiB server pod**. Raise either both (memory + cap) or neither —
the cap is not currently a server flag (see [Knobs](#knobs-the-operator-actually-has)),
so reaching for a larger footprint needs a recompile today.

---

## Per-entry memory footprint

One index entry is one `(tenant, model, hash_scheme, prefix_hash) → replica_id` tuple.
A prefix held by two replicas is two entries; a prefix held by one replica is one.

Measured footprint after GC + `debug.FreeOSMemory()` (peak RSS via `getrusage.Maxrss`):

| Total entries | `heap_alloc` | Peak RSS | bytes / entry (heap) | bytes / entry (peak RSS) |
|---:|---:|---:|---:|---:|
| 500,000 | 241 MiB | 299 MiB | 504 | 628 |
| 1,000,000 | 481 MiB | 544 MiB | 504 | 570 |
| 1,500,000 | 641 MiB | 701 MiB | 448 | 490 |

Scaling is linear in the number of entries above ~100K (the per-entry RSS share drops as
the fixed Go-runtime overhead — ~30–50 MiB — amortizes). A working model that fits the
measurements within ~5 %:

```
heap_bytes      ≈  50 MiB  +  500 × distinct_prefix_keys  +  50 × additional_replica_holdings
peak_RSS_bytes  ≈  heap_bytes × 1.10
```

Where:

- *distinct prefix keys* — unique `(tenant, model, hash_scheme, prefix_hash)` tuples.
  This is the unit `tenants[].indexEntries` reports and `CacheTenant.spec.quota.maxIndexEntries`
  bounds.
- *additional replica holdings* — every replica beyond the first that reports the same
  key. Holding the same prefix on R replicas costs ~500 + (R-1) × 50 bytes, not R × 500.

### Where the bytes go

A single entry is a chain of small Go allocations under two nested `map`s:

- outer `map[prefixKey]…` bucket header + the prefix-key strings (tenant, model,
  hash_scheme, prefix_hash);
- inner `map[replicaID]*replicaEntry` header + the replica-id string;
- the `replicaEntry` struct (token count + lastSeen `time.Time` + `atomic.Int64`
  LFU counter).

Stats are tracked separately (`map[statsKey]statEntry`, one row per
`(tenant, model, replica_id)`) and are negligible at any realistic replica count —
hundreds of bytes per replica, vs. hundreds of bytes per prefix entry.

**The prefix-hash byte width does NOT dominate.** Going from 32-byte hashes (LMCache /
SHA-256-style) to 16-byte hashes (vLLM-native xxhash) shaved only ~16 B/entry on the heap.
The map machinery and `time.Time` are the bulk of the cost.

---

## Workload-shape sizing

The right number for `maxIndexEntries` is a function of three multipliers:

1. **Distinct prefix-hash density** — how many unique prefix bytes the workload generates
   per second.
2. **Block-hash chain length** — how many per-block entries each prompt expands into.
   The index stores one entry **per block hash**, not per prompt. A 10-block prompt
   reported as a chain costs 10 entries per replica.
3. **Effective TTL** — how long entries stay reachable. Steady-state population
   = arrival_rate × TTL, until the cap kicks in.

The dominant input is "distinct block-hash entries seen within the TTL window, per
replica" — call it E. The table below derives E from a workload sketch and reads off
whether E (× replicas, if you want a cluster total) fits under the default 1M global cap.
Replicas multiply E by R but only at ~50 B/extra-replica per shared entry.

| Shape | Distinct prompts in TTL window | × block-chain length | = E (per replica) | Sits at default 1M cap |
|---|---:|---:|---:|---|
| Single-tenant chatbot, ~5K QPS, ~5 blocks / prompt | 100K | × 5 | ~500K | Comfortable. |
| Multi-tenant chatbot, 20 tenants × 100K prompts | 2M (Σ across tenants) | × 5 | ~10M | **Cap-bound.** Choices: shorten `evictionTTL` per namespace (linear win), tighten per-tenant `maxIndexEntries`, or raise the cap + pod memory. |
| RAG, 50K documents, ~10-block chunks, TTL=2h | 50K | × 10 | ~500K | Comfortable single-tenant; cap-bound at scale. |
| Long-context coding assistant, 1K active sessions, ~500-block prompts | 1K | × 500 | ~500K | Comfortable, but watch `index_evictions_total{reason="cap"}` — if chain length grows, this is the workload most likely to outgrow the cap silently. |

### Block-hash expansion is the most-overlooked multiplier

The index stores **one entry per block hash**, not per prompt. The vLLM KV-event
subscriber maps each `BlockStored` event into one `PrefixEntry` per block hash (see
[`pkg/adapters/engine/mapper.go`](../../pkg/adapters/engine/mapper.go) — `StoredPrefixes`),
each with a cumulative `token_count`. A 1000-token prompt at vLLM's 16-token block size
produces ~62 block hashes, which becomes ~62 entries per replica. Plan for this expansion,
not the single-blob shape, when sizing.

---

## TTL trade-offs

`CachePolicy.spec.evictionTTL` is a per-namespace knob with a server-side default
of 30 minutes (`pkg/index.DefaultTTL`). Tenants without a CachePolicy use the global
default; a namespace with one uses its own value.

The TTL controls two things at once:

- **Freshness.** An entry older than TTL is treated as stale at lookup time
  (`freshness = 1 - age / ttl`, clamped at 0). It is then physically swept on the
  next sweep tick (default 1 minute).
- **Memory.** Steady-state population is bounded by `arrival_rate × TTL` until the
  global cap kicks in. Halving the TTL roughly halves steady-state memory for a
  given arrival rate (assuming the workload still exceeds the cap without TTL pressure).

| TTL choice | Effect |
|---|---|
| **Shorter than the prefix-reuse window** | Entries age out before they get reused → routing falls back to `NO_HINT` more often → gateway round-robins → engine prefix-cache hit rate drops. Don't go below the typical conversation/session length. |
| **Default (30 min)** | Good fit for chat-style workloads where sessions span a few minutes and prefix re-use within an hour dominates. |
| **Hours** | Works for long-running document workloads (RAG, coding assistant) where the same prefix is consulted across a workday. Costs memory linearly; only safe if the cap headroom exists. |
| **`0` or negative** | Rejected at admission. |

**Soft-state guarantee.** Whatever TTL you pick, a stale hint is a cache miss — never a
wrong answer. The gateway routes to the suggested replica, the replica doesn't have the
prefix warm, the engine recomputes. So the cost of "TTL too long" is wasted index memory
+ a small wasted gateway routing decision; it is *not* a correctness risk.

---

## Knobs the operator actually has

| Lever | Type | Default | Effect |
|---|---|---|---|
| `CachePolicy.spec.evictionTTL` | per-namespace duration | server default 30m | Bounds freshness window + steady-state memory for the namespace. |
| `CachePolicy.spec.eviction` | per-namespace enum (`LRU`/`LFU`) | `LRU` | Picks the victim under cap pressure. LFU keeps frequently-hit prefixes warm. |
| `CacheTenant.spec.quota.maxIndexEntries` | per-tenant int64 | unset = unbounded | Bounds the per-tenant slice of the index; over-budget evicts the tenant's own oldest entries (Fairness). |
| Global entry cap | server compile-time constant | `DefaultMaxEntries = 1,000,000` | Backstop. Not currently a flag (see follow-up below). |
| Global TTL fallback | server compile-time constant | `DefaultTTL = 30m` | Applies to tenants in namespaces without a CachePolicy. |
| Sweep interval | server compile-time constant | `DefaultSweepInterval = 1m` | How often the TTL pass runs. Higher = more lag, less CPU. |

**Follow-up.** The global cap, global TTL, and sweep interval are hardcoded; flipping any
of them today requires a rebuild. Filing the flag work as a follow-up to this guide; track
under your own ticket if you need it before that lands.

---

## Tuning workflow

Start from observation, not guess.

### 1. Watch the metrics that already exist

Every signal below is exported on `/metrics` (port 8080) — no code change needed.

| Signal | Series | What it tells you |
|---|---|---|
| Steady-state size | `inferencecache_index_entries{model}` | **Distinct prefix keys per model** — not total replica×prefix entries. A prefix held by R replicas counts once here. Use this for trend / hit-rate work; it is **not** a direct cap-closeness signal in multi-replica setups (see next row). |
| Cap pressure (authoritative) | `rate(inferencecache_index_evictions_total{reason="cap"}[10m])` | The global cap is on total replica×prefix entries; this counter is what fires when it's exceeded. Anything sustained > 0 means the cap is the binding constraint — use this signal, not `index_entries`, to detect cap pressure. An info-severity alert ships in [`config/observability/alerting-rules.yaml`](../../config/observability/alerting-rules.yaml) (`IndexEvictionsSpike` — fires at sustained > 10 cap-evictions/sec for 10m). |
| Quota pressure | `rate(inferencecache_tenant_evictions_total[10m])` | A tenant is exceeding `spec.quota.maxIndexEntries`. Usually fine (Fairness), but a sustained signal means the quota is too tight for the workload. |
| TTL churn | `rate(inferencecache_index_evictions_total{reason="ttl"}[10m])` | The TTL sweep is doing work. High and steady = entries arriving and aging out at a healthy rate. High and *increasing* together with `index_entries` falling = the workload is shrinking. |
| Pod RSS | `container_memory_working_set_bytes{pod="inference-cache-server-..."}` | What you'd compare against the model in [Per-entry footprint](#per-entry-memory-footprint). |

### 2. Pick a starting point

1. Estimate **distinct prefix entries** for the workload using the multipliers above.
2. Map to a **pod memory budget** via the table in [TL;DR](#tldr).
3. Set `evictionTTL` (per CachePolicy) to the smallest value that keeps the typical
   re-use window inside the TTL — see the [TTL choice table](#ttl-trade-offs).
4. If the workload is multi-tenant, set `CacheTenant.spec.quota.maxIndexEntries` per
   tenant to roughly `cluster_cap × tenant_share`. Leave it unset for single-tenant
   clusters.

### 3. Watch and adjust

After a representative workload window (typically one peak hour + one trough):

- `index_evictions_total{reason="cap"}` non-zero → the workload exceeds the global cap.
  (Do not rely on `index_entries` alone to detect this — that gauge counts distinct
  prefix keys per model, but the cap is on total replica×prefix entries, so on a
  multi-replica deployment the gauge can sit well below 1M while the cap is firing.)
  Choices: shorten `evictionTTL` (cheap), tighten per-tenant quotas (operator-controlled),
  or raise the cap (requires the follow-up flag).
- `index_evictions_total{reason="ttl"}` ≈ 0 *and* the gateway's prefix-cache hit rate is
  low → TTL is set so high that nothing ever ages out, but routing isn't paying off.
  Either the workload doesn't reuse prefixes (no fix needed at the cache plane) or the
  KV-event subscriber isn't reporting (a separate diagnostic — check `replica_id`
  population in `/snapshot`).
- `tenant_evictions_total` non-zero on a tenant whose `status.indexEntries` is well
  below their quota → racing/stale CR. Reconcile.
- RSS climbing without `index_entries` climbing → not the index. Check the metrics
  registry, snapshot cache, or a recent code change.

### 4. The escape hatch (today, until the cap is a flag)

If you genuinely need more than 1M entries before the cap flag lands:

- **Shorter TTL** trims steady-state. A namespace seeing 100K new prefixes/min at
  TTL=30m steady-states at 3M; the same workload at TTL=10m steady-states at 1M.
- **Per-tenant quotas** localize the pain. A multi-tenant cluster where one noisy
  tenant fills the index → set that tenant's `spec.quota.maxIndexEntries` to bound it.

---

## Why the defaults are what they are

| Default | Value | Rationale |
|---|---|---|
| `DefaultMaxEntries` | 1,000,000 | One-pod fit for a 1 GiB server budget at ~500 B/entry. Internal cache-stress benchmarks peaked at ~200K entries; the cap is comfortably above realistic single-tenant load and gives multi-tenant clusters a 10× headroom before they need to think about it. Re-evaluated against the measurements above — the value stayed; what changed is that we now document the relationship to pod memory explicitly. |
| `DefaultTTL` | 30 minutes | Matches typical chat-style prefix reuse windows. Long enough that the same conversation's continuation lands on the same replica; short enough that a half-hour of cold prefixes don't bloat the index. CachePolicy lets per-namespace workloads override (raise for long-context, lower for tight memory). |
| `DefaultSweepInterval` | 1 minute | The sweep is a full-walk over the prefix map looking for expired entries; CPU cost scales linearly with index size, but it runs on its own goroutine off the request path. One minute bounds memory lag without being noticeably hot on a 1M-entry index. Not yet directly benchmarked — file an issue if you observe sweep contention on the hot path. |

None of these are tuned per workload-shape because the realistic spread (chat at the low
end, RAG / long-context at the high end) needs more knobs than one cluster-global default
can carry. The right interface is the per-namespace/per-tenant CRs above, with the cap
as the backstop.

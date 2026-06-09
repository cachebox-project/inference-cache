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

## What you can and can't directly set

There is **no `globalMaxEntries` CRD field**. The levers you actually have are:

1. **Pod memory limit** — `Deployment.spec.containers[].resources.limits.memory`. This is the only hard ceiling Kubernetes enforces; everything else is soft state inside the process.
2. **`CachePolicy.spec.evictionTTL`** — per-namespace, default 30m. Shorter TTL ⇒ smaller steady-state index.
3. **`CachePolicy.spec.eviction`** — per-namespace `LRU` (default) or `LFU`. Picks which entries get evicted under cap pressure; doesn't change *how much* memory you use.
4. **`CacheTenant.spec.quota.maxIndexEntries`** — per-tenant. Bounds one tenant's slice of the index.

The global cap (`DefaultMaxEntries = 1,000,000`), the global TTL fallback, and the sweep interval are **compile-time constants** in the server binary today. Raising them requires a rebuild. See [Knobs the operator actually has](#knobs-the-operator-actually-has) for the full table; the rest of this doc is mostly about how to use the levers above intelligently.

---

## TL;DR

| Storage entries (distinct prefix keys × replicas reporting each) | Peak RSS (process high-water mark) | Pod memory budget |
|---|---|---|
| 100K storage entries | ~110 MiB | 256 MiB |
| 500K storage entries | ~300 MiB | 512 MiB |
| 1M storage entries (`DefaultMaxEntries`) | ~540 MiB | **1 GiB (recommended floor)** |
| 1.5M storage entries | ~700 MiB | 1.5 GiB |

(With one cache replica per prefix, "storage entries" equals "distinct prefix keys" — multi-replica deployments amplify the storage count but only at ~50 B per extra-replica per shared key, not a full per-entry cost. See [Per-entry memory footprint](#per-entry-memory-footprint).)

The "peak RSS" column is `Maxrss` from the harness run — the high-water mark the process ever reached, not current RSS. For a one-shot bulk ingest the peak ≈ steady-state + transient ingest allocations; in production the steady-state is somewhat lower. Treat the column as a **conservative pod-budget figure**: if you provision for the peak, the steady-state has headroom built in.

The default global entry cap of `DefaultMaxEntries = 1,000,000` (see [`pkg/index/index.go`](../../pkg/index/index.go))
is sized for a **1 GiB server pod**. Raise either both (memory + cap) or neither —
the cap is not currently a server flag (see [Knobs](#knobs-the-operator-actually-has)),
so reaching for a larger footprint needs a recompile today.

---

## Per-entry memory footprint

**Two units that get conflated.** The CRD field name `maxIndexEntries`, the snapshot
field `tenants[].indexEntries`, and the metric `inferencecache_index_entries{model}` all
count **distinct prefix keys** — one per `(tenant, model, hash_scheme, prefix_hash)`,
regardless of how many replicas hold it. The internal `pkg/index.DefaultMaxEntries` cap,
by contrast, counts **total storage entries** — one per `(prefix_key, replica)` tuple.
A single prefix held by R replicas is 1 "indexEntries" but R "storage entries". When
the doc says "entries" below, the column header makes which unit explicit.

A prefix held by two replicas is two storage entries; a prefix held by one replica is one.

Measured footprint after GC + `debug.FreeOSMemory()` (peak RSS via `getrusage.Maxrss`):

| Total entries | `heap_alloc` | Peak RSS | bytes / entry (heap) | bytes / entry (peak RSS) |
|---:|---:|---:|---:|---:|
| 500,000 | 241 MiB | 299 MiB | 504 | 628 |
| 1,000,000 | 481 MiB | 544 MiB | 504 | 570 |
| 1,500,000 | 641 MiB | 701 MiB | 448 | 490 |

Scaling is linear in the number of entries above ~100K (the per-entry share drops as
the ~30–50 MiB Go-runtime baseline amortizes). Per-entry heap cost trends slightly
*down* as the index grows (504 B/entry at 500K-1M → 448 B/entry at 1.5M), likely Go
map bucket-fill effects.

**Planning rule of thumb.** Plan **~500 bytes per distinct prefix key** plus
**~50 bytes per additional-replica holding** plus a **~50 MiB Go-runtime baseline**.
Apply a **~20 % pod-memory headroom over the table values above** to absorb heap
fragmentation, transient ingest peaks, and growth. For any sizing commitment, read the
table — the rule of thumb is for back-of-envelope work, not a tight predictor.

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
SHA-256-style) to 16-byte hashes shaved only ~16 B/entry on the heap. The in-tree vLLM
adapter ([`pkg/adapters/engine/events.go`](../../pkg/adapters/engine/events.go) —
`uint64BE`) normalizes integer block hashes to **8-byte big-endian** under the `vllm`
hash_scheme, which would shave a few more bytes. The map machinery and `time.Time` are
the bulk of the cost, not the hash bytes — narrower hashes don't materially change pod
sizing.

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
| Single-tenant chatbot, ~100K distinct prompt prefixes seen in the TTL window after dedup (much smaller than raw arrivals — chats share system prompts, few-shots, and recent turns), ~5 blocks / prompt | 100K | × 5 | ~500K | Comfortable. |
| Multi-tenant chatbot, 20 tenants × 100K prompts | 2M (Σ across tenants) | × 5 | ~10M | **Cap-bound.** Choices: shorten `evictionTTL` per namespace (linear win), tighten per-tenant `maxIndexEntries`, or raise the cap + pod memory. |
| RAG, 50K documents, ~10-block chunks, TTL=2h | 50K | × 10 | ~500K | Comfortable single-tenant; cap-bound at scale. |
| Long-context coding assistant, 1K active sessions, ~500-block prompts | 1K | × 500 | ~500K | Comfortable, but watch `index_evictions_total{reason="cap"}` — if chain length grows, this is the workload most likely to outgrow the cap silently. |

### Block-hash expansion is the most-overlooked multiplier

The index stores **one entry per block hash**, not per prompt. The vLLM KV-event
subscriber maps each `BlockStored` event into one `PrefixEntry` per block hash (see
[`pkg/adapters/engine/mapper.go`](../../pkg/adapters/engine/mapper.go) — `StoredPrefixes`),
each with a cumulative `token_count`. A 1000-token prompt at vLLM's 16-token block size
produces ~63 block hashes (`ceil(1000/16)`), which becomes ~63 entries per replica. Plan
for this expansion, not the single-blob shape, when sizing.

### Worked example — a chatbot that overshoots the cap

Suppose you run a single-tenant chatbot with ~50K distinct conversation prefixes alive
within a 30-minute window (after dedup — most chats share system prompts + few-shots),
each averaging ~800 tokens (≈ 50 block hashes), reported by a single cache replica.

```
E = distinct_prompts × block_chain × replicas
  = 50,000 × 50 × 1
  = 2,500,000 storage entries
```

That's **2.5× the default 1M global cap**. The cap will fire, evictions will trim the
working set down to ~1M, and the rest of the prefixes won't have hints. Three choices:

1. **Accept the cap.** Pod stays at 1 GiB; the cache plane still routes ~1M worth of
   prefixes (the most recently-seen ones under LRU, or the most-accessed under LFU —
   see [`CachePolicy.spec.eviction`](#knobs-the-operator-actually-has)), the rest miss.
   Hit rate is lower than it could be, but routing stays correct (soft-state
   guarantee). This is the right answer when the workload is at the margin and the
   cost of tuning isn't worth the lift.
2. **Shorten `evictionTTL`.** TTL=10m instead of 30m → steady-state population drops
   roughly 3×, to ~830K, comfortably under the cap. Pod stays at 1 GiB. Cost: prefixes
   re-used at the 15-minute mark go to miss instead of hit. Cheapest tuning option and
   usually the right one.
3. **Rebuild with a higher cap.** Bump `DefaultMaxEntries` in `pkg/index/index.go` to,
   say, 3M. At ~500 B/entry that's ~1.4 GiB peak RSS — size the pod for at least 2 GiB
   to leave the 20 % headroom on top of the linear scaling. Most heavyweight option;
   reach for it when the workload can't tolerate the hit-rate loss from option 1 or
   the TTL trim from option 2.

Same shape applies to any "over the cap" case: pick A, B, or C based on whether you can
tolerate lower hit rate, tighter TTL, or a custom build.

---

## TTL trade-offs

`CachePolicy.spec.evictionTTL` is a per-namespace knob with a server-side default
of 30 minutes (`pkg/index.DefaultTTL`). The fallback fires whenever the resolver
returns ≤ 0 — i.e. **only a CachePolicy that explicitly sets `evictionTTL`
overrides the default**. A namespace with no CachePolicy, or a CachePolicy that
omits `evictionTTL`, both fall through to `DefaultTTL`.

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
| Global entry cap | server compile-time constant | `DefaultMaxEntries = 1,000,000` | Backstop. Compile-time today; see the "State today" callout under [Knobs the operator actually has](#knobs-the-operator-actually-has). |
| Global TTL fallback | server compile-time constant | `DefaultTTL = 30m` | Applies to tenants in namespaces without a CachePolicy. |
| Sweep interval | server compile-time constant | `DefaultSweepInterval = 1m` | How often the TTL pass runs. Higher = more lag, less CPU. |

**State today.** The global cap, global TTL, and sweep interval are compile-time
constants in `pkg/index/index.go`; flipping any of them requires a server rebuild. The
per-namespace and per-tenant CRs above are the runtime-tunable surface.

---

## Tuning workflow

Start from observation, not guess.

### 1. Watch the metrics that already exist

All `inferencecache_*` signals below are exported on the server's `/metrics` (port 8080).
The pod-RSS row is a cluster-level metric from kubelet/cAdvisor, scraped separately
(usually by kube-prometheus's `kubelet` `ServiceMonitor`) — included here because it is
the right RSS signal to compare against the [Per-entry footprint](#per-entry-memory-footprint)
table.

| Signal | Series | Source | What it tells you |
|---|---|---|---|
| Steady-state size | `inferencecache_index_entries{model}` | server `/metrics` | **Distinct prefix keys per model** — not total replica×prefix entries. A prefix held by R replicas counts once here. Use this for trend / hit-rate work; it is **not** a direct cap-closeness signal in multi-replica setups (see next row). |
| Cap pressure (authoritative) | `rate(inferencecache_index_evictions_total{reason="cap"}[10m])` | server `/metrics` | The global cap is on total replica×prefix entries; this counter is what fires when it's exceeded. Anything sustained > 0 means the cap is the binding constraint — use this signal, not `index_entries`, to detect cap pressure. An info-severity alert ships in [`config/observability/alerting-rules.yaml`](../../config/observability/alerting-rules.yaml) (`IndexEvictionsSpike` — fires at sustained > 10 cap-evictions/sec for 10m). |
| Quota pressure | `rate(inferencecache_tenant_evictions_total[10m])` | server `/metrics` | A tenant is exceeding `spec.quota.maxIndexEntries`. Usually fine (Fairness), but a sustained signal means the quota is too tight for the workload. |
| TTL churn | `rate(inferencecache_index_evictions_total{reason="ttl"}[10m])` | server `/metrics` | The TTL sweep is doing work. High and steady = entries arriving and aging out at a healthy rate. High and *increasing* together with `index_entries` falling = the workload is shrinking. |
| Pod RSS | `container_memory_working_set_bytes{pod="inference-cache-server-..."}` | **kubelet/cAdvisor** (not the server) | What you'd compare against the model in [Per-entry footprint](#per-entry-memory-footprint). |

### 2. Pick a starting point

1. Estimate **distinct prefix keys** for the workload using the multipliers above.
   Then derive **total storage entries** = distinct keys × R, where R is the number
   of cache replicas reporting each key. The global cap is enforced on the storage
   total, the per-tenant quota is enforced on the distinct-key total.
2. Map to a **pod memory budget**. For **single-replica** clusters, use the storage-entry
   row in the [TL;DR](#tldr) table directly. For **multi-replica** clusters, the
   storage-entry row in TL;DR is a *conservative upper bound* — extra replicas only
   cost ~50 B per shared key, not the full ~500 B/entry the table assumes. For a
   tighter number use the formula `heap ≈ 50 MiB + (500 + 50 × (R-1)) × distinct_keys`
   from [Per-entry footprint](#per-entry-memory-footprint), then add ~20 % headroom.
3. Set `evictionTTL` (per CachePolicy) to the smallest value that keeps the typical
   re-use window inside the TTL — see the [TTL choice table](#ttl-trade-offs).
4. If the workload is multi-tenant, set `CacheTenant.spec.quota.maxIndexEntries` per
   tenant. **Mind the unit mismatch:** `maxIndexEntries` is distinct prefix keys per
   tenant; the global cap is total replica×prefix entries. Starting heuristic:
   `(cluster_cap / R_replicas) × tenant_share`, where `R_replicas` is the typical
   number of cache replicas reporting each prefix. Without the `/ R_replicas` divisor,
   per-tenant quotas summed across tenants can exceed what the global cap admits.
   Leave it unset for single-tenant clusters.

### 3. Watch and adjust

After a representative workload window (typically one peak hour + one trough):

- `index_evictions_total{reason="cap"}` non-zero → the workload exceeds the global cap.
  (Do not rely on `index_entries` alone to detect this — that gauge counts distinct
  prefix keys per model, but the cap is on total replica×prefix entries, so on a
  multi-replica deployment the gauge can sit well below 1M while the cap is firing.)
  Choices: shorten `evictionTTL` (cheap), tighten per-tenant quotas (operator-controlled),
  or raise the cap (requires a server rebuild today — see [Knobs the operator actually has](#knobs-the-operator-actually-has)).
- `index_evictions_total{reason="ttl"}` ≈ 0 *and* the gateway's prefix-cache hit rate is
  low → TTL is set so high that nothing ever ages out, but routing isn't paying off.
  Either the workload doesn't reuse prefixes (no fix needed at the cache plane) or the
  KV-event subscriber isn't reporting (a separate diagnostic — check `replica_id`
  population in `/snapshot`).
- `tenant_evictions_total` non-zero on a tenant whose `status.indexEntries` is at or
  just below their quota → **this is the expected steady-state.** Ingest pushes the
  tenant over budget, the per-ingest eviction trims it back to ≤ quota, the counter
  bumps. As long as the *rate* is bounded (you're not seeing it climb without bound),
  Fairness is doing exactly what it should. Look closer only if the rate keeps rising
  alongside a stable `status.indexEntries` — that would mean the quota is too tight
  for arrivals and you're burning eviction work on every batch.
- RSS climbing with `index_entries` flat → could still be the index, in two ways:
  (1) a new replica started reporting an existing prefix set (each replica adds ~50 B
  per shared prefix, undetected by the per-distinct-prefix gauge), or (2) `index_entries`
  is unchanged but per-tenant distribution shifted. Confirm by reading `/snapshot` and
  summing `replicas[].prefixCount` — if the sum grew, it's the index. Only rule out the
  index after that check; otherwise look at metrics registry, snapshot cache, or recent
  code changes.

### 4. The escape hatch (without rebuilding)

If you genuinely need more than 1M entries and don't want to rebuild the server:

- **Shorter TTL** trims steady-state. A namespace seeing 100K new prefixes/min at
  TTL=30m steady-states at 3M; the same workload at TTL=10m steady-states at 1M.
- **Per-tenant quotas** localize the pain. A multi-tenant cluster where one noisy
  tenant fills the index → set that tenant's `spec.quota.maxIndexEntries` to bound it.

---

## Why the defaults are what they are

| Default | Value | Rationale |
|---|---|---|
| `DefaultMaxEntries` | 1,000,000 | One-pod fit for a 1 GiB server budget at ~500 B/entry (see [Per-entry footprint](#per-entry-memory-footprint) — measurements in this guide go up to 1.5M). **Not a general sizing guarantee**: the worked examples in [Workload-shape sizing](#workload-shape-sizing) show a single-tenant chatbot can land at 2.5M and a multi-tenant chatbot at 10M. Treat 1M as a sensible default for small / moderate single-tenant workloads; large or multi-tenant deployments need the workload-shape estimate, not the default. Re-evaluated against the measurements above — the value stayed; what changed is that we now document the relationship to pod memory explicitly. |
| `DefaultTTL` | 30 minutes | Matches typical chat-style prefix reuse windows. Long enough that the same conversation's continuation lands on the same replica; short enough that a half-hour of cold prefixes don't bloat the index. CachePolicy lets per-namespace workloads override (raise for long-context, lower for tight memory). |
| `DefaultSweepInterval` | 1 minute | The sweep is a full-walk over the prefix map looking for expired entries; CPU cost scales linearly with index size, but it runs on its own goroutine off the request path. One minute bounds memory lag without being noticeably hot on a 1M-entry index. Not yet directly benchmarked — file an issue if you observe sweep contention on the hot path. |

None of these are tuned per workload-shape because the realistic spread (chat at the low
end, RAG / long-context at the high end) needs more knobs than one cluster-global default
can carry. The right interface is the per-namespace/per-tenant CRs above, with the cap
as the backstop.

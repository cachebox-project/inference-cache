---
title: "Index sizing"
linkTitle: "Index sizing"
weight: 2
description: >
  How the in-memory index consumes memory, the pod-budget table, and the levers that keep it
  in bounds.
---

The server's cache-state index lives in memory. This page helps you size the server pod and
choose eviction settings so the index never approaches the pod's memory limit (an OOM-kill
is the one failure the soft-state design cannot hide).

## Pod budget at a glance

| Distinct prefix keys | Peak RSS | Recommended pod memory |
|---|---|---|
| 100K | ~110 MiB | 256 MiB |
| 500K | ~300 MiB | 512 MiB |
| 1M (the default cap) | ~540 MiB | **1 GiB (recommended floor)** |
| 1.5M | ~700 MiB | 1.5 GiB |

These include roughly 20% headroom over measured heap. The default entry cap is 1,000,000.

## Where the memory goes

Rough per-entry footprint:

- ~500 bytes per **distinct prefix key**,
- ~50 bytes per **additional replica** holding that key,
- ~50 MiB fixed Go-runtime baseline.

So a first-order estimate is:

```
heap ≈ 50 MiB + (500 + 50 × (replicas − 1)) × distinct_keys
```

The prefix-hash byte width does **not** dominate — the Go map machinery and the per-entry
`time.Time` timestamps do.

### The block-chain multiplier is the trap

The most-overlooked multiplier is block-level expansion. A prompt is not one entry — it is
one entry per block in its prefix chain. A 1000-token prompt at a 16-token block size is
~63 entries *per replica*. So:

```
distinct_keys ≈ distinct_prompts × block_chain_length × replicas
```

A workload with 5,000 distinct prompts, ~60-block chains, across 3 replicas is already
~900K keys — near the default cap. Size for the block-expanded number, not the prompt count.

{{% alert title="Two different units" color="info" %}}
`maxIndexEntries`, `tenants[].indexEntries`, and `inferencecache_index_entries{model}` count
**distinct prefix keys.** The `DefaultMaxEntries` cap counts **total storage entries** —
`(prefix_key, replica)` tuples. On a multi-replica cluster the gauge trends but is not a
direct read of cap-closeness; watch cap evictions for that.
{{% /alert %}}

## The levers

Runtime-tunable (no rebuild):

| Lever | Where | Effect |
|---|---|---|
| **Pod memory limit** | `CacheBackend.spec.resources` | The only hard ceiling. |
| **`evictionTTL`** | `CachePolicy` (default 30m) | Shorter TTL ⇒ smaller index. The cheapest lever, roughly linear. |
| **`eviction`** | `CachePolicy` (`LRU`/`LFU`) | Which entries go first under the cap. |
| **`maxIndexEntries`** | `CacheTenant.spec.quota` | Per-tenant distinct-key cap; over-budget evicts oldest (Fairness). |

Compile-time constants (need a rebuild to change): `DefaultMaxEntries = 1,000,000`,
`DefaultTTL = 30m`, `DefaultSweepInterval = 1m`.

## When you're over the cap

Three choices, cheapest first:

1. **Accept the cap** — cap eviction is normal; a stale/evicted hint is just a cache miss.
2. **Shorten `evictionTTL`** — reduces the working set roughly linearly.
3. **Raise the memory limit** (and, if needed, rebuild with a higher `DefaultMaxEntries`).

`evictionTTL: 0` or negative is rejected at admission.

## Signals to watch

| Signal | Metric | Read as |
|---|---|---|
| Index trend | `inferencecache_index_entries{model}` | Growth trend (not cap-closeness on multi-replica). |
| Cap pressure | `rate(inferencecache_index_evictions_total{reason="cap"}[10m])` | The authoritative "over budget" signal. Alert `IndexEvictionsSpike` at >10/sec. |
| Quota pressure | `rate(inferencecache_tenant_evictions_total[10m])` | A bounded non-zero rate is normal Fairness. |
| TTL churn | `inferencecache_index_evictions_total{reason="ttl"}` | Healthy background aging. |
| OOM proximity | `container_memory_working_set_bytes` | The kubelet's OOM signal — keep well under the limit. |

## Related pages

- [CachePolicy](/docs/concepts/cachepolicy/) — the TTL and eviction fields.
- [CacheTenant](/docs/concepts/cachetenant/) — the entry-count quota.
- [Observability & Alerts](/docs/administration/observability-and-alerts/) — the
  `IndexEvictionsSpike` alert.

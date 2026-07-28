---
title: "LookupRoute & ranking"
linkTitle: "LookupRoute & ranking"
weight: 8
description: >
  How the core cache-aware routing hint works — matching, ranking, fallbacks, and
  diagnostics.
---

## The routing hint

`LookupRoute` is the hot-path RPC a gateway calls for every request. Given a prompt-prefix
key plus `model_id` and `tenant_id`, it returns the replicas that hold that prefix warm,
ranked best-first, plus a `reason_code`. The gateway routes to the top replica for a prefix
cache hit — reusing KV / skipping prefill, lowering TTFT and cost — or round-robins when
there is no hint.

It is a **hint, not a command.** The gateway owns routing; inference-cache only surfaces
state. Lookups are fail-open and side-effect-free apart from metrics — an empty result and
`NO_HINT` is a perfectly valid answer, and the call never errors on the hot path.

## What the request carries

A `LookupRouteRequest` identifies the prefix in one of three ways, in precedence order:

1. **`prefix_hash`** (and, for chain matching, an ordered `block_hashes` chain with
   `block_token_counts`) — the engine-aligned key, computed by the observation path.
2. **`token_ids`** — the caller's tokenized prompt; the server computes the fingerprint.
3. **`prompt_text`** — a raw prompt; the server tokenizes and fingerprints it (requires the
   server to be built with tokenizer support and pointed at a models directory, else it
   fails open to `NO_HINT`).

It also carries `hash_scheme` (`vllm` / `sglang`) and an optional `SLO` (`ttft_ms`,
`tbt_ms`).

### The hash_scheme domain

`prefix_hash` and `block_hashes` are **engine-opaque** and matched only within a matching
`hash_scheme`. vLLM and SGLang keys stay disjoint — a vLLM hint can never match an SGLang
request. An **empty `hash_scheme` is not a valid domain**: it is dropped on ingest and
yields `NO_HINT` on lookup, so a missing tag can never collapse two engines together.

### Why a content fingerprint

vLLM's own KV-block hash is process-random (it is seeded per process and not reproducible
across replicas), so it cannot be a routing key. For the `vllm` scheme the index therefore
keys on a **deterministic content fingerprint** — XXH3-64 (seed 1337) over little-endian
token IDs per block, chained across blocks, with partial tails dropped. The same scheme is
implemented identically in the Go server, the Python benchmark gateway, and the Rust router,
and is locked by shared golden vectors. This is what lets a tokenizer-less gateway pass
`token_ids` or `prompt_text` and get a matching key.

## Matching

### Exact and longest-prefix (block-chain) matching

The baseline is an exact prefix-hash match within the scheme. With chain matching enabled,
the prefix is expressed as an ordered chain of block hashes, and the server finds the
**longest leading run** each replica holds:

- Walk the chain block by block, intersecting the set of replicas that still hold every
  block so far.
- Once a replica drops out, it is out (leading-run semantics — a later block cannot
  re-add it).
- `matched_tokens` = the sum of `block_token_counts` over the matched run.
- Freshness is the **weakest link** — the oldest `lastSeen` across the matched blocks.

Longest-prefix matching gives far higher hit rates than requiring a full exact match,
because two prompts that share a long leading prefix but diverge at the end still match on
the shared part.

## Ranking

Matched replicas are ranked by a product of factors (this is ranking v2; the Phase-1
baseline was just `matched_tokens × freshness`):

```
score = matched_tokens
      × freshness            # max(0, 1 − age/TTL): linear decay
      × pressure_factor      # max(0, 1 − PressureWeight × pressure)
      × slo_bias             # 1 + freshness × SLOTightBias  when ttft is tight
      × distinguishing_power # 1 − (replicas matching at this depth / total_replicas)
```

- **Freshness** decays linearly with age toward the eviction TTL.
- **Pressure** demotes replicas under memory pressure (reads only fresh replica stats).
- **SLO bias** slightly boosts fresher replicas when the request's `ttft_ms` is tight
  (below the configured threshold, default 200 ms). `tbt_ms` is plumbed but not yet used.
  Note the request `SLO` is a *ranking* input, not enforcement — it is distinct from
  `CachePolicy.lookupTimeoutMs`, which is the server's own time budget.
- **Distinguishing power** is the key idea for chat workloads: a prefix that *every* replica
  holds (a shared system prompt) tells you nothing about where to route, so its factor
  collapses toward 0 and it gets filtered by the score floor. A prefix only one replica
  holds has full distinguishing power.

Two policy filters bound the result (see [CachePolicy](/docs/concepts/cachepolicy/)):

1. **`minimumMatchedTokens`** (default 64) — a per-replica floor applied first.
2. **`routingFloorScore`** (default `"0.1"`) — a whole-response floor on the top score.

If any replica clears both, the reason is `PREFIX_MATCH`.

## Fallbacks

When there is no usable prefix match, the server falls back in this order:

1. **`TENANT_HOT`** — route toward replicas that are *warm for this tenant* (recent stats,
   `hit_rate` above a floor, serving the requested scheme), with `matched_tokens = 0`. Soft
   locality when the exact prefix is missing. Gated by `strategy.enableTenantHot`.
2. **`AFFINITY_HINT`** — a single stable replica chosen by a consistent hash over the
   resolved block-hash chain, modulo the scheme-aware serving replica set. Good for diffuse,
   single-turn workloads where there is no reuse to exploit but pinning still helps. Gated by
   `affinityRouting: Enabled`.
3. **`NO_HINT`** — the fail-open default. The gateway round-robins.

## Diagnostics: telling a novel prefix from a misconfiguration

A plain `NO_HINT` cannot distinguish "this prompt is genuinely new" from "the gateway is
sending the wrong contract keys" — the second is a silent, expensive misconfiguration (for
example ingesting under `hash_scheme: vllm` but looking up under `vllm-v1`, or a subscriber
reporting `tenant_id = <namespace>` while a naive gateway sends `"default"`).

So on a miss with a non-empty scheme, the server classifies the *contract key* that failed,
outer-to-inner:

| Reason code | Meaning |
|---|---|
| `UNKNOWN_TENANT` | The `tenant_id` has zero entries anywhere (and the index is not globally empty). |
| `UNKNOWN_MODEL` | The tenant is known but `(tenant, model)` has zero entries. |
| `UNKNOWN_HASH_SCHEME` | `(tenant, model)` has entries, but none under the request's `hash_scheme`. |

These are O(1) secondary-index checks. A globally empty index (cold start) and empty keys
stay `NO_HINT` — they are not mismatches. Treat `UNKNOWN_*` like an HTTP 4xx: log it, emit a
metric, fail open, and **do not retry** — it means your client configuration is wrong, not
that the server is down. The `inferencecache_lookup_route_calls_total{reason_code=…}` metric
picks up these label values automatically.

See the full list in [reason codes](/docs/reference/reason-codes/).

## Related pages

- [CachePolicy](/docs/concepts/cachepolicy/) — the knobs that tune matching and ranking.
- [The gRPC contract](/docs/concepts/grpc-contract/) — the message shapes.
- [Reason codes](/docs/reference/reason-codes/) — every code and when it is emitted.

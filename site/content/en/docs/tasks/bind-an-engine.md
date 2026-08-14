---
title: "Bind an engine"
linkTitle: "Bind an engine"
weight: 2
description: >
  The selector → webhook → typed-MP injection lifecycle and its failure modes.
---

Binding is how a `CacheBackend` claims inference-engine Pods in its namespace.
The engine Deployment and image remain inference-system owned.

## Lifecycle

1. Apply a typed `CacheBackend` with `spec.lmCache.topology: PodLocal`.
2. Create engine Pods whose labels include every
   `spec.engineSelector.matchLabels` entry.
3. At Pod CREATE, the webhook atomically injects the LMCache MP native sidecar,
   shared memory, and the runtime-specific connector wire.
4. When configured, the `kvevent-subscriber` sidecar reports KV events to the
   routing index.

{{% alert title="The match is evaluated once, at Pod CREATE" color="warning" %}}
Relabeling an existing Pod does not re-run admission. Recreate the Pod after
changing labels or CacheBackend configuration.
{{% /alert %}}

## Typed vLLM MP wire

The vLLM adapter injects `LMCacheMPConnector` with the module path
`lmcache.integration.vllm.lmcache_mp_connector`, loopback host/port,
`--disable-hybrid-kv-cache-manager`, `PYTHONHASHSEED=0`, and the fail-open
value. Reserved entries are:

- `--kv-transfer-config`;
- `--disable-hybrid-kv-cache-manager`;
- `PYTHONHASHSEED`; and
- `INFERENCECACHE_FAIL_OPEN`.

## Typed SGLang MP wire

SGLang uses `--enable-lmcache`, `--lmcache-config-file`, and
`LMCACHE_USE_EXPERIMENTAL=True`. Its reserved set is those two arguments plus
`LMCACHE_USE_EXPERIMENTAL` and `INFERENCECACHE_FAIL_OPEN`.

Both engines share the same typed `lmcache-mp-server` renderer, but their
launch surfaces are intentionally separate. LMCache currently supports only
`integration.role: ReadWrite`.

Use `spec.lmCache.chunkSizeTokens` and
`spec.lmCache.podLocal.server` for cache configuration. Do not use
engineOverrides to replace the connector wire.

## Common failure modes

| Symptom | Cause | Fix |
|---|---|---|
| `MATCHED: 0` | Selector and Pod labels differ. | Align labels and recreate the Pod. |
| Matching Pod has no injection annotation | Admission failed open on a collision, invalid Pod shape, or unavailable managed endpoint. | Inspect webhook logs and Pod Events, fix the shape, recreate the Pod. |
| Engine crash-loops after injection | The runtime-owned image lacks a compatible connector/package or has another startup failure. | Inspect engine logs and select a compatible pinned image. |
| Two CacheBackends match one Pod | Selectors overlap. | Narrow selectors; the lexicographically first name otherwise wins. |

Set `inferencecache.io/skip-inject: "true"` on the Pod template for an
intentional opt-out.

Legacy topology-less vLLM/IP injection is retained only for compatibility tests
until Phase 7 and is not a current binding recipe.

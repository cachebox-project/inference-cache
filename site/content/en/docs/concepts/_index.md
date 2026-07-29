---
title: "Concepts"
linkTitle: "Concepts"
weight: 4
description: >
  Core inference-cache concepts
no_list: true
---

This section explains the components, APIs, and abstractions inference-cache uses to make
LLM inference routing cache-aware.

## Start here

### [Architecture]({{< relref "/docs/concepts/architecture/" >}})

The two control-plane binaries (controller + server), the observation sidecar, and the
bidirectional in-cluster bridge that connects them — and the invariants (fail-open, soft
state, "we hint, the gateway decides") that shape every design decision.

## The API — Custom Resources

inference-cache defines six CRDs in the group `inferencecache.io`, version `v1alpha1`.
Five are namespaced; `CacheIndex` is the only cluster-scoped type. Controllers exist for
`CacheBackend` and `CacheIndex` today; the others are declarative.

### [CacheBackend]({{< relref "/docs/concepts/cachebackend/" >}})

The primary resource an operator writes. It binds to inference-engine pods by label,
provisions a managed cache-server workload, and makes the engine's KV cache reusable across
requests. Short name `cb`.

### [CachePolicy]({{< relref "/docs/concepts/cachepolicy/" >}})

Opt-in, per-namespace tuning of cache lookup and eviction — matched-token floors, score
floors, timeouts, eviction algorithm and TTL, and the ranking strategy gates. Short name
`cpol`.

### [CacheTenant]({{< relref "/docs/concepts/cachetenant/" >}})

Gives an external tenant a stable identity the index isolates on, plus an optional
entry-count quota. Short name `ct`.

### [CacheIndex]({{< relref "/docs/concepts/cacheindex/" >}})

A cluster-scoped, status-only singleton that mirrors the server's in-memory aggregate — the
cache "world map" for observability. Short name `ci`.

### [PromptTemplate]({{< relref "/docs/concepts/prompttemplate/" >}})

Declares a cache-aware prompt template and its stable/mutable slots for the mutable-slot
render pipeline. Short name `pt`.

### [PDTopology]({{< relref "/docs/concepts/pdtopology/" >}})

Declares prefill/decode topology for phase-disaggregated serving. Short name `pdt`.

## The data plane API — gRPC

### [LookupRoute & ranking]({{< relref "/docs/concepts/lookuproute/" >}})

How the core cache-aware routing hint works: prefix matching, longest-prefix block-chain
matching, the ranking formula, `TENANT_HOT` and affinity fallbacks, and the diagnostic
reason codes.

### [The gRPC contract]({{< relref "/docs/concepts/grpc-contract/" >}})

The `InferenceCache` service — its eight RPCs, message shapes, and the guarantees
(fail-open, metadata-only, string reason codes, additive deltas) that let clients depend on
it.

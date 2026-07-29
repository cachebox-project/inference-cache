---
title: "Overview"
linkTitle: "Overview"
weight: 1
description: >
  Why inference-cache?
---

**inference-cache** is a vendor-neutral, open-source, Kubernetes-native **cache-policy
control plane for LLM inference**. It makes routing and cache decisions *cache-aware* so
that requests land on replicas that already hold the prompt's KV cache warm — turning a
prefix cache hit into lower time-to-first-token (TTFT) and lower cost.

It **orchestrates** the KV-cache technology you already run (LMCache, Mooncake) rather than
replacing it. inference-cache is **not** a new distributed cache, and it is **not** the
data-plane gateway. It is the brain that decides *where a request should go*; the gateway
follows the hint.

## Guiding principle: "we decide routing; the gateway follows"

All routing intelligence — replica scoring, tie-breaks, threshold filtering, freshness
ranking — lives in one place: the inference-cache **server**. The gateway stays
deliberately dumb:

1. Tokenize the prompt (or hand the raw prompt to the server).
2. Call `LookupRoute`.
3. Route to the returned replica.
4. Round-robin when there is no hint.

Centralizing the decision means one place to evolve ranking algorithms, one implementation
for every gateway client, and no routing logic fragmented across clients.

## Features

- 🧠 **Cache-aware routing** — a `LookupRoute` gRPC hint returns which replicas hold a
  prompt-prefix warm, ranked by matched tokens × freshness (and, with ranking v2, replica
  pressure, SLO, and distinguishing power).

- 🔌 **Engine integration by admission webhook** — label an engine pod and the mutating Pod
  webhook injects the KV-connector configuration automatically. The observation sidecar is
  also injected when the controller's opt-in `--kvevent-subscriber-image` is configured.

- 🧩 **Pluggable KV-cache backends** — the default in-memory LMCache backend for the simple
  path; Mooncake for a durable, shared, peer-to-peer store; or point at an `External`
  endpoint you manage.

- 🏠 **Multi-tenant by construction** — the cache-state index is keyed by
  `(tenant, model, hash_scheme, prefix_hash)`, so tenants' hints can never collide, and an
  optional entry-count quota caps a tenant's index footprint.

- 🛟 **Fail-open and soft-state** — the cache is an optimization, never a serving
  dependency. Lost state degrades to a cache miss, never a wrong answer. The hot path never
  errors.

- 📊 **First-class observability** — Prometheus metrics (`inferencecache_*`) on both
  binaries, an opt-in alert bundle for the silent-failure patterns this system has hit in
  production, a cluster-wide `CacheIndex` you can `kubectl get`, and a read-only
  `inferencecache doctor` pre-flight diagnostic.

- 🔒 **Secure by default posture** — the server fails closed on startup unless auth is
  configured; controller↔server bridges are ServiceAccount-token + audience-bound; gRPC TLS
  ships as an opt-in overlay.

## Architecture

inference-cache is one operator split across **two control-plane binaries** plus a CLI and
an in-cluster sidecar:

| Component | Role |
|---|---|
| **`inferencecache-controller`** | Reconciles the CRDs, serves the admission webhooks (defaulting/validation + the mutating Pod webhook that injects engine config), and runs the bridge that mirrors server state into `CacheIndex` and pushes resolved policy to the server. |
| **`inferencecache-server`** | Serves the `InferenceCache` gRPC API (`LookupRoute`, `RenderTemplate`, …), holds the in-memory cache-state index, and exposes health, metrics, and the controller-facing snapshot/policy/probe endpoints. |
| **`inferencecache` CLI** | `doctor` runs a read-only pre-flight diagnostic across the install. |
| **`kvevent-subscriber`** | A sidecar injected next to each engine pod that consumes the engine's KV-cache event stream and reports it to the server. |

### Data flow

```
                    ┌─────────────────────────────────────────────┐
                    │              inference-cache-server          │
   gateway ──gRPC──▶│  LookupRoute / RenderTemplate  (:9090)       │
   (dumb)  ◀─hint───│                                              │
                    │  in-memory cache-state index                 │
                    └───▲─────────────────────────────▲────────────┘
      ReportCacheState  │ (gRPC :9090)      /snapshot  │  /policy (:8081)
                        │                    (pull)    │  (push)
             ┌──────────┴───────┐          ┌───────────┴──────────────┐
             │ kvevent-subscriber│         │ inferencecache-controller │
             │  (sidecar in the  │         │  reconciles CRDs,         │
             │   engine pod)     │         │  writes CacheIndex.status │
             └──────────▲────────┘         │  serves admission webhooks│
              ZMQ KV     │                 └───────────────────────────┘
              events     │
             ┌───────────┴───────┐
             │  inference engine │  (vLLM / SGLang, labeled to a CacheBackend)
             │  + KV-cache tier  │
             └───────────────────┘
```

- Engine pods publish **KV-cache events** (block stored/removed) over ZMQ. The
  `kvevent-subscriber` sidecar computes a deterministic content fingerprint in-pod and calls
  `ReportCacheState`, keeping the server's index current.
- The gateway calls `LookupRoute` on the hot path and routes to the returned replica for a
  prefix cache hit.
- The controller runs a **bidirectional in-cluster bridge**: it *pulls* the server's
  aggregate (`GET /snapshot`) to populate the cluster-wide `CacheIndex`, and *pushes*
  operator intent (`POST /policy`) from `CachePolicy`/`CacheTenant` resources.

## Where to go next

- New here? Start with the [Installation]({{< relref "/docs/installation/" >}}) guide, then the
  [Quickstart]({{< relref "/docs/tasks/deploy-a-cache-backend/" >}}).
- Want the mental model? Read [Concepts]({{< relref "/docs/concepts/" >}}) — the CRDs, the gRPC contract,
  and how `LookupRoute` ranks replicas.
- Operating a cluster? See [Administration]({{< relref "/docs/administration/" >}}) for observability,
  index sizing, TLS, and troubleshooting.
- Need field-level detail? The [Reference]({{< relref "/docs/reference/" >}}) section covers the CRD API,
  the gRPC contract, metrics, reason codes, and the CLI.

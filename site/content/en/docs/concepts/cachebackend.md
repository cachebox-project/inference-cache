---
title: "CacheBackend"
linkTitle: "CacheBackend"
weight: 2
description: >
  Bind inference-engine Pods to a typed cache data plane and expose cache-aware routing state.
---

## What is a CacheBackend?

A namespaced `CacheBackend` selects an inference runtime, its engine-side cache
integration, and an optional remote L3. The inference system owns the engine
Deployment and image. CacheBackend injects the selected connector and the cache
components it owns into matching Pods.

Current LMCache offload uses typed PodLocal multiprocess mode:

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: llama3-cache
  namespace: serving
spec:
  runtime: VLLM
  type: LMCache
  integration:
    role: ReadWrite
  engineSelector:
    matchLabels:
      app: llama3-vllm
  lmCache:
    topology: PodLocal
    chunkSizeTokens: 256
    podLocal:
      server:
        image: docker.io/lmcache/standalone@sha256:b813bf0bb616d1012b6a6edcbd4a44f1576dbbdaa857962e56d48b9f7c127d13
        port: 5555
        l1Capacity: 4Gi
        maxWorkers: 4
        resources:
          requests:
            cpu: "1"
            memory: 5Gi
          limits:
            cpu: "2"
            memory: 6Gi
  observation:
    modelID: meta-llama/Llama-3.1-8B-Instruct
```

Omitting `remoteStorage` intentionally selects host-only MP. L1 is per engine
Pod; add an explicit managed or external Redis L3 when cross-Pod sharing is
required.

{{% alert title="Runtime-owned engine image" color="info" %}}
CacheBackend never rewrites the engine image. Normal engine initialization is
the authoritative check that the image contains a compatible LMCache client.
PodLocal native sidecars require Kubernetes 1.29 or newer.
{{% /alert %}}

## Cache types and remote storage

| Type | Current behavior |
|---|---|
| `LMCache` | Typed PodLocal MP for vLLM or SGLang; optional Redis L3. |
| `SGLangHiCache` | SGLang's native engine-local host cache; no remote binding. |

`remoteStorage` is optional L3 only. Redis may be `Managed` or `External`.
The removed topology-less IP providers are not accepted or automatically
mapped to Redis because that would change sharing semantics.

For `Managed` Redis, `remoteStorage.workload` controls provider Pod scheduling
and security. The built-in managed Redis is a standalone singleton; a future
managed Redis Cluster requires provider-specific shard and replica semantics,
not a generic Deployment replica count. Mooncake is likewise planned as a new
typed MP L2 adapter rather than a restoration of its former IP wire.

## Engine integration

| Field | Meaning |
|---|---|
| `mode` | `Offload` by default; `EventsOnly` observes KV events without injecting a cache connector. |
| `role` | LMCache currently admits only `ReadWrite`; directional roles are future work. |
| `failOpen` | Defaults to `true`; remote L3 loss degrades independently from the required PodLocal connector. |
| `engineOverrides` | Amends non-reserved engine args/env. Use typed `lmCache` fields for MP configuration. |

The webhook binds Pods by `spec.engineSelector.matchLabels` at Pod CREATE. A
Pod can opt out with `inferencecache.io/skip-inject: "true"`. Recreate Pods
after changing binding or cache configuration.

## Readiness and status

Typed MP exposes connector and remote-storage health separately:

- `ConnectorReady` covers selected engines and their required PodLocal MP
  servers;
- `RemoteStorageReady` is present only when a Redis L3 is configured; and
- `Ready` composes the implemented readiness and observation gates.

`status.remoteStorage.endpoint` exists only when a remote L3 is configured. It
never publishes the loopback connector address. Other useful fields include
`matchedEnginePods`, `connector`, `remoteStorage`,
`indexParticipation`, `conditions`, and `observedGeneration`.

## Related pages

- [Bind an engine]({{< relref "/docs/tasks/bind-an-engine/" >}})
- [CachePolicy]({{< relref "/docs/concepts/cachepolicy/" >}})
- [CRD API reference]({{< relref "/docs/reference/crd-api/" >}})

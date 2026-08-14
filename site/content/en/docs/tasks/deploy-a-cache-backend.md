---
title: "Deploy a CacheBackend"
linkTitle: "Deploy a CacheBackend"
weight: 1
description: >
  The five-minute quickstart — the minimum CacheBackend, what it wires up, and how to read
  its readiness.
---

Get a cache-aware inference setup running in about five minutes. This assumes the
inference-cache operator (controller + policy server + CRDs) is already
[installed]({{< relref "/docs/installation/" >}}).

## 1. Write the minimum CacheBackend

A `CacheBackend` binds to your inference-engine pods by label and makes their KV cache
reusable across requests. Here is the minimum-viable spec:

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: my-cache
spec:
  runtime: VLLM
  type: LMCache                 # backing cache implementation
  engineSelector:
    matchLabels:
      app: my-engine            # must match your engine pods' labels
  lmCache:
    topology: PodLocal
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
    modelID: Qwen/Qwen2.5-0.5B-Instruct
```

Omitting `remoteStorage` intentionally selects host-only PodLocal MP. The
readiness gate's `observation.firstEventTimeout` defaults to `5m`, and
`integration.failOpen` is treated as `true`.

{{% alert title="One label does the binding" color="warning" %}}
The value under `engineSelector.matchLabels` must also appear on your engine pods' template
labels. That label match is what lets the mutating Pod webhook inject the cache wiring at
pod CREATE. Drift them apart and the engine runs uncached — `kubectl get cachebackend` then
shows `MATCHED: 0`.
{{% /alert %}}

## 2. Add engine pods (copy a recipe)

The CacheBackend injects an MP server into matching Pods but does not replace
the engine image. For a working end-to-end setup, supply a connector-compatible
image and publish KV events. A paired repository shape is:

```bash
kubectl apply -f \
  https://raw.githubusercontent.com/cachebox-project/inference-cache/main/config/samples/cachebackend-with-engine.yaml
```

That file ships a typed host-only CacheBackend plus a matching vLLM Deployment.
Normal engine startup is the authoritative connector/package compatibility
check. Acting on `LookupRoute` hints remains the gateway's job.

The recipe catalog under `config/samples/` includes CPU dev, GPU production, external cache,
multi-tenant, and engine-tuning scenarios.

## 3. Enable KV-event observability

The `kvevent-subscriber` sidecar publishes the KV events used by the readiness and
observability surfaces below. Auto-attach is disabled by default. Enable it before creating
engine pods, replacing `<tag>` with the same release or development tag used for the other
inference-cache images:

```bash
kubectl -n inference-cache-system patch deployment \
  inference-cache-controller-manager --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kvevent-subscriber-image=ghcr.io/cachebox-project/inference-cache-subscriber:<tag>"}]'
kubectl -n inference-cache-system rollout status \
  deployment/inference-cache-controller-manager
```

The Pod webhook runs only on CREATE. If the engine Deployment already exists, recreate its
pods after the controller rollout:

```bash
kubectl rollout restart deployment/qwen-engine
kubectl rollout status deployment/qwen-engine
```

Finally, send one request so vLLM publishes the first KV event. Keep the port-forward running
in one terminal:

```bash
kubectl port-forward deployment/qwen-engine 8000:8000
```

Then call the engine from another:

```bash
curl http://127.0.0.1:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"Qwen/Qwen2.5-0.5B-Instruct","messages":[{"role":"user","content":"Hello"}],"max_tokens":8}'
```

Without the subscriber, engine↔cache wiring (KV reuse) still works, but no KV events are
reported: a managed backend holds at `Ready=False` (`AwaitingFirstKVEvent`) and then, after
`firstEventTimeout`, flips to `Ready=False` (`NoKVEventsObserved`) with `Degraded=True`.
External backends are exempt from this gate.

## 4. Read the readiness

```
$ kubectl get cachebackend
NAME       TYPE      READY   MATCHED   ENDPOINT   PREFIXES   LASTEVENT   AGE
my-cache   LMCache   True    1         <none>     128        12s         3m
```

- `MATCHED` — the number of engine pods the selector binds.
- `PREFIXES` / `LASTEVENT` — evidence the cache is actually receiving state.
- `READY` flips to `True` only after all three gates pass:

`READY` composes **managed-readiness → KV-event gate → functional-probe gate**. When it is
not `True`, the reason lives in `.status.conditions[]`:

```bash
kubectl get cachebackend my-cache -o yaml | yq '.status.conditions'
```

| Condition / reason | What it means |
|---|---|
| `Ready=False` / `AwaitingFirstKVEvent` | No KV event seen yet — usually the subscriber image is unset, or the engine hasn't served a request. |
| `Ready=False` / `NoKVEventsObserved` + `Degraded=True` | `firstEventTimeout` elapsed with no event. Same diagnosis as above. |
| `Ready=False` / `ProbeIngestFailed \| ProbeRoutingFailed \| ProbeT2Failed` | The synthetic functional round-trip failed at that stage. Read the condition `.message` — the controller embeds the server's stage diagnostic. |
| `FunctionalProbeOK=Unknown` / `ProbeError` | The controller couldn't reach the server's `/probe` endpoint. Transient outages don't flap `Ready` — unless a real stage failure was already published (that stays sticky). |
| `FunctionalProbeOK=True` / `ProbeBypassed` | Someone set `inferencecache.io/skip-functional-probe: "true"`. Remove it once you no longer need the bypass. |

See [Troubleshooting]({{< relref "/docs/administration/troubleshooting/" >}}) for the full per-reason runbook.

## What you get

Once the backend is Ready and engine pods are bound, three things are live:

- **Cache-aware routing** — the server answers `LookupRoute` with which replicas hold which
  prefixes warm, so a gateway can route for a prefix cache hit.
- **KV reuse** — matched engine Pods get the typed MP wiring injected
  automatically; host-only L1 is per Pod, and Redis L3 is optional.
- **Observability** — `kubectl get cachebackend` and the cluster-wide `CacheIndex` surface
  live state.

## Next steps

- [Bind an engine]({{< relref "/docs/tasks/bind-an-engine/" >}}) — the selector → webhook → injection model
  and its failure modes.
- [Tune lookup and eviction]({{< relref "/docs/tasks/tune-lookup-and-eviction/" >}}) — per-namespace
  `CachePolicy`.
- [CacheBackend reference]({{< relref "/docs/concepts/cachebackend/" >}}) — every field.

---
title: "Bind an engine"
linkTitle: "Bind an engine"
weight: 2
description: >
  The selector → webhook → injection lifecycle, the reserved args/env you cannot override,
  and the ways binding goes wrong.
---

Binding is how a `CacheBackend` claims inference-engine pods and injects the KV-connector
wiring into them. Three actors participate:

- **`CacheBackend`** — its `spec.engineSelector.matchLabels` is a label selector over pods
  in the same namespace, with the same semantics as `Service.spec.selector`.
- **Engine pod** — a vLLM or SGLang pod, typically owned by a user-managed Deployment. Its
  `template.metadata.labels` are what the selector matches.
- **The mutating Pod webhook** — intercepts pod CREATE, matches the selector, and stamps the
  engine container with the KV-connector env and CLI args (and, when enabled, the
  `kvevent-subscriber` sidecar).

## The lifecycle

1. **Apply the CacheBackend.** The reconciler provisions the managed cache-server Deployment
   + Service and publishes the address in `status.endpoint`.
2. **Deploy the engine** with pod-template labels that include every key/value in
   `spec.engineSelector.matchLabels`.
3. **The webhook claims matching pods** — but only once `status.endpoint` is populated. It
   injects the LMCache env, the `--kv-transfer-config` arg, and stamps
   `inferencecache.io/injected-by: <ns>/<name>`. When the controller runs with
   `--kvevent-subscriber-image` set **and** the backend has `backendConfig.model`, it also
   appends the subscriber sidecar.
4. **KV events flow** (when the sidecar is present) into the server's index and surface in
   `CacheBackend.status`.

{{% alert title="The match is evaluated once, at pod CREATE" color="warning" %}}
Relabeling an existing pod does **not** re-evaluate it, and the wiring is sticky to the
pod's lifetime. To rewire after changing labels, delete the pod (the Deployment recreates
it) or `kubectl rollout restart` the Deployment.
{{% /alert %}}

## The one-label rule (annotated)

The selector value must appear in two places — that is the whole binding:

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: qwen-demo-cache       # CR name — must differ from the engine Deployment name
spec:
  type: LMCache
  integration:
    engine: vllm
    role: ReadWrite
  engineSelector:
    matchLabels:
      app: qwen-demo          # selector key/value (1 of 2)
  backendConfig:
    model: Qwen/Qwen2.5-0.5B-Instruct
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: qwen-engine           # must differ from the CR name (the CR reconciles into a
                              # cache-server Deployment named after the CR — sharing names collides)
spec:
  selector:
    matchLabels:
      app: qwen-demo
  template:
    metadata:
      labels:
        app: qwen-demo        # selector key/value (2 of 2) — this is what the webhook sees
    spec:
      containers:
        - name: vllm
          image: vllm/vllm-openai-cpu:latest-x86_64
          args: ["--model", "Qwen/Qwen2.5-0.5B-Instruct"]
```

{{% alert title="Matched > 0 is not proof of injection" color="warning" %}}
`status.matchedEnginePods` counts pods whose *labels* match. If `status.endpoint` was empty
when a pod was admitted (engine applied before the cache server was ready), the webhook
fail-opens and the pod is admitted **unwired** — yet still counted. The authoritative wiring
signals are the per-pod `inferencecache.io/injected-by` annotation and the
`InjectedByCacheBackend` Event on the pod. Recovery is `kubectl rollout restart`.
{{% /alert %}}

## What gets injected, and what you can override

The webhook injects a canonical set of args and env. Some entries are **reserved** — they
carry correctness guarantees, and `spec.integration.engineOverrides` is **hard-rejected at
admission** if it touches them.

### vLLM + LMCache

Always injected (reserved — not overridable):

- `--kv-transfer-config '{"kv_connector":"LMCacheConnectorV1","kv_role":"<role>"}'`
  (`role` maps from `integration.role`: ReadOnly→`kv_consumer`, WriteOnly→`kv_producer`,
  ReadWrite→`kv_both`)
- `LMCACHE_REMOTE_URL=lm://<endpoint>`
- `VLLM_USE_V1=1`
- `INFERENCECACHE_FAIL_OPEN=<bool>`
- `PYTHONHASHSEED=0` — a correctness invariant (pins the engine's hash seed so LMCache
  reloads match under tensor parallelism)

Tunable via `backendConfig` (not overrides):

| `backendConfig` key | Env var | Default |
|---|---|---|
| `chunkSize` | `LMCACHE_CHUNK_SIZE` | 256 |
| `remoteSerde` | `LMCACHE_REMOTE_SERDE` | naive |
| `localCPU` | `LMCACHE_LOCAL_CPU` | False |
| `maxLocalCPU` | `LMCACHE_MAX_LOCAL_CPU_SIZE` | 20 GiB |

### SGLang + LMCache

Reserved args: `--enable-lmcache`. Reserved env: `LMCACHE_REMOTE_URL`,
`LMCACHE_USE_EXPERIMENTAL`, `INFERENCECACHE_FAIL_OPEN`. Only `role: ReadWrite` is supported.

{{% alert title="Known limitation — SGLang tier-2 data plane" color="warning" %}}
The `hash_scheme: sglang` routing path and the `--enable-lmcache` flag are correct, but the
current LMCache *data plane* wiring for SGLang is not — SGLang uses a node-local MP-mode
worker that reads a config file rather than the injected `LMCACHE_*` env, so a `(sglang,
LMCache)` backend can reconcile `Ready` while offloading nothing (admission emits a warning).
The MP-mode fix is designed but not yet shipped. Use vLLM for tier-2 offload today.
{{% /alert %}}

### Overriding safely

```yaml
spec:
  integration:
    engineOverrides:
      args: ["--max-model-len", "8192"]     # add
      suppressArgs: ["--some-default-arg"]   # remove a non-reserved canonical arg
      env:
        - name: MY_TUNABLE
          value: "1"
      suppressEnv: ["SOME_DEFAULT_ENV"]
```

Overriding a reserved arg/env is rejected — the rejection is the point (a silently-ignored
warning would let the engine crash later with no breadcrumb).

## Opting a pod out

Set `inferencecache.io/skip-inject: "true"` on the **pod template** (not the Deployment).
The pod is admitted vanilla, stamped `inferencecache.io/inject-skipped`, and gets a
`SkippedByOperator` Event so an intentional opt-out is distinguishable from selector drift.

## Common failure modes

| Symptom | Cause | Fix |
|---|---|---|
| Engine runs uncached, `Matched: 0` | Selector and pod labels don't overlap | Reconcile the label sets on the CR or the Deployment template. |
| Two CacheBackends match one pod | Overlapping selectors | The webhook picks the lexicographically-first CR by name; narrow the selectors so each pod matches exactly one. |
| Relabeled pod still uncached | Match is CREATE-only | Delete the pod; the Deployment recreates it. |
| Old wiring after deleting the CR | Wiring is sticky to pod lifetime | Rolling-restart the engine Deployment. |
| `Matched > 0` but no injection | `status.endpoint` empty at admission | `kubectl rollout restart`; check `injected-by` annotation. |

## Related pages

- [CacheBackend]({{< relref "/docs/concepts/cachebackend/" >}}) — the resource in full.
- [Troubleshooting]({{< relref "/docs/administration/troubleshooting/" >}}) — the readiness runbook.

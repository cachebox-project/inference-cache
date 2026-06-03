# Quickstart

Get a cache-aware inference setup running in about five minutes. This page
shows the minimum CacheBackend you write today, what it wires up, and where to
go next. It assumes the inference-cache operator (controller + policy server +
CRDs) is already installed in the cluster.

## 5-minute quickstart

A `CacheBackend` binds to your inference engine pods by label and makes their
KV cache reusable across requests. Here is the minimum-viable spec:

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: my-cache
spec:
  type: LMCache                 # backing cache implementation
  integration:
    engine: vllm                # optional — defaults to vllm
  engineSelector:
    matchLabels:
      app: my-engine            # must match your engine pods' labels
  backendConfig:
    model: Qwen/Qwen2.5-0.5B-Instruct
```

That is the whole CacheBackend. Everything else is defaulted: `spec.replicas`
becomes `1`, the readiness gate's `firstEventTimeout` becomes `5m`, and
`integration.failOpen` is treated as `true` (the cache is an optimization,
never a serving dependency). Field-by-field defaulting work in progress will
shrink this further over time; for now `type` and the three keys above are what
you set.

> **One label does the binding.** The value under
> `engineSelector.matchLabels` must also appear on your engine pods'
> template labels. That label match is what lets the mutating Pod webhook
> inject the cache wiring at pod CREATE. Drift them apart and the engine runs
> uncached — `kubectl get cachebackend` then shows `MATCHED: 0`.

The CacheBackend on its own provisions the managed cache server. To get a
**working end-to-end setup** you also need engine pods carrying that label and
publishing KV events. Rather than hand-assemble that here, copy a runnable
recipe — the fastest is the CPU dev path, which needs no GPU:

```bash
kubectl apply -f config/samples/recipe-cpu-dev.yaml
```

That single file ships the CacheBackend above plus a matching tiny-model vLLM
engine Deployment, with the engine wired to the cache (KV offload/reuse) and the
backend producing `LookupRoute` hints. Acting on those hints to actually route
requests is the gateway's job, which integrates separately — so this recipe is
the cache half, not a full gateway round-trip. (On a cold cluster the first
engine pod can race ahead of the cache server's endpoint being published; if so,
wait for the endpoint and `kubectl rollout restart` the engine — see the comment
at the top of the recipe.)

> **One install-time prerequisite for observability.** The piece that publishes
> KV events — the `kvevent-subscriber` sidecar — is only auto-attached when the
> controller runs with `--kvevent-subscriber-image` set, which is **empty by
> default**. Engine↔cache wiring (KV reuse) works without it, but until it is
> set no KV events are reported, so the backend stays `Ready=False`
> (`AwaitingFirstKVEvent`) and `PREFIXES` stays `0`. Set that flag on the
> controller to get the Ready/observability surface below.

## What you get

Once the backend is Ready and engine pods are bound, three things are live:

- **Cache-aware routing.** The policy server records which engine replicas hold
  which prompt prefixes warm and answers `LookupRoute` with that hint, so a
  gateway can route a request to a replica that already has its prefix cached —
  lower time-to-first-token, less recompute. (inference-cache provides the
  hint; the gateway owns the routing decision.)
- **KV reuse.** Matched engine pods get the LMCache wiring injected
  automatically, so their KV cache is offloaded to and reused from the managed
  cache backend instead of being recomputed per request.
- **Observability.** `kubectl get cachebackend` surfaces the live state:

  ```
  $ kubectl get cachebackend
  NAME            TYPE      READY   MATCHED   ENDPOINT              PREFIXES   LASTEVENT   AGE
  my-cache        LMCache   True    1         my-cache.default...   128        12s         3m
  ```

  `READY` flips to `True` only after a real KV event is observed (not merely
  when the pod is reachable), `MATCHED` is the engine-pod count the selector
  binds, and `PREFIXES` / `LASTEVENT` show the cache actually receiving state.

## Next steps

- **Recipe catalog** — copy-paste a curated scenario:
  [config/samples/README.md](../config/samples/README.md). Includes CPU dev,
  GPU production, external cache, multi-tenant, and engine tuning.
- **Engine binding mental model** — how the selector → webhook → injection
  lifecycle works and its failure modes:
  [concepts/cachebackend-engine-binding.md](concepts/cachebackend-engine-binding.md).
- **Tuning engine injection** — the `engineOverrides` surface (amend the
  injected args/env without losing the integration):
  [concepts/cachebackend-engine-overrides.md](concepts/cachebackend-engine-overrides.md).
- **Per-namespace lookup/eviction tuning** — configured on `CachePolicy`
  (`evictionTTL`, `minimumPrefixTokens`, `lookupTimeoutMs`). Until a dedicated
  concept doc lands, `kubectl explain cachepolicy.spec` is the field reference;
  the `recipe-gpu-production.yaml` recipe shows a production CachePolicy.
- **Full field reference** — `kubectl explain cachebackend.spec` (and
  `cachepolicy.spec`, `cachetenant.spec`) for every field and its defaults.

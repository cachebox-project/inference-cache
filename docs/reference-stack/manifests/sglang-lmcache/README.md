# SGLang + typed LMCache multiprocess reference

This manifest is the SGLang sibling of the vLLM reference. A typed
`CacheBackend` selects the engine pod and inference-cache injects:

- the `lmcache-mp-server` native sidecar;
- a shared memory-backed L1 and generated MP configuration;
- SGLang's LMCache enablement/config-file arguments; and
- optional Redis L3 arguments from the explicit `remoteStorage` choice.

The manifest does not hand-author those fields. This keeps the reference on the
same production renderer exercised by admission and controller tests.

## Scope and prerequisites

- Kubernetes 1.29 or later, because the MP server is a native sidecar.
- One NVIDIA GPU; this TP=1 reference has no supported CPU fallback.
- The inference-cache controller and mutating webhook installed first.
- A digest-pinned SGLang engine image containing an LMCache client compatible
  with the pinned MP-server sidecar. The manifest's all-zero digest is
  deliberately non-pullable.
- A Hugging Face token for the gated model.

CacheBackend does not own or replace the engine image. It also does not use an
image allowlist, a capability annotation, or a verifier init container. Engine
startup is the authoritative connector/package compatibility check.

## Topology

```text
SGLang engine container
        |
        | loopback MP control + shared /dev/shm data
        v
injected lmcache-mp-server native sidecar
        |
        | RESP, selected explicitly
        v
external Redis Service in this manifest
```

Redis is an explicit operator choice. Omitting `remoteStorage` produces a
host-only MP backend. A removed LMCacheServer or legacy IP-wired Mooncake
configuration is not silently converted to Redis because that would change
cross-Pod/L3 semantics. Mooncake requires a future typed MP L2 adapter.

## Deploy

From `docs/reference-stack`:

```bash
kubectl apply -f manifests/namespace.yaml
kubectl -n cache-substrate create secret generic hf-token \
  --from-literal=token="$HF_TOKEN"

# Install inference-cache first, replace the engine placeholder digest in the
# manifest, then create Redis, CacheBackend, and the matching engine Deployment.
kubectl apply -f manifests/sglang-lmcache/deployment.yaml
kubectl -n cache-substrate rollout status deploy/redis-l2 --timeout=5m
kubectl -n cache-substrate rollout status \
  deploy/sglang-lmcache-llama-8b --timeout=20m
```

Confirm admission injected the sidecar and connector wiring:

```bash
kubectl -n cache-substrate get cachebackend sglang-lmcache-llama-8b -o yaml
kubectl -n cache-substrate get pod -l app=sglang-lmcache-llama-8b \
  -o jsonpath='{.items[0].spec.initContainers[*].name}{"\n"}'
kubectl -n cache-substrate logs deploy/sglang-lmcache-llama-8b \
  -c lmcache-mp-server --tail=100
```

Expected operator surfaces include the `inferencecache.io/injected-by`
annotation, `ConnectorReady=True`, and `RemoteStorageReady=True` after Redis is
reachable. Redis loss should change the remote-storage condition without
rolling the engine pod.

## Functional check

Port-forward the engine API and send the same long prefix twice, clearing only
the engine-local cache between the store and retrieve steps if you are proving
LMCache reuse. Keep the request model equal to the engine model and
`CacheBackend.spec.observation.modelID`.

```bash
kubectl -n cache-substrate port-forward \
  svc/sglang-lmcache-llama-8b 30000:30000

curl -sS http://127.0.0.1:30000/health
```

The authoritative Phase 3 live evidence is recorded in
[`../../VERSIONS.md`](../../VERSIONS.md) and the
[migration roadmap](../../../design/lmcache-multiprocess-migration-roadmap.md).
It validated the controller-rendered SGLang TP=1 path, including host-only
retrieve, bounded L1 eviction, real events/status, and Redis retrieval by a
replacement engine pod. This hand manifest has not independently completed the
same GPU run with its placeholder replaced.

## Known limits

SGLang TP>1, multi-node execution, MLA, directional producer/consumer roles,
and MP-server restart/re-registration are outside this phase. Only
`spec.integration.role: ReadWrite` is supported.

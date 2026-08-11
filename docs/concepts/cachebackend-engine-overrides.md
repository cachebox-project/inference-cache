# CacheBackend engine overrides

`spec.integration.engineOverrides` amends the runtime-owned engine container
after a runtime adapter renders its canonical connector wire. Use typed
CacheBackend fields for cache topology and MP-server settings; use overrides
only for engine arguments or environment values that are not part of that
wire.

If a Pod should receive no cache injection, set
`inferencecache.io/skip-inject: "true"` on its template instead.

## The four primitives

| Primitive | Semantics |
|---|---|
| `env` | Upsert an adapter-contributed environment variable by name, or append a new name. It does not rewrite unrelated Pod-template environment. |
| `suppressEnv` | Remove a non-reserved adapter-contributed environment variable. |
| `args` | Replace an adapter-contributed flag by leading token, or append a new flag absent from both the adapter and Pod template. |
| `suppressArgs` | Remove a non-reserved adapter-contributed flag. |

Entries that overlap the selected adapter's reserved arguments or environment
are rejected at CacheBackend admission. This prevents an override from silently
disconnecting the engine from the CacheBackend contract.

## Typed vLLM PodLocal MP baseline

The current vLLM LMCache adapter injects:

```yaml
args:
  - --kv-transfer-config
  - '{"kv_connector":"LMCacheMPConnector","kv_connector_module_path":"lmcache.integration.vllm.lmcache_mp_connector","kv_role":"kv_both","kv_connector_extra_config":{"lmcache.mp.host":"tcp://127.0.0.1","lmcache.mp.port":"5555"}}'
  - --disable-hybrid-kv-cache-manager
env:
  - name: PYTHONHASHSEED
    value: "0"
  - name: INFERENCECACHE_FAIL_OPEN
    value: "true"
```

It also injects the typed `lmcache-mp-server` native sidecar and shared
`/dev/shm`. These MP settings are not engineOverrides:

- `spec.lmCache.chunkSizeTokens` controls chunk size;
- `spec.lmCache.podLocal.server.l1Capacity` controls per-Pod host capacity;
- `spec.lmCache.podLocal.server.image`, `port`, `maxWorkers`, and `resources`
  control the injected server; and
- `spec.remoteStorage` explicitly selects an optional Redis L3.

The vLLM MP reserved set is:

- arguments: `--kv-transfer-config`, `--disable-hybrid-kv-cache-manager`;
- environment: `PYTHONHASHSEED`, `INFERENCECACHE_FAIL_OPEN`.

SGLang has a different reserved set because its launch surface differs:
`--enable-lmcache`, `--lmcache-config-file`, `LMCACHE_USE_EXPERIMENTAL`, and
`INFERENCECACHE_FAIL_OPEN`.

## Safe example

This changes the typed MP chunk size and independently appends a logging value
to the engine container:

```yaml
spec:
  runtime: VLLM
  type: LMCache
  lmCache:
    topology: PodLocal
    chunkSizeTokens: 128
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
  integration:
    engineOverrides:
      env:
        - name: LMCACHE_LOG_LEVEL
          value: DEBUG
      args:
        - --max-model-len
        - "32768"
```

Trying to replace `PYTHONHASHSEED`, suppress
`--disable-hybrid-kv-cache-manager`, or replace `--kv-transfer-config` is
rejected with a field-scoped error naming the adapter and reserved token.

## Discoverability

`kubectl explain cachebackend.spec.integration.engineOverrides` documents the
four primitives. The complete reserved lists currently live in the runtime
adapters under `internal/adapters/builtin/runtime`; there is no CLI command that
prints them. That is a discoverability gap, not permission to override the
connector wire.

Legacy topology-less vLLM/IP overrides remain covered only by compatibility
tests until Phase 7. They are not a current tuning surface.

See also:

- [`cachebackend-api.md`](../design/cachebackend-api.md)
- [`cachebackend-engine-binding.md`](cachebackend-engine-binding.md)
- [`config/samples/cachebackend-with-override.yaml`](../../config/samples/cachebackend-with-override.yaml)

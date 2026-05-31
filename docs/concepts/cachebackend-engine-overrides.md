# CacheBackend engine overrides

`CacheBackend.spec.integration.engineOverrides` is the in-between knob
between **"take the adapter's defaults"** and **"skip injection entirely."**
Use it when you want the LMCache integration on your engine pods, but need
to tune one knob — chunk size, log level, an extra vLLM flag — without
losing the rest of the canonical wiring.

If you want no injection at all on a particular pod, set the
`inferencecache.io/skip-inject: "true"` annotation on the pod instead.
Overrides cannot un-wire the integration: every entry the adapter declares
as **reserved** (the args/env the integration cannot function without) is
hard-rejected at admission.

## The four primitives

| Primitive | Semantics |
|---|---|
| `env: [{name, value}, ...]` | Upsert by name. An override `name` matching an adapter-injected env replaces it; an override `name` the adapter did not inject is appended. An override `name` matching an env on your pod template that the adapter did NOT touch is a silent no-op (the override surface never mutates pod-template state the adapter did not invite the CR to touch). |
| `suppressEnv: [name, ...]` | Remove from the adapter's canonical env by name. Scoped to adapter-injected entries; suppressing a name the adapter did not contribute is a silent no-op. |
| `args: [...]` | For each entry, if its leading flag token (e.g. `--max-model-len`) matches an adapter-injected flag, replace the canonical entry; otherwise append. Collision with a user-template flag the adapter did not inject is a silent no-op. Order is preserved. |
| `suppressArgs: [flag, ...]` | Remove from the adapter's canonical args by leading flag token. Scoped to adapter-injected entries. |

> **Mental model.** Canonical injection is what the adapter knows you
> need: the LMCache server URL, the connector config, the v1-engine flag.
> Overrides are what *you* know about *your* environment that the adapter
> doesn't: the chunk size your node memory tolerates, the log level your
> on-call playbook expects, the context length your tenant requested.

## Baseline — what the vLLM + LMCache adapter injects today

For a `CacheBackend` named `qwen-demo` in namespace `default`, with no
`engineOverrides` block set, the controller's mutating Pod admission
webhook stamps the following on every matched engine container (the
`vllm` container, or the sole container if the pod has only one):

```yaml
# vLLM container after webhook mutation — NO engineOverrides
args:
  # ... your pod-template args, unchanged ...
  - --kv-transfer-config                                                  # RESERVED
  - '{"kv_connector":"LMCacheConnectorV1","kv_role":"kv_both"}'
env:
  - name: LMCACHE_REMOTE_URL                                              # RESERVED
    value: lm://qwen-demo.default.svc.cluster.local:65432
  - name: LMCACHE_REMOTE_SERDE                                            # TUNABLE
    value: naive
  - name: LMCACHE_CHUNK_SIZE                                              # TUNABLE
    value: "256"
  - name: LMCACHE_LOCAL_CPU                                               # TUNABLE
    value: "False"
  - name: LMCACHE_MAX_LOCAL_CPU_SIZE                                      # TUNABLE
    value: "20"
  - name: VLLM_USE_V1                                                     # RESERVED
    value: "1"
  - name: INFERENCECACHE_FAIL_OPEN                                        # RESERVED
    value: "true"
```

`RESERVED` entries cannot be overridden or suppressed — admission
rejects the CR with a field-scoped error (see worked example 5). `TUNABLE`
entries are the ones the worked examples below amend.

## Worked examples

Each example shows the CR fragment first, then the resulting engine
container args/env after webhook mutation. Lines marked `# ←` highlight
the difference from baseline.

### 1. Tune `LMCACHE_CHUNK_SIZE` for a memory-tight node

A node where the default 256-token chunks cost too much memory per replica.
Drop to 64.

```yaml
spec:
  integration:
    engine: vllm
    engineOverrides:
      env:
        - name: LMCACHE_CHUNK_SIZE
          value: "64"
```

After webhook mutation:

```yaml
env:
  - name: LMCACHE_REMOTE_URL
    value: lm://qwen-demo.default.svc.cluster.local:65432
  - name: LMCACHE_REMOTE_SERDE
    value: naive
  - name: LMCACHE_CHUNK_SIZE
    value: "64"                                # ← was "256"
  - name: LMCACHE_LOCAL_CPU
    value: "False"
  - name: LMCACHE_MAX_LOCAL_CPU_SIZE
    value: "20"
  - name: VLLM_USE_V1
    value: "1"
  - name: INFERENCECACHE_FAIL_OPEN
    value: "true"
```

### 2. Add a debug env the adapter doesn't inject

Turn on LMCache verbose logging and pipe trace output to a known path so a
sidecar can collect it. Neither variable is in the adapter's canonical set
— both are appended.

```yaml
spec:
  integration:
    engine: vllm
    engineOverrides:
      env:
        - name: LMCACHE_LOG_LEVEL
          value: DEBUG
        - name: LMCACHE_TRACE_FILE
          value: /tmp/lmcache-trace.log
```

After webhook mutation (canonical block unchanged, two entries appended):

```yaml
env:
  - name: LMCACHE_REMOTE_URL
    value: lm://qwen-demo.default.svc.cluster.local:65432
  - name: LMCACHE_REMOTE_SERDE
    value: naive
  - name: LMCACHE_CHUNK_SIZE
    value: "256"
  - name: LMCACHE_LOCAL_CPU
    value: "False"
  - name: LMCACHE_MAX_LOCAL_CPU_SIZE
    value: "20"
  - name: VLLM_USE_V1
    value: "1"
  - name: INFERENCECACHE_FAIL_OPEN
    value: "true"
  - name: LMCACHE_LOG_LEVEL                    # ← appended
    value: DEBUG
  - name: LMCACHE_TRACE_FILE                   # ← appended
    value: /tmp/lmcache-trace.log
```

### 3. Suppress the local CPU tier

A topology where the engine never offloads to a local CPU memory tier (all
KV blocks live on the remote `lmcache-server`). The canonical defaults
already disable the tier (`LMCACHE_LOCAL_CPU="False"`); suppressing the two
variables removes them from the engine env entirely so reviewers and
audit tooling see the minimal set.

```yaml
spec:
  integration:
    engine: vllm
    engineOverrides:
      suppressEnv:
        - LMCACHE_LOCAL_CPU
        - LMCACHE_MAX_LOCAL_CPU_SIZE
```

After webhook mutation (the two adapter-owned env entries are stripped):

```yaml
env:
  - name: LMCACHE_REMOTE_URL
    value: lm://qwen-demo.default.svc.cluster.local:65432
  - name: LMCACHE_REMOTE_SERDE
    value: naive
  - name: LMCACHE_CHUNK_SIZE
    value: "256"
  # LMCACHE_LOCAL_CPU — suppressed
  # LMCACHE_MAX_LOCAL_CPU_SIZE — suppressed
  - name: VLLM_USE_V1
    value: "1"
  - name: INFERENCECACHE_FAIL_OPEN
    value: "true"
```

### 4. Append a vLLM flag the adapter doesn't inject

Extend the engine's context window to 32 768 tokens. The adapter does not
inject `--max-model-len`, so the override is appended.

```yaml
spec:
  integration:
    engine: vllm
    engineOverrides:
      args:
        - --max-model-len
        - "32768"
```

After webhook mutation (canonical `--kv-transfer-config` preserved, two
override args appended):

```yaml
args:
  # ... your pod-template args, unchanged ...
  - --kv-transfer-config
  - '{"kv_connector":"LMCacheConnectorV1","kv_role":"kv_both"}'
  - --max-model-len                            # ← appended
  - "32768"
```

### 5. Attempt to override `LMCACHE_REMOTE_URL` — hard-rejected at admission

`LMCACHE_REMOTE_URL` is reserved by the vLLM runtime adapter: changing it
would silently re-point the engine at a different cache than the one the
CacheBackend reconciler stood up. Admission rejects the CR with a
field-scoped error that names both the offending env and the adapter, so
the operator gets the failure at `kubectl apply` rather than discovering a
silently-wrong wiring later.

Concretely, take the paired sample at
`config/samples/cachebackend-with-override.yaml` and replace the
`engineOverrides` block on the `qwen-demo` CacheBackend with the
following:

```yaml
spec:
  integration:
    engine: vllm
    engineOverrides:
      env:
        - name: LMCACHE_REMOTE_URL
          value: lm://my-other-cache.default.svc.cluster.local:65432
```

Save the edited file as `bad-override.yaml` and apply:

```text
$ kubectl apply -f bad-override.yaml
Error from server (Invalid): error when creating "bad-override.yaml": admission webhook "vcachebackend.inferencecache.io" denied the request: CacheBackend.inferencecache.io "qwen-demo" is invalid: spec.integration.engineOverrides.env[0].name: Forbidden: env "LMCACHE_REMOTE_URL" is reserved by the "vllm" runtime adapter and cannot be overridden or suppressed via spec.integration.engineOverrides; the adapter strictly requires this env for the integration to function
```

The same shape applies to `suppressEnv` overlap, `args` overlap (e.g.
`--kv-transfer-config`), and `suppressArgs` overlap — the error names the
field path, the offending token, and the adapter every time.

## How to discover what's reserved

Two surfaces today:

- `kubectl explain cachebackend.spec.integration.engineOverrides`
  documents the four primitives and their merge semantics.
- The reserved list for the vLLM + LMCache adapter lives in the adapter
  source: `pkg/adapters/runtime/vllm_lmcache.go`'s `ReservedArgs()` and
  `ReservedEnv()` methods. Each entry is commented with WHY it is
  reserved.

There is **no CLI surface today** for "show me adapter X's reserved
list" — the validator only surfaces the list when a rejected CR happens to
overlap it. That is a real discoverability gap worth a separate ticket;
this doc names it so operators are not surprised.

## When NOT to use it

`engineOverrides` is for tuning **within** an integration. If you find
yourself wanting to override the wiring itself —

- repoint the engine at a different cache (`LMCACHE_REMOTE_URL`),
- swap the connector (`--kv-transfer-config`),
- disable the v1 engine codepath the LMCache connector targets
  (`VLLM_USE_V1`),

— you probably want a different `CacheBackend` CR (e.g. one whose
reconciler stands up the cache server you actually want the engine to
talk to). Trying to express "switch between integrations" through an
override is a hard-reject at admission by design. The `External` backend
type is intended for the "engine should attach to a pre-existing cache
the controller does not manage" use case, but the engine-side wiring for
External is not in the default adapter set today — track its rollout
before reaching for it.

## See also

- [`docs/design/cachebackend-api.md`](../design/cachebackend-api.md) —
  the ADR covering why the override surface has this shape and the
  hard-reject-vs-warn admission posture.
- `docs/concepts/cachebackend-engine-binding.md` — companion concept doc
  on how CacheBackend pods bind to engine pods via the engine-selector
  label match. *(TODO: add the cross-link from the binding doc back to
  this page once it lands.)*
- `config/samples/cachebackend-with-override.yaml` — a runnable paired
  sample (CacheBackend + matching engine Deployment) that exercises a
  small override block.

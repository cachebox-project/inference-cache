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
> set no KV events are reported: a managed backend holds at `Ready=False`
> (`AwaitingFirstKVEvent`) and then, once the `firstEventTimeout` window
> (default `5m`) elapses, flips to `Ready=False` (`NoKVEventsObserved`) with
> `Degraded=True`; `PREFIXES` stays `0` throughout. Set that flag on the
> controller to get the Ready/observability surface below. (External backends
> are exempt from this gate — they go Ready as soon as admission accepts the
> endpoint.)

> **A second readiness gate composes on top.** Once the KV-event gate above
> clears, the controller runs a synthetic functional self-test (ingest a
> synthetic prefix through the server's in-process `index.Ingest` → look
> it up → optional tier-2 round-trip; the gRPC `PublishEvent` /
> `ReportCacheState` subscriber path is NOT exercised) and publishes a
> `FunctionalProbeOK` condition on the CR. Every stage `ok` or
> `skipped` (the passing states `ProbeResult.AllPassed` accepts; today's
> clean installs report `t2=skipped` because no `T2Prober` is wired) →
> `Ready=True` stays;
> a per-stage failure downgrades `Ready=False` with a stage-specific reason
> (`ProbeIngestFailed` / `ProbeRoutingFailed` / `ProbeT2Failed`). The gate
> is cascade-prevented — `FunctionalProbeOK` does **not** appear while the
> upstream KV-event gate is still pending. Disabled by passing
> `--server-probe-url=""` to the controller (the condition then never
> appears); External backends are exempt from this gate too. See
> [Troubleshooting](#troubleshooting) for the per-reason runbook.

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

  `READY` flips to `True` only after the managed-readiness baseline
  (pods Up, Service endpoints) **and** the KV-event gate (a real
  event observed, not merely the pod being reachable). When functional
  probing is enabled and not bypassed, a per-stage *failed* probe
  outcome additionally downgrades `Ready=False`. On a `/probe`
  transport/HTTP error the gate is fail-soft AND sticky in two
  distinct ways depending on prior state:
  - If no `FunctionalProbeOK` is present, or it's currently `True`,
    a transport error publishes
    `FunctionalProbeOK=Unknown/ProbeError` and leaves `Ready`
    alone — a transient server outage does NOT hold an otherwise-Ready
    backend out of `Ready=True`.
  - If `FunctionalProbeOK=False/Probe*Failed` is already published,
    a transport error preserves the False condition AND keeps
    `Ready=False` with the prior stage reason. The False is sticky
    until a successful probe explicitly resolves it — a transient
    server outage must NOT mask a known per-stage regression by
    fading the condition to `Unknown` and then to `Ready=True`.

  The functional-probe gate can also be disabled cluster-wide
  (`--server-probe-url=""`) or skipped
  per-CR via the `inferencecache.io/skip-functional-probe: "true"`
  annotation. Any active gate that reports a per-stage failure can
  hold the backend at `Ready=False` with a stage-specific reason on
  `.status.conditions[]` — see [Troubleshooting](#troubleshooting).
  `MATCHED` is the engine-pod count the selector binds, and
  `PREFIXES` / `LASTEVENT` show the cache actually receiving state.

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

## Troubleshooting

A managed backend's `Ready` status is the composition of three gates
(managed-readiness → KV-event gate → functional-probe gate), so the
`READY` column of `kubectl get cachebackend` only tells half the story.
`kubectl get cachebackend <name> -o yaml` and scan
`.status.conditions[]` for which gate is unhappy. The condition `.reason`
is the actionable string.

### `Ready=False` / `AwaitingFirstKVEvent`

The KV-event gate is still waiting for the first KV event. Common causes:

- The controller is running with `--kvevent-subscriber-image` unset (the
  default). No subscriber sidecar is injected, so no events ever flow.
  Set the flag on the controller Deployment.
- The subscriber sidecar is present but cannot reach the engine's
  KV-event publisher. Check the subscriber pod's logs for ZMQ connect
  errors; verify the engine container has `--kv-events-config` set
  (see the `recipe-*` samples for the canonical shape).
- The engine container is running but has not received any prompts yet
  (the first BlockStored is published on the first request). Send a
  single chat completion through the engine and re-check.

If `firstEventTimeout` elapses (default `5m`) without an event, the
condition flips to `Ready=False / NoKVEventsObserved` and `Degraded=True`;
diagnostic steps are the same.

### `Ready=False` / `Probe*Failed` (with `FunctionalProbeOK=False`)

The KV-event gate cleared (real engine pods reported state) but the
controller's synthetic functional round-trip failed. Read the
condition's `.message` first — the controller embeds the server's
stage-specific diagnostic. By `.reason`:

| `.reason` | Stage | What it means | First-response |
|---|---|---|---|
| `ProbeIngestFailed` | ingest | The server's in-process index `Ingest` path is dropping writes. Does **not** indicate a subscriber problem — that path isn't exercised by the probe (the gRPC `PublishEvent` / `ReportCacheState` subscriber surface is bypassed by design; see the stage description at the top of this section). | Read the `FunctionalProbeOK` condition's `.message` for the server's stage diagnostic; check `inferencecache_backend_probe_result_total{backend="<namespace>/<name>", stage="ingest", result="failed"}` for the trend. The server-side `inferencecache_index_entries` gauge is fine as background index-health context but cannot confirm whether the synthetic probe entry landed — probe entries live under the reserved `inferencecache.io/probe` tenant, which is excluded from the cap-accounting gauge by design. Inspect server logs for `pkg/index` errors. Confirm `inferencecache_server_up == 1`. |
| `ProbeRoutingFailed` | lookup | `LookupRoute` did not return a clean `PREFIX_MATCH` for the probe's reserved replica. Two failure modes share this reason and are disambiguated by the condition `.message`: (a) the lookup returned a non-`PREFIX_MATCH` strategy (`NO_HINT`, `TENANT_HOT`, `TIMEOUT`, etc.) — likely an internal `hash_scheme` regression that dropped the probe's scheme on ingest (an empty scheme fails open and produces `NO_HINT`) or a lookup-filter regression in `pkg/index`/`pkg/server`; (b) the lookup did return `PREFIX_MATCH` but the probe's reserved replica isn't among the scored replicas — a probe-id-derivation or reserved-replica-collision regression. The probe's `hashScheme` derives from `spec.integration.engine` (admission rejects unknown runtime IDs, so a typo never reaches this stage). | Read the `FunctionalProbeOK` condition's `.message` first — the server names which failure mode hit. Check `inferencecache_backend_probe_result_total{backend="<namespace>/<name>", stage="routing", result="failed"}` for the trend. (The server-side `inferencecache_lookup_route_calls_total` is NOT the right surface here — the probe calls `index.LookupRoute` directly through an in-process seam, bypassing the gRPC handler that emits that metric.) Inspect server logs for `pkg/index` lookup-path errors. |
| `ProbeT2Failed` | tier-2 | The tier-2 put/get cycle failed (LMCache, today). Only reachable when a `T2Prober` is wired into the server — none is registered in the current revision, so this condition does **not** appear on a clean install. | Will be applicable once a `T2Prober` ships; not actionable today. |

### `FunctionalProbeOK=Unknown` / `ProbeError`

The controller could not reach the server's `/probe` endpoint at all
(transport error, 5xx, audience-bound TokenReview rejected the call). A
brief server outage produces this state without flapping `Ready` —
**unless** `FunctionalProbeOK` is *already* `False/Probe*Failed`, in
which case the prior stage failure is sticky and `Ready` stays
downgraded (a transient server outage must not mask a real regression
by fading the condition back to `Unknown` and then to `Ready=True`).

First-response:

- Check that `--server-probe-url` is reachable from the controller pod
  (intra-cluster `inference-cache-server:8081` by default).
- Verify the projected SA token is mounted at
  `/var/run/secrets/inferencecache.io/controller-token/token`.
- Confirm the audience-bound TokenReview accepts the controller SA —
  the server's `--controller-audience` flag must equal
  `inferencecache.io/controller`. A mismatch surfaces as repeated
  `ProbeError` on every reconcile.

### `FunctionalProbeOK=True` / `ProbeBypassed`

An operator has annotated the CR with
`inferencecache.io/skip-functional-probe: "true"`. The probe is
skipped entirely and the gate does not downgrade `Ready`. Remove the
annotation when you no longer need the bypass — a bypassed backend
with a real regression still ships broken cache state.

### `FunctionalProbeOK` is missing from `.status.conditions[]`

Four possible causes, in order of how common they are on a healthy
install:

- **Any upstream gate is holding `Ready != True`.** The probe gate is
  cascade-prevented from running whenever the composed upstream
  verdict (managed-readiness baseline + KV-event gate) is not
  `Ready=True`. That includes rollout-in-progress, replicas
  unavailable, scaled-to-zero, and `AwaitingFirstKVEvent` /
  `NoKVEventsObserved` — not only KV-event-pending. Resolve the
  upstream condition first; the probe condition appears on the next
  reconcile after the upstream clears.
- **Functional probing is disabled on the controller.** The
  `--server-probe-url=""` flag turns the gate off entirely. Any
  stale `FunctionalProbeOK` left over from a previous wiring is also
  cleared on the next reconcile in this mode.
- **The CR is `spec.type: External`.** External backends are wholly
  exempt from the probe gate — the controller never drives a
  round-trip against a cache it does not manage.
- **The CR is on an Unmanaged path** (unsupported runtime, deferred
  `deploymentKind: StatefulSet`, or other reconcile branch that sheds
  the managed workload). The probe gate is exempt from these paths
  and any prior `FunctionalProbeOK` condition is removed on transition.

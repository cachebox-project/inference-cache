---
title: "Troubleshooting"
linkTitle: "Troubleshooting"
weight: 4
description: >
  The CacheBackend readiness runbook and the doctor check catalog.
---

## Reading CacheBackend readiness

A managed backend's `Ready` is the composition of three gates
(**managed-readiness → KV-event gate → functional-probe gate**), so the `READY` column of
`kubectl get cachebackend` only tells half the story. The reason lives in
`.status.conditions[]`, and the condition `.reason` is the actionable string:

```bash
kubectl get cachebackend <name> -o yaml | yq '.status.conditions'
```

### `Ready=False` / `AwaitingFirstKVEvent`

The KV-event gate is still waiting for the first KV event. Common causes:

- The controller is running with `--kvevent-subscriber-image` **unset** (the default) — no
  subscriber sidecar is injected, so no events ever flow. Set the flag on the controller
  Deployment.
- The subscriber sidecar is present but cannot reach the engine's KV-event publisher. Check
  the subscriber pod's logs for ZMQ connect errors; verify the engine container has
  `--kv-events-config` set (see the `recipe-*` samples).
- The engine is running but has served no prompts yet (the first block event is published on
  the first request). Send one chat completion and re-check.

After `firstEventTimeout` (default 5m) with no event, this flips to
`Ready=False / NoKVEventsObserved` + `Degraded=True`. Same diagnosis.

### `Ready=False` / `Probe*Failed` (with `FunctionalProbeOK=False`)

The KV-event gate cleared but the controller's synthetic functional round-trip failed. Read
the condition `.message` first — the controller embeds the server's stage diagnostic.

| Reason | Stage | Meaning | First response |
|---|---|---|---|
| `ProbeIngestFailed` | ingest | The server's in-process index ingest path is dropping writes. (Not a subscriber problem — the probe bypasses the gRPC ingest surface by design.) | Read `.message`; check `inferencecache_backend_probe_result_total{stage="ingest",result="failed"}`; confirm `inferencecache_server_up == 1`; inspect server `pkg/index` logs. |
| `ProbeRoutingFailed` | routing | `LookupRoute` did not return a clean `PREFIX_MATCH` for the probe's reserved replica — usually an internal `hash_scheme` regression that dropped the probe's scheme on ingest, or a lookup-filter regression. | Read `.message` (the server names the failure mode); check the `stage="routing"` probe counter; inspect server lookup-path logs. |
| `ProbeT2Failed` | tier-2 | The tier-2 put/get cycle failed. Only reachable once a tier-2 prober is wired — none ships today, so this does not appear on a clean install. | Not actionable today. |

### `FunctionalProbeOK=Unknown` / `ProbeError`

The controller could not reach the server's `/probe` endpoint at all (transport error, 5xx,
or the audience-bound TokenReview rejected the call). A brief outage produces this state
*without* flapping `Ready` — unless `FunctionalProbeOK` was already `False/Probe*Failed`, in
which case the prior failure stays sticky and `Ready` stays downgraded (a transient outage
must not mask a real regression). First response:

- Confirm `--server-probe-url` is reachable from the controller pod (defaults to
  `inference-cache-server:8081`).
- Verify the projected SA token is mounted at
  `/var/run/secrets/inferencecache.io/controller-token/token`.
- Confirm the server's `--controller-audience` equals `inferencecache.io/controller` — a
  mismatch surfaces as repeated `ProbeError` on every reconcile.

### `FunctionalProbeOK=True` / `ProbeBypassed`

Someone annotated the CR with `inferencecache.io/skip-functional-probe: "true"`. The probe is
skipped and the gate does not downgrade `Ready`. Remove the annotation when you no longer
need the bypass — a bypassed backend with a real regression still ships broken cache state.

### `FunctionalProbeOK` missing from conditions

The probe gate is cascade-prevented while any upstream gate holds `Ready != True` (rollout in
progress, replicas unavailable, scaled-to-zero, `AwaitingFirstKVEvent`). It is also absent
when functional probing is disabled (`--server-probe-url=""`), for `External` backends
(exempt), and on unmanaged paths. Resolve the upstream condition first.

## The `inferencecache doctor` check catalog

`inferencecache doctor` runs a read-only pre-flight diagnostic. Nine checks in fixed order,
each emitting stable, greppable finding codes:

| # | Check | Code prefix |
|---|---|---|
| 1 | Server gRPC health is `SERVING` | `API`, `SV` |
| 2 | `/snapshot` reachable | `SN` |
| 3 | `/policy` wired | `PL` |
| 4 | `/probe` wired | `PB` |
| 5 | Per-`CacheBackend` health | `CB` |
| 6 | Engine-pod injection audit | `EP` |
| 7 | Orphan-pod check | `OP` |
| 8 | `CacheTenant` health | `CT` |
| 9 | `CachePolicy` coverage | `CP` |

Exit codes: `0` (≤ INFO), `1` (≥ WARN), `2` (≥ FAIL) — CI-friendly.

```bash
inferencecache doctor -n serving              # full run
inferencecache doctor --config-only           # skip live probes (workstation / TLS overlay)
inferencecache doctor -o json                 # machine-readable
```

Useful flags: `--kubeconfig`, `--context`, `-n/--namespace`, `--server-endpoint`,
`--snapshot-token-file`, `-o/--output` (`human`/`json`/`table`), `--no-color`,
`--config-only`, `--timeout` (default 30s).

Under the [gRPC TLS overlay](/docs/administration/grpc-tls/), use `--config-only` — the
doctor dials gRPC plaintext.

## Related pages

- [Deploy a CacheBackend](/docs/tasks/deploy-a-cache-backend/) — the readiness gates in
  context.
- [CLI reference](/docs/reference/cli-doctor/) — every finding code.
- [Reason codes](/docs/reference/reason-codes/) — the lookup outcomes referenced above.

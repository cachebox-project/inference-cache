---
title: "Monitor the cache plane"
linkTitle: "Monitor the cache plane"
weight: 5
description: >
  The kubectl surfaces, the metrics that matter, and the doctor pre-flight check.
---

There are three complementary ways to watch a running install: the Kubernetes status
surfaces, the Prometheus metrics, and the `inferencecache doctor` pre-flight diagnostic.

## kubectl surfaces

```bash
# Per-backend health, matched pods, and live prefix/event evidence
kubectl get cachebackend -A

# The cluster-wide cache "world map"
kubectl get cacheindex cluster-default -o yaml

# Per-tenant entry counts
kubectl get cachetenant -A
```

When a backend is not `Ready`, the reason is in `.status.conditions[]`:

```bash
kubectl get cachebackend my-cache -o yaml | yq '.status.conditions'
```

See [Troubleshooting]({{< relref "/docs/administration/troubleshooting/" >}}) for the per-reason runbook.

## The metrics that matter

Both binaries expose Prometheus metrics on their pod's `:8080/metrics`, prefixed
`inferencecache_*`. The [metrics reference]({{< relref "/docs/reference/metrics/" >}}) lists them all; these
are the ones to watch day-to-day:

| Signal | Metric | Why |
|---|---|---|
| Is the server alive and has state? | `inferencecache_server_up`, `inferencecache_index_entries{model}` | An up server with an empty index while lookups flow means ingestion is broken. |
| Are lookups producing hints? | `inferencecache_lookup_route_calls_total{model,reason_code,hint_used}` | A high `NO_HINT` share (or `UNKNOWN_*`) means no reuse — or a client misconfiguration. |
| Lookup latency | `inferencecache_lookup_route_latency_seconds{model}` | The hot path should stay in the sub-millisecond buckets. |
| Tier-2 offload health | `inferencecache_backend_t2_hit_rate{backend}` | `0` while queries flow = a silently-degraded offload tier. |
| Cap pressure | `inferencecache_index_evictions_total{reason="cap"}` | Sustained cap evictions mean the index is over budget — see [index sizing]({{< relref "/docs/administration/index-sizing/" >}}). |
| Functional probe | `inferencecache_backend_probe_result_total{backend,stage,result}` | Repeated `result="failed"` is the controller-side health signal. |

The opt-in alert bundle turns these into alerts — see
[Observability & Alerts]({{< relref "/docs/administration/observability-and-alerts/" >}}).

## The doctor pre-flight check

`inferencecache doctor` runs a read-only diagnostic across the install — the cache-plane
analogue of `istioctl analyze`. It runs nine checks (server gRPC health, the three
controller-facing endpoints, per-backend health, engine-pod injection audit, orphan pods,
tenants, and policy coverage) and prints stable, greppable finding codes.

```bash
# Full run against the in-cluster server
inferencecache doctor -n serving

# Config-only (skip live probes) — the right mode from a workstation
inferencecache doctor --config-only

# Machine-readable
inferencecache doctor -o json
```

Exit codes make it CI-friendly: `0` (≤ INFO), `1` (≥ WARN), `2` (≥ FAIL). Under the gRPC TLS
overlay, use `--config-only` (the doctor dials gRPC plaintext). See
[Troubleshooting]({{< relref "/docs/administration/troubleshooting/" >}}) for the full check catalog and
finding codes.

## Related pages

- [Observability & Alerts]({{< relref "/docs/administration/observability-and-alerts/" >}}) — the alert
  bundle and how to wire scraping.
- [Metrics reference]({{< relref "/docs/reference/metrics/" >}}) — every series.
- [CLI reference]({{< relref "/docs/reference/cli-doctor/" >}}) — every flag and finding code.

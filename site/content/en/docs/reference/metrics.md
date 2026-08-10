---
title: "Metrics"
linkTitle: "Metrics"
weight: 3
description: >
  Every inferencecache_* series, its labels, and which binary emits it.
---

Both inference-cache binaries expose Prometheus metrics on their pod's `:8080/metrics`, all prefixed
`inferencecache_*`. The two binaries use **separate registries** — the server's series cover
the index and gRPC handlers; the controller's cover the reconcilers. Standard `go_*` and
`process_*` collectors are also present but are not part of this schema. Successfully
injected PodLocal LMCache sidecars expose their upstream `lmcache_mp_*` metrics separately;
the observability overlay includes a cross-namespace PodMonitor for them.

## Server metrics (`inference-cache-server`)

### Gauges

| Metric | Labels | Meaning |
|---|---|---|
| `inferencecache_server_up` | — | `1` when the server is serving. |
| `inferencecache_server_grpc_tls_enabled` | — | `1` when gRPC TLS is configured. |
| `inferencecache_index_entries` | `model` | Distinct prefix keys in the index (excludes the reserved probe tenant). Drained models are zeroed, not left stale. |

### Counters

| Metric | Labels | Meaning |
|---|---|---|
| `inferencecache_lookup_route_calls_total` | `model`, `reason_code`, `hint_used` | Lookups by outcome. `hint_used="true"` ⇔ a non-empty score list (`PREFIX_MATCH` / `TENANT_HOT` / `AFFINITY_HINT`). |
| `inferencecache_index_evictions_total` | `algorithm` (`lru`/`lfu`), `reason` (`cap`/`ttl`) | Index evictions. `reason="cap"` is the authoritative over-budget signal. |
| `inferencecache_tenant_evictions_total` | `tenant_id`, `reason` (`over_entries`) | Per-tenant quota evictions. A bounded non-zero rate is normal Fairness. |
| `inferencecache_snapshot_auth_total` | `result` (`ok`/`unauth`/`forbidden`/`error`) | `/snapshot` auth outcomes. Wrong audience lands in `unauth`. |
| `inferencecache_policy_auth_total` | `result` | `/policy` auth outcomes. |
| `inferencecache_probe_auth_total` | `result` | `/probe` auth outcomes. |

### Histogram

| Metric | Labels | Buckets |
|---|---|---|
| `inferencecache_lookup_route_latency_seconds` | `model` | 100µs, 250µs, 500µs, 1ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms |

## Controller metrics (`inferencecache-controller`)

The controller pod has **no Service** in front of it — scrape it with a `PodMonitor` (the
shipped observability bundle includes one) or pod-IP service discovery, or the controller-side
alerts have no series to evaluate.

### Gauge

| Metric | Labels | Meaning |
|---|---|---|
| `inferencecache_backend_t2_hit_rate` | `backend` (`<ns>/<name>`) | Tier-2 offload hit rate. Present once exercised; `0` while queries flow = a degraded tier. |

### Counters

| Metric | Labels | Meaning |
|---|---|---|
| `inferencecache_backend_probe_result_total` | `backend`, `stage` (`ingest`/`routing`/`t2`), `result` (`ok`/`failed`/`skipped`) | Functional-probe stage results — three increments per successful call. |
| `inferencecache_backend_t2_query_tokens_total` | `backend` | Monotonic tier-2 activity signal (positive deltas only). |
| `inferencecache_backend_server_restart_cascades_total` | `namespace`, `backend`, `reason` (`server_instance_changed`) | Cache-server restart cascades, rate-limited (~30s). |

## Endpoints

| Binary | Port | Paths |
|---|---|---|
| Server | `:8080` (`--http-bind-address`) | `/healthz`, `/readyz` (unauth), `/metrics` |
| Server | `:8081` (`--snapshot-bind-address`) | `/snapshot`, `/policy`, `/probe` (auth-gated) |
| Controller | `:8080` (`--metrics-bind-address`) | `/metrics` (unauth by default; `--metrics-secure` to gate) |
| Injected LMCache MP sidecar | `:8080` (`lmcache-http`) | `/healthcheck`, `/metrics` (unauthenticated on the Pod network) |

## Related pages

- [Observability & Alerts]({{< relref "/docs/administration/observability-and-alerts/" >}}) — the alerts
  built on these series.
- [Index sizing]({{< relref "/docs/administration/index-sizing/" >}}) — the eviction/memory relationship.
- [Monitor the cache plane]({{< relref "/docs/tasks/monitor-the-cache-plane/" >}}) — the day-to-day view.

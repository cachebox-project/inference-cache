# Metrics

A living reference of every Prometheus metric the cache plane exposes on
`/metrics`. The tech spec §4.3 defines the *public schema* (the F3 work owns
ratcheting toward full §4.3 coverage); this file is the *what is exposed today*
and the operational meaning of each.

**Update this file in the same change that adds, renames, retires, or
re-labels a metric.** A new metric without a row here is invisible to ops and
the gateway team; a renamed metric without a row here breaks dashboards
silently.

## Surface conventions

- **Namespace.** Every metric the cache plane owns is prefixed
  `inferencecache_*` (constant `metricNamespace` in
  [`pkg/server/metrics.go`](../../pkg/server/metrics.go)). Anything not
  matching that prefix is from a standard collector (see below) and not part
  of the §4.3 schema.
- **Registry isolation.** The server uses a **per-`Service` Prometheus
  registry**, not the global default. This keeps the server binary's metrics
  separate from the controller binary's controller-runtime registry, and lets
  tests construct multiple Services without "duplicate metrics collector"
  panics.
- **Standard collectors are exposed but not in the schema.** The same
  registry registers `collectors.NewGoCollector()` and
  `collectors.NewProcessCollector(...)`. They emit `go_*` and `process_*`
  series — Prometheus convention, useful for ops, **not** part of §4.3 and
  **not** to be treated as contract.
- **Drained models are zeroed**, not left as ghost series. When the index
  drops the last entry for a model, the controller-side caller of
  `SetIndexEntries(model, 0)` ensures the gauge reports `0` rather than
  retaining the previous value indefinitely.

---

## Server metrics (`inferencecache_*`) — exposed today

### Gauges

| Metric | Labels | Meaning | Moves when |
|---|---|---|---|
| `inferencecache_server_up` | *(none)* | `1` if the cache policy server is serving requests, `0` otherwise. | Server starts (→`1`) / shuts down (→`0`). Liveness signal. |
| `inferencecache_index_entries` | `model` | **Distinct prefix entries** the in-memory `CacheIndex` currently holds for that model. One entry = one unique `(tenant, model, hash_scheme, prefix_hash)` tuple, regardless of how many replicas hold it. | Rises on new `(scheme, hash)` from `ReportCacheState`; falls on `BlockRemoved` / `AllBlocksCleared` / TTL eviction / max-entries cap. Idempotent re-reports do **not** move it. |

### Counters

| Metric | Labels | Meaning | Notes |
|---|---|---|---|
| `inferencecache_lookup_route_calls_total` | `model`, `reason_code`, `hint_used` | One increment per `LookupRoute` call, labeled by outcome. | `reason_code` ∈ values defined in [reason-codes.md](reason-codes.md). `hint_used="true"` ⇔ the response's `replica_scores` was non-empty — it covers **both** `PREFIX_MATCH` and `TENANT_HOT` (the latter is a softer locality hint emitted on a prefix miss when the tenant has warm replicas). The **prefix-hit SLO** of the cache plane is the ratio of `reason_code="PREFIX_MATCH"` over the total; the broader **hint ratio** (`hint_used="true"` over total) tracks how often any locality hint surfaces. Dashboards should chart both, since a healthy prefix-hit ratio is what drives the TTFT win. |
| `inferencecache_snapshot_auth_total` | `result` | One increment per `/snapshot` request reaching the auth middleware, labeled by outcome. | `result` ∈ `ok` (bearer validated, controller SA, handler invoked), `unauth` (missing/empty/malformed bearer, OR TokenReview said `Authenticated=false` regardless of whether `Status.Error` is populated — see note below), `forbidden` (TokenReview authenticated a non-controller identity), `error` (TokenReview Go-level transport failure, nil response, RBAC reject before review even ran — fail-closed 503). The `unauth` bucket intentionally collapses both "kube-apiserver checked the token and rejected it" AND "an authenticator in the chain populated `Status.Error`" because the two are NOT distinguishable from the response shape (verified empirically: kube-apiserver's SA-token authenticator populates `Status.Error` for routine JWT-parse failures of garbage strings, the same field a webhook-authenticator timeout would set). When `Status.Error` is non-empty the middleware logs it at WARN to the server log so the operator can still tell apart "webhook timeout" from "invalid bearer token" via the diagnostic. A non-trivial `unauth` rate against a steady controller poller cadence is the first signal that the projected SA token / RBAC / apiserver path is broken; `error` rate firing means the review never actually ran ("investigate the apiserver"). |

### Histograms

| Metric | Labels | Meaning | Buckets |
|---|---|---|---|
| `inferencecache_lookup_route_latency_seconds` | `model` | Server-side `LookupRoute` latency: from handler entry to response, including ranking. | `[100µs, 250µs, 500µs, 1ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms]`. Targets sub-ms (cache-hot path); buckets exist up to 100 ms to catch tail/regression. |

---

## Standard collectors (exposed, out of `inferencecache_*` schema)

| Family | Source | Examples | Use |
|---|---|---|---|
| `go_*` | `collectors.NewGoCollector()` | `go_goroutines`, `go_memstats_*`, `go_gc_duration_seconds`, … | Standard Go runtime telemetry. |
| `process_*` | `collectors.NewProcessCollector(...)` | `process_resident_memory_bytes`, `process_cpu_seconds_total`, `process_open_fds`, … | Standard process telemetry. |

These are conventional for Prometheus exporters and useful in ops dashboards
but are **not** load-bearing contract — they may be removed or replaced (e.g.
with OTEL collectors) without bumping `v1alpha1`.

---

## Where each metric is owned in code

- **Definitions:** [`pkg/server/metrics.go`](../../pkg/server/metrics.go) (the
  `serverMetrics` struct + `newServerMetrics`).
- **`indexEntries` writers:** the index pushes via the `index.Metrics`
  interface (`SetIndexEntries`); see [`pkg/index/`](../../pkg/index/). The
  snapshot is taken under `reportMu` so concurrent reporters can't publish a
  stale count.
- **`lookupCalls` + `lookupLatency` writers:** the `LookupRoute` handler in
  [`pkg/server/inferencecache_service.go`](../../pkg/server/inferencecache_service.go)
  calls `metrics.observeLookup(...)` exactly once per request.

---

## How `/metrics` is served

- HTTP endpoint **`/metrics`** on the server's public HTTP listener (default
  `:8080`, flag `--http-bind-address`). Format: Prometheus exposition.
- Companion endpoints on the public listener: **`/healthz`** (liveness),
  **`/readyz`** (readiness → `index.Ready()`), **`/policy`** (controller →
  server push of resolved `CachePolicy` snapshots). These are intentionally
  unauthenticated — kubelet probes and Prometheus scrapes need them open,
  and the controller is the only writer of `/policy`.
- Separate **`/snapshot`** listener (default `:8081`, flag
  `--snapshot-bind-address`) carries the JSON cluster-wide cache aggregate
  the controller's `CacheIndexPoller` scrapes to populate the `CacheIndex`
  CR status. The split is intentional: a `NetworkPolicy` restricts L3/L4
  ingress to the controller's pod selector, and a TokenReview-backed bearer
  middleware on the listener rejects everything that isn't the configured
  controller `ServiceAccount` (`--snapshot-allowed-sa`).
  `inferencecache_snapshot_auth_total` (see the counter table above) is the
  observability surface for that gate.

---

## How to add a new metric

1. **Confirm it's necessary.** Each new label dimension multiplies cardinality;
   each new histogram bucket array is a commitment. If an existing metric can
   carry the signal with a new label value (e.g. a new `reason_code`), prefer
   that. Use this checklist:
   - Is this a *new fact* the system can report, or a *new slice* of an
     existing one?
   - Will an operator dashboard care, or is this a debug-only counter? (Debug
     counters belong in logs, not `/metrics`.)
2. **Define the collector** in `pkg/server/metrics.go`: add a field to
   `serverMetrics`, construct it in `newServerMetrics`, and register it on the
   `prometheus.NewRegistry()` block. Use the `metricNamespace` constant so
   the name is consistently prefixed `inferencecache_*`.
3. **Add a typed writer method** on `*serverMetrics` (e.g.
   `observeLookup`, `SetIndexEntries`) and call it from the relevant handler
   or index path. Don't let handlers touch the prometheus collector directly
   — keeping the surface narrow makes it test-mockable.
4. **Update the table above** in the correct sub-section (Gauges / Counters /
   Histograms). Include labels, meaning, and what makes it move. If the metric
   is histogram, document the bucket array and *why* those buckets.
5. **Wire test coverage.** Add an assertion in `pkg/server/metrics_test.go`
   that the new metric appears in `/metrics` output with the expected name
   and labels.
6. **Flag the schema impact in the PR description.** If the metric is a
   candidate for the §4.3 public schema (F3 owns that effort), say so —
   F3 tracks which `inferencecache_*` series are promoted to the public
   contract vs which remain internal/advisory.
7. **Dashboards.** If you have a Grafana panel in mind, drop the PromQL in the
   PR description so F4 (dashboards + CLI) can pick it up cleanly.

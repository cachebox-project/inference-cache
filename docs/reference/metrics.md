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
| `inferencecache_snapshot_auth_total` | `result` | One increment per `/snapshot` request reaching the auth middleware, labeled by outcome. | `result` ∈ `ok` (bearer validated, controller SA, handler invoked), `unauth` (missing/empty/malformed bearer, OR TokenReview said `Authenticated=false` regardless of whether `Status.Error` is populated — this includes the **wrong-audience** case: a token whose JWT audience does not match the server's `--controller-audience` is reported by the apiserver as not authenticated, surfaced here in the same bucket), `forbidden` (TokenReview authenticated a non-controller identity), `error` (TokenReview Go-level transport failure, nil response, RBAC reject before review even ran — fail-closed 503). The `unauth` bucket intentionally collapses several apiserver-reported denials (plain bad token, `Status.Error` populated by the SA-token authenticator's JWT parser, wrong audience) because they're not distinguishable from the response shape; the middleware logs the apiserver's diagnostic at WARN to the server log so the operator can still tell them apart (e.g. `token audiences [...] is invalid for the target audiences [...]` clearly signals an audience mismatch — usually a manifest/flag drift between controller projection and server `--controller-audience`). A non-trivial `unauth` rate against a steady controller poller cadence is the first signal that the projected SA token / RBAC / audience / apiserver path is broken; `error` rate firing means the review never actually ran ("investigate the apiserver"). |
| `inferencecache_policy_auth_total` | `result` | One increment per `/policy` request reaching the auth middleware, labeled by outcome. Mirror of `inferencecache_snapshot_auth_total` for the controller's write-side push. | Same `result` semantics as `inferencecache_snapshot_auth_total` above — `ok` / `unauth` / `forbidden` / `error` — and the same collapsed-bucket caveat: a wrong-audience controller token shows up in `unauth`, with the apiserver's `token audiences [...] is invalid` diagnostic at WARN. The two counters live in parallel so dashboards distinguish read-side auth failures (snapshot scrape / info-leak attempt) from write-side ones (CachePolicy push / active-tampering attempt). Write-side `forbidden` rate is the alarming signal: it means a bearer was valid but issued to a non-controller identity, i.e. some other workload is trying to override cluster-wide cache policy. Read- and write-side caches are independent — the same controller token cache-misses once per endpoint per TTL window in the steady state. |
| `inferencecache_tenant_evictions_total` | `tenant_id`, `reason` | One increment per **distinct prefix** evicted from a tenant to bring it back within its `CacheTenant.spec.quota.maxIndexEntries` budget at ingest time (Fairness mode evicts the tenant's own oldest prefixes). | `reason` ∈ `over_entries` (only dimension today — the index-entry budget). A multi-replica prefix counts once (the eviction unit is the distinct prefix key, matching `maxIndexEntries`). A steadily rising rate for a `tenant_id` means that tenant is sustainably over budget — its declared cap is too small for its working set, or a client is churning prefixes. The series is created lazily on the first eviction, so a tenant that never exceeds budget emits nothing. |

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
- **`tenantEvictions` writer:** the index calls `AddTenantEvictions(...)` via the
  `index.Metrics` interface after a quota-driven eviction at ingest; see
  [`pkg/index/`](../../pkg/index/). One increment per evicted distinct prefix.
- **`snapshotAuth` + `policyAuth` writers:** the TokenReview middleware in
  [`pkg/server/auth/`](../../pkg/server/auth/) reports one outcome per
  request via the `auth.ResultRecorder` interface. The recorders themselves
  are returned by `serverMetrics.SnapshotAuthRecorder()` and
  `serverMetrics.PolicyAuthRecorder()` (in `pkg/server/metrics.go`) and
  wired into the per-endpoint authenticators in `pkg/server/server.go`.
  One increment per `/snapshot` or `/policy` request reaching the
  middleware, labeled by `result`.

---

## How `/metrics` is served

- HTTP endpoint **`/metrics`** on the server's public HTTP listener (default
  `:8080`, flag `--http-bind-address`). Format: Prometheus exposition.
- Companion endpoints on the public listener: **`/healthz`** (liveness),
  **`/readyz`** (readiness → `index.Ready()`). These stay unauthenticated —
  kubelet probes and Prometheus scrapes cannot present a SA bearer.
- Separate **controller-facing** listener (default `:8081`, flag
  `--snapshot-bind-address`) carries **both** `/snapshot` (controller-read
  of the cluster-wide cache aggregate; populates the `CacheIndex` CR
  status) and `/policy` (controller-write of the combined resolved
  snapshot — `CachePolicy` entries plus `CacheTenant` quota entries;
  replace-on-write). The gate has **THREE independent layers**, each
  meant to catch a failure mode the others can't, and the same gate
  applies to BOTH endpoints uniformly (one middleware identity):
  - **L3/L4:** a `NetworkPolicy` restricts ingress to the controller's
    pod selector.
  - **L7 identity:** TokenReview-backed bearer middleware rejects every
    request whose token does not resolve to the configured controller
    `ServiceAccount` (`--allowed-controller-sa`).
  - **L7 audience:** the controller mounts an audience-bound projected
    SA token (audience `inferencecache.io/controller` by default, mounted
    at `/var/run/secrets/inferencecache.io/controller-token/token`); the
    server passes `TokenReviewSpec.Audiences=[--controller-audience]` so
    a default-audience apiserver token from the same controller SA is
    rejected on either endpoint. Under the default apiserver audience
    configuration, a leaked controller-audience token is useless against
    the apiserver and vice versa; the cross-surface property holds only
    while the apiserver is not configured to also accept
    `inferencecache.io/controller` as an apiserver audience (keep the
    two distinct).

  `inferencecache_snapshot_auth_total` and
  `inferencecache_policy_auth_total` (see the counter table above) are
  the parallel observability surfaces — one per endpoint — for the
  **two L7 layers (identity + audience)**. NetworkPolicy drops happen
  at the CNI before the listener and cannot increment either counter
  — observe those via kube state metrics on the policy resource + the
  CNI's flow logs (Calico / Cilium / etc.), separately from the auth
  counters. Audience-mismatch denials land in `result="unauth"` with
  the apiserver's diagnostic in the server WARN log — operators
  chasing a binding regression should grep for
  `token audiences [...] is invalid for the target audiences`.

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

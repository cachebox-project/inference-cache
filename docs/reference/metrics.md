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
  `inferencecache_*`, in both binaries. Server-binary metrics derive
  the prefix from the `metricNamespace` constant in
  [`pkg/server/metrics.go`](../../pkg/server/metrics.go); controller-
  binary metrics declare it inline on each `prometheus.NewXVec`
  declaration in `internal/controller/` (see
  `backendServerRestartCascadesTotal` for the pattern) — the two
  processes use separate Prometheus registries, so no shared
  package-level constant is used to enforce the prefix today.
  Anything not matching that prefix is from a standard collector
  (see below) and not part of the §4.3 schema.
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
| `inferencecache_server_grpc_tls_enabled` | *(none)* | `1` if the gRPC server (`:9090`) is terminating TLS, `0` if serving plaintext. | Set once at startup from `--tls-cert-file`/`--tls-key-file` (both set → `1`, both empty → `0`). Confirms the prod wire posture from Prometheus. See `docs/design/grpc-tls.md`. |
| `inferencecache_index_entries` | `model` | **Distinct prefix entries** the in-memory `CacheIndex` currently holds for that model, **excluding reserved-tenant (`inferencecache.io/probe`) entries**. One entry = one unique `(tenant, model, hash_scheme, prefix_hash)` tuple, regardless of how many replicas hold it. | Rises on new `(scheme, hash)` from `ReportCacheState`; falls on `AllBlocksCleared` / TTL eviction / max-entries cap. A `BlockRemoved` only drops the entry in **single-tier (non-L2) mode**, where the subscriber forwards it as `PREFIX_EVICTED`; in **L2/Offload mode** a `BlockRemoved` **retains** the entry — the subscriber re-reports it at tier T2 rather than deleting it, so the count holds (the entry ages out only via TTL). Idempotent re-reports and T1↔T2 tier changes on an existing `(scheme, hash)` do **not** move it. The probe's synthetic state IS in the index during a Run but is excluded from this gauge so a scrape that races Stage C cannot transiently surface a probe-tenant count on a real model bucket — see `WithReservedTenants` in `pkg/index/index.go`. |

### Counters

| Metric | Labels | Meaning | Notes |
|---|---|---|---|
| `inferencecache_lookup_route_calls_total` | `model`, `reason_code`, `hint_used` | One increment per `LookupRoute` call, labeled by outcome. | `reason_code` ∈ values defined in [reason-codes.md](reason-codes.md). `hint_used="true"` ⇔ the response's `replica_scores` was non-empty — it covers **all three** non-empty-scores codes: `PREFIX_MATCH`, `TENANT_HOT`, and `AFFINITY_HINT` (the consistent-hash fallback that fires on the StrategyNone branch when `CachePolicy.spec.affinityRouting` is `Enabled` — the kubebuilder default — and a usable seed + serving replica exists). The **prefix-hit SLO** of the cache plane is the ratio of `reason_code="PREFIX_MATCH"` over the total; the broader **hint ratio** (`hint_used="true"` over total) tracks how often any locality hint surfaces; the **affinity share** (`reason_code="AFFINITY_HINT"` over total) tracks how often the consistent-hash fallback is doing the work — a high affinity share with a low PREFIX_MATCH share is the diffuse-single-turn signature this feature targets. Dashboards should chart all three, since a healthy PREFIX_MATCH ratio is what drives the TTFT win, and the AFFINITY_HINT share tells you whether the gateway is round-robin-blind or pin-stable on the non-match path. |
| `inferencecache_snapshot_auth_total` | `result` | One increment per `/snapshot` request reaching the auth middleware, labeled by outcome. | `result` ∈ `ok` (bearer validated, controller SA, handler invoked), `unauth` (missing/empty/malformed bearer, OR TokenReview said `Authenticated=false` regardless of whether `Status.Error` is populated — this includes the **wrong-audience** case: a token whose JWT audience does not match the server's `--controller-audience` is reported by the apiserver as not authenticated, surfaced here in the same bucket), `forbidden` (TokenReview authenticated a non-controller identity), `error` (TokenReview Go-level transport failure, nil response, RBAC reject before review even ran — fail-closed 503). The `unauth` bucket intentionally collapses several apiserver-reported denials (plain bad token, `Status.Error` populated by the SA-token authenticator's JWT parser, wrong audience) because they're not distinguishable from the response shape; the middleware logs the apiserver's diagnostic at WARN to the server log so the operator can still tell them apart (e.g. `token audiences [...] is invalid for the target audiences [...]` clearly signals an audience mismatch — usually a manifest/flag drift between controller projection and server `--controller-audience`). A non-trivial `unauth` rate against a steady controller poller cadence is the first signal that the projected SA token / RBAC / audience / apiserver path is broken; `error` rate firing means the review never actually ran ("investigate the apiserver"). |
| `inferencecache_policy_auth_total` | `result` | One increment per `/policy` request reaching the auth middleware, labeled by outcome. Mirror of `inferencecache_snapshot_auth_total` for the controller's write-side push. | Same `result` semantics as `inferencecache_snapshot_auth_total` above — `ok` / `unauth` / `forbidden` / `error` — and the same collapsed-bucket caveat: a wrong-audience token shows up in `unauth`, with the apiserver's `token audiences [...] is invalid` diagnostic at WARN. `/policy` uses the write-side `--policy-audience` (default `inferencecache.io/policy`), so a controller-audience snapshot/probe token is rejected here. The two counters live in parallel so dashboards distinguish read-side auth failures (snapshot scrape / info-leak attempt) from write-side ones (CachePolicy push / active-tampering attempt). Write-side `forbidden` rate is the alarming signal: it means a bearer was valid but issued to a non-controller identity, i.e. some other workload is trying to override cluster-wide cache policy. Endpoint caches are independent — each endpoint's valid token cache-misses once per TTL window in the steady state. |
| `inferencecache_probe_auth_total` | `result` | One increment per `/probe` request reaching the auth middleware, labeled by outcome. Third controller↔server endpoint on the snapshot listener; this counter mirrors the snapshot/policy pair so dashboards distinguish probe-side auth failures from read-side (`/snapshot`) and write-side (`/policy`) ones. | Same `result` semantics as `inferencecache_snapshot_auth_total` above — `ok` / `unauth` / `forbidden` / `error` — and the same collapsed-bucket caveat. The probe-specific alarming signal is a non-trivial `unauth` rate without a paired `ok` rate: the CacheBackend reconciler drives `/probe` once per CacheBackend per ~30s, so silent `unauth` here means probe results never reach the reconciler and the `FunctionalProbeOK` condition degrades to unknown — invisible to operators unless this metric is on the dashboard. `forbidden` mirrors the policy-write semantics (some other workload is trying to drive a probe — alarming). All three caches are independent. |
| `inferencecache_tenant_evictions_total` | `tenant_id`, `reason` | One increment per **distinct prefix** evicted from a tenant to bring it back within its `CacheTenant.spec.quota.maxIndexEntries` budget at ingest time (Fairness mode evicts the tenant's own oldest prefixes). | `reason` ∈ `over_entries` (only dimension today — the index-entry budget). A multi-replica prefix counts once (the eviction unit is the distinct prefix key, matching `maxIndexEntries`). A steadily rising rate for a `tenant_id` means that tenant is sustainably over budget — its declared cap is too small for its working set, or a client is churning prefixes. The series is created lazily on the first eviction, so a tenant that never exceeds budget emits nothing. |
| `inferencecache_index_evictions_total` | `algorithm`, `reason` | One increment per **replica×prefix entry** removed by the index's own sweeps (distinct from the quota path above). | `algorithm` ∈ `lru` / `lfu` — the namespace's resolved `CachePolicy.spec.eviction`. `reason` ∈ `cap` (global `MaxEntries` exceeded — victims chosen by the algorithm: oldest-`lastSeen` for `lru`, lowest-access-count for `lfu`) / `ttl` (freshness sweep; algorithm-independent removal, but labeled with the namespace algorithm for attribution). Series are created lazily on the first eviction. A rising `reason="cap"` rate means the index is sustainably above `MaxEntries`; `lfu` keeps frequently-hit prefixes longer than `lru` under the same pressure. |

### Histograms

| Metric | Labels | Meaning | Buckets |
|---|---|---|---|
| `inferencecache_lookup_route_latency_seconds` | `model` | Server-side `LookupRoute` latency: from handler entry to response, including ranking. | `[100µs, 250µs, 500µs, 1ms, 2.5ms, 5ms, 10ms, 25ms, 50ms, 100ms]`. Targets sub-ms (cache-hot path); buckets exist up to 100 ms to catch tail/regression. |

---

## Controller metrics (`inferencecache_*`) — exposed today

Emitted by the `cmd/controller` binary, registered into the controller-runtime metrics registry (`sigs.k8s.io/controller-runtime/pkg/metrics`), and served at the manager's `--metrics-bind-address` (default `:8080` on the controller binary — separate process from the server binary's `:8080`). This is a deliberately separate registry from the server's `pkg/server/metrics.go` one; the two processes have disjoint scrape targets.

### Gauges

| Metric | Labels | Meaning | Notes |
|---|---|---|---|
| `inferencecache_backend_t2_hit_rate` | `backend` | Query-weighted tier-2 (external offload, e.g. LMCache) reload hit-rate for the CacheBackend, in `[0,1]`. Set by the CacheIndex poller from `status.indexParticipation.t2HitRate`. **A series exists only once the tier has been exercised** (external lookups observed for the backend's replicas); `0` means the tier was queried but served zero reloads — a silently-degraded offload tier (store/connection failure, under-sized remote server, or scheduler/worker hash mismatch). | **Series lifecycle:** present-when-exercised; the value is the cumulative hit-rate, so it persists while a backend is idle and is pruned only when the backend's replicas leave the index snapshot (engine pods gone / index entries TTL-expired) or the backend is deleted — taint-aware (a namespace whose pod lookups failed this tick is left untouched). A degraded-then-idle backend keeps exporting `0` until its replicas drain, so tier-2 alerting gates on `rate(inferencecache_backend_t2_query_tokens_total)` (in Counters below) rather than series disappearance. The `backend` label is the canonical `<namespace>/<name>` (matching `inferencecache_backend_probe_result_total`, so one label identifies the CacheBackend; Prometheus injects the install `namespace` from the controller scrape target — this vec carries no own `namespace` label that would collide with it). `backend` cardinality is bounded by the CacheBackend fleet. This is the Alertmanager surface for the `T2Degraded` condition (CR `.status` is not scraped); see the `T2Degraded` row in [`docs/design/cachebackend-api.md`](../design/cachebackend-api.md) → Conditions. |

### Counters

| Metric | Labels | Meaning | Notes |
|---|---|---|---|
| `inferencecache_backend_probe_result_total` | `backend`, `stage`, `result` | One increment per stage per probe call the CacheBackend reconciler issues to the server's `/probe` endpoint. `backend` is the canonical `<namespace>/<name>`; `stage` ∈ `ingest` / `routing` / `t2`; `result` ∈ `ok` / `failed` / `skipped`. A successful probe call emits three increments (one per stage); an HTTP-level failure to reach `/probe` emits zero (no per-stage outcome was observed — the call itself failed). Skipped stages count too — the metric reflects "what the probe round-trip looked like," not "what was exercised." | `backend × stage × result` cardinality is bounded by the size of the CacheBackend fleet × 3 × 3, comfortably small. Probe rate (~once per backend per 30s) keeps total emission tame. Dashboards key off the `result="failed"` slice for the alerting signal — a steady rate means the cache plane has a known regression for that backend; the `stage` label points at which layer broke. See [`docs/design/cachebackend-api.md#functional-probe-gate`](../design/cachebackend-api.md#functional-probe-gate) for the semantics. The `ServerProbeFail` alert in `config/observability/{alerting-rules,prometheus-rules}.yaml` is wired off this metric. |
| `inferencecache_backend_server_restart_cascades_total` | `namespace`, `backend`, `reason` | One increment per cascade-restart **decision** the `CacheBackend` reconciler emits when it observes a cache-server-pod replacement that warrants engine recovery. **The counter advances per cascade EVENT, not per Deployment patched** — a cascade that matches zero injected engine `Deployment`s today still counts as one event (the controller decided recovery was needed; the engine fleet may simply not be deployed yet, or `spec.engineSelector` is being rewired). The decision fires after the rate-limit window has elapsed and after the engine-Deployment annotates succeed — BEFORE the subsequent `status.observedServerInstance` patch. The metric reflects the cascade decision the moment it commits — any matched engine `Deployment`s have already been annotated and the rollout that drives the recovery is in flight — rather than lagging behind a transient status-write failure. A zero-match cascade (no engines injected yet, or `spec.engineSelector` is being rewired) still increments because the controller's "decided to recover" state is operator-actionable even when no engine rolled. Double-counting on retry is prevented by an in-process `(key, currentID)` ledger: a subsequent reconcile that re-enters the cascade branch with the same identifier does not advance the counter. | NOT a raw restart count: the cascade is rate-limited to at most once per ~30s per backend (see `DefaultMinServerRestartCascadeInterval`), so a crash-looping cache-server that restarts 10× inside one window still increments this counter once. For raw cache-server pod restart rate, scrape `kube_pod_container_status_restarts_total` from kube-state-metrics instead. Today `reason` is always `server_instance_changed`; future operator-initiated "force cascade" surfaces would add their own value. A series is created lazily on the first cascade — a backend that never cascades emits nothing. The cascade itself is the operator-side recovery for the upstream LMCache `LMServerConnector` EPIPE-on-restart bug ([LMCache/LMCache#3565](https://github.com/LMCache/LMCache/issues/3565)); see [`docs/design/cachebackend-api.md` `observedServerInstance`](../design/cachebackend-api.md). |
| `inferencecache_backend_t2_query_tokens_total` | `backend` | **Monotonic** count of tier-2 (external offload) query tokens observed per CacheBackend — the **activity signal** for tier-2-degradation alerting: `rate(...)` separates an actively-queried backend (degraded when its hit-rate is `0`) from one that took a few cold misses and went idle. The CacheIndex poller accumulates only the **positive per-tick deltas** of the per-backend aggregate cumulative, so a drop from replica/tenant churn or an engine restart is clamped out and `rate()` never sees a phantom reset. | `backend` is the canonical `<namespace>/<name>` (Prometheus injects the install `namespace`); same present-when-exercised lifecycle + stale-series pruning as `inferencecache_backend_t2_hit_rate`. Intended for `rate(...) > 1000` tokens/sec activity gating in a tier-2-degradation alert (shipped separately in the observability bundle). |

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

### Server binary (`cmd/server`)

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
- **`indexEvictions` writer:** the index calls `AddIndexEvictions(algorithm, reason, n)`
  via the `index.Metrics` interface after the cap sweep (`reason="cap"`, on ingest)
  and the TTL sweep (`reason="ttl"`); see [`pkg/index/`](../../pkg/index/). The
  per-algorithm tally is emitted after the index lock is released.
- **`snapshotAuth` + `policyAuth` + `probeAuth` writers:** the TokenReview
  middleware in [`pkg/server/auth/`](../../pkg/server/auth/) reports one
  outcome per request via the `auth.ResultRecorder` interface. The
  recorders themselves are returned by `serverMetrics.SnapshotAuthRecorder()`,
  `serverMetrics.PolicyAuthRecorder()`, and `serverMetrics.ProbeAuthRecorder()`
  (in `pkg/server/metrics.go`) and wired into the per-endpoint authenticators
  in `pkg/server/server.go`. One increment per `/snapshot`, `/policy`, or
  `/probe` request reaching the middleware, labeled by `result`. All three
  endpoints share the controller ServiceAccount identity profile but emit
  per-endpoint counters and enforce endpoint-specific audiences so a dashboard
  can distinguish read-side, write-side, and probe-side failures.

### Controller binary (`cmd/controller`)

- **Definitions:** package-level vars in the relevant reconciler files,
  registered into the controller-runtime metrics registry on `init()`.
  This is a separate `prometheus.Registry` from the server binary's per-
  Service registry.
- **`backendServerRestartCascadesTotal` writer:** the `CacheBackend`
  reconciler increments it once per cascade in
  [`internal/controller/cachebackend_server_restart.go`](../../internal/controller/cachebackend_server_restart.go).
  See the `reconcileServerInstance` godoc for when a cascade is and is
  not emitted (rate-limit, strict-superset midpoints, converged
  scale-ups, stale-while-unavailable).
- **`inferencecache_backend_probe_result_total` writer:** the
  `CacheBackend` reconciler in
  [`internal/controller/cachebackend_probe.go`](../../internal/controller/cachebackend_probe.go)
  invokes `recordProbeResult(backendKey, result)` after each successful
  `/probe` call (HTTP-level failures emit no increment — they're not a
  per-stage outcome). Three increments per successful call (one per
  stage); skipped stages count so the metric reflects the full probe
  shape. The Go variable backing it is `probeResultMetric` (a
  `prometheus.CounterVec`).
- **`inferencecache_backend_t2_hit_rate` + `inferencecache_backend_t2_query_tokens_total` writer:** the `CacheIndexPoller`
  sets both per CacheBackend in
  [`internal/controller/cacheindex_controller.go`](../../internal/controller/cacheindex_controller.go)
  (`refreshCacheBackendParticipation`) — the hit-rate from the projected
  `status.indexParticipation.t2HitRate`, the query-token counter by accumulating
  the positive per-tick deltas of the aggregate cumulative (see `t2QueryDelta`)
  — and prunes both stale series — taint-aware
  — via `reconcileBackendT2HitRateSeries` in
  [`internal/controller/cacheindex_metrics.go`](../../internal/controller/cacheindex_metrics.go).
  The Go variables backing them are `backendT2HitRate` (a `prometheus.GaugeVec`)
  and `backendT2QueryTokensTotal` (a `prometheus.CounterVec`).

---

## How `/metrics` is served

Two binaries each expose their own `/metrics` endpoint — separate processes, separate Prometheus registries, separate scrape targets.

### Server binary (`cmd/server`)

- HTTP endpoint **`/metrics`** on the server's public HTTP listener (default
  `:8080`, flag `--http-bind-address`). Format: Prometheus exposition.
- Companion endpoints on the public listener: **`/healthz`** (liveness),
  **`/readyz`** (readiness → `index.Ready()`). These stay unauthenticated —
  kubelet probes and Prometheus scrapes cannot present a SA bearer.
- Separate **controller-facing** listener (default `:8081`, flag
  `--snapshot-bind-address`) carries `/snapshot` (controller-read
  of the cluster-wide cache aggregate; populates the `CacheIndex` CR
  status), `/policy` (controller-write of the combined resolved
  snapshot — `CachePolicy` entries plus `CacheTenant` quota entries;
  replace-on-write), and `/probe` (controller-driven functional
  self-test, per CacheBackend). The gate has **THREE independent
  layers**, each meant to catch a failure mode the others can't, with one
  ServiceAccount identity and endpoint-specific audiences:
  - **L3/L4:** a `NetworkPolicy` restricts ingress to the controller's
    pod selector.
  - **L7 identity:** TokenReview-backed bearer middleware rejects every
    request whose token does not resolve to the configured controller
    `ServiceAccount` (`--allowed-controller-sa`).
  - **L7 audience:** the controller mounts two audience-bound projected
    SA tokens: `inferencecache.io/controller` at
    `/var/run/secrets/inferencecache.io/controller-token/token` for
    `/snapshot` + `/probe`, and `inferencecache.io/policy` at
    `/var/run/secrets/inferencecache.io/policy-token/token` for `/policy`.
    The server passes `TokenReviewSpec.Audiences=[--controller-audience]`
    on `/snapshot` + `/probe`, and `[--policy-audience]` on `/policy`, so a
    default-audience apiserver token from the same controller SA is rejected,
    and a leaked snapshot/probe token cannot push policy. Under the default
    apiserver audience configuration, a leaked audience-bound bridge token is
    useless against the apiserver and vice versa; the cross-surface property
    holds only while the apiserver is not configured to also accept either
    inference-cache audience as an apiserver audience.

  `inferencecache_snapshot_auth_total`, `inferencecache_policy_auth_total`,
  and `inferencecache_probe_auth_total` (see the counter table above)
  are the parallel observability surfaces — one per endpoint — for the
  **two L7 layers (identity + audience)**. NetworkPolicy drops happen
  at the CNI before the listener and cannot increment any of these counters
  — observe those via kube state metrics on the policy resource + the
  CNI's flow logs (Calico / Cilium / etc.), separately from the auth
  counters. Audience-mismatch denials land in `result="unauth"` with
  the apiserver's diagnostic in the server WARN log — operators
  chasing a binding regression should grep for
  `token audiences [...] is invalid for the target audiences`.

### Controller binary (`cmd/controller`)

- HTTP endpoint **`/metrics`** on the controller-runtime manager's metrics
  listener (default `:8080`, flag `--metrics-bind-address`). This is a
  different process from the server binary's `:8080`, so the two can
  share the same port number on different pods without conflict.
- Format: Prometheus exposition. Includes both the
  `inferencecache_backend_*` controller metrics (defined in this repo)
  and the standard controller-runtime metrics (`controller_runtime_*`,
  `workqueue_*`, `rest_client_*`, …) that controller-runtime registers
  by default.
- Unauthenticated by default (`secureMetrics=false`); operators who
  want a bearer-gated controller metrics surface should set
  `--metrics-secure` and front it with the same TokenReview pattern
  the server's `:8081` listener uses.

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
2. **Decide which binary owns the metric.** Pick by where the work that
   moves it actually runs — server-side request handling and the in-memory
   index belong in the server binary; reconciler / webhook / controller-
   loop behaviors belong in the controller binary. The two processes use
   separate Prometheus registries and serve different `/metrics` endpoints
   (see "How `/metrics` is served"); pick the wrong one and operators
   scrape the wrong target. Each binary has its own definition and
   registration pattern:

   - **Server binary (`cmd/server`)**: add a field to `serverMetrics` in
     `pkg/server/metrics.go`, construct it in `newServerMetrics`, and
     register it on the `prometheus.NewRegistry()` block. Add a typed
     writer method on `*serverMetrics` (e.g. `observeLookup`,
     `SetIndexEntries`) and call it from the relevant handler or index
     path. Don't let handlers touch the prometheus collector directly —
     keeping the surface narrow makes it test-mockable.
   - **Controller binary (`cmd/controller`)**: declare a package-level
     `prometheus.NewCounterVec` / `NewGaugeVec` / etc. var in the
     reconciler / webhook file that uses it (e.g.
     `backendServerRestartCascadesTotal` in
     `internal/controller/cachebackend_server_restart.go`); register it
     into `sigs.k8s.io/controller-runtime/pkg/metrics.Registry` from
     an `init()` so it appears on the manager's `/metrics` endpoint
     without a separate plumbing path. Add a package-private
     `reset*ForTest()` helper so unit tests can clear state between
     runs.

   In both cases use the `inferencecache_*` prefix so the surface stays
   consistently namespaced.
3. **Update the relevant table above** in the correct binary section
   (Server / Controller) and sub-section (Gauges / Counters / Histograms).
   Include labels, meaning, and what makes it move. If the metric is a
   histogram, document the bucket array and *why* those buckets.
4. **Wire test coverage.** Server-binary metrics: add an assertion in
   `pkg/server/metrics_test.go`. Controller-binary metrics: add an
   assertion in a `_test.go` file alongside the reconciler that increments
   them (e.g. `cachebackend_server_restart_test.go` — see the
   `cascadeRestartsCount` helper for the pattern). In both cases verify
   the metric appears in `/metrics` output with the expected name and
   labels.
5. **Flag the schema impact in the PR description.** If the metric is a
   candidate for the §4.3 public schema (F3 owns that effort), say so —
   F3 tracks which `inferencecache_*` series are promoted to the public
   contract vs which remain internal/advisory.
6. **Dashboards.** If you have a Grafana panel in mind, drop the PromQL in the
   PR description so F4 (dashboards + CLI) can pick it up cleanly.

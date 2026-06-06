# Alerts runbook

The inference-cache operator ships a default Prometheus alert bundle under
[`config/observability/`](../../config/observability/). Each alert below catches
a degenerate metric pattern that has surfaced in production at least once: the
metric was on `/metrics` from day one, but nothing watched the pattern, so the
failure ran silently for days. The alerts make those patterns loud.

The alert annotations are deliberately short (one sentence + `runbook_url`).
This file is the long form: causes, triage steps, example PromQL.

## How to enable

There are two distribution shapes, same rule set, drift-gated by
`make verify-prometheus`:

- **prometheus-operator / kube-prometheus installs**: apply the
  [PrometheusRule CR](../../config/observability/prometheus-rules.yaml):

  ```bash
  kubectl apply -k config/observability
  ```

  The CR is pinned to namespace `inference-cache-system` (the operator
  namespace) and carries the default kube-prometheus selector labels
  (`prometheus: kube-prometheus`, `role: alert-rules`).

  > **Heads-up — Prometheus may scope rule discovery by namespace.** Most
  > prometheus-operator installs run a `Prometheus` CR with both a
  > `ruleSelector` (matches `PrometheusRule.metadata.labels`) AND a
  > `ruleNamespaceSelector` (matches the namespaces the rules live in).
  > The default kube-prometheus install allows all namespaces, but a
  > hardened install may restrict to e.g. `monitoring`. If
  > `kubectl get prometheusrule -A` shows the CR landed but Prometheus
  > never loads it, check your `Prometheus.spec.ruleNamespaceSelector`
  > and either widen it, override the namespace in
  > `config/observability/kustomization.yaml`, or move the CR by hand.

- **Vanilla Prometheus / Helm**: mount
  [`alerting-rules.yaml`](../../config/observability/alerting-rules.yaml) into
  Prometheus via the `rule_files:` config block, a ConfigMap, or the Helm
  `prometheus.serverFiles` value (depending on your install).

Both files contain the same five Stage 1 alerts plus commented-out
placeholders for three more that depend on metrics not yet exposed (see
[Deferred alerts](#deferred-alerts) below).

---

## Stage 1 alerts

### `IndexEmpty`

- **Severity**: `critical`
- **For**: 2 minutes
- **Source metrics**: `inferencecache_server_up`,
  `inferencecache_index_entries{model}`

The cache policy server reports `server_up=1` but the index holds zero
prefix entries across every model. That means `ReportCacheState` is not
receiving (or not recording) any KV events from engines. The cache plane
is effectively a no-op until this clears: every `LookupRoute` returns
`NO_HINT`; the gateway falls back to its default routing.

A cold-start dwell (server just started, first engine still booting) is
normal and dampened by the 2-minute `for:` window. Sustained firing means
the subscriber → server pipeline is genuinely broken.

#### Likely causes

1. **`kvevent-subscriber` sidecar not injected**: the operator's
   `--kvevent-subscriber-image` flag is empty on the controller, or the
   pod carries the `inferencecache.io/skip-inject: "true"` annotation,
   so the mutating webhook does not attach the sidecar.
2. **Engine prefix-cache disabled or KV events publisher off**: the
   engine needs to be configured to emit KV events. For vLLM that means
   both `--enable-prefix-caching` and a `--kv-events-config` block. If
   prefix caching is on but events are not configured, the engine
   internally has cache state but never publishes it.
3. **Subscriber → server gRPC dial failing**: NetworkPolicy too tight,
   TLS misconfig (TLS opted in at the server but plaintext at the
   subscriber), DNS not resolving the Service, or the subscriber is
   silently exiting on a bad flag.

#### First-response runbook

```bash
# 1. Confirm the server reports up.
kubectl -n inference-cache-system port-forward svc/inference-cache-server 8080:8080 &
curl -s localhost:8080/metrics | grep -E 'inferencecache_server_up|inferencecache_index_entries'

# 2. Confirm the subscriber sidecar is attached and running on an engine pod.
kubectl get pod -l <engine-selector-label> -o jsonpath='{.items[0].spec.containers[*].name}'
kubectl logs <engine-pod> -c kvevent-subscriber --tail=50

# 3. Confirm the engine is emitting KV events.
kubectl exec <engine-pod> -- curl -s localhost:8000/metrics | grep -E 'prefix_cache_(queries|hits)'

# 4. Confirm the controller is wiring the sidecar image.
kubectl -n inference-cache-system get deploy/inference-cache-controller-manager \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="manager")].args}'
```

Triage queries:

```promql
# Per-model index population
inferencecache_index_entries

# Server liveness across all instances
inferencecache_server_up
```

---

### `LMCacheT2NoHits`

- **Severity**: `warning`
- **For**: 5 minutes
- **Source metrics**: `vllm:external_prefix_cache_queries_total{pod}`,
  `vllm:external_prefix_cache_hits_total{pod}` (emitted by vLLM, not by
  this operator)

The engine is hitting the external (offload) prefix cache tier at more
than 100 queries per second but is getting zero hits. This is a textbook
silent-failure signal: the offload tier looks "wired" to Kubernetes (the
sidecar is up, the CacheBackend is `Ready`), but no offloaded prefix is
being recalled. Every offload `put` is wasted work; every `get` returns
empty; the engine refills T2 forever without ever benefiting from it.

The 100 queries/sec floor avoids false positives on a near-idle replica
that has only made a handful of queries. The 5-minute `for:` window
requires sustained zero-hit behavior — a transient miss-streak (e.g.
after a backend restart with empty T2) does not trip it.

#### Likely causes

1. **Client/server version skew in the offload subsystem** (most common
   in practice). An old offload-client library baked into the engine
   image is talking to a newer offload-server image, or vice versa. The
   TCP wire handshake succeeds (so the engine *thinks* it is connected)
   but `put`/`get` opcodes diverge: `put` succeeds at the client and
   returns silently, `get` returns empty at the server. The cache reports
   "stored N tokens" in the engine logs and nothing ever comes back.
2. **External backend down or unreachable** for an `External`-type
   CacheBackend with a wrong endpoint configured.
3. **Authentication mismatch** between the offload client and server
   (when the offload backend supports auth — most offload backends
   today do not).

#### First-response runbook

```bash
# 1. Confirm the symptom on the engine pod.
kubectl exec <engine-pod> -- curl -s localhost:8000/metrics \
  | grep -E 'vllm:external_prefix_cache_(queries|hits)_total'

# 2. Compare the offload-client version (in the engine image) against
#    the offload-server pod image tag. Skew is the root cause in most
#    incidents.
kubectl exec <engine-pod> -- pip show <offload-client-pkg> | grep Version
kubectl get pod -l <offload-server-selector> -o jsonpath='{.items[0].spec.containers[0].image}'

# 3. Inspect the offload server log for protocol errors.
kubectl logs <offload-server-pod> --tail=200 | grep -iE 'invalid|version|protocol|scheme'
```

Triage queries:

```promql
# External cache hit rate per pod (should be > 0 on a working offload)
sum by (pod) (rate(vllm:external_prefix_cache_hits_total[10m]))
/
sum by (pod) (rate(vllm:external_prefix_cache_queries_total[10m]))

# Stores vs. hits over the last hour
sum by (pod) (increase(vllm:external_prefix_cache_queries_total[1h]))
sum by (pod) (increase(vllm:external_prefix_cache_hits_total[1h]))
```

> The `vllm:` prefix is how vLLM exposes its metrics. If your install
> uses a `metricRelabeling` rule to strip the colon (some Helm charts
> do), adjust the alert expressions accordingly.

---

### `LookupRouteDegenerate`

- **Severity**: `warning`
- **For**: 5 minutes
- **Source metric**: `inferencecache_lookup_route_calls_total{model, reason_code}`

For at least one model, more than 90% of `LookupRoute` calls over the
past 10 minutes returned `reason_code="NO_HINT"`. The cache plane is
surfacing no replica hints; the gateway is falling back to its default
routing (round-robin, least-loaded, …) and the prefix-cache hit benefit
is not being realized.

The 0.1 q/s rate gate on total calls avoids tripping on an idle model
that happens to have published one NO_HINT and nothing else.

#### Likely causes

1. **Tenant ID mismatch**: the gateway is calling `LookupRoute` with a
   `tenant` that does not match the tenant key used at `ReportCacheState`
   ingest. The index has entries, but they are filed under a different
   key. Look for sustained NO_HINT alongside `inferencecache_index_entries > 0`.
2. **Engine not emitting KV events** (overlaps with `IndexEmpty` but at
   sub-cluster scale — only some models broken).
3. **`hash_scheme` mismatch**: the gateway's request carries an empty or
   unrecognized `hash_scheme`. An empty `hash_scheme` is dropped on
   lookup as a forward-compat safeguard ([reason-codes.md](../reference/reason-codes.md)).
4. **`CachePolicy.minimumPrefixTokens` set above the real prompt-prefix
   length**: the chain walk filter rejects every candidate before the
   ranker sees it.
5. **`CachePolicy.lookupTimeoutMs` too tight** also presents as
   degenerate routing — see [`LookupRouteHighTimeout`](#lookuproutehightimeout)
   first to disambiguate.

#### First-response runbook

```bash
# 1. Confirm the reason_code distribution per model.
kubectl -n inference-cache-system port-forward svc/inference-cache-server 8080:8080 &
curl -s localhost:8080/metrics | grep 'inferencecache_lookup_route_calls_total'

# 2. Spot-check what the gateway sends. The fastest way is gRPC client
#    debug logging in the gateway; failing that, take a tcpdump on the
#    server pod and decode a few LookupRoute frames.

# 3. Confirm the index has entries for the model and tenant.
curl -s localhost:8081/snapshot -H "Authorization: Bearer $TOKEN" | jq '.tenants[]'
```

Triage queries:

```promql
# Per-model NO_HINT ratio over the last hour
sum by (model) (rate(inferencecache_lookup_route_calls_total{reason_code="NO_HINT"}[1h]))
/
sum by (model) (rate(inferencecache_lookup_route_calls_total[1h]))

# Distribution across reason codes per model
sum by (model, reason_code) (rate(inferencecache_lookup_route_calls_total[1h]))
```

---

### `LookupRouteHighTimeout`

- **Severity**: `warning`
- **For**: 5 minutes
- **Source metric**: `inferencecache_lookup_route_calls_total{model, reason_code="TIMEOUT"}`

For at least one model, more than 5% of `LookupRoute` calls hit the
lookup-timeout budget over the past 10 minutes. The fail-open path
returned an empty hint; the gateway routed those requests by its own
default policy. Every TIMEOUT is a missed cache-hit opportunity.

#### Likely causes

1. **Server overload**: the lookup ranking is in-memory and should be
   sub-millisecond. A surprising p95/p99 tail usually means the working
   set has grown past what the index's global `MaxEntries` cap was sized for. Check
   `inferencecache_lookup_route_latency_seconds`.
2. **`CachePolicy.spec.lookupTimeoutMs` too tight**: a 5 ms timeout on a
   gateway-side 50 ms deadline is asymmetric. Either raise the timeout
   or reduce index pressure.
3. **gRPC backpressure** at the server: too many concurrent streaming
   subscribers, file-descriptor pressure, or a slow connection holding
   the ranker mutex. Inspect `go_goroutines` for the server pod.

#### First-response runbook

```bash
# 1. Confirm the per-model TIMEOUT ratio.
kubectl -n inference-cache-system port-forward svc/inference-cache-server 8080:8080 &
curl -s localhost:8080/metrics | grep 'inferencecache_lookup_route_calls_total'

# 2. Inspect the lookup latency tail.
curl -s localhost:8080/metrics | grep 'inferencecache_lookup_route_latency_seconds'

# 3. Inspect server resource consumption.
kubectl top pod -n inference-cache-system -l app.kubernetes.io/name=inference-cache,app.kubernetes.io/component=server
```

Triage queries:

```promql
# Lookup-latency p99 per model
histogram_quantile(0.99,
  sum by (model, le) (rate(inferencecache_lookup_route_latency_seconds_bucket[5m]))
)

# Server pod CPU + memory pressure
sum by (pod) (rate(process_cpu_seconds_total[1m]))
sum by (pod) (process_resident_memory_bytes)
```

---

### `IndexEvictionsSpike`

- **Severity**: `info`
- **For**: 10 minutes
- **Source metric**: `inferencecache_index_evictions_total{algorithm, reason="cap"}`

The cache index is evicting more than 10 entries per second under the
`reason="cap"` policy. That means the working set has outgrown the
**index's global `MaxEntries` cap** — the cluster-wide upper bound on
total `(replica × prefix)` entries the server's in-memory index will hold
(default `1_000_000`, configured via the `WithMaxEntries` option on the
server's index constructor; not currently exposed as a CRD or CLI flag).
Recent prefix entries are being dropped on top of recording new ones —
the cache is doing useful work but is under sustained capacity pressure.

This is informational, not an outage signal. It is a tuning lever:

1. **Raise the global `MaxEntries` cap** to fit the observed working set.
   Today this requires a code-side server-binary build change; expose it
   as a CLI flag if you need to retune in place. (Filed for follow-up.)
2. **Accept the reduced hit rate** at the current cap.
3. **Shorten the index TTL** (server's `WithTTL` option, default 30m) so
   old prefixes age out before the cap kicks in.
4. **Tighten per-tenant budgets** via
   [`CacheTenant.spec.quota.maxIndexEntries`](../reference/metrics.md#counters)
   so a runaway tenant cannot starve the global cap. The
   `inferencecache_tenant_evictions_total{tenant_id}` counter attributes
   pressure per tenant.

The 10/sec threshold is conservative for steady-state operation; tune
it locally to your install's expected baseline by editing the alert's
`expr`.

`reason="ttl"` evictions are excluded — those are healthy lifecycle.

Triage queries:

```promql
# Cap vs. TTL eviction rate
sum by (algorithm, reason) (rate(inferencecache_index_evictions_total[10m]))

# Current index population, per model (sum gives the total against the cap)
sum(inferencecache_index_entries)

# Per-tenant eviction pressure (CacheTenant quota — distinct from the
# global cap above)
sum by (tenant_id) (rate(inferencecache_tenant_evictions_total[10m]))
```

---

## Deferred alerts

Three more alerts are scoped to ship as part of the same observability
bundle, but they depend on metrics not yet exposed on `/metrics`. The
placeholder rules sit in [`alerting-rules.yaml`](../../config/observability/alerting-rules.yaml)
and the [`PrometheusRule` CR](../../config/observability/prometheus-rules.yaml)
as comments; uncomment them in the same change that ships the corresponding
metric.

| Alert | Blocked on | What it would catch |
|---|---|---|
| `VersionSkewDetected` | `inferencecache_backend_version_skew` gauge — exposed by a follow-up that detects engine-vs-cache-server version skew | The `LMCacheT2NoHits` failure class, but BEFORE it manifests as zero hits — caught proactively by the operator detecting the skew at admit time. |
| `KvEventsStaleness` | `inferencecache_replica_last_event_at` gauge — exposed by the Ready-on-first-event follow-up | A replica that *was* emitting KV events stopped (engine crash, OOMKill, NetworkPolicy regression, subscriber dead). Distinct from `IndexEmpty` (replica never published) — this catches *post-warmup* silence. |
| `ServerProbeFail` | `inferencecache_backend_probe_result` counter — exposed by the synthetic end-to-end Ready gate | A managed backend's synthetic publish → index → lookup round-trip is failing. The basic Service-endpoint probe cannot catch this; only the end-to-end probe can. |

---

## How alerts compose

The five Stage 1 alerts are not independent — they map onto a small set
of recurring failure modes:

| Failure mode | Alerts that fire | Where to look first |
|---|---|---|
| Subscriber sidecar not injected | `IndexEmpty` (critical) | controller flags + webhook config |
| Engine prefix-cache off | `IndexEmpty` (critical) | engine flags `--enable-prefix-caching` + `--kv-events-config` |
| Offload tier version skew | `LMCacheT2NoHits` (warning) + maybe `LookupRouteDegenerate` if T2 is the only cache path | offload client/server image tags |
| Tenant or `hash_scheme` mismatch | `LookupRouteDegenerate` (warning) alongside `inferencecache_index_entries > 0` | gateway-side request shape |
| Server overload | `LookupRouteHighTimeout` (warning) + maybe `LookupRouteDegenerate` | `lookup_route_latency_seconds` p99, `process_resident_memory_bytes`, server's global `MaxEntries` |
| Working set outgrew config | `IndexEvictionsSpike` (info) | server's global `MaxEntries` vs. observed working-set size; `CacheTenant.spec.quota.maxIndexEntries` per tenant |

If two alerts fire together, work the more-severe one first; the lower
one usually clears once the root cause does.

---

## Related references

- [Prometheus metrics inventory](../reference/metrics.md) — the full
  surface this bundle reads from
- [Reason-code vocabulary](../reference/reason-codes.md) — meaning of
  each `reason_code` label on `inferencecache_lookup_route_calls_total`
- [Operator install](../../README.md) — where the alert bundle is
  surfaced in the install README

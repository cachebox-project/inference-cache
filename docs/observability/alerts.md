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

- **prometheus-operator / kube-prometheus installs**: apply the bundle:

  ```bash
  kubectl apply -k config/observability
  ```

  Ships TWO CRs together — `kubectl apply -k` applies both:
  1. A [`ServiceMonitor`](../../config/observability/servicemonitor.yaml)
     that tells Prometheus to scrape `inference-cache-server:8080/metrics`.
     Without this, kube-prometheus installs will load the rules but
     never collect the `inferencecache_*` series the rules read —
     `prometheus.io/scrape` annotations are commonly ignored in favor of
     explicit `ServiceMonitor` / `PodMonitor` CRs.
  2. The [`PrometheusRule`](../../config/observability/prometheus-rules.yaml)
     carrying the alerts.

  Both CRs are pinned to namespace `inference-cache-system`. The
  example selector labels each CR carries are:
  - `PrometheusRule` →
    `prometheus: k8s`, `role: alert-rules` (matched by
    `Prometheus.spec.ruleSelector`).
  - `ServiceMonitor` →
    `prometheus: k8s` (matched by
    `Prometheus.spec.serviceMonitorSelector`).

  Both target the **upstream kube-prometheus stack**, whose default
  `Prometheus` is named `k8s`. The `prometheus-community/kube-prometheus-stack`
  Helm chart uses a DIFFERENT convention — its selector matches
  `release: <helm-release-name>` (no `prometheus:` label). Custom
  Prometheus CRs use whatever their `ruleSelector` /
  `serviceMonitorSelector` specifies. If your install uses a different
  label set, edit each CR's labels to match — the YAML comments next
  to each label spell out the exact `kubectl get prometheus -A -o
  jsonpath=...` introspection command.

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
  `prometheus.serverFiles` value (depending on your install). You ALSO
  need a `scrape_configs:` entry — and to keep the alerts' per-install
  scoping working, that scrape must inject a `namespace` label. Two
  valid shapes:
  1. **Recommended** — Kubernetes service discovery
     (`kubernetes_sd_configs: pod` or `endpoints`) with `relabel_configs:`
     that copies `__meta_kubernetes_namespace` to a `namespace` label.
     Works in single-install AND shared-Prometheus setups.
  2. **Single-install only** — a static DNS scrape of
     `inference-cache-server.inference-cache-system.svc.cluster.local:8080`.
     Simpler but produces NO `namespace` label, so the alerts collapse
     into one unlabeled group. Acceptable when one Prometheus only ever
     scrapes one inference-cache install; do not use it for shared
     Prometheus deployments.

  ServiceMonitor in the operator bundle is the prometheus-operator
  equivalent of shape (1); both shapes (1) and (2) require you to wire
  scrape config explicitly when you are not on prometheus-operator.

Both files contain the same six active alerts (five Stage 1 alerts plus
the controller-side `ServerProbeFail`) plus commented-out placeholders for
two more that depend on metrics not yet exposed (see [Deferred
alerts](#deferred-alerts) below).

> **One alert depends on a separate scrape this bundle does NOT ship.**
> [`LMCacheT2NoHits`](#lmcachet2nohits) reads `vllm:external_prefix_cache_*`,
> which vLLM exposes on its own `/metrics`. The included `ServiceMonitor`
> covers only `inference-cache-server`. To make `LMCacheT2NoHits` light
> up, your install must also scrape engine pods — typically a separate
> **`PodMonitor`** for your vLLM Deployment, or a `ServiceMonitor` on
> a headless / per-pod Service (Endpoints discovery), or
> `kubernetes_sd_configs: pod` for vanilla Prometheus.
>
> **The scrape MUST preserve both `namespace` and `pod` labels** —
> the alert groups `sum by (namespace, pod)` and its summary
> substitutes `{{ $labels.pod }}`. A scrape against a load-balanced
> ClusterIP Service (single target, single `instance` label) would
> aggregate all replicas under one series with no `pod` label and
> render the summary with an empty pod. PodMonitor + standard
> prometheus-operator relabel rules give you both labels for free;
> ServiceMonitor against a headless Service does too (one Endpoints
> entry per Pod). Static targets do NOT.
>
> **And the PodMonitor MUST scope to cache-bound engine pods only.**
> The alert reads `vllm:external_prefix_cache_*` from EVERY scraped
> vLLM pod — there is no controller-injected label on engine pods that
> Prometheus can filter on, because the operator stamps an
> `inferencecache.io/injected-by` *annotation* (not a label), and
> annotations don't show up on scraped series. So scope at scrape time:
> the PodMonitor's `selector.matchLabels` should match the same labels
> your `CacheBackend.spec.engineSelector.matchLabels` uses. Otherwise
> an unrelated vLLM workload in the cluster (no cache-plane wiring,
> no LMCache offload) will trip `LMCacheT2NoHits` on
> inference-cache's behalf. A minimal scoped PodMonitor looks like:
>
> ```yaml
> apiVersion: monitoring.coreos.com/v1
> kind: PodMonitor
> metadata:
>   name: vllm-cache-bound
>   namespace: <your-engine-namespace>
> spec:
>   selector:
>     matchLabels:
>       app: my-engine                # MUST match CacheBackend.spec.engineSelector.matchLabels
>   podMetricsEndpoints:
>     - port: http                    # the port-name your vLLM container exposes /metrics on
>       path: /metrics
>       interval: 30s
> ```
>
> The other four alerts work as-is once this bundle is applied — they
> only read `inferencecache_*` series, which the shipped ServiceMonitor
> already scopes to `inference-cache-server`.
>
> **The alerts rely on a `namespace` label per install.** Both the shipped
> `ServiceMonitor` for `inference-cache-server` and any prometheus-operator
> `PodMonitor` you add for vLLM automatically inject `namespace` via the
> operator's standard relabel rules (sourced from the
> `__meta_kubernetes_namespace` Kubernetes SD label). Vanilla Prometheus
> with hand-written `scrape_configs:` does NOT inject this by default —
> you must add a `relabel_configs:` entry that copies
> `__meta_kubernetes_namespace` to a `namespace` label. Without it,
> per-install isolation collapses into one unlabeled group and one
> install's outage can mask another's.

---

## Stage 1 alerts

### `IndexEmpty`

- **Severity**: `critical`
- **For**: 2 minutes
- **Source metrics**: `inferencecache_server_up`,
  `inferencecache_index_entries{model}`,
  `inferencecache_lookup_route_calls_total`

The cache policy server reports `inferencecache_server_up=1` but the
index holds zero prefix entries across every model **while a gateway is
actively making `LookupRoute` calls**. That means `ReportCacheState` is
not receiving (or not recording) any KV events from engines. The cache
plane is effectively a no-op until this clears: every `LookupRoute`
returns `NO_HINT`; the gateway falls back to its default routing.

> The traffic gate (`sum(rate(inferencecache_lookup_route_calls_total[10m])) > 0`)
> distinguishes a genuine pipeline outage from a healthy idle install —
> e.g. an operator applied the bundle but has not deployed any engines
> yet, or no gateway is sending traffic. Those installs SHOULD have an
> empty index; alerting critical there would page every fresh opt-in
> deployment.

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

# 3. Confirm the engine's KV-event publisher is ON. Prefix-cache metrics
#    being present is NOT proof of this — vLLM emits prefix_cache_*
#    independently of the KV-events publisher. The signal we actually
#    want is the publisher's ZMQ socket / config:
kubectl exec <engine-pod> -- ps -ef | grep -E 'kv-events-config|vllm'
kubectl exec <engine-pod> -- cat /etc/vllm/kv-events.yaml 2>/dev/null || \
  kubectl get pod <engine-pod> -o jsonpath='{.spec.containers[?(@.name=="engine")].args}' \
    | tr ',' '\n' | grep -i kv-events

# 4. Confirm the subscriber sidecar is healthy. The subscriber logs
#    `subscribed to engine KV events ...` once on startup and is then
#    silent on the success path; failures (ZMQ recv error, gRPC stream
#    open/send/close error) are logged at WARN. So the working signal
#    is "startup log line present, no recent WARN":
kubectl logs <engine-pod> -c kvevent-subscriber --tail=200 \
  | grep -E 'subscribed to engine KV events|level=WARN|level=ERROR'
#    If you see only the "subscribed" line and no WARN/ERROR, the
#    subscriber half is healthy. To prove forwarding is actually
#    landing at the server, check that the server's index has at least
#    one entry attributed to this pod's replica:
kubectl -n inference-cache-system port-forward svc/inference-cache-server 8081:8081 &
TOKEN=$(kubectl -n inference-cache-system create token \
  inference-cache-controller-manager --audience=inferencecache.io/controller)
curl -s localhost:8081/snapshot -H "Authorization: Bearer $TOKEN" \
  | jq '.replicas[] | select(.replicaId | startswith("<engine-pod>"))'

# 5. Confirm the controller is wiring the sidecar image.
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
- **Source metrics**: `vllm:external_prefix_cache_queries{pod}` (or
  `_total`-suffixed variant), `vllm:external_prefix_cache_hits{pod}` (or
  `_total`-suffixed variant). Emitted by vLLM, not by this operator —
  upstream's metrics page lists the unsuffixed names but the Python
  prometheus_client convention appends `_total` to counters at
  exposition time, so deployments see one or the other depending on
  vLLM build/client. The alert accepts both.

The engine is hitting the external (offload) prefix cache tier at more
than 1000 **tokens** per second of queries but is getting zero hit
tokens. This is a textbook silent-failure signal: the offload tier looks
"wired" to Kubernetes (the sidecar is up, the CacheBackend is `Ready`),
but no offloaded prefix is being recalled. Every offload `put` is wasted
work; every `get` returns empty; the engine refills T2 forever without
ever benefiting from it.

> **vLLM's `external_prefix_cache_{queries,hits}_total` count tokens,
> not requests.** A single 1500-token shared prefix counts as 1500
> queries when it's checked against the offload tier. The 1000 tokens/sec
> floor catches a single moderately-prefixed request per second; idle
> replicas (no prefix-caching traffic at all) stay below it. Tune the
> threshold if your workload's prefix size differs substantially.

The 5-minute `for:` window requires sustained zero-hit behavior — a
transient miss-streak (e.g. after a backend restart with empty T2)
does not trip it.

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

#### Verify the metric is exposed before relying on the alert

vLLM emits `vllm:external_prefix_cache_{queries,hits}` (or, under the
Python prometheus_client exposition convention, the `_total`-suffixed
variants) on its own `/metrics` endpoint (typically `:8000/metrics`),
not via this operator. The upstream metric names are documented at
[`docs.vllm.ai/.../usage/metrics/`](https://docs.vllm.ai/en/latest/usage/metrics/);
the series have been present since vLLM 0.18 (the first release tagged
in the upstream v0.18 docs page). Our alert and triage queries accept
both the unsuffixed and `_total` forms via `{__name__=~"...(_total)?"}`.

This operator has no in-process scrape of those upstream metrics — its
own scraper (`pkg/adapters/engine/metrics_scraper.go`) only reads the T1
`vllm:prefix_cache_{hits,queries}` plus `vllm:*_cache_usage_perc`. That
means the alert binds directly to vLLM's exposition, and an upstream
rename, deprecation, or version skew can silently make the alert inert
while `promtool test rules` (which uses synthetic series) still passes.

**Operator responsibility:** before enabling the alert in production,
confirm at least one engine pod publishes the series:

```bash
# Matches BOTH the Python-prometheus-client convention
# (vllm:external_prefix_cache_queries_total) AND the unsuffixed form
# (vllm:external_prefix_cache_queries) — the alert uses
# `{__name__=~"...(_total)?"}` so it accepts whichever form your vLLM
# build emits.
kubectl exec <engine-pod> -- curl -s localhost:8000/metrics \
  | grep -E '^vllm:external_prefix_cache_(queries|hits)(_total)?'
```

A pod running an older vLLM (no offload support) will return no lines;
the alert won't fire there either (the `unless` guard handles absent
hits-series, but the queries gate of >1000 tokens/sec assumes the
metric IS exposed). If your install runs vLLM <0.18 for some pods,
exclude them via a label in the alert expression or upgrade. If a
future vLLM release renames the metric, update the alert (and this
runbook) accordingly.

#### First-response runbook

```bash
# 1. Confirm the symptom on the engine pod (accepts both the
#    unsuffixed and `_total` forms; see "Verify the metric is
#    exposed" above for the upstream-versions distinction).
kubectl exec <engine-pod> -- curl -s localhost:8000/metrics \
  | grep -E '^vllm:external_prefix_cache_(queries|hits)(_total)?'

# 2. Compare the offload-client version (in the engine image) against
#    the offload-server pod image tag. Skew is the root cause in most
#    incidents.
kubectl exec <engine-pod> -- pip show <offload-client-pkg> | grep Version
kubectl get pod -l <offload-server-selector> -o jsonpath='{.items[0].spec.containers[0].image}'

# 3. Inspect the offload server log for protocol errors.
kubectl logs <offload-server-pod> --tail=200 | grep -iE 'invalid|version|protocol|scheme'
```

Triage queries (use the `{__name__=~"...(_total)?"}` form so the query
matches whichever exposition shape your vLLM uses):

```promql
# External cache hit rate per pod (should be > 0 on a working offload)
  sum by (namespace, pod) (rate({__name__=~"vllm:external_prefix_cache_hits(_total)?"}[10m]))
/
  sum by (namespace, pod) (rate({__name__=~"vllm:external_prefix_cache_queries(_total)?"}[10m]))

# Stores vs. hits over the last hour
sum by (namespace, pod) (increase({__name__=~"vllm:external_prefix_cache_queries(_total)?"}[1h]))
sum by (namespace, pod) (increase({__name__=~"vllm:external_prefix_cache_hits(_total)?"}[1h]))
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
# 1. Port-forward the public + controller-facing HTTP listeners.
#    :8080 carries /metrics, /healthz, /readyz. :8081 carries the
#    controller-only /snapshot + /policy endpoints (bearer-auth gated).
kubectl -n inference-cache-system port-forward svc/inference-cache-server 8080:8080 &
kubectl -n inference-cache-system port-forward svc/inference-cache-server 8081:8081 &

# 2. Confirm the reason_code distribution per model.
curl -s localhost:8080/metrics | grep 'inferencecache_lookup_route_calls_total'

# 3. Spot-check what the gateway sends. The fastest way is gRPC client
#    debug logging in the gateway; failing that, take a tcpdump on the
#    server pod and decode a few LookupRoute frames.

# 4. Confirm the index has entries for the model and tenant.
#    The snapshot endpoint is gated by a SA bearer with the
#    `inferencecache.io/controller` audience (see pkg/server/auth/audience.go).
#    From a controller pod:
#      TOKEN=$(cat /var/run/secrets/inferencecache.io/controller-token/token)
#    Or generate a one-off via `kubectl create token` against the
#    controller ServiceAccount with `--audience=inferencecache.io/controller`.
curl -s localhost:8081/snapshot -H "Authorization: Bearer $TOKEN" | jq '.tenants[]'
```

Triage queries:

```promql
# Per-model NO_HINT ratio over the last hour
sum by (namespace, model) (rate(inferencecache_lookup_route_calls_total{reason_code="NO_HINT"}[1h]))
/
sum by (namespace, model) (rate(inferencecache_lookup_route_calls_total[1h]))

# Distribution across reason codes per model
sum by (namespace, model, reason_code) (rate(inferencecache_lookup_route_calls_total[1h]))
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
  sum by (namespace, model, le) (rate(inferencecache_lookup_route_latency_seconds_bucket[5m]))
)

# Server pod CPU + memory pressure
sum by (namespace, pod) (rate(process_cpu_seconds_total[1m]))
sum by (namespace, pod) (process_resident_memory_bytes)
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
   as a CLI flag if you need to retune in place.
2. **Accept the reduced hit rate** at the current cap.
3. **Shorten the index TTL** (server's `WithTTL` option, default 30m) so
   old prefixes age out before the cap kicks in.
4. **Tighten per-tenant budgets** via
   [`CacheTenant.spec.quota.maxIndexEntries`](../concepts/cachetenant-identity-and-quota.md)
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
sum by (namespace, algorithm, reason) (rate(inferencecache_index_evictions_total[10m]))

# Current index population, per (namespace, model). Sum across models in a
# namespace gives the total against the cap.
sum by (namespace, model) (inferencecache_index_entries)
sum by (namespace) (inferencecache_index_entries)

# Per-tenant eviction pressure (CacheTenant quota — distinct from the
# global cap above)
sum by (namespace, tenant_id) (rate(inferencecache_tenant_evictions_total[10m]))
```

---

### `ServerProbeFail`

- **Severity**: `critical`
- **For**: 5 minutes
- **Source metric**: `inferencecache_backend_probe_result_total{backend, stage, result}` — **emitted by the controller binary, not the server.** Requires the controller-side `PodMonitor` shipped in this same observability overlay; without it the alert loads but never has a series to evaluate.

The CacheBackend controller drives a synthetic round-trip against each
managed backend on a 30-second cadence — an `ingest → routing → tier-2`
self-test — and records the per-stage outcome (`result="ok"`, `"failed"`,
`"skipped"`) in this counter. A sustained `result="failed"` rate means the
cache-plane internal pipeline is broken in a way the basic Service-endpoint
probe and Ready gate cannot catch:

| `stage` label | What `failed` means |
|---|---|
| `ingest` | The probe published a synthetic prefix entry but the index did not record it. The KV-event subscriber → server → index path is silently dropping state — exactly the class of regression that motivated this probe. |
| `routing` | The probe published, the index recorded, but `LookupRoute` returned `NO_HINT` for a hash that should match. Likely an index-key-scheme mismatch (e.g. `hash_scheme` empty / wrong / dropped on ingest) or a lookup-filter regression. |
| `tier-2` | (When a T2Prober is wired into the server.) The tier-2 fetch path is failing — engine `external_prefix_cache_*` is reporting hits below threshold. Not wired into the server today, so this stage reports `skipped` on every clean install; an alert here only fires once a follow-up plumbs a real T2Prober. |

The alert uses `increase(...{result="failed"}[5m]) >= 2 for: 5m` — a
single transient flake (one failed probe that recovers on the next 30s
tick) does **not** page. The alert requires at least two failed
increments within a 5-minute window, sustained for another 5 minutes
before firing. This is calibrated to the controller's 30s probe cadence:
~10 probes per 5m window, so the `>= 2` threshold is a real signal
(≥20% failure rate), not a baseline.

#### Likely causes

By `stage` label:

- `ingest` — the `kvevent-subscriber` sidecar is not delivering events
  to the server. Check the subscriber pod's logs for ZMQ connection
  errors. Verify the server is up (`inferencecache_server_up == 1`),
  the controller `/policy` push is reaching it, and no NetworkPolicy
  regression is blocking the subscriber → server gRPC path on `:9090`.
- `routing` — the index is recording entries but lookup can't find
  them. Check that the probe's `hash_scheme` (currently
  `"functional-self-test"`) is not being silently dropped (an empty
  scheme fails open in the index). Check the lookup-filter logs on the
  server side for `reason_code=NO_HINT` on calls that should match.
- `tier-2` — not applicable today; no T2Prober is wired into the
  server. If you're seeing `t2=failed` on a live install, a follow-up
  has shipped a T2Prober and its connection to vLLM
  `external_prefix_cache_*` is broken (engine pod selector, port,
  scrape labels).

#### First-response runbook

1. Identify which backend(s) and which stage are failing:

   ```promql
   sum by (backend, stage) (
     increase(inferencecache_backend_probe_result_total{result="failed"}[5m])
   )
   ```

2. Compare to the success rate for the same backend — if `ok` is
   non-zero, the probe is at least *running*, so the issue is
   stage-specific, not "controller can't reach server at all":

   ```promql
   sum by (backend, stage, result) (
     increase(inferencecache_backend_probe_result_total[5m])
   )
   ```

3. Inspect the `CacheBackend.status.conditions[?type=="FunctionalProbeOK"]`
   on the affected backend — the `reason` field
   (`ProbeIngestFailed` / `ProbeRoutingFailed` / `ProbeT2Failed`) and
   the `message` payload mirror what the probe handler reported. The
   condition is the operator-visible signal; the metric is the alerting
   signal.

4. If the controller is also reporting `Ready=False` on the backend
   with the same reason, the gate is doing its job — the backend's
   `Ready=True` posture has been downgraded for as long as the probe
   keeps failing. Routing-aware clients see a degraded backend; the
   alert is the operator's signal to investigate.

5. To temporarily suppress the alert during a known-bad investigation
   without modifying the rule, annotate the affected backend(s) with
   `inferencecache.io/skip-functional-probe: "true"` — the probe is
   bypassed (`reason=ProbeBypassed`, `result="skipped"`) and the
   `result="failed"` rate drains within the 5-minute window. **Remove
   the annotation when you're done**; a bypassed backend with a real
   regression still ships broken cache state.

Triage queries:

```promql
# Per-backend, per-stage failure rate over the alert window
sum by (backend, stage) (
  increase(inferencecache_backend_probe_result_total{result="failed"}[5m])
)

# Same backend, all results — sanity-check that the probe is firing at all
sum by (backend, stage, result) (
  increase(inferencecache_backend_probe_result_total[5m])
)
```

---

## Deferred alerts

Two more alerts are scoped to ship as part of the same observability
bundle, but they depend on metrics not yet exposed on `/metrics`. The
placeholder rules sit in [`alerting-rules.yaml`](../../config/observability/alerting-rules.yaml)
and the [`PrometheusRule` CR](../../config/observability/prometheus-rules.yaml)
as comments; uncomment them in the same change that ships the corresponding
metric.

| Alert | Blocked on | What it would catch |
|---|---|---|
| `VersionSkewDetected` | `inferencecache_backend_version_skew` gauge — exposed by a follow-up that detects engine-vs-cache-server version skew | The `LMCacheT2NoHits` failure class, but BEFORE it manifests as zero hits — caught proactively by the operator detecting the skew at admit time. |
| `KvEventsStaleness` | `inferencecache_replica_last_event_at` gauge — exposed by the Ready-on-first-event follow-up | A replica that *was* emitting KV events stopped (engine crash, OOMKill, NetworkPolicy regression, subscriber dead). Distinct from `IndexEmpty` (replica never published) — this catches *post-warmup* silence. |

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
| Server overload | `LookupRouteHighTimeout` (warning) + maybe `LookupRouteDegenerate` | `inferencecache_lookup_route_latency_seconds` p99, `process_resident_memory_bytes`, server's global `MaxEntries` |
| Working set outgrew config | `IndexEvictionsSpike` (info) | server's global `MaxEntries` vs. observed working-set size; `CacheTenant.spec.quota.maxIndexEntries` per tenant |

If two alerts fire together, work the more-severe one first; the lower
one usually clears once the root cause does.

---

## Related references

- [Prometheus metrics inventory](../reference/metrics.md) — the
  `inferencecache_*` surface (what THIS operator emits). The bundle
  also reads vLLM-emitted `vllm:external_prefix_cache_*` for the
  `LMCacheT2NoHits` alert; those metrics are documented at
  [`docs.vllm.ai/.../usage/metrics/`](https://docs.vllm.ai/en/latest/usage/metrics/).
- [Reason-code vocabulary](../reference/reason-codes.md) — meaning of
  each `reason_code` label on `inferencecache_lookup_route_calls_total`
- [Operator install](../../README.md) — where the alert bundle is
  surfaced in the install README

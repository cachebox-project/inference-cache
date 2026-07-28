---
title: "Observability & Alerts"
linkTitle: "Observability & Alerts"
weight: 1
description: >
  The opt-in alert bundle, what each alert means, and the scraping you must configure.
---

## The bundle

A Prometheus alert bundle for the operational silent-failure patterns this system has hit in
production ships under `config/observability/`. It is **not** part of `config/default` — the
alerts are opt-in so that installs without prometheus-operator CRDs are not affected by an
unknown `apiVersion`.

```bash
kubectl apply -k config/observability
```

This ships three resources in the `inference-cache-system` namespace:

- a **`ServiceMonitor`** — scrapes `inference-cache-server:8080/metrics`;
- a **`PodMonitor`** — scrapes the controller pod's `:8080/metrics` (required for the
  controller-side alerts to have a series to evaluate — the controller has no Service in
  front of it);
- a **`PrometheusRule`** — the alerts.

`make verify-prometheus` lints and unit-tests the rules.

{{% alert title="Selector mismatch fails silently" color="warning" %}}
All three custom resources carry example labels (`prometheus: k8s`) matching the upstream
kube-prometheus stack. The `kube-prometheus-stack` Helm chart uses a *different* convention
(`release: <name>`, no `prometheus:` label). If your `Prometheus` custom resource's
`ruleSelector` / `serviceMonitorSelector` / `podMonitorSelector` uses a different label set,
`kubectl apply -k` succeeds but **Prometheus silently ignores the resources.** Check your
selectors with `kubectl get prometheus -A -o yaml` and relabel the CRs to match.
{{% /alert %}}

For vanilla Prometheus (ConfigMap mounts, `prometheus.serverFiles`), use the flat
`config/observability/alerting-rules.yaml` and configure scraping yourself — **for both the
server and the controller pod**. Scope per install with `kubernetes_sd_configs` + a
`relabel_configs` that copies `__meta_kubernetes_namespace` to `namespace` (the alerts scope
per install by that label).

## The alerts

| Alert | Severity | For | Fires when |
|---|---|---|---|
| **`IndexEmpty`** | critical | 2m | The server is up but the index is empty **while** lookups are flowing — ingestion is broken. |
| **`LookupRouteDegenerate`** | warning | 5m | More than 90% of lookups return `NO_HINT` over 10m for a model — no reuse, or a client sending wrong contract keys. |
| **`LookupRouteHighTimeout`** | warning | 5m | More than 5% of lookups return `TIMEOUT` over 10m for a model. |
| **`IndexEvictionsSpike`** | info | 10m | More than 10 cap-evictions/sec (`reason="cap"`; TTL evictions excluded) — the index is over budget. |
| **`ServerProbeFail`** | critical | 5m | Two or more functional-probe failures in 5m (controller-emitted — needs the PodMonitor). |
| **`LMCacheT2NoHits`** | warning | 5m | More than 1000 tier-2 query tokens/sec but zero hit tokens — a silently-degraded offload tier. |

Four of these (`IndexEmpty`, `LookupRouteDegenerate`, `LookupRouteHighTimeout`,
`IndexEvictionsSpike`) become active as soon as the operator is installed and the selectors
match; they stay quiet on a healthy or idle install (each is gated by traffic/rate/eviction
thresholds).

{{% alert title="LMCacheT2NoHits needs an extra scrape" color="warning" %}}
`LMCacheT2NoHits` reads `vllm:external_prefix_cache_*` from the **engine pods directly**, not
from inference-cache. The shipped `ServiceMonitor` covers only `inference-cache-server`. To
make this alert effective, add a separate `PodMonitor` for your engine Deployment (scoped to
the pods your `CacheBackend.spec.engineSelector` matches) that preserves both `namespace` and
`pod` labels.
{{% /alert %}}

## Related pages

- [Metrics reference](/docs/reference/metrics/) — the underlying series.
- [Index sizing](/docs/administration/index-sizing/) — the eviction/memory relationship
  behind `IndexEvictionsSpike`.
- [Monitor the cache plane](/docs/tasks/monitor-the-cache-plane/) — the day-to-day view.

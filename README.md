# inference-cache

A Kubernetes-native cache plane for LLM inference.

## Repository layout

One operator, split across two control-plane binaries (controller + server) plus
the operator CLI and the CRDs.

**CRDs — the API**
- `api/v1alpha1/` — Go types (`CacheBackend`; `CachePolicy`, `CacheTenant`, `PromptTemplate`, `PDTopology`, `CacheIndex` as they land) + generated deepcopy
- `config/` — generated CRD, RBAC, and sample manifests

**`inferencecache-controller`** (`cmd/controller`) — watches CRDs and provisions cache backends
- `cmd/controller/` — controller-runtime manager entrypoint
- `internal/controller/` — reconcilers
- `pkg/adapters/runtime/` — `KVCacheRuntimeAdapter`s render the cache-server pod/service and inject engine/router pod config; the reconciler drives them through the in-package `Registry`

**`inferencecache-server`** (`cmd/server`) — gRPC policy server + cache-state index + metrics
- `cmd/server/` — gRPC + HTTP server entrypoint
- `pkg/server/` — gRPC service (`LookupRoute`, `RenderTemplate`, …), health, metrics
- `proto/` (+ generated stubs) — the gRPC contract
- `pkg/index/` — cache-state aggregator (`CacheIndex`)
- `pkg/render/` — mutable-slot prompt rendering engine (the wedge); importable library
- `pkg/adapters/engine/` — engine KV-event hook (feeds the index)

**`inferencecache`** (`cmd/inferencecache`) — operator CLI; `doctor` runs a read-only pre-flight diagnostic
- `cmd/inferencecache/` — cobra entrypoint
- `pkg/cli/doctor/` — diagnostic checks + output formatters (see `docs/cli/doctor.md`)

**Shared** — `pkg/version/`, `hack/`, `dockerfiles/`, `.githooks/`

## Quick Start

```bash
make proto-gen
make build
make test
```

Run the server locally:

```bash
bin/server \
  --grpc-bind-address=:9090 \
  --http-bind-address=:8080 \
  --snapshot-bind-address=:8081 \
  --insecure-disable-auth   # local-dev only; production passes --allowed-controller-sa instead
curl -i http://localhost:8080/healthz   # liveness
curl -i http://localhost:8080/readyz    # readiness
curl -s http://localhost:8080/metrics   # Prometheus metrics (inferencecache_*)
curl -s http://localhost:8081/snapshot  # internal aggregate (auth-gated in production)
curl -i -XPOST http://localhost:8081/policy -d '{"version":3,"policies":[]}'  # controller push (auth-gated in production)
curl -i -XPOST http://localhost:8081/probe \
  -H 'Content-Type: application/json' \
  -d '{"backend":"local/dev","model":"dev-model","hashScheme":"vllm"}'        # functional self-test (auth-gated in production)
```

The server fails closed by default: omitting both `--allowed-controller-sa` and
`--insecure-disable-auth` causes it to exit 2 with a stderr message. That keeps
an operator who forgets the flag from accidentally shipping unauthenticated
`/snapshot`, `/policy`, and `/probe` endpoints on a real cluster.

## Cluster Prerequisites

The controller serves admission webhooks — defaulting + validation for
`CacheBackend`, plus a mutating Pod webhook that auto-injects the LMCache
engine configuration into pods labeled to match a `CacheBackend`'s
`spec.engineSelector` — over TLS, so deploying it requires [cert-manager][cm]
v1.0+ in the target cluster. The default install (`config/default`)
provisions a self-signed `Issuer` plus a `Certificate` for the webhook
serving cert, and relies on cert-manager's `cert-manager.io/inject-ca-from`
annotation to inject the CA bundle into the generated
`MutatingWebhookConfiguration` and `ValidatingWebhookConfiguration`.

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
```

[cm]: https://cert-manager.io/

## Install

The default Kustomize overlay brings up both control-plane components:

- `inference-cache-controller-manager` — the reconciler + admission webhooks.
- `inference-cache-server` — the gRPC policy server (`InferenceCache`) and the
  HTTP `/healthz`, `/readyz`, `/metrics` probe surface, plus a dedicated
  controller-facing listener carrying `/snapshot` (controller read), `/policy`
  (controller write), and `/probe` (functional self-test driven by the
  CacheBackend reconciler), all gated by ServiceAccount bearer auth + a
  `NetworkPolicy`. Fronted by a `ClusterIP` Service `inference-cache-server`
  in the `inference-cache-system` namespace with named ports `grpc:9090`
  (gRPC API), `http:8080` (probes / metrics), and `snapshot:8081`
  (controller-only `/snapshot` + `/policy` + `/probe`). The controller's
  CacheIndex poller scrapes `http://inference-cache-server:8081/snapshot`,
  the CachePolicy reconciler POSTs to
  `http://inference-cache-server:8081/policy`, and the CacheBackend
  reconciler POSTs to `http://inference-cache-server:8081/probe` (default
  for the `--server-probe-url` flag — set empty to disable the gate);
  `/snapshot` + `/probe` send the controller-audience projected
  ServiceAccount token, while `/policy` sends the write-side policy-audience
  token. Once both pods are Ready
  `kubectl get cacheindex` reports live cluster-wide cache state.
  `FunctionalProbeOK` appears on each managed `CacheBackend` only after it
  clears the upstream KV-event readiness gate (i.e. real engine pods have
  published at least one KV event for it) — see
  [docs/design/cachebackend-api.md#functional-probe-gate](docs/design/cachebackend-api.md#functional-probe-gate);
  it is intentionally not visible on a default install with no engine workload.

```bash
kubectl apply -k config/default
kubectl -n inference-cache-system wait --for=condition=Available deployment --all --timeout=180s
kubectl get cacheindex cluster-default -o yaml
```

## Monitoring

Both binaries expose Prometheus metrics on their pod's `:8080/metrics`
(prefixed `inferencecache_*`) — the server binary's series cover the
in-memory index and gRPC handlers; the controller binary's series cover
the reconcilers (e.g. `inferencecache_backend_probe_result_total`,
`inferencecache_backend_server_restart_cascades_total`). A default
alert bundle for the operational silent-failure patterns this code has
hit in production ships under
[`config/observability/`](config/observability/) and is **not** included
in `config/default` — the alerts are opt-in so that installs without
prometheus-operator CRDs are not affected by an unknown `apiVersion`.

For prometheus-operator / kube-prometheus installs:

```bash
kubectl apply -k config/observability
```

This ships THREE resources: a `ServiceMonitor` (so Prometheus scrapes
`inference-cache-server:8080/metrics`), a `PodMonitor` (so Prometheus
scrapes the controller pod's `:8080/metrics` — required for the
controller-side alerts like `ServerProbeFail` to have a series to
evaluate), and the `PrometheusRule` carrying the alerts.

> **Caveat — Prometheus Operator selectors.** All three CRs carry
> example labels (`prometheus: k8s`, plus `role: alert-rules` on the
> PrometheusRule) that match the upstream kube-prometheus stack
> (default `Prometheus` named `k8s`). The `kube-prometheus-stack`
> Helm chart uses a different convention (`release: <release-name>`,
> no `prometheus:` label) — its rule / serviceMonitor / podMonitor
> selectors do not match what's shipped here. If your `Prometheus`
> CR's `ruleSelector` / `serviceMonitorSelector` / `podMonitorSelector`
> uses a different label set (`release: my-prom`, etc.), `kubectl
> apply -k` succeeds but Prometheus silently ignores the resources.
> The YAML comments next to each label spell out the introspection
> command (`kubectl get prometheus -A -o jsonpath=...`); see
> [`docs/observability/alerts.md`](docs/observability/alerts.md) for
> the full discussion.

Four of the five Stage 1 alerts (`IndexEmpty`, `LookupRouteDegenerate`,
`LookupRouteHighTimeout`, `IndexEvictionsSpike`) become active as soon
as the operator is installed AND the selectors match — they remain
quiet on a healthy or idle install (each rule is gated by traffic/
rate/eviction thresholds; see `for:` + the rate floors in the alert
expressions) and only fire when the conditions are met.

> **The fifth alert needs a vLLM scrape this bundle does NOT ship.**
> [`LMCacheT2NoHits`](docs/observability/alerts.md#lmcachet2nohits) reads
> `vllm:external_prefix_cache_*` from vLLM engine pods directly. The
> shipped `ServiceMonitor` covers only `inference-cache-server`. To make
> that alert effective, add a separate `PodMonitor` for your vLLM
> Deployment (or `kubernetes_sd_configs: pod` for vanilla Prometheus)
> so engine `/metrics` is scraped with both `namespace` and `pod` labels
> attached. See alerts.md "How to enable" for the requirement.

For vanilla Prometheus, ConfigMap mounts, or Helm `prometheus.serverFiles`,
use the flat [`alerting-rules.yaml`](config/observability/alerting-rules.yaml).
**You must also configure scraping yourself, for BOTH the server AND
the controller pod.** The server's `:8080` exposes the index, lookup,
and auth series; the controller pod's `:8080` exposes the per-stage
probe-result counter (`inferencecache_backend_probe_result_total`)
and the cache-server restart-cascade counter — the controller-side
alerts (`ServerProbeFail` today) load against the controller's
series, so a server-only scrape leaves them inert.

For multi-install or per-install isolation, use Kubernetes service
discovery (`kubernetes_sd_configs: pod` or `endpoints`) with
`relabel_configs:` that copies `__meta_kubernetes_namespace` to
`namespace` — the alerts scope per install by that label. A pair of
static DNS scrapes (e.g.
`inference-cache-server.inference-cache-system.svc.cluster.local:8080`
PLUS pod-IP discovery for the controller manager pods, which have no
Service in front of them) is acceptable for a single install but
loses per-install isolation; do NOT use it if you scrape multiple
inference-cache installs into one Prometheus. Without a working
scrape on both endpoints, the rules load but fire on nothing (server
alerts) or load but never have a series to evaluate (controller
alerts like `ServerProbeFail`).

Per-alert runbooks (causes, triage steps, example PromQL): see
[`docs/observability/alerts.md`](docs/observability/alerts.md). For the
underlying metric surface, see
[`docs/reference/metrics.md`](docs/reference/metrics.md).

## Local Development Cluster

Create a kind cluster for controller development:

```bash
make dev-cluster
```

By default this creates or reuses a cluster named `inference-cache`. You can override the name and node image:

```bash
make dev-cluster KIND_CLUSTER=cache-dev KIND_NODE_IMAGE=kindest/node:v1.31.0
```

## Common Targets

- `make build`: build the controller, server, kvevent-subscriber, and inferencecache binaries
- `make test`: run unit tests
- `make lint`: run gofmt and go vet
- `make ci-lint`: run golangci-lint
- `make verify-prometheus`: lint + unit-test the Prometheus alerting rules under `config/observability/`
- `make proto-gen`: regenerate protobuf Go code
- `make generate`: regenerate Kubernetes deepcopy code
- `make manifests`: regenerate CRD, RBAC, and webhook manifests
- `make image-build`: build controller, server, and kvevent-subscriber images

## Documentation

Design docs live under [`docs/`](docs/):

- [`docs/design/cachebackend-api.md`](docs/design/cachebackend-api.md) — the `CacheBackend` CRD contract
- [`docs/design/grpc-contract.md`](docs/design/grpc-contract.md) — the `InferenceCache` gRPC service contract (B4)
- [`docs/design/policy-crds.md`](docs/design/policy-crds.md) — policy CRDs (`CachePolicy`, `CacheTenant`, `PromptTemplate`, `PDTopology`, `CacheIndex`)

Contributor guide: [`CONTRIBUTING.md`](CONTRIBUTING.md) (layout, naming rule, push/PR gates).

## License

Apache-2.0

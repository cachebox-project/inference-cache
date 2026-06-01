# inference-cache

A Kubernetes-native cache plane for LLM inference.

## Repository layout

One operator, split across two binaries plus the CRDs.

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
curl -i -XPOST http://localhost:8081/policy -d '{"version":1,"policies":[]}'  # controller push (auth-gated in production)
```

The server fails closed by default: omitting both `--allowed-controller-sa` and
`--insecure-disable-auth` causes it to exit 2 with a stderr message. That keeps
an operator who forgets the flag from accidentally shipping unauthenticated
`/snapshot` + `/policy` endpoints on a real cluster.

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
  controller-facing listener carrying `/snapshot` (controller read) and
  `/policy` (controller write), both gated by ServiceAccount bearer auth +
  a `NetworkPolicy`. Fronted by a `ClusterIP` Service `inference-cache-server`
  in the `inference-cache-system` namespace with named ports `grpc:9090`
  (gRPC API), `http:8080` (probes / metrics), and `snapshot:8081`
  (controller-only `/snapshot` + `/policy`). The controller's CacheIndex
  poller scrapes `http://inference-cache-server:8081/snapshot` and the
  CachePolicy reconciler POSTs to `http://inference-cache-server:8081/policy`
  by default; both send the projected ServiceAccount token. Once both pods
  are Ready `kubectl get cacheindex` reports live cluster-wide cache state.

```bash
kubectl apply -k config/default
kubectl -n inference-cache-system wait --for=condition=Available deployment --all --timeout=180s
kubectl get cacheindex cluster-default -o yaml
```

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

- `make build`: build controller and server binaries
- `make test`: run unit tests
- `make lint`: run gofmt and go vet
- `make ci-lint`: run golangci-lint
- `make proto-gen`: regenerate protobuf Go code
- `make generate`: regenerate Kubernetes deepcopy code
- `make manifests`: regenerate CRD, RBAC, and webhook manifests
- `make image-build`: build controller and server images

## Documentation

Design docs live under [`docs/`](docs/):

- [`docs/design/cachebackend-api.md`](docs/design/cachebackend-api.md) — the `CacheBackend` CRD contract
- [`docs/design/grpc-contract.md`](docs/design/grpc-contract.md) — the `InferenceCache` gRPC service contract (B4)
- [`docs/design/policy-crds.md`](docs/design/policy-crds.md) — policy CRDs (`CachePolicy`, `CacheTenant`, `PromptTemplate`, `PDTopology`, `CacheIndex`)

Contributor guide: [`CONTRIBUTING.md`](CONTRIBUTING.md) (layout, naming rule, push/PR gates).

## License

Apache-2.0

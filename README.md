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
bin/server --grpc-bind-address=:9090 --http-bind-address=:8080
curl -i http://localhost:8080/healthz   # liveness
curl -i http://localhost:8080/readyz    # readiness
curl -s http://localhost:8080/metrics   # Prometheus metrics (inferencecache_*)
```

## Cluster Prerequisites

The controller serves admission webhooks (defaulting + validation for
`CacheBackend`) over TLS, so deploying it requires [cert-manager][cm]
v1.0+ in the target cluster. The default install (`config/default`)
provisions a self-signed `Issuer` plus a `Certificate` for the webhook
serving cert, and relies on cert-manager's `cert-manager.io/inject-ca-from`
annotation to inject the CA bundle into the generated
`MutatingWebhookConfiguration` and `ValidatingWebhookConfiguration`.

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
```

[cm]: https://cert-manager.io/

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

- [`docs/design/grpc-contract.md`](docs/design/grpc-contract.md) — the `InferenceCache` gRPC service contract (B4)

Contributor guide: [`CONTRIBUTING.md`](CONTRIBUTING.md) (layout, naming rule, push/PR gates).

## License

Apache-2.0

# inference-cache

A Kubernetes-native cache plane for LLM inference.

This repo is standalone and OSS-ready, while mirroring OME's kubebuilder conventions so the team can reuse familiar controller workflows.

## What Is Here

- `api/v1alpha1`: Kubernetes API types for `CacheBackend`
- `cmd/controller`: controller-runtime manager with a no-op `CacheBackend` reconciler
- `cmd/server`: empty gRPC server with HTTP `/healthz`
- `proto`: protobuf definitions for cache service APIs
- `pkg/adapters`: integration seams for engines, runtimes, and backends
- `pkg/render`: Kubernetes object rendering helpers
- `pkg/index`: cache index abstractions
- `config`: CRD, RBAC, manager, and sample manifests

## Quick Start

```bash
make proto-gen
make build
make test
```

Run the server locally:

```bash
bin/server --grpc-bind-address=:9090 --http-bind-address=:8080
curl -i http://localhost:8080/healthz
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
- `make manifests`: regenerate CRD and RBAC manifests
- `make image-build`: build controller and server images

## License

Apache-2.0

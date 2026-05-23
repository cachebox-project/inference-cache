# Contributing

Thanks for helping build inference-cache. This repository follows kubebuilder-style controller conventions and keeps generated code checked in.

## Local Setup

Requirements:

- Go 1.23 or newer
- Make
- Docker
- kind
- protoc

Run the baseline checks before sending a PR:

```bash
make proto-gen
make lint
make test
make build
```

`make ci-lint` runs the golangci-lint configuration used by CI.

## Development Cluster

Create a local kind cluster:

```bash
make dev-cluster
```

The default cluster name is `inference-cache`. Override it with `KIND_CLUSTER=<name>`.

## Generated Code

After changing API types or controller RBAC markers, run:

```bash
make generate
make manifests
```

After changing protobuf files, run:

```bash
make proto-gen
```

Commit generated artifacts with the source changes.

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

## Vendor-neutral naming (required)

This is a vendor-neutral open-source project. **No cloud-vendor-specific domain or namespace may appear in any public or core identifier** — applies to both writing code and reviewing PRs.

Banned in core/public identity: `oci` / `oracle` tokens, and `*.oci.com` / `oraclecloud.com` domains, used as an API group, CRD group, kubebuilder domain, proto package, gRPC service/package, Go module path segment, default Kubernetes namespace, Helm chart name, or container image registry.

Canonical identity (use these everywhere):

| Identifier | Value |
|---|---|
| API group / CRD group / domain | `inferencecache.io` |
| proto package | `inferencecache.v1alpha1` |
| gRPC service | `InferenceCache` |
| Go module | `github.com/cachebox-project/inference-cache` |

Cloud-specific integration (including OCI) **is** allowed, but only as an isolated, optional adapter under `pkg/adapters/.../` — never in core API / CRD / proto / controller identity or default config. The rule bans vendors from the project's *identity and defaults*, not from *integration capability*.

### Enforcement

```bash
make install-hooks    # one-time per clone: installs the pre-commit naming guard (core.hooksPath)
make verify-naming    # run the same check on demand (also wire into CI)
```

The pre-commit hook (`.githooks/pre-commit`) blocks any commit that introduces a banned token into a core-identity path. Run `make install-hooks` after cloning regardless of which editor or AI assistant you use.

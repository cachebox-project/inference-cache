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
make proto-lint
make lint
make test-race
make build
make vulncheck   # advisory: vulnerability scan (needs network); currently
                 # reports known CVEs pending a Go/grpc upgrade, so CI does not
                 # fail on it yet
```

`make test-race` runs the unit tests under the race detector — it's what the
pre-push gate and CI use; `make test` is the faster, non-race variant for quick
local iteration. `make ci-lint` runs the golangci-lint configuration used by CI.
`make proto-lint` lints the gRPC contract with [buf](https://buf.build) (configured in `buf.yaml`);
buf is used for linting only — code generation stays on `protoc` (`make proto-gen`).

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
make proto-lint
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

## Before pushing / opening a PR

Run `make install-hooks` once per clone. Thereafter:

- **On every push**, the `pre-push` hook runs `make ci` (naming + format + vet + race tests + build) and blocks the push if anything fails. Reproduce it anytime with `make ci`.
- **Before opening a PR**, run `make pre-pr` — it runs `make ci`, then a generated-code drift check, then prints the review checklist. Review the diff against the tech spec before submitting.

Emergency override for the push gate: `git push --no-verify` (discouraged). CI runs the full `make ci-lint` (golangci-lint) in addition to the above.

## Repository layout — where new code goes

See the README's "Repository layout" for the full map. In short:

| You're adding… | Put it in |
|---|---|
| A CRD field / new API type | `api/v1alpha1/` → then `make manifests generate` |
| Controller / reconciler logic | `internal/controller/` |
| gRPC handlers, server wiring | `pkg/server/` |
| Cache-state index logic | `pkg/index/` |
| Mutable-slot rendering (the wedge) | `pkg/render/` |
| Engine / runtime / backend adapters | `pkg/adapters/{engine,runtime,backend}/` |
| The gRPC contract | `proto/` → then `make proto-gen` |

Each package's `doc.go` states which binary (`inferencecache-controller` or `inferencecache-server`) it belongs to.

**Generated code** — `config/crd/`, `config/rbac/role.yaml`, `api/**/zz_generated*.go`, `pkg/server/proto/` — is committed but never hand-edited. Regenerate and commit it with the source change (`make pre-pr` verifies there's no drift).

**gRPC contract:** when you change `proto/`, update [`docs/design/grpc-contract.md`](docs/design/grpc-contract.md) in the same commit so the design doc stays accurate. The pre-commit hook blocks a commit that touches a `.proto` without touching that doc (override with `--no-verify` only if the change truly doesn't affect the contract).

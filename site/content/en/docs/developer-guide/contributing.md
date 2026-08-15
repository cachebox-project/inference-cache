---
title: "Contributing"
linkTitle: "Contributing"
weight: 1
description: >
  Local setup, the required checks, and the project rules the tooling enforces.
---

This page summarizes the contributor workflow. The authoritative version is
[`CONTRIBUTING.md`](https://github.com/cachebox-project/inference-cache/blob/main/CONTRIBUTING.md)
in the repository.

## Local setup

Requirements: **Go 1.26.6+**, Make, Docker, kind, and protoc.

Install the git hooks once per clone — they enforce the project rules below:

```bash
make install-hooks
```

Run the baseline checks before sending a PR:

```bash
make proto-gen
make proto-lint
make lint
make test-race       # unit tests under the race detector (what CI + the pre-push gate use)
make build
make cover-check     # fails if logic-package coverage drops below COVER_MIN (90%)
make vulncheck       # vulnerability scan (needs network); blocking in CI
```

`make test` is the faster non-race variant for quick iteration. `make cover-check` enforces a
coverage floor over the hand-written logic packages (generated code, `cmd/`, `hack/`, and
test helpers are excluded); the floor is a ratchet — raise it as coverage improves.

## Generated code

After changing API types or RBAC markers:

```bash
make generate
make manifests
```

After changing protobuf files:

```bash
make proto-gen
make proto-lint
```

Commit the generated artifacts with the source change. When you change `proto/`, update
`docs/design/grpc-contract.md` in the same commit — the pre-commit hook enforces it.

## The two project rules

Both are enforced by the pre-commit hook, `make` targets, and CI — not just documented.

### Vendor-neutral naming

This is a vendor-neutral open-source project. **No cloud-vendor-specific domain or namespace
may appear in any public or core identifier.** Cloud-specific integration is allowed, but only
as an isolated, optional adapter under `pkg/adapters/.../` — never in core API / CRD / proto /
controller identity or default config.

Canonical identity:

| Identifier | Value |
|---|---|
| API group / CRD group / domain | `inferencecache.io` |
| proto package | `inferencecache.v1alpha1` |
| gRPC service | `InferenceCache` |
| Go module | `github.com/cachebox-project/inference-cache` |

```bash
make verify-naming
```

### No internal issue-tracker references

This is a public repository. **Tracked files must not reference an internal issue tracker** —
neither ticket IDs nor tracker URLs. Link work from a PR using GitHub issues (e.g.
`Closes #123`).

```bash
make verify-no-internal-refs
```

## Before pushing / opening a PR

- **On every push**, the `pre-push` hook runs `make ci` (naming + internal-refs + format +
  vet + golangci-lint + Prometheus rules + golden vectors + race tests + build).
- **Before opening a PR**, run `make pre-pr` — `make ci`, then a generated-code drift check,
  then `make verify-samples` (server-side dry-run of every sample against an envtest apiserver
  + the admission webhook), then the review checklist.

### Operator-facing changes must extend the install-smoke gate

When a change alters an operator-facing surface — CRD fields or printer columns, `.status`
surfaces, CLI output, gRPC/HTTP behavior, the default-install bundle, or sample manifests —
extend the per-PR install-smoke script with an assertion that drives that surface end-to-end
in a real kind cluster. The smoke runs with no engine traffic, so assert the observed-zero
steady state (for example, the cluster `CacheIndex` reports a populated
`.status.observedServer`).

## Optional agent tooling

The repository documents optional, personal semantic-navigation and workflow tooling for
coding agents. None of it is required to build or contribute, and no agent-specific
configuration is committed — keep any such config local and git-ignored.

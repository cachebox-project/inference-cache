---
title: "Developer Guide"
linkTitle: "Developer Guide"
weight: 8
description: >
  Resources for developers contributing to and extending inference-cache.
no_list: true
---

inference-cache is developed in the open under the
[`cachebox-project`](https://github.com/cachebox-project) organization, following
kubebuilder-style controller conventions with generated code checked in.

## Getting started

### [Contributing]({{< relref "/docs/developer-guide/contributing/" >}})

Local setup, the build/test/verify targets, the required checks before a PR, and the two
project rules the tooling enforces (vendor-neutral naming and no internal issue-tracker
references).

## Where code lives

The repository is one operator split across two binaries plus the CRDs. In short:

| You're adding… | Put it in |
|---|---|
| A CRD field / new API type | `api/v1alpha1/` → `make manifests generate` |
| Controller / reconciler logic | `internal/controller/` |
| gRPC handlers, server wiring | `pkg/server/` |
| Cache-state index logic | `pkg/index/` |
| Mutable-slot rendering | `pkg/render/` |
| Built-in runtime / storage adapters | `internal/adapters/builtin/{runtime,storage}/` |
| Public adapter extension contracts | `pkg/adapters/{runtime,backend}/` |
| The gRPC contract | `proto/` → `make proto-gen` |

Generated code (`config/crd/`, `config/rbac/role.yaml`, `zz_generated*.go`,
`pkg/server/proto/`) is committed but never hand-edited — regenerate and commit it with the
source change. Each package's `doc.go` states which binary it belongs to.

## Design docs

The in-repo `docs/design/` directory holds the authoritative contracts — the gRPC contract,
the CRD contract, policy propagation, the ranking algorithm, TLS posture, and more. Much of
this documentation site draws directly from those docs; when a contract detail matters, they
are the source of truth alongside the generated code and the `.proto`.

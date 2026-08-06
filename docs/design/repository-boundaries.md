# Repository boundaries

Status: staged migration in progress.

This document defines package ownership and dependency direction. Repository
moves should be behavior-preserving and small enough to review independently;
the target layout is a sequence of migrations, not a flag day.

## Dependency rules

1. `api/v1alpha1` owns the Kubernetes API. The CRDs remain one Go package.
2. `internal/controlplaneapi` owns private HTTP DTOs shared by the controller
   and server. Neither binary imports the other's implementation for wire
   types.
3. `internal/enginebinding` owns pod annotations and other metadata shared by
   admission and controllers. Controllers do not import webhook packages.
4. `pkg/adapters` contains extension contracts that out-of-tree adapters may
   implement. Shipping implementations and registration belong under
   `internal/adapters`.
5. `internal/adapters/builtin` is the sole composition root for adapters
   shipped by repository binaries. Production and nil fallbacks use the same
   complete registries.
6. Implementation packages may depend on extension contracts. Extension
   contracts must not depend on controller, webhook, server, or built-in
   implementation packages.

## Current neutral contracts

The JSON contracts for `POST /policy` and `POST /probe` live in
`internal/controlplaneapi`. `pkg/server` exposes temporary type and constant
aliases so existing in-repository tests and callers continue to compile while
imports migrate. The JSON field names, policy version band, and probe result
semantics are unchanged.

The engine-pod binding annotations and skip-value parser live in
`internal/enginebinding`. The pod webhook retains compatibility aliases, but
controllers consume the neutral owner directly.

## Adapter composition

`internal/adapters/builtin.New` constructs both registries used by the
controller binary:

- the CRD-admissible runtime set: vLLM LMCache, SGLang LMCache, and SGLang
  HiCache;
- the complete managed and external remote-storage provider set.

`pkg/adapters/runtime.NewCoreRegistry` intentionally contains only adapters
implemented in that package. Its old `DefaultRegistry` name remains as a
deprecated compatibility wrapper and must not be used as a shipping default.

## `pkg/` classification

The following table is the migration inventory. "Supported" means an
intentional extension or reusable Go API. "Internalize" means the package is
owned by repository binaries and will move under `internal/` in a later,
behavior-preserving change.

| Current package | Classification | Owner / rationale | Planned target |
|---|---|---|---|
| `pkg/adapters/backend` | Supported | Remote-storage provider and binding extension contracts | Keep |
| `internal/adapters/builtin/storage` | Internal | Shipping provider implementations | Keep |
| `pkg/adapters/runtime` | Split | Runtime extension contracts mixed with shipping vLLM implementations | Contracts stay; implementations move under `internal/adapters/builtin/runtime` |
| `pkg/adapters/runtime/sglang` | Internalize | Shipping SGLang implementations | `internal/adapters/builtin/runtime` |
| `pkg/adapters/runtime/internal/enginewire` | Internalize | Shared implementation detail of built-in runtime adapters | `internal/enginebinding` or built-in runtime subtree |
| `pkg/adapters/engine` | Internalize | Subscriber-side ingest and metrics implementation | `internal/subscriber` |
| `pkg/adapters/engineclient` | Supported | Harness-facing engine egress client with no binary owner | Keep |
| `pkg/fingerprint` | Supported | Reusable content-fingerprint contract used across integrations | Keep |
| `pkg/tokenize` | Supported | Optional tokenizer boundary, including the tagged cgo implementation | Keep |
| `pkg/index` | Internalize | Server-owned mutable cache-state implementation | `internal/index` |
| `pkg/server` and `pkg/server/auth` | Internalize | Server binary implementation | `internal/server` |
| `pkg/server/proto/...` | Migrate | Generated public gRPC API under a server-owned path | `gen/inferencecache/v1alpha1` |
| `pkg/cli/doctor/...` | Internalize | `cmd/inferencecache` implementation | `internal/cli/doctor` |
| `pkg/render` | Internalize until an external consumer exists | Server-owned rendering placeholder | `internal/server/render` |
| `pkg/testing` | Internalize | In-repository envtest helpers | `internal/testutil` |
| `pkg/version` | Internalize | Repository binary build metadata | `internal/version` |

Each move must update package documentation so a remaining `pkg/` package
states its owner or its supported external-consumer contract.

## Generated protobuf migration

The current generated import path remains
`github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1`.
Moving generated code is a source-level contract change even when the protobuf
wire is identical, so it is deliberately separate from the HTTP-contract move.

The migration sequence is:

1. Change `go_package` to
   `github.com/cachebox-project/inference-cache/gen/inferencecache/v1alpha1`
   and regenerate into `gen/`.
2. Update all repository binaries, adapters, and tests in the same change.
3. Decide before merge whether the alpha module promises old-import
   compatibility. If it does, retain a documented forwarding package for one
   release; otherwise call out the import move in release notes.
4. Make generated-code drift checks treat `gen/` as the only generated Go
   target, then remove the old generated directory.

The protobuf package and service names remain `inferencecache.v1alpha1` and
`InferenceCache`; only the Go import path changes.

## Remaining stages

1. Move implementation-only packages under `internal/`, starting with server,
   index, subscriber, doctor, and built-in adapters.
2. Split controller files by bounded context while retaining one package until
   dependency edges are clear; split large server, index, webhook, and API files
   along cohesive responsibilities.
3. Separate canonical, legacy, recipe, and invalid samples; make the site
   consume canonical user documentation.
4. Move CI-executed reference-stack assets and fake engines into `test/`, then
   split build logic while preserving public Make targets.

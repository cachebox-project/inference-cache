---
title: "Reference"
linkTitle: "Reference"
weight: 9
description: >
  Field-level and wire-level reference for inference-cache.
no_list: true
---

Precise, lookup-oriented reference material.

- **[CRD API]({{< relref "/docs/reference/crd-api/" >}})** — the six custom resources, their groups, scopes,
  short names, and key fields.
- **[gRPC API]({{< relref "/docs/reference/grpc-api/" >}})** — the `InferenceCache` service, RPC by RPC.
- **[Metrics]({{< relref "/docs/reference/metrics/" >}})** — every `inferencecache_*` series, its labels, and
  which binary emits it.
- **[Reason codes]({{< relref "/docs/reference/reason-codes/" >}})** — the string `reason_code` vocabulary and
  when each is emitted.
- **[CLI: `inferencecache doctor`]({{< relref "/docs/reference/cli-doctor/" >}})** — checks, finding codes,
  flags, and exit codes.

{{% alert title="The generated source of truth" color="info" %}}
For the exhaustive, always-current field reference, use `kubectl explain` against a live
cluster (for example `kubectl explain cachebackend.spec`) and the generated CRD manifests
under `config/crd/`. The gRPC contract's source of truth is
`proto/inferencecache/v1alpha1/inferencecache.proto`.
{{% /alert %}}

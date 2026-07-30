---
title: "gRPC TLS"
linkTitle: "gRPC TLS"
weight: 3
description: >
  The locked TLS design, why gRPC is plaintext by default, and how to enable the opt-in
  overlay.
---

## The decision

The gRPC API on `:9090` uses **one-sided Service TLS via cert-manager, terminated
in-process** — no sidecar, Envoy, or Ingress. Mutual TLS (client-certificate auth) is a
Phase-2 feature flag, not yet implemented.

The server binary supports TLS via `--tls-cert-file` / `--tls-key-file` (both-or-neither —
supplying exactly one fails startup). The certificate is served through a reloading
`GetCertificate` hook, so a cert-manager-rotated Secret is picked up **without a restart**.
The posture is exposed as the gauge `inferencecache_server_grpc_tls_enabled` (0/1).

## Why plaintext is the default

`config/default` ships **plaintext**, because both current `:9090` clients are plaintext
today:

- the in-cluster `kvevent-subscriber` (dials with insecure credentials), and
- the external gateway client (not yet built).

Flipping the default to TLS would break ingestion. TLS is therefore shipped as an **opt-in**
overlay; it will become the default once both clients are TLS-aware (the subscriber needs the
server CA distributed into engine-pod namespaces — a cross-namespace trust-anchor problem).

The dual-input `LookupRoute` path (which can carry `prompt_text`) adds request-content
sensitivity to `:9090`, strengthening the case for enabling TLS in any environment where the
gateway sends raw prompts.

## Enabling TLS

```bash
kubectl apply -k config/overlays/server-tls
```

The overlay adds a dedicated self-signed `Issuer` and a `Certificate` producing the Secret
`inference-cache-server-tls` (`tls.crt` / `tls.key` / `ca.crt`), mounts it read-only into the
server pod, and patches the Deployment with the `--tls-*` args.

{{% alert title="Self-signed caveat" color="warning" %}}
A direct self-signed Issuer's `ca.crt` equals its leaf certificate and rotates with it, so a
statically-copied client trust bundle breaks on renewal. This is fine for kind/dev.
**Production should use a real CA / CA-Issuer chain (or trust-manager)** so the trust anchor
outlives the leaf.
{{% /alert %}}

## What stays plaintext

- **kubelet liveness/readiness probes stay HTTP on `:8080`.** The native Kubernetes `grpc:`
  probe is plaintext-only, so it cannot target a TLS gRPC port; the HTTP probes already cover
  process and index health, and `:8080` stays plaintext.
- **The `:8081` controller bridge** (`/snapshot`, `/policy`, `/probe`) keeps its
  bearer-token + `NetworkPolicy` posture — a different threat model, unaffected by this
  overlay.

## Network posture

A `NetworkPolicy` does **not** gate `:9090` — gateways can live anywhere in the cluster, so
the port is open to all in-cluster clients. TLS is the only L7 control there today; client
identity (mTLS) is the Phase-2 addition.

If you run the `inferencecache doctor` under the TLS overlay, use `--config-only` — the
doctor dials gRPC plaintext.

## Related pages

- [Architecture]({{< relref "/docs/concepts/architecture/" >}}) — the listeners and the bridge.
- [The gRPC contract]({{< relref "/docs/concepts/grpc-contract/" >}}) — what flows over `:9090`.

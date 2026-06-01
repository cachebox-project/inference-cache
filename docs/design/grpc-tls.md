# Design: gRPC TLS posture (policy server `:9090`)

Status: implemented (Phase 1) · Implements: B5 (gRPC TLS posture) · Relates: gRPC contract (`grpc-contract.md`), B5 server (`pkg/server`), E1 gateway client, default install (`config/default`)

## Decision

The **locked design decision** for the gRPC policy server is **one-sided Service TLS via cert-manager** (server-only); **mTLS is a Phase 2 feature flag** (`--mtls-client-ca-file`), not implemented today. The HTTP endpoints (`/snapshot`, `/policy`) keep their **bearer-token + NetworkPolicy** posture — a separate threat model (intra-cluster controller↔server bridge), unchanged here.

TLS is **optional at the binary level**, controlled by flags (`--tls-cert-file` / `--tls-key-file`). The server *binary* fully supports TLS, but **`config/default` ships `:9090` plaintext, and TLS is an opt-in overlay** (`config/overlays/server-tls`).

**Why opt-in, not on-by-default (yet).** Both gRPC clients of `:9090` are plaintext-only today:
- the in-cluster **`kvevent-subscriber` producer** (C1) dials `:9090` to call `ReportCacheState` with `insecure.NewCredentials()` (`cmd/kvevent-subscriber`), targeting the policy Service (`DefaultPolicyServerGRPCAddress` in `pkg/adapters/runtime/vllm_lmcache.go`); and
- the **external gateway client** (E1) isn't built yet.

Flipping `config/default` to require TLS would break cache-state **ingestion** (the subscriber's handshake fails → no `ReportCacheState`). So this ticket **locks the decision and lands the full server-side mechanism** (flags, reloading cert, cert-manager Issuer/Certificate, posture metric, opt-in overlay, tests), and **defers the default flip** until both clients are TLS-aware — at which point enabling it is just making `config/overlays/server-tls` the default (and the subscriber needs the server CA distributed into engine-pod namespaces; see *Client trust anchor* below). Operators who want TLS now apply the overlay.

## Why Service TLS (not plaintext, not mTLS at Phase 1)

| Option | Pros | Cons | Verdict |
|---|---|---|---|
| Plaintext + NetworkPolicy | Simplest; no cert lifecycle | No L7 protection; prefix hashes + tenant IDs visible to in-cluster log/sniff/sidecar infra | Insufficient for prod |
| **Service TLS (one-sided)** | Encrypts the wire; cert-manager already in cluster (admission webhook, B3); one-sided keeps the client simple (verify only, no client cert) | No client identity proven at the TLS layer | **Phase 1 choice** |
| mTLS | Client identity proven at TLS layer; strongest guarantee | Client-cert distribution + rotation; complicates the external Java gateway client (E1) deployment | Phase 2 feature flag |

`LookupRoute` payloads are metadata-only (prefix hashes + tenant/model IDs, never prompt text or KV tensors — see `grpc-contract.md`), but prefix-hash + tenant-ID leakage to cluster-level logging or packet capture is a real exposure. TLS closes it without the client-cert distribution problem mTLS would impose on the gateway client.

## Architecture

```
cert-manager Issuer (self-signed)  ──mints──▶  Certificate  ──▶  Secret (inference-cache-server-tls: tls.crt, tls.key, ca.crt)
                                                                       │ mounted read-only
                                                                       ▼
                              server pod  /var/run/secrets/tls/{tls.crt,tls.key}
                                                                       │ --tls-cert-file / --tls-key-file
                                                                       ▼
       grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{GetCertificate: reload})))   ◀──TLS──  gateway (E1) / kvevent-subscriber (C1) / grpcurl
```

- **In-process termination.** TLS is terminated by the server binary itself — no sidecar, Envoy, or Ingress. Keeps the "one binary, one Service" deployment shape.
- **Reloading keypair.** The server serves the cert via a `tls.Config.GetCertificate` hook that re-reads `tls.crt`/`tls.key` when the file mtime advances, so a cert-manager-rotated Secret is picked up on the next handshake — no pod restart. A bad keypair still fails fast at startup (loaded once up front).
- **Client trust anchor + distribution.** With the self-signed Issuer, cert-manager publishes the issuing CA as `ca.crt` in the server's Secret. That Secret is mounted only into the server pod, so a client in another Deployment/namespace needs the CA distributed to it. **Caveat — the default `selfSigned` Issuer mints the serving cert as its own root, so `ca.crt` equals the leaf and rotates with it on renewal.** That makes a one-time static copy of `ca.crt` fragile: a client holding the old bundle fails after the leaf renews (cert-manager renews at ~2/3 of lifetime). It's fine for kind/dev (a short-lived cluster never reaches renewal), but for anything longer-lived a **stable** trust anchor is required — either (a) a real/org CA (clients already trust the root; the recommended production path), (b) a cert-manager CA-Issuer chain (a long-lived `isCA` CA Certificate → CA `Issuer` → rotating leaf, so `ca.crt` stays constant), or (c) dynamic propagation via [trust-manager](https://cert-manager.io/docs/trust/trust-manager/) redistributing the `Bundle` on every rotation. The concrete client-side trust wiring (and which of these an operator picks) is owned by the gateway client's connection/discovery doc (E1); this ticket fixes the server posture it must match and ships the dev-grade self-signed default. The install smoke proves the chain is real *at a point in time*: it pulls `ca.crt` from the serving Secret and runs `grpcurl -cacert <ca.crt> -authority <Service FQDN>` (so grpcurl verifies against the FQDN even though the port-forward terminates at `localhost`), asserting both that the cert verifies and that a wrong authority is rejected. The unit tests (`pkg/server/tls_test.go`) additionally verify the chain + DNS-name match in-process.
- **`grpc.health.v1` rides the same listener**, so a TLS server answers the health check over TLS for any client that dials it (gateway, grpcurl). The kubelet probe is a separate matter — see *kubelet probe compatibility* below.
- **Plaintext fallback.** With both flags empty the server builds `grpc.NewServer()` with no credentials and serves plaintext — the default posture (`config/default`), until the opt-in overlay supplies the flags.

### Issuer choice (dedicated, not shared with the webhook)

The admission webhook (B3) uses a **namespaced `Issuer`** (`selfsigned-issuer`), not a `ClusterIssuer`. A same-namespace `Certificate` *could* reference it directly, but the server ships its **own** self-signed `Issuer` (`inference-cache-server-selfsigned-issuer`) and `Certificate` (`inference-cache-server-serving-cert`) in the opt-in TLS component (`config/server/tls/`). This keeps the server's TLS wiring self-contained, touches nothing in the webhook's setup (least-disruptive), and adds no install dependency beyond cert-manager itself (already required). Operators wanting a real CA for production repoint `issuerRef` at their own `(Cluster)Issuer` — cert-manager-self-signed is fine for kind/dev but production should use a real CA (ACME, internal PKI, etc.).

## Configuration

- **Server flags** (`cmd/server`): `--tls-cert-file`, `--tls-key-file`. Both set → TLS; both empty → plaintext; **exactly one set → the server refuses to start** (the both-or-neither rule lives in `server.LoadGRPCTLSCredentials`, so it's the single, unit-tested source of truth).
- **Posture observability**: `inferencecache_server_grpc_tls_enabled` (gauge, 0/1) so operators can confirm the wire posture from Prometheus; the startup log line also carries a `grpc_tls` boolean field.
- **Default install** (`config/default`): **plaintext** — no cert, no `--tls-*` args. (This is what the C6 canary and any real engine deployment apply, so the plaintext `kvevent-subscriber` keeps ingesting.)
- **Opt-in TLS** — `config/overlays/server-tls` = `config/default` + the `config/server/tls/` kustomize component. The component ships the `Issuer` + `Certificate` (Secret `inference-cache-server-tls`) and patches the server Deployment to mount the Secret read-only at `/var/run/secrets/tls/` and pass the two `--tls-*` args. Service FQDN DNS names (`inference-cache-server.inference-cache-system.svc[.cluster.local]`) are written directly into the Certificate. Enable with `kubectl apply -k config/overlays/server-tls` (cert-manager required); the install smoke exercises exactly this overlay.
- **kind / dev**: the default is plaintext (no cert-manager dependency for a plaintext loop). To exercise TLS locally, apply the `config/overlays/server-tls` overlay (needs cert-manager).

## kubelet probe compatibility

Kubernetes native gRPC probes (the `grpc:` probe field) are **GA since 1.27** (beta + on-by-default since 1.24). **However, the kubelet's built-in gRPC prober connects in plaintext and exposes no TLS / auth parameters — there is no `-tls` equivalent in the native probe** (the probe field carries only `port` + optional `service`). Pointing a `grpc:` probe at the TLS `:9090` port would fail the handshake and the pod would never go Ready.

**Decision: the liveness/readiness probes stay HTTP on `:8080`** (`/healthz`, `/readyz`). That listener is plaintext and unchanged (HTTP is out of scope — see the threat-model split), and the HTTP probes already prove process liveness + index readiness. We deliberately do **not** add an exec-based `grpc_health_probe -tls` wrapper: it would mean shipping an extra binary into the distroless, `readOnlyRootFilesystem` server image for no signal the HTTP probes don't already provide. The gRPC `grpc.health.v1` service still works over TLS for real clients (gateway, grpcurl); it simply isn't the kubelet probe target.

> Forward note: if a future need requires the kubelet to probe gRPC-over-TLS directly, the options are (a) the exec `grpc_health_probe -tls -tls-no-verify` wrapper, or (b) upstream TLS support landing in the native prober. Neither is needed at Phase 1.

## NetworkPolicy interaction

The existing NetworkPolicy is unchanged — and, importantly, it **does not gate `:9090`**. The policy restricts only `:8081` (the controller↔server bridge: `/snapshot` + `/policy`) to the controller's pod selector; it explicitly leaves `:9090` (gRPC) and `:8080` (probes/metrics) **open to any in-cluster client**, because gateway clients and kubelet/Prometheus can't present the controller's SA and may run anywhere. So on `:9090` there is **no L3/L4 restriction**: TLS is the *only* posture control today (encryption + server authentication). The "two independent layers" composition holds for the `:8081` bridge (NetworkPolicy + bearer auth), **not** for `:9090`. Adding client identity at L7 on `:9090` is precisely what mTLS (Phase 2) is for — NetworkPolicy can't do it without breaking the gateway's any-namespace reachability.

## Phase 2 — mTLS (deferred)

mTLS is tracked for a future ticket as a feature flag, not implemented here. It would add a `--mtls-client-ca-file` flag and set `tls.Config.ClientAuth = RequireAndVerifyClientCert` (verifying client certs against that CA) on the server side. The cost it defers: the external Java gateway client (E1) would need its own client certificate, plus a rotation story — client-cert distribution + rotation is exactly the operational complexity Phase 1 avoids by staying one-sided. mTLS proves client identity at the TLS layer — which is the *only* way to add client-identity control on `:9090`, since (per the NetworkPolicy section) that port is intentionally open to all in-cluster clients and can't be L3/L4-gated without breaking gateway reachability.

## Threat model summary

Service TLS protects against **passive network observers** — in-cluster log/sniff infrastructure, sidecar proxies, mirrored traffic — so prefix hashes + tenant IDs are not readable on the wire, and clients verify they're talking to the real server (not a spoof). It does **not** protect against a **compromised pod**: `:9090` is open to any in-cluster client (the NetworkPolicy intentionally doesn't gate it — gateways live anywhere), so with one-sided TLS any pod can open a TLS connection and call `LookupRoute`. Closing that gap means proving client identity at the TLS layer — i.e. mTLS (Phase 2); NetworkPolicy is not an option for `:9090` without breaking gateway reachability. The exposure is bounded by the contract itself: `LookupRoute` is side-effect-free, fail-open, and metadata-only (prefix hashes, never prompt text or KV tensors). The HTTP bridge (`/snapshot`, `/policy` on `:8081`) is a different threat model — an intra-cluster controller↔server channel gated by bearer-token + audience binding **and** NetworkPolicy — and is intentionally left on its own posture.

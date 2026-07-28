---
title: "CLI: inferencecache doctor"
linkTitle: "CLI: doctor"
weight: 5
description: >
  Checks, finding codes, flags, and exit codes for the pre-flight diagnostic.
---

`inferencecache doctor` is a **read-only** pre-flight diagnostic — the cache-plane analogue
of `istioctl analyze`. It runs against a live install, prints stable finding codes, and
returns CI-friendly exit codes. The binary is `bin/inferencecache` (from `make build`).

```bash
inferencecache doctor -n serving
```

## Checks and finding codes

Nine checks run in a fixed order. Finding codes are stable and greppable; severity is shown
in parentheses.

| # | Check | Codes |
|---|---|---|
| 0 | Kubernetes API reachable | `API001` (FAIL) |
| 1 | Server gRPC health `SERVING` | `SV001`/`SV002` (FAIL), `SV003` (OK) |
| 2 | `/snapshot` reachable | `SN001` (FAIL), `SN002`/`SN005` (WARN), `SN003` (INFO), `SN004` (OK) |
| 3 | `/policy` wired | `PL001` (FAIL), `PL002` (OK), `PL003` (WARN) |
| 4 | `/probe` wired | `PB001` (FAIL), `PB002` (OK), `PB003` (WARN) |
| 5 | Per-`CacheBackend` health | `CB001`–`CB005` (WARN), `CB006` (OK), `CB007` (WARN — `FunctionalProbeOK` not True) |
| 6 | Engine-pod injection audit | `EP001` (WARN), `EP002` (OK) |
| 7 | Orphan-pod check | `OP001` (WARN) |
| 8 | `CacheTenant` health | `CT001` (WARN), `CT002` (OK) |
| 9 | `CachePolicy` coverage | `CP001` (INFO), `CP002` (OK), `CP003` (WARN — >1 CachePolicy in a namespace) |

`CB003` keys off KV-event observation (both the first-event latch and `lastEventAt` unset).
`External` backends have only their Ready state and endpoint checked.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Highest severity ≤ INFO. |
| `1` | At least one WARN. |
| `2` | At least one FAIL. |

## Flags

| Flag | Purpose |
|---|---|
| `--kubeconfig` | Path to the kubeconfig. |
| `--context` | Kube context to use. |
| `-n`, `--namespace` | Namespace to scope resource checks to. |
| `--server-endpoint` | Server `host[:gRPCport]` (HTTP endpoints derived on `:8081`). |
| `--snapshot-token-file` | Bearer token file for the `/snapshot` check. |
| `-o`, `--output` | `human` (default), `json`, or `table`. |
| `--no-color` | Disable ANSI color. |
| `--config-only` | Skip live probes — the right mode from a workstation, and under the TLS overlay. |
| `--timeout` | Overall timeout (default 30s). |

{{% alert title="Under the TLS overlay" color="info" %}}
The doctor dials gRPC **plaintext**. If you enabled the
[gRPC TLS overlay](/docs/administration/grpc-tls/), run with `--config-only` so it skips the
live gRPC probes.
{{% /alert %}}

Network posture: `:9090` (gRPC) is open to all in-cluster clients; the `:8081` bridge is
`NetworkPolicy`-restricted to the controller pods, so a full run is best done from within the
cluster or with an appropriate token.

## Related pages

- [Troubleshooting](/docs/administration/troubleshooting/) — the readiness runbook the
  per-backend checks map to.
- [Monitor the cache plane](/docs/tasks/monitor-the-cache-plane/) — where doctor fits.

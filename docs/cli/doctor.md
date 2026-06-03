# `inferencecache doctor`

A read-only pre-flight diagnostic for an inference-cache installation. One
command answers *"is my deployment configured correctly?"* — instead of chasing
`kubectl get cachebackend` + `kubectl describe pod` + `kubectl get cacheindex`
separately — and surfaces both what is broken **and** what is healthy so an
operator knows where to look.

It is the cache-plane analogue of `istioctl analyze`, `flux check`, or
`helm lint`: a single command, structured output, CI-gating exit codes, and no
mutation of cluster state.

## Install

Built as part of `make build` (binary at `bin/inferencecache`). Once releases
are cut it installs as a kubectl plugin via krew (`plugins/inferencecache.yaml`,
prepared but not yet submitted to krew-index) and runs as
`kubectl inferencecache doctor`.

```sh
make build
./bin/inferencecache doctor
```

## What it checks

The checks run in a fixed order — control-plane endpoints first, then the
CacheBackend / engine-pod data path, then tenant and policy configuration:

| # | Check | What it verifies |
|---|-------|------------------|
| 1 | Server reachability | gRPC dial + `grpc.health.v1` `Health/Check` (service `""`) returns `SERVING` |
| 2 | `/snapshot` reachability | HTTP GET returns 200 with a JSON-parseable body (bearer token if available; flags the unauthenticated path) |
| 3 | `/policy` reachability | the route is wired (non-mutating HEAD; any HTTP response proves it exists) |
| 4 | Per-CacheBackend health | `Ready=True`; matched engine pods > 0; index prefix count > 0; last KV event is fresh; `status.endpoint` populated and reachable |
| 5 | Engine-pod injection audit | every pod matching a CacheBackend `engineSelector` carries an `InjectedByCacheBackend` Event |
| 6 | Orphan-pod check | pods with a `NoMatchingCacheBackend` Event in the last 24h |
| 7 | CacheTenant health | `QuotaExceeded` condition is not `True` |
| 8 | CachePolicy coverage | each namespace with CacheBackends has at least one CachePolicy |

## Finding codes

Every finding carries a stable, greppable code. Codes are permanent identifiers
— scripts and dashboards can key off them.

| Code | Severity | Meaning |
|------|----------|---------|
| `API001` | FAIL | a Kubernetes read failed (apiserver unreachable / RBAC denial) |
| `SV001` | FAIL | server gRPC unreachable |
| `SV002` | FAIL | server health is not `SERVING` |
| `SV003` | OK | server health is `SERVING` |
| `SN001` | FAIL | `/snapshot` unreachable or non-200 |
| `SN002` | WARN | `/snapshot` 200 but body is not JSON |
| `SN003` | INFO | `/snapshot` answered without authentication |
| `SN004` | OK | `/snapshot` 200 with JSON body |
| `PL001` | FAIL | `/policy` route not wired |
| `PL002` | OK | `/policy` route is wired |
| `CB001` | WARN | CacheBackend `Ready` is not `True` |
| `CB002` | WARN | `matchedEnginePods == 0` (LikelySelectorMismatch) |
| `CB003` | WARN | `indexParticipation.prefixCount == 0` (EngineNotReportingState) |
| `CB004` | WARN | last KV event is stale (EngineStale) |
| `CB005` | WARN | `status.endpoint` empty or unreachable |
| `CB006` | OK | CacheBackend healthy on every axis |
| `EP001` | WARN | matched engine pod missing the injection Event |
| `EP002` | OK | matched engine pod is injected |
| `OP001` | WARN | orphaned engine pod (NoMatchingCacheBackend) |
| `CT001` | WARN | CacheTenant over quota (`QuotaExceeded=True`) |
| `CT002` | OK | CacheTenant within quota |
| `CP001` | INFO | namespace with CacheBackends has no CachePolicy (server defaults apply) |
| `CP002` | OK | namespace has CachePolicy coverage |

## Exit codes

Suitable for CI/CD gating:

| Exit | Meaning |
|------|---------|
| `0` | nothing worse than INFO |
| `1` | at least one WARN |
| `2` | at least one FAIL |

## Output formats

`--output=human` (default), `--output=json`, `--output=table`. Human output is
color-coded when stdout is a TTY (disable with `--no-color`).

Human:

```
inferencecache doctor

WARN
  WARN CB002  cachebackend/default/mismatched
      status.matchedEnginePods is 0 (LikelySelectorMismatch): spec.engineSelector
      matches no engine pods — the engine Deployment may be missing, scaled to
      zero, or its pod labels have drifted from the selector

OK
  OK   SV003  10.96.0.10:9090
      gRPC health check reports SERVING

Summary: 1 OK, 0 INFO, 1 WARN, 0 FAIL  (exit 1)
```

JSON (stable schema — `summary` + `findings[]`, each finding
`{code,status,check,resource,message}`):

```json
{
  "summary": { "ok": 1, "info": 0, "warn": 1, "fail": 0, "exitCode": 1 },
  "findings": [
    {
      "code": "CB002",
      "status": "WARN",
      "check": "CacheBackendHealth",
      "resource": "cachebackend/default/mismatched",
      "message": "status.matchedEnginePods is 0 (LikelySelectorMismatch): ..."
    }
  ]
}
```

Table — one row per finding (`STATUS  CODE  CHECK  RESOURCE  MESSAGE`).

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--kubeconfig` | env / `~/.kube/config` / in-cluster | kubeconfig path |
| `--context` | current context | kubeconfig context |
| `-n, --namespace` | all namespaces | scope the checks |
| `--server-endpoint` | discover the Service | override server `host[:gRPCport]`; HTTP probes use the same host on `:8081` |
| `--snapshot-token-file` | in-cluster SA token path | bearer token presented to `/snapshot` |
| `-o, --output` | `human` | `human`, `json`, or `table` |
| `--no-color` | off | disable ANSI color |
| `--config-only` | off | skip the live endpoint probes (checks 1–3); run only the cluster-config checks |
| `--timeout` | `30s` | overall run timeout |

## Running in-cluster vs. from a workstation

The Kubernetes-config checks (4–8) work from anywhere your kubeconfig can reach
the apiserver. The live endpoint probes (1–3) need to reach the cache-plane
server's gRPC `:9090` and snapshot/policy `:8081` ports:

- **In-cluster** (e.g. a debug pod): the server is discovered by Service DNS and
  reached directly.
- **From a workstation**: the in-cluster Service DNS does not resolve. Either
  port-forward and point doctor at it —

  ```sh
  kubectl -n inference-cache-system port-forward svc/inference-cache-server 9090:9090 8081:8081 &
  ./bin/inferencecache doctor --server-endpoint localhost
  ```

  — or skip the endpoint probes entirely with `--config-only` to validate just
  the CacheBackend / CacheTenant / CachePolicy configuration.

> Note: the doctor dials the gRPC port in plaintext, matching the default
> install. If the server is deployed with the TLS overlay, run doctor in-cluster
> (or use `--config-only`); a TLS-aware probe is a follow-up.

## Manual smoke test (kind)

For the demo / debugging-flow walkthrough, on a local kind cluster:

```sh
make dev-cluster
kubectl apply -k config/default   # install CRDs + controller + server
kubectl apply -f config/samples/cache_v1alpha1_cachebackend.yaml

# Validate configuration without the live server (works pre-port-forward):
./bin/inferencecache doctor --config-only

# Full run against the live server:
kubectl -n inference-cache-system port-forward svc/inference-cache-server 9090:9090 8081:8081 &
./bin/inferencecache doctor
```

Expected: a freshly-applied CacheBackend whose engine Deployment has not rolled
out yet reports `CB002` (LikelySelectorMismatch) and/or `CB001` (not Ready) as
WARN, exiting `1`. Once an engine pod matches the selector and starts reporting
KV events, those clear to `CB006` (OK).

## Scope

This is the diagnostic MVP. `install` / `hint` / `backends` / `tenants`
subcommands, a `--watch` mode, auto-fix/remediation, cross-cluster aggregation,
and the full krew publish flow are out of scope here and tracked with the full
CLI-plugin work.

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
| 3 | `/policy` reachability | the route is wired (non-mutating HEAD; 2xx/401/403/405 = wired, 404 = not mounted, 5xx = WARN) |
| 4 | Per-CacheBackend health | `Ready=True`; managed backends have ever observed a KV event (durable `firstKVEventObservedAt` latch) and, if `lastEventAt` is present, it is fresh (drained backends with a cleared `lastEventAt` are not flagged); `status.endpoint` populated and reachable |
| 5 | Engine-pod injection audit | every pod matching a CacheBackend `engineSelector` carries the `inferencecache.io/injected-by` annotation (or the injection Event) |
| 6 | Orphan-pod check | pods with a `NoMatchingCacheBackend` Event in the last 24h (forward-looking — no producer yet, see Notes) |
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
| `SN005` | WARN | `/snapshot` reachable but auth-gated (401/403); supply a controller-audience token |
| `PL001` | FAIL | `/policy` route not wired (connection refused, or HTTP 404 = route not mounted) |
| `PL002` | OK | `/policy` route is wired (2xx / 401 / 403 / 405) |
| `PL003` | WARN | `/policy` mounted but answered an unexpected status (e.g. 5xx) |
| `CB001` | WARN | CacheBackend `Ready` is not `True` |
| `CB002` | WARN | managed backend with a selector matches 0 engine pods (LikelySelectorMismatch) |
| `CB003` | WARN | no KV event ever observed for the backend (EngineNotReportingState) |
| `CB004` | WARN | last KV event is stale (EngineStale) |
| `CB005` | WARN | `status.endpoint` empty or unreachable |
| `CB006` | OK | CacheBackend healthy on every applicable axis |
| `EP001` | WARN | matched engine pod missing an injection marker (no `inferencecache.io/injected-by` annotation and no Event) |
| `EP002` | OK | matched engine pod is injected (annotation or Event) |
| `OP001` | WARN | orphaned engine pod (NoMatchingCacheBackend; forward-looking — see note below) |
| `CT001` | WARN | CacheTenant over quota (`QuotaExceeded=True`) |
| `CT002` | OK | CacheTenant within quota |
| `CP001` | INFO | namespace with CacheBackends has no CachePolicy (server defaults apply) |
| `CP002` | OK | namespace has CachePolicy coverage |

Notes:

- **`CB003` keys off KV-event observation, not prefix count.** Zero warm prefixes
  is a valid state for an up-but-idle backend, so doctor flags "engine not
  reporting" only when *no* KV event has ever been observed — which means BOTH
  the durable `status.firstKVEventObservedAt` latch and the current-view
  `status.indexParticipation.lastEventAt` are unset. A drained backend that has
  ever observed an event keeps the latch set (write-once, never cleared) so it
  is correctly NOT reported as "never observed" even when its current
  `lastEventAt` has been cleared by the poller. `CB004` fires only when
  `lastEventAt` IS present but has gone stale — an idle backend with a fresh
  event is healthy (`CB006`).
- **External backends** (`spec.type=External`) are checked for `Ready` and
  endpoint reachability only. Engine-pod matching (`CB002`) and index
  participation (`CB003`/`CB004`) are managed-backend concerns and are skipped,
  so a valid External config is not spuriously flagged.
- **`OP001` is forward-looking.** No controller emits the `NoMatchingCacheBackend`
  Event yet, so this check is a no-op against today's clusters; it begins
  reporting automatically once the engine-pod binding work adds the emitter.

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
      status.matchedEnginePods is 0 (LikelySelectorMismatch): spec.engineSelector matches no engine pods

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

The declarative Kubernetes-config checks (4–8, minus check 4's TCP dial) work
from anywhere your kubeconfig can reach the apiserver. The live endpoint probes
(1–3) and check 4's `status.endpoint` TCP dial need network reachability to the
cache-plane server's
gRPC `:9090` / snapshot-policy `:8081` ports and to each backend's endpoint —
which from a workstation are usually in-cluster Service DNS / ClusterIPs that do
not resolve. `--config-only` skips the endpoint probes (1–3) and check 4's TCP
dial (it still validates `status.endpoint` is published), so it is the right
mode from a workstation without a port-forward:

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

  On a default auth-gated install, `/snapshot` requires an audience-bound
  controller token; without one doctor reports `SN005` (reachable but auth-gated)
  rather than verifying the body. Provide one with
  `--snapshot-token-file=<path>` if you need the full `/snapshot` check.

> Note: the doctor dials the gRPC port in plaintext, matching the default
> install. Under the opt-in TLS overlay the live `:9090` probe cannot succeed
> (the CLI has no TLS flags yet — a follow-up), so use `--config-only` there; the
> cluster-configuration checks are unaffected.

## Manual smoke test (kind)

For the demo / debugging-flow walkthrough, on a local kind cluster:

```sh
make dev-cluster
kubectl apply -k config/default   # install CRDs + controller + server
kubectl apply -f config/samples/cache_v1alpha1_cachebackend.yaml

# Validate configuration without the live server (works pre-port-forward):
./bin/inferencecache doctor --config-only

# Full run against the live server (Service DNS does not resolve from a
# workstation, so point doctor at the port-forward):
kubectl -n inference-cache-system port-forward svc/inference-cache-server 9090:9090 8081:8081 &
./bin/inferencecache doctor --server-endpoint localhost
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

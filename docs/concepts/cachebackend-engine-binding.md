# CacheBackend ↔ engine-pod binding

CacheBackend uses Kubernetes label selectors to find the engine pods it injects cache wiring into. This page explains the model, the lifecycle, and the common ways it goes wrong.

## How it works

Three actors participate in the binding:

- **CacheBackend** — the namespaced CR you create. Its `spec.engineSelector.matchLabels` is a label selector over pods in the same namespace, with the same semantics as `Service.spec.selector`.
- **Engine pod** — a vLLM (or other supported runtime) pod, typically owned by a user-managed Deployment. Its `template.metadata.labels` are what the selector matches against.
- **Mutating Pod webhook** — the controller's admission webhook intercepts pod CREATE and stamps the matched engine pod with the LMCache wiring (env vars, CLI args, and the kvevent-subscriber observation sidecar).

```text
                                       +-----------------------+
                                       | CacheBackend (CR)     |
                                       |   spec.engineSelector |
                                       +-----------+-----------+
                                                   | label-selector match (at pod CREATE)
                                                   v
+-----------------+    pod CREATE     +------------+-----------+
| Engine          | -----------+----> | Mutating Pod webhook   |
| Deployment      |            |      | (matches selector;     |
| template:       |            |      |  injects engine config |
|   labels: {...} |            |      |  + subscriber sidecar) |
+-----------------+            |      +------------+-----------+
                               |                   |
                               |                   v
                               |        +----------+-----------+
                               |        | Engine pod           |
                               |        |  env: LMCACHE_*      |
                               |        |  args: --kv-...      |
                               |        |  sidecar: subscriber |
                               |        +----------+-----------+
                               |                   |
                               |                   v subscriber publishes
                               |        +----------+-----------+
                               |        | lmcache-server pod   |
                               +------> | (managed by the CR;  |
                                        |  endpoint published  |
                                        |  in status.endpoint) |
                                        +----------------------+
```

The match is evaluated **once at pod CREATE** by the mutating webhook. The wiring is sticky to the life of the pod — relabeling an existing pod does not re-evaluate it. To opt a pod out regardless of label match, set `inferencecache.io/skip-inject: "true"` on the pod template.

## Lifecycle

1. **Apply the CacheBackend.** `kubectl apply -f cachebackend.yaml`. The reconciler creates the managed lmcache-server Deployment + Service and publishes the resolved address in `status.endpoint`.
2. **Deploy the engine.** Apply an engine Deployment whose pod template labels include every key/value in `spec.engineSelector.matchLabels`. New pods from that Deployment hit the mutating webhook at admission time.
3. **The webhook claims matching pods.** Each new engine pod that matches gets LMCache env vars, the `--kv-transfer-config` CLI arg, and the kvevent-subscriber sidecar appended. A `Normal InjectedByCacheBackend` event lands on the pod (visible in `kubectl describe pod`).
4. **KV events flow.** The subscriber sidecar streams the engine's KV-cache events to the policy server's index, which surfaces them in `CacheBackend.status` (see `Matched` and the index-participation status fields).
5. **Observe and debug.** `kubectl get cachebackend` shows the `Matched` column — the snapshot count of pods whose labels currently match. `kubectl describe pod <engine-pod>` shows which CacheBackend (if any) claimed it.

## Annotated example

A single CacheBackend with a matching engine Deployment. The label `app: qwen-demo` appears in two places — that's what binds them:

```yaml
apiVersion: inferencecache.io/v1alpha1
kind: CacheBackend
metadata:
  name: qwen-demo
spec:
  type: LMCache
  integration:
    engine: vllm
    role: ReadWrite
  engineSelector:
    matchLabels:
      app: qwen-demo          # <-- selector key/value (1 of 2)
  backendConfig:
    model: Qwen/Qwen2.5-0.5B-Instruct
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: qwen-demo
spec:
  replicas: 1
  selector:
    matchLabels:
      app: qwen-demo
  template:
    metadata:
      labels:
        app: qwen-demo        # <-- selector key/value (2 of 2; this is what the webhook sees)
    spec:
      containers:
        - name: vllm
          image: vllm/vllm-openai-cpu:latest
          args: ["--model", "Qwen/Qwen2.5-0.5B-Instruct"]
```

A copy-pasteable version of this pair ships at [`config/samples/cachebackend-with-engine.yaml`](../../config/samples/cachebackend-with-engine.yaml).

## Common failure modes

| Symptom | Cause | How to detect | Fix |
|---|---|---|---|
| Engine pod runs uncached; no LMCache env on its container | Selector and pod labels don't overlap (typo, drift after a Deployment rename, etc.) | `kubectl describe pod <engine-pod>` shows `Normal NoMatchingCacheBackend`; `kubectl get cachebackend` shows `Matched: 0` | Reconcile the label sets — either fix `engineSelector.matchLabels` on the CR or fix the Deployment's pod template labels |
| Multiple CacheBackends overlap on the same pod | Two CacheBackends in the namespace have selectors that both match | Admission should reject the second CR — if it didn't, file a ticket; meanwhile `kubectl describe pod` shows which CR injected | Pick one CR; delete or narrow the other |
| Engine pod was labeled after creation but still uncached | Label match is evaluated once at pod CREATE; relabeling later has no effect | `kubectl describe pod <engine-pod>` shows no `InjectedByCacheBackend` event | Delete the pod (`kubectl delete pod <engine-pod>`); the Deployment will recreate it and the new pod will re-enter admission |
| CacheBackend was deleted, but engine pods are still running with the old wiring | Wiring is sticky to the pod's lifetime; deleting the CR does not retract env vars from already-admitted pods | Engine logs show LMCache connect failures to a no-longer-existing Service | Rolling-restart the engine Deployment to admit fresh pods (which will match no CR and run uncached) |
| Pod that should be skipped still gets wiring | The `inferencecache.io/skip-inject: "true"` annotation was missing or set on the Deployment, not the pod template | `kubectl get pod <pod> -o yaml` shows no `inferencecache.io/skip-inject` annotation | Add the annotation under `spec.template.metadata.annotations` of the Deployment and restart |

The `inferencecache.io/skip-inject` annotation is the explicit escape hatch: any non-empty value other than `false`/`0`/`no`/`off` opts the pod out. Use it for pods you've already pre-wired or that should run vanilla for a debugging experiment.

# Project Context

## 5. Cache Backend Substrate Decisions

### Engine-pod match cadence

`CacheBackend.status.matchedEnginePods` stays a controller-owned snapshot, not a
pod-watch-backed live counter. The controller still uses one namespaced live Pod
List per refresh via the APIReader so it avoids registering a broad Pod informer
for every controller process.

To reduce stale operator output during rolling restarts, the reconciler uses a
conditional fast cadence: when the observed matching Pod count differs from the
desired replica sum of Deployments whose pod-template labels match the
CacheBackend's `engineSelector`, it self-requeues every 5s. Once the observed
count matches desired replicas, it returns to the steady 30s cadence.

This keeps the no-Pod-watch design while making known churn converge quickly.
The tradeoff is one extra namespaced Deployment List per refresh, which is small
for the target shape of hundreds of pods and single-digit CacheBackends.

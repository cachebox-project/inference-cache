// Package external is the runtime adapter for CacheBackend{type: External}:
// the controller does NOT provision pods for the cache, the operator points
// the CR at a pre-existing remote cache they manage themselves, and the
// adapter wires engine pods to that endpoint with the same engine wire
// format the managed-LMCache path uses (see
// pkg/adapters/runtime/internal/enginewire).
//
// Owner: controller. Selected by the C5 [runtime.Registry] when the
// reconciler dispatches a managed CacheBackend (which it does not, for
// External — the dispatch path early-returns via reconcileExternal) AND when
// the pod-mutating webhook resolves an adapter for an engine pod that the
// CR's spec.engineSelector matched. The pod-webhook path is the load-
// bearing one: without this adapter the webhook fail-opens an unwired
// engine pod and the External cache never receives traffic.
//
// To keep cmd/controller's single Registry the source of truth for "which
// adapters this build ships", cmd/controller registers this adapter on top
// of runtime.DefaultRegistry(). It deliberately is NOT registered by
// DefaultRegistry itself — DefaultRegistry lives in package runtime and
// this package imports runtime, so a reciprocal import would cycle.
package external

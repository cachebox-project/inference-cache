// Package pod is the controller-owned mutating admission webhook that auto-
// wires user-provided inference engine pods to a matching cache backend —
// either a managed backend the controller provisions (LMCache today) or an
// External backend whose lifecycle the operator owns.
//
// On every Pod admission the handler:
//  1. lists CacheBackends in the pod's namespace;
//  2. picks the first whose Spec.EngineSelector.MatchLabels match the pod;
//  3. resolves a runtime adapter from the controller's runtime.Registry;
//  4. resolves the cache endpoint type-scoped (see [effectiveEndpoint]):
//     trimmed Spec.Endpoint for External CRs (authoritative; preferred
//     over Status.Endpoint so a pod admitting between an operator
//     spec.endpoint edit and the reconciler's mirror is wired to the
//     fresh address, not the stale one), Status.Endpoint for managed
//     types (the reconciler builds it from the live Service; spec.endpoint
//     is admission-rejected on managed types); and
//  5. calls adapter.InjectEngineConfig(pod.Spec, endpoint, cache) to merge
//     the cache-server endpoint + connector env/args into the pod spec.
//
// The webhook fails open: any error (no matching CacheBackend, no usable
// endpoint yet, adapter rejection, API list error) returns Allowed without
// mutation, matching the project's hot-path semantics (a webhook fault must
// never block engine admission). The adapter's InjectEngineConfig is the
// source of truth for the full injected contract (env + arg) and is itself
// idempotent at the merge level, so re-admission of an already-injected
// pod produces an empty JSON-patch set and the handler does not need a
// separate env-presence short-circuit. Trusting the adapter — rather than a
// lenient env-only check at the handler — avoids the trap where a
// partially-wired pod (e.g. only LMCACHE_REMOTE_URL set by hand) is
// admitted permanently missing the rest of the wiring.
package pod

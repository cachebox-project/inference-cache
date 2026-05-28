// Package pod is the controller-owned mutating admission webhook that auto-
// wires user-provided inference engine pods to a managed cache backend.
//
// On every Pod admission the handler:
//  1. lists CacheBackends in the pod's namespace;
//  2. picks the first whose Spec.EngineSelector.MatchLabels match the pod;
//  3. resolves a runtime adapter from the controller's runtime.Registry; and
//  4. calls adapter.InjectEngineConfig(pod.Spec, status.Endpoint, cache) to
//     merge the cache-server endpoint + connector env/args into the pod
//     spec.
//
// The webhook fails open: any error (no matching CacheBackend, no published
// endpoint yet, adapter rejection, API list error) returns Allowed without
// mutation, matching the project's hot-path semantics (a webhook fault must
// never block engine admission). The adapter's InjectEngineConfig is itself
// idempotent, but already-injected pods are short-circuited at the handler
// so the apiserver doesn't churn JSON patches on every re-admit.
package pod

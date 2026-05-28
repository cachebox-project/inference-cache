// Package v1alpha1 hosts the admission webhooks (defaulting + validating) for
// the v1alpha1 CacheBackend type. The webhook keeps "obviously broken" specs
// from reaching the reconciler: it stamps the Phase-1 defaults (lookup
// timeout, minimum prefix tokens, replicas) and rejects configurations that
// can't be reconciled at all (External backend without an Endpoint,
// persistent storage on a memory-only backend type, a cross-namespace
// Endpoint that wasn't explicitly opted into).
//
// The validator is structured as an ordered list of [ValidationRule]
// functions so additional rules can plug in as a one-line append. The
// runtime-adapter compatibility check that the M6 substrate module needs
// is the next planned rule and will land via that path.
//
// The package is owned by the controller binary; engine/server packages
// MUST NOT depend on it.
package v1alpha1

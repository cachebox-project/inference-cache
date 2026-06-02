// Package v1alpha1 hosts the admission webhooks (defaulting + validating) for
// the operator-facing v1alpha1 CRDs: CacheBackend, CachePolicy, and
// CacheTenant. Each webhook keeps "obviously broken" or operator-trust-eroding
// specs from reaching the reconciler:
//
//   - CacheBackend stamps the Phase-1 defaults (replicas, firstEventTimeout)
//     and rejects configurations that can't be reconciled at all (External
//     backend without an Endpoint, persistent storage on a memory-only backend
//     type, a cross-namespace Endpoint that wasn't explicitly opted into, an
//     unsupported runtime/backend pair, reserved engineOverrides).
//   - CachePolicy enforces at most one policy per namespace (the reconciler
//     flattens to one ResolvedPolicy per namespace, so a second CR silently
//     loses) and a strictly positive evictionTTL when set.
//   - CacheTenant enforces tenantID uniqueness within a namespace (duplicate
//     tenantIDs would collide in the index's (tenant, model, scheme, hash)
//     keying).
//
// Each validator is structured as an ordered, pluggable list of spec-only
// rule functions (DefaultValidationRules / DefaultCachePolicyValidationRules /
// DefaultCacheTenantValidationRules) so additional rules plug in as a one-line
// append. Cross-CR checks that need cluster state (the one-per-namespace and
// tenantID-uniqueness rules) read sibling CRs through the manager's live
// APIReader and run alongside the spec-only rules. Rejections are hard
// (admission Invalid), never warnings, because warnings get ignored.
//
// The package is owned by the controller binary; engine/server packages
// MUST NOT depend on it.
package v1alpha1

// Package auth provides HTTP middleware that gates the policy server's
// internal /snapshot endpoint on a Kubernetes ServiceAccount bearer token.
//
// The middleware validates an incoming `Authorization: Bearer <token>` via the
// apiserver's TokenReview API and rejects anything that isn't the configured
// controller ServiceAccount. Successful validations are cached briefly (keyed
// by SHA-256 of the token) so the poller's steady ~30s cadence does not
// hammer the apiserver. Errors against the TokenReview API are fail-closed:
// the request is rejected with 503 rather than admitted.
//
// Defence in depth: this middleware is one of two independent gates around
// /snapshot. The other is a namespace-scoped NetworkPolicy that restricts
// L3/L4 ingress to the controller's pod selector. See config/server/.
package auth

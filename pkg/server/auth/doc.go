// Package auth provides HTTP middleware that gates the policy server's
// internal controller-facing endpoints (/snapshot and /policy) on a
// Kubernetes ServiceAccount bearer token.
//
// The middleware validates an incoming `Authorization: Bearer <token>` via the
// apiserver's TokenReview API and rejects anything that isn't the configured
// controller ServiceAccount. Successful validations are cached briefly (keyed
// by SHA-256 of the token) so the controller's steady cadence — the
// CacheIndex poller scraping /snapshot every ~30s plus the CachePolicy
// reconciler pushing to /policy on every reconcile and tick — does not
// hammer the apiserver. Errors against the TokenReview API are fail-closed:
// the request is rejected with 503 rather than admitted.
//
// Both endpoints share one Authenticator-construction shape but get
// independent instances so each can publish its own outcome counter
// (inferencecache_snapshot_auth_total and inferencecache_policy_auth_total).
// That lets dashboards distinguish read-side auth failures (info leak
// attempt) from write-side ones (active tampering — /policy is replace-on-
// write, so a successful rogue POST would override every namespace's
// CachePolicy state cluster-wide).
//
// Defence in depth: this middleware is one of two independent gates around
// the controller-facing listener. The other is a namespace-scoped
// NetworkPolicy that restricts L3/L4 ingress to the controller's pod
// selector. See config/server/.
package auth

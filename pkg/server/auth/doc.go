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
// Defence in depth: this middleware is one of THREE independent gates
// around the controller-facing listener.
//   - L3/L4: a namespace-scoped NetworkPolicy restricts ingress to the
//     controller's pod selector (config/server/).
//   - L7 identity: this middleware's TokenReview-backed bearer check
//     rejects every request whose token does not resolve to the
//     configured controller ServiceAccount username.
//   - L7 audience: when Options.Audience is set, the middleware passes
//     TokenReviewSpec.Audiences so the apiserver rejects any bearer
//     whose JWT audience doesn't match — closing the cross-surface
//     replay vector (a leaked apiserver-bound token cannot scrape
//     /snapshot or push /policy, and vice versa). See audience.go for
//     the ControllerAudience constant.
package auth

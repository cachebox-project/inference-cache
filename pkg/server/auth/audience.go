package auth

// ControllerAudience is the audience identifier bound onto the projected
// ServiceAccount token the controller uses to call both /snapshot AND
// /policy, and the audience the server passes to TokenReview when validating
// that token on either endpoint. The two endpoints share one middleware
// identity (the controller SA) and so share one audience; there is one
// trust boundary, not two.
//
// Audience binding is a defense-in-depth gate layered on top of the
// bearer-token + NetworkPolicy posture: a token minted with this audience
// cannot be replayed against the apiserver (whose default audience is the
// kubernetes cluster identifier), and the apiserver's default-audience
// token cannot be replayed against /snapshot or /policy. Under the default
// apiserver audience configuration this makes a leaked token from one
// surface useless on the other; if a cluster has been explicitly configured
// to also accept inferencecache.io/controller as an apiserver audience the
// cross-surface defense degrades — operators MUST keep this audience
// distinct from any audience the apiserver accepts.
//
// This MUST agree with the audience listed in the controller's projected
// SA token volume (config/manager/manager.yaml) AND the server's
// --controller-audience flag (whose default is this constant). Three gates
// pin the agreement against drift — each covers a different surface so
// the assertions are complementary, not redundant:
//
//   - The envtest case in this package ("controller SA but wrong audience
//     -> 401") proves the apiserver actually enforces a mismatch the
//     middleware passes through. In-process; does NOT touch the manifest.
//   - The CacheIndex authed-integration envtest in internal/controller
//     ("wrong-audience token -> 401") is the over-the-wire complement:
//     it pins the controller poller's read-token-and-send path against
//     a real apiserver-validated mismatch. Still does NOT touch the
//     manifest (the controller deployment shape isn't loaded by envtest).
//   - The default-install smoke (docs/reference-stack/scripts/
//     default_install_smoke.sh) is the gate that catches manifest-level
//     drift: its earlier CacheIndex assertion (observedServer populates
//     within ~60s) succeeds only when the REAL controller's projected-
//     volume audience, BearerTokenPath, server flag, and middleware all
//     agree end-to-end. The smoke's audience-binding probe pod uses an
//     inline duplicate volume spec and so verifies the SERVER's
//     enforcement only — NOT manifest agreement.
//
// If you move this constant, move config/manager/manager.yaml and the
// server's --controller-audience default in the same change, and run all
// three gates.
//
// The string is dotted (`inferencecache.io/controller`) rather than
// hyphenated to match the canonical API group `inferencecache.io`; it
// reads naturally as "this project's controller-API audience" and aligns
// with the project's vendor-neutral identity convention. The name is
// endpoint-agnostic because the same audience gates BOTH /snapshot and
// /policy under the shared controller-auth middleware.
const ControllerAudience = "inferencecache.io/controller"

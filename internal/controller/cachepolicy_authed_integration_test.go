package controller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"

	cacheserver "github.com/cachebox-project/inference-cache/pkg/server"
	"github.com/cachebox-project/inference-cache/pkg/server/auth"
)

// TestIntegrationCachePolicyPushAgainstAuthedEndpoint is the mirror of
// TestIntegrationCacheIndexPollerAgainstAuthedSnapshot for the controller-
// write side. Drives the full reconciler-to-server auth path against a real
// apiserver:
//
//	ControlPlaneReconciler.pushSnapshot
//	  -> bearerToken() reads the SA token from a tmpfile (kubelet-shape)
//	  -> POST carries Authorization: Bearer <token>
//	  -> in-process httptest server wrapped in pkg/server/auth.Middleware
//	  -> Authenticator calls TokenReview against the envtest apiserver
//	  -> apiserver validates the token it minted via TokenRequest
//	  -> handler accepts and returns 204
//
// The auth middleware unit tests in pkg/server/auth already cover the
// middleware in isolation; this test pins the production CLIENT code (the
// reconciler) onto the same backend, since that's the surface this hardening
// changes for downstream callers.
//
// Skipped unless KUBEBUILDER_ASSETS is set — same gate as the rest of the
// envtest suite. Locally run with
//
//	KUBEBUILDER_ASSETS=$(make test-env | tail -1) go test ./...
func TestIntegrationCachePolicyPushAgainstAuthedEndpoint(t *testing.T) {
	skipWithoutEnvtest(t)

	k8s, _, cfg := startEnv(t)

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("kubernetes.NewForConfig: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Two SAs: one in the controller role (the reconciler authenticates as
	// this), one for the "any other in-namespace identity" attacker shape.
	const ns = "default"
	const controllerSA = "ic-policy-controller"
	const otherSA = "ic-policy-other"
	for _, name := range []string{controllerSA, otherSA} {
		if _, err := clientset.CoreV1().ServiceAccounts(ns).Create(ctx, &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create SA %q: %v", name, err)
		}
	}
	expectedSA := "system:serviceaccount:" + ns + ":" + controllerSA

	// Wire the authenticator the same shape cmd/server's controller-facing
	// listener does, and mount the EXACT same /policy HTTP handler the
	// production server uses (via NewPolicyHTTPHandler). Stacking both layers
	// here guards against the auth wrap accidentally allowing a different
	// decode shape than the production path: the snapshot the reconciler
	// marshals must round-trip through the real DisallowUnknownFields decoder
	// and land in a real PolicyStore. A counter wraps the policy handler so
	// the test can still assert the handler was invoked (or short-circuited)
	// on each call.
	authn, err := auth.NewAuthenticator(auth.Options{
		Reviewer:               auth.FromClientset(clientset),
		ExpectedServiceAccount: expectedSA,
		// /policy uses the write-side audience, separate from the
		// controller audience used by /snapshot and /probe. The negative
		// case below proves that a same-SA token minted for the read/probe
		// audience cannot push policy.
		Audience: auth.PolicyAudience,
	})
	if err != nil {
		t.Fatalf("auth.NewAuthenticator: %v", err)
	}
	store := cacheserver.NewPolicyStore()
	policyHandler := cacheserver.NewPolicyHTTPHandler(store)
	calls := 0
	countingHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		policyHandler.ServeHTTP(w, r)
	})
	mux := http.NewServeMux()
	mux.Handle("/policy", authn.Middleware(countingHandler))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Stage SA tokens via TokenRequest, write each to a tmpfile in the
	// kubelet shape (trailing newline, mode 0o600). bearerToken() trims the
	// newline before sending. The audience parameter is what the apiserver
	// bakes into the JWT's `aud` claim — keeping it overridable lets the
	// wrong-audience negative case mint a same-SA token with a deliberately
	// non-matching audience.
	mintTokenFile := func(saName, audience string) string {
		t.Helper()
		exp := int64(3600)
		spec := authnv1.TokenRequestSpec{ExpirationSeconds: &exp}
		if audience != "" {
			spec.Audiences = []string{audience}
		}
		tr, err := clientset.CoreV1().ServiceAccounts(ns).CreateToken(ctx, saName, &authnv1.TokenRequest{
			Spec: spec,
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("CreateToken(%s, audience=%q): %v", saName, audience, err)
		}
		path := filepath.Join(t.TempDir(), saName+"-token")
		if err := os.WriteFile(path, []byte(tr.Status.Token+"\n"), 0o600); err != nil {
			t.Fatalf("write token file: %v", err)
		}
		return path
	}
	policyTokenFile := mintTokenFile(controllerSA, auth.PolicyAudience)
	otherTokenFile := mintTokenFile(otherSA, auth.PolicyAudience)
	controllerAudienceTokenFile := mintTokenFile(controllerSA, auth.ControllerAudience)
	wrongAudienceTokenFile := mintTokenFile(controllerSA, "wrong-audience")
	emptyTokenFile := filepath.Join(t.TempDir(), "empty-token")
	if err := os.WriteFile(emptyTokenFile, []byte(""), 0o600); err != nil {
		t.Fatalf("write empty token file: %v", err)
	}

	r := &ControlPlaneReconciler{
		Client:          k8s,
		ServerPolicyURL: srv.URL + "/policy",
		HTTPClient:      srv.Client(),
		BearerTokenPath: policyTokenFile,
	}

	// Happy path: controller SA token → middleware admits → handler 204
	// → reconciler returns nil.
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile with controller SA token: %v", err)
	}
	if calls != 1 {
		t.Fatalf("handler invoked %d times after happy push, want 1", calls)
	}

	// Wrong SA: TokenReview authenticates the bearer (it's a real, valid
	// token) but the username doesn't match → middleware returns 403; the
	// reconciler surfaces this as a push error so controller-runtime backs
	// off and retries. The handler must NOT have been invoked.
	r.BearerTokenPath = otherTokenFile
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err == nil {
		t.Fatal("Reconcile with other SA token: expected non-nil error (403), got nil")
	}
	if calls != 1 {
		t.Fatalf("handler invoked %d times after 403, want 1 (the auth gate must short-circuit)", calls)
	}

	// No token: empty file → bearerToken() returns ("", nil) → POST goes out
	// without an Authorization header → middleware returns 401. Reconciler
	// surfaces as error, handler not invoked.
	r.BearerTokenPath = emptyTokenFile
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err == nil {
		t.Fatal("Reconcile with empty token file: expected non-nil error (401), got nil")
	}
	if calls != 1 {
		t.Fatalf("handler invoked %d times after 401, want 1", calls)
	}

	// Wrong audience: same controller SA, valid token shape, but the JWT
	// audience is the read/probe audience, NOT the policy audience the
	// middleware passes to TokenReviewSpec.Audiences. The apiserver rejects
	// on audience grounds → TokenReview returns !Authenticated → middleware
	// 401. This pins that a leaked /snapshot token cannot push /policy.
	r.BearerTokenPath = controllerAudienceTokenFile
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err == nil {
		t.Fatal("Reconcile with controller-audience controller-SA token: expected non-nil error (401), got nil")
	}
	if calls != 1 {
		t.Fatalf("handler invoked %d times after controller-audience 401, want 1", calls)
	}

	// Wrong audience: same controller SA, valid token shape, but an arbitrary
	// JWT audience still does NOT match the middleware's
	// TokenReviewSpec.Audiences. This keeps the generic mismatch regression
	// alongside the cross-endpoint audience check above.
	r.BearerTokenPath = wrongAudienceTokenFile
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err == nil {
		t.Fatal("Reconcile with wrong-audience controller-SA token: expected non-nil error (401), got nil")
	}
	if calls != 1 {
		t.Fatalf("handler invoked %d times after wrong-audience 401, want 1", calls)
	}

	// Recovery: re-pointing at the controller token converges again — proves
	// the auth path is not sticky-broken after a rejection tick.
	r.BearerTokenPath = policyTokenFile
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile after re-pointing at controller SA token: %v", err)
	}
	if calls != 2 {
		t.Fatalf("handler invoked %d times after recovery, want 2", calls)
	}
}

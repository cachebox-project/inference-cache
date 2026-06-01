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
	// listener does. The /policy handler we mount behind it is the EXACT same
	// HTTP handler the production server uses (via NewPolicyHTTPHandler),
	// guarding against the auth wrap accidentally allowing a different decode
	// shape than the production path.
	authn, err := auth.NewAuthenticator(auth.Options{
		Reviewer:               auth.FromClientset(clientset),
		ExpectedServiceAccount: expectedSA,
	})
	if err != nil {
		t.Fatalf("auth.NewAuthenticator: %v", err)
	}
	calls := 0
	mux := http.NewServeMux()
	mux.Handle("/policy", authn.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusNoContent)
	})))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Stage SA tokens via TokenRequest, write each to a tmpfile in the
	// kubelet shape (trailing newline, mode 0o600). bearerToken() trims the
	// newline before sending.
	mintTokenFile := func(saName string) string {
		t.Helper()
		exp := int64(3600)
		tr, err := clientset.CoreV1().ServiceAccounts(ns).CreateToken(ctx, saName, &authnv1.TokenRequest{
			Spec: authnv1.TokenRequestSpec{ExpirationSeconds: &exp},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("CreateToken(%s): %v", saName, err)
		}
		path := filepath.Join(t.TempDir(), saName+"-token")
		if err := os.WriteFile(path, []byte(tr.Status.Token+"\n"), 0o600); err != nil {
			t.Fatalf("write token file: %v", err)
		}
		return path
	}
	controllerTokenFile := mintTokenFile(controllerSA)
	otherTokenFile := mintTokenFile(otherSA)
	emptyTokenFile := filepath.Join(t.TempDir(), "empty-token")
	if err := os.WriteFile(emptyTokenFile, []byte(""), 0o600); err != nil {
		t.Fatalf("write empty token file: %v", err)
	}

	r := &ControlPlaneReconciler{
		Client:          k8s,
		ServerPolicyURL: srv.URL + "/policy",
		HTTPClient:      srv.Client(),
		BearerTokenPath: controllerTokenFile,
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

	// Recovery: re-pointing at the controller token converges again — proves
	// the auth path is not sticky-broken after a rejection tick.
	r.BearerTokenPath = controllerTokenFile
	if _, err := r.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile after re-pointing at controller SA token: %v", err)
	}
	if calls != 2 {
		t.Fatalf("handler invoked %d times after recovery, want 2", calls)
	}
}

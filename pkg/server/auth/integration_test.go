package auth_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/cachebox-project/inference-cache/pkg/server/auth"
)

// TestAuthMiddleware_AgainstEnvtestAPIServer exercises the middleware against
// a real apiserver: it mints a TokenRequest for a fresh ServiceAccount, sends
// the bound token on the request, and verifies the TokenReview round-trip
// admits the controller's SA and rejects everyone else.
//
// Skipped unless KUBEBUILDER_ASSETS is set — CI's envtest setup turns it on;
// locally run with `KUBEBUILDER_ASSETS=$(make test-env | tail -1) go test ./...`.
func TestAuthMiddleware_AgainstEnvtestAPIServer(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; run with `KUBEBUILDER_ASSETS=$(make test-env | tail -1) go test`")
	}
	logf.SetLogger(zap.New(zap.UseDevMode(true), zap.WriteTo(os.Stderr)))

	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Logf("stop envtest: %v", err)
		}
	})

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("kubernetes.NewForConfig: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const ns = "default"
	const wantSA = "test-controller"
	const otherSAName = "test-other"

	for _, name := range []string{wantSA, otherSAName} {
		if _, err := clientset.CoreV1().ServiceAccounts(ns).Create(ctx, &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create SA %q: %v", name, err)
		}
	}

	wantUsername := "system:serviceaccount:" + ns + ":" + wantSA

	a, err := auth.NewAuthenticator(auth.Options{
		Reviewer:               auth.FromClientset(clientset),
		ExpectedServiceAccount: wantUsername,
	})
	if err != nil {
		t.Fatalf("NewAuthenticator: %v", err)
	}
	handler := a.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("snapshot"))
	}))
	srv := httptest.NewServer(handler)
	defer srv.Close()

	mintToken := func(saName string) string {
		t.Helper()
		tr, err := clientset.CoreV1().ServiceAccounts(ns).CreateToken(ctx, saName, &authnv1.TokenRequest{
			Spec: authnv1.TokenRequestSpec{ExpirationSeconds: ptrInt64(3600)},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("CreateToken(%s): %v", saName, err)
		}
		return tr.Status.Token
	}

	do := func(authHeader string) int {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		if authHeader != "" {
			req.Header.Set("Authorization", authHeader)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}

	t.Run("missing bearer -> 401", func(t *testing.T) {
		if code := do(""); code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", code)
		}
	})

	t.Run("bogus bearer -> 401", func(t *testing.T) {
		if code := do("Bearer not-a-real-token"); code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want 401", code)
		}
	})

	t.Run("wrong SA -> 403", func(t *testing.T) {
		if code := do("Bearer " + mintToken(otherSAName)); code != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", code)
		}
	})

	t.Run("controller SA -> 200", func(t *testing.T) {
		if code := do("Bearer " + mintToken(wantSA)); code != http.StatusOK {
			t.Fatalf("status = %d, want 200", code)
		}
	})
}

func ptrInt64(v int64) *int64 { return &v }

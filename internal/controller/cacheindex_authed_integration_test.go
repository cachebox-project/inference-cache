package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/index"
	"github.com/cachebox-project/inference-cache/pkg/server/auth"
)

// TestIntegrationCacheIndexPollerAgainstAuthedSnapshot drives the full
// client-to-server auth path against a real apiserver:
//
//	CacheIndexPoller.refresh
//	  -> bearerToken() reads the SA token from a tmpfile (kubelet-shape)
//	  -> fetchSnapshot() sends Authorization: Bearer <token>
//	  -> in-process httptest server wrapped in pkg/server/auth.Middleware
//	  -> Authenticator calls TokenReview against the envtest apiserver
//	  -> apiserver validates the token it minted via TokenRequest
//	  -> handler returns a synthetic index.Snapshot
//	  -> poller decodes it and writes CacheIndex.status against envtest
//
// pkg/server/auth/integration_test.go already covers the middleware in
// isolation with raw http requests. This test stitches the production
// CLIENT code (the poller) onto the same backend, which is the surface
// the bearer-token rollout actually changes for downstream callers.
//
// Skipped unless KUBEBUILDER_ASSETS is set — matches the rest of the
// envtest suite. CI installs envtest before `make test-race`, so the test
// runs there; locally:
//
//	KUBEBUILDER_ASSETS=$(make test-env | tail -1) go test ./...
func TestIntegrationCacheIndexPollerAgainstAuthedSnapshot(t *testing.T) {
	skipWithoutEnvtest(t)

	k8s, _, cfg := startEnv(t)

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("kubernetes.NewForConfig: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Two SAs — one in the controller role this poller authenticates as,
	// one playing the "any other in-namespace identity" attacker shape.
	const ns = "default"
	const controllerSA = "ic-controller"
	const otherSA = "ic-other"
	for _, name := range []string{controllerSA, otherSA} {
		if _, err := clientset.CoreV1().ServiceAccounts(ns).Create(ctx, &corev1.ServiceAccount{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		}, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create SA %q: %v", name, err)
		}
	}
	expectedSA := "system:serviceaccount:" + ns + ":" + controllerSA

	// Wire the authenticator the same way cmd/server's snapshot listener
	// does, then mount /snapshot behind it on an httptest server. The
	// served snapshot is non-trivial so the happy-path assertion can pin
	// shape (not just status code).
	authn, err := auth.NewAuthenticator(auth.Options{
		Reviewer:               auth.FromClientset(clientset),
		ExpectedServiceAccount: expectedSA,
		Audience:               auth.ControllerAudience,
	})
	if err != nil {
		t.Fatalf("auth.NewAuthenticator: %v", err)
	}
	// LastUpdate must be non-zero or buildCacheIndexStatus skips the replica
	// (the controller treats prefix-only replicas with no stats reported as
	// hidden from the cluster-wide CacheIndex.status surface — see the
	// per-backend CacheBackend.status.indexParticipation path for those).
	served := index.Snapshot{
		TotalPrefixes: 7,
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "r1", CacheMemoryBytes: 200, HitRate: 0.75, LastUpdate: time.Now()},
		},
	}
	mux := http.NewServeMux()
	mux.Handle("/snapshot", authn.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(served)
	})))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Mint SA tokens via TokenRequest and stage them on disk the way
	// kubelet projects them — trailing newline, mode 0o600. The poller's
	// bearerToken() trims the newline before sending. mintTokenFile binds
	// the snapshot audience to mirror the production projected-volume
	// shape (kubelet bakes audience into the JWT, apiserver enforces it
	// at TokenReview); mintTokenFileWithAudience supports the wrong-
	// audience negative case below.
	mintTokenFileWithAudience := func(saName, audience string) string {
		t.Helper()
		exp := int64(3600)
		tr, err := clientset.CoreV1().ServiceAccounts(ns).CreateToken(ctx, saName, &authnv1.TokenRequest{
			Spec: authnv1.TokenRequestSpec{
				Audiences:         []string{audience},
				ExpirationSeconds: &exp,
			},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("CreateToken(%s, audience=%s): %v", saName, audience, err)
		}
		path := filepath.Join(t.TempDir(), saName+"-token")
		if err := os.WriteFile(path, []byte(tr.Status.Token+"\n"), 0o600); err != nil {
			t.Fatalf("write token file: %v", err)
		}
		return path
	}
	mintTokenFile := func(saName string) string {
		return mintTokenFileWithAudience(saName, auth.ControllerAudience)
	}
	controllerTokenFile := mintTokenFile(controllerSA)
	otherTokenFile := mintTokenFile(otherSA)
	emptyTokenFile := filepath.Join(t.TempDir(), "empty-token")
	if err := os.WriteFile(emptyTokenFile, []byte(""), 0o600); err != nil {
		t.Fatalf("write empty token file: %v", err)
	}

	// Use a singleton name distinct from cluster-default so this test does
	// not collide with anything an in-cluster controller would touch.
	const ciName = "cluster-default-auth-it"
	poller := &CacheIndexPoller{
		Client:          k8s,
		SnapshotURL:     srv.URL + "/snapshot",
		BearerTokenPath: controllerTokenFile,
		HTTPClient:      srv.Client(),
		Name:            ciName,
	}

	// Happy path: controller SA token → middleware admits → handler returns
	// the snapshot → poller decodes and writes CacheIndex.status.
	if err := poller.refresh(ctx); err != nil {
		t.Fatalf("refresh with controller SA token: %v", err)
	}
	var ci cachev1alpha1.CacheIndex
	if err := k8s.Get(ctx, types.NamespacedName{Name: ciName}, &ci); err != nil {
		t.Fatalf("get CacheIndex after happy-path refresh: %v", err)
	}
	if ci.Status.Prefixes.Summary.Total != 7 {
		t.Fatalf("status.prefixes.summary.total = %d, want 7", ci.Status.Prefixes.Summary.Total)
	}
	if len(ci.Status.Replicas) != 1 || ci.Status.Replicas[0].ID != "r1" {
		t.Fatalf("status.replicas = %+v, want one r1", ci.Status.Replicas)
	}
	happyRV := ci.ResourceVersion

	// Wrong-SA: a TokenRequest for a different SA, sent on the same wire,
	// still passes TokenReview (the token is valid) but is admitted as the
	// wrong username — middleware returns 403, fetchSnapshot surfaces a
	// non-nil error. Critical fail-soft check: the previously written
	// status must NOT be cleared / clobbered.
	poller.BearerTokenPath = otherTokenFile
	if err := poller.refresh(ctx); err == nil {
		t.Fatal("refresh with other SA token: expected non-nil error (403), got nil")
	}
	if err := k8s.Get(ctx, types.NamespacedName{Name: ciName}, &ci); err != nil {
		t.Fatalf("get CacheIndex after 403 refresh: %v", err)
	}
	if ci.Status.Prefixes.Summary.Total != 7 {
		t.Fatalf("403 clobbered status; total = %d, want preserved 7", ci.Status.Prefixes.Summary.Total)
	}
	if ci.ResourceVersion != happyRV {
		t.Fatalf("403 path wrote a new status revision: rv %s -> %s", happyRV, ci.ResourceVersion)
	}

	// Wrong-audience: a TokenRequest minted for the CORRECT SA but with a
	// different audience. The apiserver bakes audience into the JWT and
	// rejects it under TokenReview.Audiences=[snapshot], so the middleware
	// returns 401 even though the SA identity would otherwise be admitted.
	// This is the over-the-wire complement to the in-process middleware
	// envtest in pkg/server/auth and pins the same audience-binding contract
	// against the controller's actual poller code path. Fail-soft expectation
	// matches the wrong-SA / no-token branches above.
	wrongAudienceTokenFile := mintTokenFileWithAudience(controllerSA, "https://kubernetes.default.svc")
	poller.BearerTokenPath = wrongAudienceTokenFile
	if err := poller.refresh(ctx); err == nil {
		t.Fatal("refresh with wrong-audience token: expected non-nil error (401), got nil")
	}
	if err := k8s.Get(ctx, types.NamespacedName{Name: ciName}, &ci); err != nil {
		t.Fatalf("get CacheIndex after wrong-audience 401 refresh: %v", err)
	}
	if ci.Status.Prefixes.Summary.Total != 7 || ci.ResourceVersion != happyRV {
		t.Fatalf("wrong-audience 401 mutated status; total=%d (want 7), rv=%s (want %s)",
			ci.Status.Prefixes.Summary.Total, ci.ResourceVersion, happyRV)
	}

	// No-token: an empty file → bearerToken() returns ("", nil), so
	// fetchSnapshot sends no Authorization header — middleware returns
	// 401. Same fail-soft expectation: prior status preserved.
	poller.BearerTokenPath = emptyTokenFile
	if err := poller.refresh(ctx); err == nil {
		t.Fatal("refresh with empty token file: expected non-nil error (401), got nil")
	}
	if err := k8s.Get(ctx, types.NamespacedName{Name: ciName}, &ci); err != nil {
		t.Fatalf("get CacheIndex after 401 refresh: %v", err)
	}
	if ci.Status.Prefixes.Summary.Total != 7 || ci.ResourceVersion != happyRV {
		t.Fatalf("401 mutated status; total=%d (want 7), rv=%s (want %s)",
			ci.Status.Prefixes.Summary.Total, ci.ResourceVersion, happyRV)
	}

	// Returning to a valid token after the rejections must converge again:
	// proves the auth path is not sticky-broken after an error tick, which
	// matches the poller's per-tick fail-soft loop.
	poller.BearerTokenPath = controllerTokenFile
	if err := poller.refresh(ctx); err != nil {
		t.Fatalf("refresh after re-pointing at controller SA token: %v", err)
	}
	if err := k8s.Get(ctx, types.NamespacedName{Name: ciName}, &ci); err != nil {
		t.Fatalf("get CacheIndex after recovery refresh: %v", err)
	}
	if ci.Status.Prefixes.Summary.Total != 7 {
		t.Fatalf("recovery refresh wrote unexpected status: total = %d, want 7", ci.Status.Prefixes.Summary.Total)
	}
}

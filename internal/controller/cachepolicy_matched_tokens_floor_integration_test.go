package controller

import (
	"context"
	"net/http/httptest"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	cacheserver "github.com/cachebox-project/inference-cache/pkg/server"
)

// TestIntegrationCachePolicyMinimumMatchedTokensFloor pins the wiring
// end-to-end against a real apiserver: the CRD's MinimumMatchedTokens spec
// flattens through the controller into ResolvedPolicy.MinimumMatchedTokens,
// reaches the PolicyStore via /policy push, and the server-side resolver
// reports the correct effective floor for three operator-meaningful shapes:
//
//   - A namespace whose CachePolicy explicitly sets MinimumMatchedTokens=256
//     reports 256 (the policy override wins over the kubebuilder default).
//   - A namespace whose CachePolicy explicitly disables the floor with
//     MinimumMatchedTokens=0 reports 0 (the opt-out reaches the server).
//   - A namespace with NO CachePolicy reports DefaultMinimumMatchedTokens
//     (the server-wide fallback fires for unconfigured tenants).
//
// This test complements the pkg/server unit tests by exercising the real
// apiserver-side kubebuilder defaulting AND the controller→server propagation
// path together — the C2 reconcile hot-loop class of bug (envtest exposed it
// in the cap-eviction work and motivates this entire integration tier).
func TestIntegrationCachePolicyMinimumMatchedTokensFloor(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, _, _ := startEnv(t)
	ctx := context.Background()

	store := cacheserver.NewPolicyStore()
	policySrv := httptest.NewServer(cacheserver.NewPolicyHTTPHandler(store))
	defer policySrv.Close()
	reconciler := &ControlPlaneReconciler{Client: k8s, ServerPolicyURL: policySrv.URL, HTTPClient: policySrv.Client()}

	nsExplicit := freshNS(t, k8s)
	nsDisabled := freshNS(t, k8s)
	nsUnconfigured := freshNS(t, k8s)

	createCP := func(ns string, floor int32) {
		t.Helper()
		v := floor
		cp := &cachev1alpha1.CachePolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "policy", Namespace: ns},
			Spec:       cachev1alpha1.CachePolicySpec{MinimumMatchedTokens: &v},
		}
		if err := k8s.Create(ctx, cp); err != nil {
			t.Fatalf("create CachePolicy(%s, floor=%d): %v", ns, floor, err)
		}
	}
	createCP(nsExplicit, 256)
	createCP(nsDisabled, 0)
	// nsUnconfigured deliberately gets no CachePolicy — exercises the
	// server-wide DefaultMinimumMatchedTokens fallback for the common case
	// of a tenant without an installed CachePolicy.

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// The controller flattened both populated CRs into ResolvedPolicy entries
	// and pushed them; the PolicyStore the server reads through is now hydrated.
	if got := store.MinimumMatchedTokens(nsExplicit); got != 256 {
		t.Fatalf("explicit-floor namespace = %d, want 256 (policy override)", got)
	}
	if got := store.MinimumMatchedTokens(nsDisabled); got != 0 {
		t.Fatalf("disabled-floor namespace = %d, want 0 (explicit opt-out)", got)
	}
	if got := store.MinimumMatchedTokens(nsUnconfigured); got != cacheserver.DefaultMinimumMatchedTokens {
		t.Fatalf("unconfigured namespace = %d, want DefaultMinimumMatchedTokens (%d) — server-wide fallback failed",
			got, cacheserver.DefaultMinimumMatchedTokens)
	}
}

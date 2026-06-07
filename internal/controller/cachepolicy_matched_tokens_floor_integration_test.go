package controller

import (
	"context"
	"net/http/httptest"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	cacheserver "github.com/cachebox-project/inference-cache/pkg/server"
)

// TestIntegrationCachePolicyMinimumMatchedTokensFloor pins the wiring
// end-to-end against a real apiserver: the CRD's MinimumMatchedTokens spec
// flattens through the controller into ResolvedPolicy.MinimumMatchedTokens,
// reaches the PolicyStore via /policy push, and the server-side resolver
// reports the correct effective floor for FOUR operator-meaningful shapes:
//
//   - A namespace whose CachePolicy explicitly sets MinimumMatchedTokens=256
//     reports 256 (the policy override wins over the kubebuilder default).
//   - A namespace whose CachePolicy OMITS MinimumMatchedTokens entirely
//     reports 64 — the apiserver applies the kubebuilder default before the
//     controller flattens, so an operator who writes a "bare" CachePolicy
//     (typically just evictionTTL or LFU) still gets the safety floor. This
//     is the CRITICAL path: the canonical operator-facing case (a
//     hand-written CR that doesn't know about this field yet).
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
	nsOmitted := freshNS(t, k8s)
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

	// nsOmitted exercises kubebuilder defaulting: the CR carries no
	// MinimumMatchedTokens field; the apiserver fills in the marker default
	// (64) BEFORE the controller flattens, so the resolved policy must carry
	// 64 without the operator ever having typed the field. This is the
	// production-canonical case — most operators write a bare CachePolicy
	// with only evictionTTL or eviction set.
	bare := &cachev1alpha1.CachePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "policy", Namespace: nsOmitted},
		// Deliberately empty spec — no MinimumMatchedTokens pointer at all.
		Spec: cachev1alpha1.CachePolicySpec{},
	}
	if err := k8s.Create(ctx, bare); err != nil {
		t.Fatalf("create bare CachePolicy(%s): %v", nsOmitted, err)
	}

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
	if got := store.MinimumMatchedTokens(nsOmitted); got != cacheserver.DefaultMinimumMatchedTokens {
		t.Fatalf("omitted-field namespace = %d, want DefaultMinimumMatchedTokens (%d) — "+
			"kubebuilder default did not fill in 64 at apiserver admission, OR the controller "+
			"didn't flatten the apiserver-defaulted value",
			got, cacheserver.DefaultMinimumMatchedTokens)
	}
	if got := store.MinimumMatchedTokens(nsDisabled); got != 0 {
		t.Fatalf("disabled-floor namespace = %d, want 0 (explicit opt-out)", got)
	}
	if got := store.MinimumMatchedTokens(nsUnconfigured); got != cacheserver.DefaultMinimumMatchedTokens {
		t.Fatalf("unconfigured namespace = %d, want DefaultMinimumMatchedTokens (%d) — server-wide fallback failed",
			got, cacheserver.DefaultMinimumMatchedTokens)
	}

	// Belt-and-braces: read the omitted-field CR back from the apiserver and
	// confirm the kubebuilder default literally materialized on the object —
	// not just that the controller-side resolver agreed by coincidence (e.g.
	// nil pointer flattened to a zero ResolvedPolicy, which happens to coincide
	// with the server-side fallback). If this assertion drifts from the
	// resolver assertion above, one of them is masking a real bug.
	var readback cachev1alpha1.CachePolicy
	if err := k8s.Get(ctx, client.ObjectKey{Name: "policy", Namespace: nsOmitted}, &readback); err != nil {
		t.Fatalf("get bare CachePolicy(%s) back: %v", nsOmitted, err)
	}
	if readback.Spec.MinimumMatchedTokens == nil {
		t.Fatalf("bare CachePolicy(%s).spec.minimumMatchedTokens is nil after apiserver round-trip — "+
			"the +kubebuilder:default=64 marker did not materialize on the stored object", nsOmitted)
	}
	if got := *readback.Spec.MinimumMatchedTokens; got != cacheserver.DefaultMinimumMatchedTokens {
		t.Fatalf("bare CachePolicy(%s).spec.minimumMatchedTokens = %d after apiserver round-trip, want %d "+
			"(the kubebuilder default)", nsOmitted, got, cacheserver.DefaultMinimumMatchedTokens)
	}
}

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

// TestIntegrationCachePolicyAffinityRouting exercises the full
// CachePolicy.spec.affinityRouting propagation loop against a real
// apiserver: the controller flattens the CRD's Enabled/Disabled enum to
// a *bool, pushes the snapshot, and the server-side PolicyStore.
// AffinityRoutingEnabled resolver reports the correct effective state
// for the four operator-meaningful shapes:
//
//   - A namespace whose CachePolicy explicitly sets affinityRouting=Enabled
//     reports true.
//   - A namespace whose CachePolicy OMITS the field reports true via the
//     kubebuilder default Enabled — the apiserver materializes the default
//     at admission BEFORE the controller flattens, so an operator who writes
//     a "bare" CachePolicy (typically just evictionTTL) still opts in. This
//     is the CRITICAL path: the canonical operator-facing case.
//   - A namespace whose CachePolicy explicitly disables with
//     affinityRouting=Disabled reports false (the opt-out reaches the server
//     untouched).
//   - A namespace with NO CachePolicy reports DefaultAffinityRoutingEnabled
//     (the server-wide default fires for unconfigured tenants).
//
// Complements the pkg/server unit tests by exercising the real
// apiserver-side kubebuilder defaulting AND the controller→server
// propagation path together.
func TestIntegrationCachePolicyAffinityRouting(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, _, _ := startEnv(t)
	ctx := context.Background()

	store := cacheserver.NewPolicyStore()
	policySrv := httptest.NewServer(cacheserver.NewPolicyHTTPHandler(store))
	defer policySrv.Close()
	reconciler := &ControlPlaneReconciler{Client: k8s, ServerPolicyURL: policySrv.URL, HTTPClient: policySrv.Client()}

	nsEnabled := freshNS(t, k8s)
	nsOmitted := freshNS(t, k8s)
	nsDisabled := freshNS(t, k8s)
	nsUnconfigured := freshNS(t, k8s)

	enabled := cachev1alpha1.CachePolicyAffinityRoutingEnabled
	disabled := cachev1alpha1.CachePolicyAffinityRoutingDisabled

	createCP := func(ns string, ar *cachev1alpha1.CachePolicyAffinityRouting) {
		t.Helper()
		cp := &cachev1alpha1.CachePolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "policy", Namespace: ns},
			Spec:       cachev1alpha1.CachePolicySpec{AffinityRouting: ar},
		}
		if err := k8s.Create(ctx, cp); err != nil {
			t.Fatalf("create CachePolicy(%s, affinity=%v): %v", ns, ar, err)
		}
	}
	createCP(nsEnabled, &enabled)
	createCP(nsDisabled, &disabled)
	// nsOmitted: bare spec, no AffinityRouting pointer — exercises the
	// kubebuilder default materializing at apiserver admission.
	createCP(nsOmitted, nil)
	// nsUnconfigured: deliberately no CachePolicy.

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if got := store.AffinityRoutingEnabled(nsEnabled); got != true {
		t.Fatalf("explicit-Enabled namespace = %v, want true", got)
	}
	if got := store.AffinityRoutingEnabled(nsOmitted); got != true {
		t.Fatalf("omitted-field namespace = %v, want true — kubebuilder default did not fill Enabled at apiserver admission, OR the controller didn't flatten the apiserver-defaulted value", got)
	}
	if got := store.AffinityRoutingEnabled(nsDisabled); got != false {
		t.Fatalf("explicit-Disabled namespace = %v, want false (operator opt-out)", got)
	}
	if got := store.AffinityRoutingEnabled(nsUnconfigured); got != cacheserver.DefaultAffinityRoutingEnabled {
		t.Fatalf("unconfigured namespace = %v, want DefaultAffinityRoutingEnabled (%v) — server-wide fallback failed",
			got, cacheserver.DefaultAffinityRoutingEnabled)
	}

	// Belt-and-braces: read the omitted-field CR back from the apiserver and
	// confirm the kubebuilder default literally materialized on the object
	// — not just that the controller-side resolver agreed by coincidence
	// (e.g. nil pointer flattened to a nil *bool, which happens to coincide
	// with the server-side default-true fallback). If this assertion drifts
	// from the resolver assertion above, one of them is masking a real bug.
	var readback cachev1alpha1.CachePolicy
	if err := k8s.Get(ctx, client.ObjectKey{Name: "policy", Namespace: nsOmitted}, &readback); err != nil {
		t.Fatalf("get bare CachePolicy(%s) back: %v", nsOmitted, err)
	}
	if readback.Spec.AffinityRouting == nil {
		t.Fatalf("bare CachePolicy(%s).spec.affinityRouting is nil after apiserver round-trip — the +kubebuilder:default=Enabled marker did not materialize on the stored object", nsOmitted)
	}
	if got := *readback.Spec.AffinityRouting; got != enabled {
		t.Fatalf("bare CachePolicy(%s).spec.affinityRouting = %q after apiserver round-trip, want %q", nsOmitted, got, enabled)
	}
}

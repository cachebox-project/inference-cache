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

// TestIntegrationCachePolicyRoutingFloorScore exercises the full
// CachePolicy.spec.routingFloorScore propagation loop against a real
// apiserver: the controller flattens the CRD's stringified-float field
// into ResolvedPolicy.RoutingFloorScore, pushes the snapshot, and the
// server-side resolver reports the correct effective floor for the four
// operator-meaningful shapes:
//
//   - A namespace whose CachePolicy explicitly sets RoutingFloorScore="5"
//     reports 5.0 (the policy override wins).
//   - A namespace whose CachePolicy OMITS the field entirely reports the
//     kubebuilder default "0.1" → 0.1 — the apiserver materializes the
//     default at admission before the controller flattens, so an operator
//     who writes a "bare" CachePolicy (typically just evictionTTL or LFU)
//     still gets the safety floor. This is the CRITICAL path: the
//     canonical operator-facing case (a hand-written CR that does not know
//     about this field yet).
//   - A namespace whose CachePolicy explicitly disables the floor with
//     RoutingFloorScore="0" reports 0 (the opt-out reaches the server
//     untouched, not silently clamped to the default).
//   - A namespace with NO CachePolicy reports DefaultRoutingFloorScore
//     (the server-wide safety floor fires for unconfigured tenants).
//
// Complements the pkg/server unit tests by exercising the real
// apiserver-side kubebuilder defaulting AND the controller→server
// propagation path together — the C2 reconcile hot-loop class of bug
// (envtest exposed it in the cap-eviction work).
func TestIntegrationCachePolicyRoutingFloorScore(t *testing.T) {
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

	createCP := func(ns, floor string) {
		t.Helper()
		v := floor
		cp := &cachev1alpha1.CachePolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "policy", Namespace: ns},
			Spec:       cachev1alpha1.CachePolicySpec{RoutingFloorScore: &v},
		}
		if err := k8s.Create(ctx, cp); err != nil {
			t.Fatalf("create CachePolicy(%s, floor=%s): %v", ns, floor, err)
		}
	}
	createCP(nsExplicit, "5")
	createCP(nsDisabled, "0")

	// nsOmitted exercises kubebuilder defaulting: the CR carries no
	// RoutingFloorScore at all; the apiserver fills in the marker default
	// ("0.1") BEFORE the controller flattens, so the resolved policy must
	// carry 0.1 without the operator ever having typed the field. This is
	// the production-canonical case — most operators write a bare
	// CachePolicy with only evictionTTL / eviction set.
	bare := &cachev1alpha1.CachePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "policy", Namespace: nsOmitted},
		// Deliberately empty spec — no RoutingFloorScore pointer at all.
		Spec: cachev1alpha1.CachePolicySpec{},
	}
	if err := k8s.Create(ctx, bare); err != nil {
		t.Fatalf("create bare CachePolicy(%s): %v", nsOmitted, err)
	}

	// nsUnconfigured deliberately gets no CachePolicy — exercises the
	// server-wide DefaultRoutingFloorScore fallback for the common case
	// of a tenant without an installed CachePolicy.

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	const tol = 1e-3
	approx := func(a, b float32) bool { d := a - b; return d > -tol && d < tol }

	if got := store.RoutingFloorScore(nsExplicit); !approx(got, 5.0) {
		t.Fatalf("explicit-floor namespace = %v, want 5.0 (policy override)", got)
	}
	if got := store.RoutingFloorScore(nsOmitted); !approx(got, cacheserver.DefaultRoutingFloorScore) {
		t.Fatalf("omitted-field namespace = %v, want DefaultRoutingFloorScore (%v) — "+
			"kubebuilder default did not fill in 0.1 at apiserver admission, OR the controller "+
			"didn't flatten the apiserver-defaulted value",
			got, cacheserver.DefaultRoutingFloorScore)
	}
	if got := store.RoutingFloorScore(nsDisabled); got != 0 {
		t.Fatalf("disabled-floor namespace = %v, want 0 (explicit opt-out)", got)
	}
	if got := store.RoutingFloorScore(nsUnconfigured); !approx(got, cacheserver.DefaultRoutingFloorScore) {
		t.Fatalf("unconfigured namespace = %v, want DefaultRoutingFloorScore (%v) — server-wide fallback failed",
			got, cacheserver.DefaultRoutingFloorScore)
	}

	// Belt-and-braces: read the omitted-field CR back from the apiserver and
	// confirm the kubebuilder default literally materialized on the object
	// — not just that the controller-side resolver agreed by coincidence
	// (e.g. nil pointer flattened to a zero ResolvedPolicy, which happens
	// to coincide with the server-side fallback). If this assertion drifts
	// from the resolver assertion above, one of them is masking a real bug.
	var readback cachev1alpha1.CachePolicy
	if err := k8s.Get(ctx, client.ObjectKey{Name: "policy", Namespace: nsOmitted}, &readback); err != nil {
		t.Fatalf("get bare CachePolicy(%s) back: %v", nsOmitted, err)
	}
	if readback.Spec.RoutingFloorScore == nil {
		t.Fatalf("bare CachePolicy(%s).spec.routingFloorScore is nil after apiserver round-trip — "+
			"the +kubebuilder:default=\"0.1\" marker did not materialize on the stored object", nsOmitted)
	}
	if got := *readback.Spec.RoutingFloorScore; got != "0.1" {
		t.Fatalf("bare CachePolicy(%s).spec.routingFloorScore = %q after apiserver round-trip, want \"0.1\"", nsOmitted, got)
	}
}

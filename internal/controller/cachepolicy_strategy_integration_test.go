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

func TestIntegrationCachePolicyStrategyGates(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, _, _ := startEnv(t)
	ctx := context.Background()

	store := cacheserver.NewPolicyStore()
	policySrv := httptest.NewServer(cacheserver.NewPolicyHTTPHandler(store))
	defer policySrv.Close()
	reconciler := &ControlPlaneReconciler{Client: k8s, ServerPolicyURL: policySrv.URL, HTTPClient: policySrv.Client()}

	nsExplicit := freshNS(t, k8s)
	nsDefaulted := freshNS(t, k8s)
	nsUnconfigured := freshNS(t, k8s)

	enableChain := true
	requireChain := true
	enableTenantHot := false
	explicit := &cachev1alpha1.CachePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "policy", Namespace: nsExplicit},
		Spec: cachev1alpha1.CachePolicySpec{
			Strategy: &cachev1alpha1.CachePolicyStrategySpec{
				EnableChainMatching: &enableChain,
				RequireChain:        &requireChain,
				EnableTenantHot:     &enableTenantHot,
			},
		},
	}
	if err := k8s.Create(ctx, explicit); err != nil {
		t.Fatalf("create explicit strategy CachePolicy(%s): %v", nsExplicit, err)
	}

	bare := &cachev1alpha1.CachePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "policy", Namespace: nsDefaulted},
		Spec:       cachev1alpha1.CachePolicySpec{Strategy: &cachev1alpha1.CachePolicyStrategySpec{}},
	}
	if err := k8s.Create(ctx, bare); err != nil {
		t.Fatalf("create defaulted strategy CachePolicy(%s): %v", nsDefaulted, err)
	}

	if _, err := reconciler.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if !store.ChainMatchingEnabled(nsExplicit) || !store.ChainRequired(nsExplicit) || store.TenantHotEnabled(nsExplicit) {
		t.Fatalf("explicit strategy = chain=%v require=%v tenantHot=%v, want true/true/false",
			store.ChainMatchingEnabled(nsExplicit), store.ChainRequired(nsExplicit), store.TenantHotEnabled(nsExplicit))
	}
	if !store.ChainMatchingEnabled(nsDefaulted) || store.ChainRequired(nsDefaulted) || !store.TenantHotEnabled(nsDefaulted) {
		t.Fatalf("defaulted strategy = chain=%v require=%v tenantHot=%v, want true/false/true",
			store.ChainMatchingEnabled(nsDefaulted), store.ChainRequired(nsDefaulted), store.TenantHotEnabled(nsDefaulted))
	}
	if !store.ChainMatchingEnabled(nsUnconfigured) || store.ChainRequired(nsUnconfigured) || !store.TenantHotEnabled(nsUnconfigured) {
		t.Fatalf("unconfigured strategy = chain=%v require=%v tenantHot=%v, want true/false/true",
			store.ChainMatchingEnabled(nsUnconfigured), store.ChainRequired(nsUnconfigured), store.TenantHotEnabled(nsUnconfigured))
	}
}

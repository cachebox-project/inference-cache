package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/index"
	cacheserver "github.com/cachebox-project/inference-cache/pkg/server"
)

// TestIntegrationCacheTenantQuota exercises the full CacheTenant quota loop
// against a real apiserver, wired with the real components via their exported
// APIs:
//
//   - a real PolicyStore + index.Index (the index's quota resolver IS the store,
//     exactly as pkg/server.New wires it);
//   - the real /policy push handler and a /snapshot handler over the index;
//   - the real ControlPlaneReconciler (CRD → push) and CacheIndexPoller (snapshot
//     → CacheTenant.status).
//
// That covers what the fake client can't: real CRD validation/defaulting and
// real Status().Patch semantics on CacheTenant.
func TestIntegrationCacheTenantQuota(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, _, _ := startEnv(t)
	ctx := context.Background()

	store := cacheserver.NewPolicyStore()
	idx := index.New(index.WithTenantQuotaResolver(store))

	policySrv := httptest.NewServer(cacheserver.NewPolicyHTTPHandler(store))
	defer policySrv.Close()
	snapSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(idx.Snapshot())
	}))
	defer snapSrv.Close()

	reconciler := &ControlPlaneReconciler{Client: k8s, ServerPolicyURL: policySrv.URL, HTTPClient: policySrv.Client()}
	poller := &CacheIndexPoller{Client: k8s, SnapshotURL: snapSrv.URL, HTTPClient: snapSrv.Client(), Log: logr.Discard()}
	push := func() {
		t.Helper()
		if _, err := reconciler.Reconcile(ctx, ctrl.Request{}); err != nil {
			t.Fatalf("push: %v", err)
		}
	}
	scrape := func() {
		t.Helper()
		if err := poller.reconcileTenantStatuses(ctx, idx.Snapshot()); err != nil {
			t.Fatalf("scrape: %v", err)
		}
	}

	ns := freshNS(t, k8s)
	const tenantID = "team-vision"
	ct := &cachev1alpha1.CacheTenant{
		ObjectMeta: metav1.ObjectMeta{Name: "vision", Namespace: ns},
		Spec: cachev1alpha1.CacheTenantSpec{
			TenantID: tenantID,
			Quota:    &cachev1alpha1.CacheTenantQuotaSpec{MaxIndexEntries: i64IntegrationPtr(10)},
		},
	}
	if err := k8s.Create(ctx, ct); err != nil {
		t.Fatalf("create CacheTenant: %v", err)
	}
	getCT := func() cachev1alpha1.CacheTenant {
		t.Helper()
		var out cachev1alpha1.CacheTenant
		if err := k8s.Get(ctx, types.NamespacedName{Name: "vision", Namespace: ns}, &out); err != nil {
			t.Fatalf("get CacheTenant: %v", err)
		}
		return out
	}
	condStatus := func(ct cachev1alpha1.CacheTenant, condType string) metav1.ConditionStatus {
		for _, c := range ct.Status.Conditions {
			if c.Type == condType {
				return c.Status
			}
		}
		return ""
	}

	// Push the quota to the server, then ingest 5 prefixes (under the budget of 10).
	push()
	if _, ok := store.TenantQuota(tenantID); !ok {
		t.Fatal("quota should have been pushed to the store")
	}
	ingestTenantPrefixes(idx, tenantID, 0, 5)
	if got := snapshotEntries(idx, tenantID); got != 5 {
		t.Fatalf("entries under budget = %d, want 5", got)
	}

	scrape()
	if ct := getCT(); ct.Status.IndexEntries == nil || *ct.Status.IndexEntries != 5 {
		t.Fatalf("status.indexEntries = %v, want 5", ct.Status.IndexEntries)
	} else if s := condStatus(ct, "Ready"); s != metav1.ConditionTrue {
		t.Fatalf("Ready = %q, want True", s)
	} else if s := condStatus(ct, "QuotaExceeded"); s != metav1.ConditionFalse {
		t.Fatalf("QuotaExceeded = %q, want False", s)
	}

	// Lower the budget below current usage WITHOUT a new ingest. Eviction is
	// ingest-triggered, so the index still holds 5 — the snapshot now reports
	// over budget and QuotaExceeded flaps True.
	live := getCT()
	live.Spec.Quota.MaxIndexEntries = i64IntegrationPtr(3)
	if err := k8s.Update(ctx, &live); err != nil {
		t.Fatalf("lower budget: %v", err)
	}
	push()
	scrape()
	if ct := getCT(); condStatus(ct, "QuotaExceeded") != metav1.ConditionTrue {
		t.Fatalf("QuotaExceeded = %q, want True after lowering budget below usage", condStatus(ct, "QuotaExceeded"))
	}

	// Now ingest again: enforcement runs and evicts the tenant's oldest entries
	// down to the new budget of 3. Status reflects the post-eviction count and
	// QuotaExceeded resets to False.
	ingestTenantPrefixes(idx, tenantID, 5, 6) // one more distinct prefix
	if got := snapshotEntries(idx, tenantID); got != 3 {
		t.Fatalf("entries after over-budget ingest = %d, want 3 (evicted to budget)", got)
	}
	scrape()
	if ct := getCT(); ct.Status.IndexEntries == nil || *ct.Status.IndexEntries != 3 {
		t.Fatalf("post-eviction status.indexEntries = %v, want 3", ct.Status.IndexEntries)
	} else if s := condStatus(ct, "QuotaExceeded"); s != metav1.ConditionFalse {
		t.Fatalf("QuotaExceeded = %q, want False after eviction", s)
	}

	// Delete the CacheTenant: the next push omits it, so the server reverts to
	// no enforcement and subsequent ingest is unrestricted.
	if err := k8s.Delete(ctx, &live); err != nil {
		t.Fatalf("delete CacheTenant: %v", err)
	}
	push()
	if _, ok := store.TenantQuota(tenantID); ok {
		t.Fatal("quota should be gone after the tenant was deleted (replace-on-write)")
	}
	ingestTenantPrefixes(idx, tenantID, 6, 26)            // 20 more prefixes, no cap now
	if got := snapshotEntries(idx, tenantID); got != 23 { // 3 survivors + 20 new
		t.Fatalf("entries after delete = %d, want 23 (unrestricted)", got)
	}
}

// TestIntegrationCacheTenantBackwardCompat pins the no-regression invariant:
// with no CacheTenant in the cluster, ingest behaves exactly as before — no
// enforcement, every entry retained.
func TestIntegrationCacheTenantBackwardCompat(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, _, _ := startEnv(t)
	ctx := context.Background()

	store := cacheserver.NewPolicyStore()
	idx := index.New(index.WithTenantQuotaResolver(store))
	policySrv := httptest.NewServer(cacheserver.NewPolicyHTTPHandler(store))
	defer policySrv.Close()

	reconciler := &ControlPlaneReconciler{Client: k8s, ServerPolicyURL: policySrv.URL, HTTPClient: policySrv.Client()}
	if _, err := reconciler.Reconcile(ctx, ctrl.Request{}); err != nil {
		t.Fatalf("push with no tenants: %v", err)
	}
	if len(store.TenantQuotas()) != 0 {
		t.Fatalf("no CacheTenant exists, but the store holds %d quotas", len(store.TenantQuotas()))
	}

	const tenantID = "unbounded"
	ingestTenantPrefixes(idx, tenantID, 0, 50)
	if got := snapshotEntries(idx, tenantID); got != 50 {
		t.Fatalf("entries = %d, want 50 (no enforcement without a CacheTenant)", got)
	}
}

func i64IntegrationPtr(v int64) *int64 { return &v }

// TestIntegrationCacheTenantRejectsReservedTenantID pins the CEL admission rule
// against a real apiserver (the fake client skips x-kubernetes-validations):
// "_default" is reserved for the cluster-wide untenanted-traffic bucket, so a
// CacheTenant may not claim it as a tenantID.
func TestIntegrationCacheTenantRejectsReservedTenantID(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, _, _ := startEnv(t)
	ctx := context.Background()
	ns := freshNS(t, k8s)

	ct := &cachev1alpha1.CacheTenant{
		ObjectMeta: metav1.ObjectMeta{Name: "reserved", Namespace: ns},
		Spec:       cachev1alpha1.CacheTenantSpec{TenantID: index.DefaultTenantSentinel},
	}
	err := k8s.Create(ctx, ct)
	if err == nil {
		t.Fatalf("creating a CacheTenant with the reserved tenantID %q must be rejected", index.DefaultTenantSentinel)
	}
	if !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("rejection error = %v, want it to cite the reserved-tenantID rule", err)
	}
}

// ingestTenantPrefixes ingests distinct prefixes [from,to) for a tenant, each at
// a strictly increasing timestamp so "oldest" is well-defined for eviction.
func ingestTenantPrefixes(idx *index.Index, tenantID string, from, to int) {
	base := time.Unix(20_000_000, 0)
	for i := from; i < to; i++ {
		idx.Ingest(index.Update{
			ReplicaID:  "r1",
			Model:      "m",
			Tenant:     tenantID,
			HashScheme: "vllm",
			Timestamp:  base.Add(time.Duration(i) * time.Second),
			Prefixes:   []index.PrefixRef{{PrefixHash: []byte(fmt.Sprintf("p%04d", i)), TokenCount: 1}},
		})
	}
}

func snapshotEntries(idx *index.Index, tenantID string) int64 {
	for _, t := range idx.Snapshot().Tenants {
		if t.TenantID == tenantID {
			return t.IndexEntries
		}
	}
	return 0
}

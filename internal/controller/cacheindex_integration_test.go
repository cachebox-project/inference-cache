package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/index"
)

// TestIntegrationCacheIndexPoller exercises the CacheIndex poller against a
// real envtest apiserver. It mirrors the CacheBackend integration suite's
// coverage boundary: CRD schema pruning, status subresource semantics, and
// resourceVersion no-churn are all real-apiserver behaviors the fake client can
// accidentally hide.
//
// Skipped unless KUBEBUILDER_ASSETS is set (see skipWithoutEnvtest).
func TestIntegrationCacheIndexPoller(t *testing.T) {
	skipWithoutEnvtest(t)
	k8s, _, _ := startEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	t.Run("StatusSurfaceOnCreate", func(t *testing.T) {
		lastUpdate := time.Unix(1_700_000_000, 0).UTC()
		snap := index.Snapshot{
			TotalPrefixes: 7,
			// HotPrefixes intentionally left at 0: the controller should render
			// the deferred access-counting surface explicitly as hot: 0.
			Replicas: []index.ReplicaSnapshot{{
				ReplicaID:        "vllm-0",
				Tenant:           "tenant-a",
				CacheMemoryBytes: 2048,
				HitRate:          0.75,
				Pressure:         0.25,
				LastUpdate:       lastUpdate,
			}},
			Tenants: []index.TenantSnapshot{{
				TenantID:     "tenant-a",
				IndexEntries: 7,
				MemoryUsed:   2048,
				HitRate:      0.75,
			}},
		}
		srv := newSnapshotServer(t, &snap, nil)
		defer srv.Close()

		poller := &CacheIndexPoller{
			Client:      k8s,
			SnapshotURL: srv.URL + "/snapshot",
			HTTPClient:  srv.Client(),
			Name:        DefaultCacheIndexName,
		}
		if err := poller.refresh(ctx); err != nil {
			t.Fatalf("refresh: %v", err)
		}

		ci := getCacheIndex(ctx, t, k8s, DefaultCacheIndexName)
		if ci.Status.ObservedServer != srv.URL+"/snapshot" {
			t.Fatalf("observedServer = %q, want %q", ci.Status.ObservedServer, srv.URL+"/snapshot")
		}
		if ci.Status.LastUpdated.IsZero() {
			t.Fatal("lastUpdated was not populated")
		}
		if ci.Status.Prefixes.Summary.Total != 7 {
			t.Fatalf("prefixes.summary.total = %d, want 7", ci.Status.Prefixes.Summary.Total)
		}
		if ci.Status.Prefixes.Summary.Hot != 0 {
			t.Fatalf("prefixes.summary.hot = %d, want 0 until access-counting is wired", ci.Status.Prefixes.Summary.Hot)
		}
		raw := getCacheIndexUnstructured(ctx, t, k8s, DefaultCacheIndexName)
		hot, found, err := unstructured.NestedInt64(raw.Object, "status", "prefixes", "summary", "hot")
		if err != nil {
			t.Fatalf("read persisted status.prefixes.summary.hot: %v", err)
		}
		if !found {
			t.Fatal("persisted status.prefixes.summary.hot is missing, want explicit hot: 0")
		}
		if hot != 0 {
			t.Fatalf("persisted status.prefixes.summary.hot = %d, want 0", hot)
		}
		if len(ci.Status.Replicas) != 1 {
			t.Fatalf("replicas = %+v, want exactly one row", ci.Status.Replicas)
		}
		replica := ci.Status.Replicas[0]
		if replica.ID != "vllm-0" || replica.Tenant != "tenant-a" ||
			replica.CacheMemoryBytes != 2048 || replica.HitRate != "0.75" ||
			replica.Pressure != "0.25" || !replica.LastUpdate.Time.Equal(lastUpdate) {
			t.Fatalf("replica status = %+v, want vllm-0 tenant-a memory/hit/pressure/lastUpdate from snapshot", replica)
		}
		if len(ci.Status.Tenants) != 1 {
			t.Fatalf("tenants = %+v, want exactly one row", ci.Status.Tenants)
		}
		tenant := ci.Status.Tenants[0]
		if tenant.ID != "tenant-a" || tenant.IndexEntries != 7 ||
			tenant.MemoryUsed != 2048 || tenant.HitRate != "0.75" {
			t.Fatalf("tenant status = %+v, want tenant-a aggregate from snapshot", tenant)
		}
	})

	t.Run("SnapshotPollWritesOnlyOnChange", func(t *testing.T) {
		var mu sync.Mutex
		served := index.Snapshot{
			TotalPrefixes: 3,
			Replicas: []index.ReplicaSnapshot{{
				ReplicaID:        "vllm-0",
				Tenant:           "tenant-a",
				CacheMemoryBytes: 100,
				HitRate:          0.8,
				LastUpdate:       time.Unix(1_700_000_000, 0).UTC(),
			}},
			Tenants: []index.TenantSnapshot{{TenantID: "tenant-a", IndexEntries: 3, MemoryUsed: 100, HitRate: 0.8}},
		}
		var requests int
		srv := newSnapshotServer(t, &served, &snapshotServerHooks{
			Lock: &mu,
			OnRequest: func() {
				requests++
			},
		})
		defer srv.Close()

		const name = "cacheindex-no-churn-it"
		poller := &CacheIndexPoller{
			Client:      k8s,
			SnapshotURL: srv.URL + "/snapshot",
			HTTPClient:  srv.Client(),
			Name:        name,
		}
		if err := poller.refresh(ctx); err != nil {
			t.Fatalf("first refresh: %v", err)
		}
		ci := getCacheIndex(ctx, t, k8s, name)
		if ci.Status.Prefixes.Summary.Total != 3 || len(ci.Status.Replicas) != 1 {
			t.Fatalf("first status = %+v, want total 3 and one replica", ci.Status)
		}
		rvAfterFirstWrite := ci.ResourceVersion

		if err := poller.refresh(ctx); err != nil {
			t.Fatalf("second refresh: %v", err)
		}
		ci = getCacheIndex(ctx, t, k8s, name)
		if ci.ResourceVersion != rvAfterFirstWrite {
			t.Fatalf("identical snapshot churned CacheIndex resourceVersion: %s -> %s", rvAfterFirstWrite, ci.ResourceVersion)
		}

		func() {
			mu.Lock()
			defer mu.Unlock()
			if requests != 2 {
				t.Fatalf("snapshot requests after two refreshes = %d, want 2", requests)
			}
			served = index.Snapshot{
				TotalPrefixes: 9,
				Replicas: []index.ReplicaSnapshot{{
					ReplicaID:        "vllm-0",
					Tenant:           "tenant-a",
					CacheMemoryBytes: 500,
					HitRate:          0.9,
					LastUpdate:       time.Unix(1_700_000_100, 0).UTC(),
				}},
				Tenants: []index.TenantSnapshot{{TenantID: "tenant-a", IndexEntries: 9, MemoryUsed: 500, HitRate: 0.9}},
			}
		}()

		if err := poller.refresh(ctx); err != nil {
			t.Fatalf("third refresh: %v", err)
		}
		ci = getCacheIndex(ctx, t, k8s, name)
		if ci.ResourceVersion == rvAfterFirstWrite {
			t.Fatal("changed snapshot did not write a new CacheIndex status revision")
		}
		if ci.Status.Prefixes.Summary.Total != 9 ||
			len(ci.Status.Replicas) != 1 ||
			ci.Status.Replicas[0].CacheMemoryBytes != 500 ||
			len(ci.Status.Tenants) != 1 ||
			ci.Status.Tenants[0].IndexEntries != 9 {
			t.Fatalf("changed status = %+v, want updated total/replica/tenant from second snapshot", ci.Status)
		}
	})

	t.Run("StatusOnlySpecPrunedAndCreateStatusIgnored", func(t *testing.T) {
		// CacheIndex is status-only by structural schema + status subresource:
		// the empty spec schema prunes user fields, and the apiserver ignores
		// status written through the main resource endpoint.
		u := &unstructured.Unstructured{}
		u.SetAPIVersion("inferencecache.io/v1alpha1")
		u.SetKind("CacheIndex")
		u.SetName("cacheindex-status-only-it")
		if err := unstructured.SetNestedField(u.Object, "user-value", "spec", "userField"); err != nil {
			t.Fatalf("set spec.userField: %v", err)
		}
		if err := unstructured.SetNestedField(u.Object, int64(99), "status", "prefixes", "summary", "total"); err != nil {
			t.Fatalf("set status.prefixes.summary.total: %v", err)
		}
		if err := k8s.Create(ctx, u); err != nil {
			t.Fatalf("create CacheIndex with user spec/status: %v", err)
		}

		got := &unstructured.Unstructured{}
		got.SetAPIVersion("inferencecache.io/v1alpha1")
		got.SetKind("CacheIndex")
		if err := k8s.Get(ctx, client.ObjectKey{Name: "cacheindex-status-only-it"}, got); err != nil {
			t.Fatalf("get CacheIndex: %v", err)
		}
		if _, found, err := unstructured.NestedFieldNoCopy(got.Object, "spec", "userField"); err != nil {
			t.Fatalf("read spec.userField: %v", err)
		} else if found {
			t.Fatalf("user-writable spec field survived real-apiserver pruning: %#v", got.Object["spec"])
		}
		if _, found, err := unstructured.NestedFieldNoCopy(got.Object, "status", "prefixes", "summary", "total"); err != nil {
			t.Fatalf("read status.prefixes.summary.total: %v", err)
		} else if found {
			t.Fatalf("status supplied on create survived status-subresource isolation: %#v", got.Object["status"])
		}
		spec, found, err := unstructured.NestedMap(got.Object, "spec")
		if err != nil {
			t.Fatalf("read spec: %v", err)
		}
		if found && len(spec) != 0 {
			t.Fatalf("spec after pruning = %#v, want omitted or legacy empty object", spec)
		}

		snap := index.Snapshot{TotalPrefixes: 1, Tenants: []index.TenantSnapshot{{TenantID: "tenant-a", IndexEntries: 1}}}
		srv := newSnapshotServer(t, &snap, nil)
		defer srv.Close()
		poller := &CacheIndexPoller{
			Client:      k8s,
			SnapshotURL: srv.URL + "/snapshot",
			HTTPClient:  srv.Client(),
			Name:        "cacheindex-status-only-it",
		}
		if err := poller.refresh(ctx); err != nil {
			t.Fatalf("refresh after create-status was ignored: %v", err)
		}
		ci := getCacheIndex(ctx, t, k8s, "cacheindex-status-only-it")
		if ci.Status.Prefixes.Summary.Total != 1 {
			t.Fatalf("controller status update did not persist through status subresource; total = %d, want 1", ci.Status.Prefixes.Summary.Total)
		}
	})
}

type snapshotServerHooks struct {
	Lock      *sync.Mutex
	OnRequest func()
}

func newSnapshotServer(t *testing.T, served *index.Snapshot, hooks *snapshotServerHooks) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/snapshot" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if hooks != nil && hooks.Lock != nil {
			hooks.Lock.Lock()
			defer hooks.Lock.Unlock()
		}
		if hooks != nil && hooks.OnRequest != nil {
			hooks.OnRequest()
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(*served); err != nil {
			t.Errorf("encode snapshot: %v", err)
		}
	}))
}

func getCacheIndex(ctx context.Context, t *testing.T, k8s client.Client, name string) *cachev1alpha1.CacheIndex {
	t.Helper()
	var ci cachev1alpha1.CacheIndex
	if err := k8s.Get(ctx, types.NamespacedName{Name: name}, &ci); err != nil {
		t.Fatalf("get CacheIndex %s: %v", name, err)
	}
	return &ci
}

func getCacheIndexUnstructured(ctx context.Context, t *testing.T, k8s client.Client, name string) *unstructured.Unstructured {
	t.Helper()
	ci := &unstructured.Unstructured{}
	ci.SetAPIVersion("inferencecache.io/v1alpha1")
	ci.SetKind("CacheIndex")
	if err := k8s.Get(ctx, types.NamespacedName{Name: name}, ci); err != nil {
		t.Fatalf("get unstructured CacheIndex %s: %v", name, err)
	}
	return ci
}

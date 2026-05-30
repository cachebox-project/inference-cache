package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/index"
)

func TestBuildCacheIndexStatus(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	snap := index.Snapshot{
		TotalPrefixes: 5,
		HotPrefixes:   0,
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "r1", CacheMemoryBytes: 100, HitRate: 0.8, Pressure: 0.5, LastUpdate: now},
		},
		Tenants: []index.TenantSnapshot{
			{TenantID: "t1", MemoryUsed: 100, HitRate: 0.8},
		},
	}

	st := buildCacheIndexStatus(snap, "http://server/snapshot", now)

	if st.Prefixes.Summary.Total != 5 || st.Prefixes.Summary.Hot != 0 {
		t.Fatalf("prefixes = %+v, want {Total:5 Hot:0}", st.Prefixes)
	}
	if st.ObservedServer != "http://server/snapshot" {
		t.Fatalf("observedServer = %q", st.ObservedServer)
	}
	if len(st.Replicas) != 1 || st.Replicas[0].ID != "r1" || st.Replicas[0].HitRate != "0.8" || st.Replicas[0].Pressure != "0.5" {
		t.Fatalf("replica = %+v, want id r1 hitRate 0.8 pressure 0.5 (decimal strings)", st.Replicas[0])
	}
	if len(st.Tenants) != 1 || st.Tenants[0].ID != "t1" || st.Tenants[0].HitRate != "0.8" {
		t.Fatalf("tenant = %+v", st.Tenants[0])
	}
}

func TestEmptyIndexStatusRendersZeroSummary(t *testing.T) {
	// An empty index must still render prefixes.summary.{total,hot}=0 explicitly
	// (not omit them), matching the contract shape.
	st := buildCacheIndexStatus(index.Snapshot{}, "http://server/snapshot", time.Unix(1, 0))
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	js := string(b)
	if !strings.Contains(js, `"total":0`) || !strings.Contains(js, `"hot":0`) {
		t.Fatalf("empty summary should render total:0 and hot:0, got %s", js)
	}
}

func TestStatusEqualIgnoresTimestamps(t *testing.T) {
	base := cachev1alpha1.CacheIndexStatus{
		Prefixes: cachev1alpha1.PrefixStatus{Summary: cachev1alpha1.PrefixSummary{Total: 3}},
		Replicas: []cachev1alpha1.ReplicaCacheStatus{{ID: "r1", CacheMemoryBytes: 100, HitRate: "0.8"}},
	}
	// Same meaningful data, different timestamps → equal.
	a := *base.DeepCopy()
	b := *base.DeepCopy()
	a.LastUpdated.Time = time.Unix(1, 0)
	b.LastUpdated.Time = time.Unix(2, 0)
	a.Replicas[0].LastUpdate.Time = time.Unix(10, 0)
	b.Replicas[0].LastUpdate.Time = time.Unix(20, 0)
	if !statusEqual(a, b) {
		t.Fatal("statuses differing only by timestamps should be equal")
	}

	// A real change (memory) → not equal.
	c := *base.DeepCopy()
	c.Replicas[0].CacheMemoryBytes = 999
	if statusEqual(base, c) {
		t.Fatal("statuses differing by replica memory should NOT be equal")
	}
}

func TestFetchSnapshot(t *testing.T) {
	want := index.Snapshot{TotalPrefixes: 7, Replicas: []index.ReplicaSnapshot{{ReplicaID: "r1"}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()

	got, err := fetchSnapshot(context.Background(), srv.Client(), srv.URL)
	if err != nil {
		t.Fatalf("fetchSnapshot: %v", err)
	}
	if got.TotalPrefixes != 7 || len(got.Replicas) != 1 || got.Replicas[0].ReplicaID != "r1" {
		t.Fatalf("decoded snapshot = %+v", got)
	}
}

func TestFetchSnapshotNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	if _, err := fetchSnapshot(context.Background(), srv.Client(), srv.URL); err == nil {
		t.Fatal("expected error on non-200 snapshot response")
	}
}

func TestRefreshCreatesThenUpdatesOnlyOnChange(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheIndex{}).
		Build()

	var mu sync.Mutex
	served := index.Snapshot{TotalPrefixes: 3, Replicas: []index.ReplicaSnapshot{{ReplicaID: "r1", CacheMemoryBytes: 100, HitRate: 0.8}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		_ = json.NewEncoder(w).Encode(served)
	}))
	defer srv.Close()

	p := &CacheIndexPoller{Client: cl, SnapshotURL: srv.URL, HTTPClient: srv.Client(), Name: "cluster-default"}
	ctx := context.Background()

	// First refresh: creates the singleton + writes status.
	if err := p.refresh(ctx); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	get := func() cachev1alpha1.CacheIndex {
		var ci cachev1alpha1.CacheIndex
		if err := cl.Get(ctx, types.NamespacedName{Name: "cluster-default"}, &ci); err != nil {
			t.Fatalf("get CacheIndex: %v", err)
		}
		return ci
	}
	ci := get()
	if ci.Status.Prefixes.Summary.Total != 3 || len(ci.Status.Replicas) != 1 {
		t.Fatalf("status after create = %+v", ci.Status)
	}
	rvAfterCreate := ci.ResourceVersion

	// Second refresh with identical data → no write (resourceVersion unchanged).
	if err := p.refresh(ctx); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	if rv := get().ResourceVersion; rv != rvAfterCreate {
		t.Fatalf("no-op refresh wrote status (rv %s → %s)", rvAfterCreate, rv)
	}

	// Change the served snapshot → status updates.
	mu.Lock()
	served = index.Snapshot{TotalPrefixes: 9, Replicas: []index.ReplicaSnapshot{{ReplicaID: "r1", CacheMemoryBytes: 500, HitRate: 0.9}}}
	mu.Unlock()
	if err := p.refresh(ctx); err != nil {
		t.Fatalf("third refresh: %v", err)
	}
	ci = get()
	if ci.Status.Prefixes.Summary.Total != 9 || ci.Status.Replicas[0].CacheMemoryBytes != 500 {
		t.Fatalf("status after change = %+v, want total 9 / memory 500", ci.Status)
	}
	if ci.ResourceVersion == rvAfterCreate {
		t.Fatal("changed snapshot should have written a new status revision")
	}
}

// buildPollerWithFixtures spins up a fake client + httptest server and returns
// a poller wired to both. CacheBackends and engine pods are pre-loaded; the
// served Snapshot is read under the supplied mutex.
func buildPollerWithFixtures(t *testing.T, backends []*cachev1alpha1.CacheBackend, enginePods []*corev1.Pod, served *index.Snapshot, mu *sync.Mutex) (*CacheIndexPoller, client.Client, *httptest.Server) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	builder := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheIndex{}, &cachev1alpha1.CacheBackend{})
	for _, cb := range backends {
		builder = builder.WithObjects(cb)
	}
	for _, pod := range enginePods {
		builder = builder.WithObjects(pod)
	}
	cl := builder.Build()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		_ = json.NewEncoder(w).Encode(*served)
	}))
	p := &CacheIndexPoller{Client: cl, SnapshotURL: srv.URL, HTTPClient: srv.Client(), Name: "cluster-default"}
	return p, cl, srv
}

// cbFixture builds a CacheBackend whose EngineSelector picks engine pods with
// the supplied labels. The match-labels must be non-empty: the poller refuses
// to attribute to backends without an EngineSelector, mirroring the "no
// selector ⇒ no claim" guard in matchLabelsSelects.
func cbFixture(name, ns string, selectorLabels map[string]string) *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: cachev1alpha1.CacheBackendSpec{
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: selectorLabels},
		},
	}
}

// enginePod builds an engine Pod the kvevent-subscriber sidecar would attach
// to: the subscriber reports replica_id=<pod-name>, tenant_id=<pod-namespace>,
// so the poller looks the pod up by that (namespace, name) and matches its
// labels against each CacheBackend.Spec.EngineSelector.
func enginePod(name, ns string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: labels},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "vllm", Image: "vllm/vllm-openai:latest"}}},
	}
}

func getBackendDirect(t *testing.T, cl client.Client, name, ns string) *cachev1alpha1.CacheBackend {
	t.Helper()
	var cb cachev1alpha1.CacheBackend
	if err := cl.Get(context.Background(), types.NamespacedName{Name: name, Namespace: ns}, &cb); err != nil {
		t.Fatalf("get CacheBackend %s/%s: %v", ns, name, err)
	}
	return &cb
}

// TestRefreshProjectsParticipationPerBackend (happy path): two CacheBackends
// select engine pods via distinct EngineSelector labels; the snapshot reports
// per-replica prefix counts; indexParticipation reflects the per-backend
// sum/max after the poller resolves each replica to its engine pod.
func TestRefreshProjectsParticipationPerBackend(t *testing.T) {
	cbA := cbFixture("backend-a", "default", map[string]string{"app": "vllm-a"})
	cbB := cbFixture("backend-b", "default", map[string]string{"app": "vllm-b"})
	podA0 := enginePod("vllm-a-0", "default", map[string]string{"app": "vllm-a"})
	podA1 := enginePod("vllm-a-1", "default", map[string]string{"app": "vllm-a"})
	podB0 := enginePod("vllm-b-0", "default", map[string]string{"app": "vllm-b"})

	t1 := time.Unix(1_700_000_000, 0).UTC()
	t2 := time.Unix(1_700_000_500, 0).UTC()
	t3 := time.Unix(1_700_000_100, 0).UTC()

	var mu sync.Mutex
	served := index.Snapshot{
		TotalPrefixes: 6,
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-a-0", Tenant: "default", PrefixCount: 2, LastEventAt: t1},
			{ReplicaID: "vllm-a-1", Tenant: "default", PrefixCount: 3, LastEventAt: t2},
			{ReplicaID: "vllm-b-0", Tenant: "default", PrefixCount: 1, LastEventAt: t3},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbA, cbB},
		[]*corev1.Pod{podA0, podA1, podB0},
		&served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	gotA := getBackendDirect(t, cl, "backend-a", "default")
	if gotA.Status.IndexParticipation == nil {
		t.Fatal("backend-a indexParticipation not populated")
	}
	if gotA.Status.IndexParticipation.PrefixCount != 5 {
		t.Fatalf("backend-a prefixCount = %d, want 5", gotA.Status.IndexParticipation.PrefixCount)
	}
	if gotA.Status.IndexParticipation.LastEventAt == nil || !gotA.Status.IndexParticipation.LastEventAt.Time.Equal(t2) {
		t.Fatalf("backend-a lastEventAt = %v, want %v", gotA.Status.IndexParticipation.LastEventAt, t2)
	}

	gotB := getBackendDirect(t, cl, "backend-b", "default")
	if gotB.Status.IndexParticipation == nil {
		t.Fatal("backend-b indexParticipation not populated")
	}
	if gotB.Status.IndexParticipation.PrefixCount != 1 {
		t.Fatalf("backend-b prefixCount = %d, want 1", gotB.Status.IndexParticipation.PrefixCount)
	}
	if gotB.Status.IndexParticipation.LastEventAt == nil || !gotB.Status.IndexParticipation.LastEventAt.Time.Equal(t3) {
		t.Fatalf("backend-b lastEventAt = %v, want %v", gotB.Status.IndexParticipation.LastEventAt, t3)
	}
}

// TestRefreshNoEventsForBackendPublishesZeroParticipation: a CacheBackend with
// no matching engine pods must have indexParticipation published as
// {prefixCount: 0, lastEventAt: nil} — semantically "I'm visible but holding
// no warm prefixes right now."
func TestRefreshNoEventsForBackendPublishesZeroParticipation(t *testing.T) {
	cbA := cbFixture("backend-a", "default", map[string]string{"app": "vllm-a"})
	cbQuiet := cbFixture("backend-quiet", "default", map[string]string{"app": "vllm-quiet"})
	podA := enginePod("vllm-a-0", "default", map[string]string{"app": "vllm-a"})

	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-a-0", Tenant: "default", PrefixCount: 1, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbA, cbQuiet},
		[]*corev1.Pod{podA},
		&served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	q := getBackendDirect(t, cl, "backend-quiet", "default")
	if q.Status.IndexParticipation == nil {
		t.Fatal("backend-quiet should have indexParticipation published (prefixCount 0)")
	}
	if q.Status.IndexParticipation.PrefixCount != 0 {
		t.Fatalf("backend-quiet prefixCount = %d, want 0", q.Status.IndexParticipation.PrefixCount)
	}
	if q.Status.IndexParticipation.LastEventAt != nil {
		t.Fatalf("backend-quiet lastEventAt = %v, want nil", q.Status.IndexParticipation.LastEventAt)
	}
	a := getBackendDirect(t, cl, "backend-a", "default")
	if a.Status.IndexParticipation == nil || a.Status.IndexParticipation.PrefixCount != 1 {
		t.Fatalf("backend-a participation = %+v, want prefixCount 1", a.Status.IndexParticipation)
	}
}

// TestRefreshClearsStaleParticipationOnReplicaDrain: once a backend has
// published a positive prefixCount, a later successful snapshot with no
// matching replicas must reset prefixCount to 0 (and clear lastEventAt).
// Otherwise the operator sees stale "still contributing" state forever.
func TestRefreshClearsStaleParticipationOnReplicaDrain(t *testing.T) {
	cbA := cbFixture("backend-a", "default", map[string]string{"app": "vllm-a"})
	podA := enginePod("vllm-a-0", "default", map[string]string{"app": "vllm-a"})
	var mu sync.Mutex
	tEvent := time.Unix(1_700_000_000, 0).UTC()
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-a-0", Tenant: "default", PrefixCount: 5, LastEventAt: tEvent},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbA},
		[]*corev1.Pod{podA},
		&served, &mu)
	defer srv.Close()
	ctx := context.Background()

	if err := p.refresh(ctx); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	a := getBackendDirect(t, cl, "backend-a", "default")
	if a.Status.IndexParticipation == nil || a.Status.IndexParticipation.PrefixCount != 5 {
		t.Fatalf("seed participation = %+v, want prefixCount 5", a.Status.IndexParticipation)
	}

	// Drain: a successful scrape with zero matching replicas.
	mu.Lock()
	served = index.Snapshot{Replicas: nil}
	mu.Unlock()
	if err := p.refresh(ctx); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	a = getBackendDirect(t, cl, "backend-a", "default")
	if a.Status.IndexParticipation == nil || a.Status.IndexParticipation.PrefixCount != 0 {
		t.Fatalf("drained participation = %+v, want prefixCount 0", a.Status.IndexParticipation)
	}
	if a.Status.IndexParticipation.LastEventAt != nil {
		t.Fatalf("drained lastEventAt = %v, want nil", a.Status.IndexParticipation.LastEventAt)
	}
}

// TestRefreshSameNameDifferentNamespaceAttributesByLabel: two CacheBackends
// share a metadata.name across namespaces but each lives in its own
// namespace and matches its own engine pods. Each must see only its own
// namespace's replicas — namespace scoping prevents cross-tenant bleed.
func TestRefreshSameNameDifferentNamespaceAttributesByLabel(t *testing.T) {
	cbNS1 := cbFixture("backend-a", "ns-1", map[string]string{"app": "vllm"})
	cbNS2 := cbFixture("backend-a", "ns-2", map[string]string{"app": "vllm"})
	podNS1 := enginePod("vllm-0", "ns-1", map[string]string{"app": "vllm"})
	podNS2 := enginePod("vllm-0", "ns-2", map[string]string{"app": "vllm"})
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-0", Tenant: "ns-1", PrefixCount: 2, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
			{ReplicaID: "vllm-0", Tenant: "ns-2", PrefixCount: 5, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbNS1, cbNS2},
		[]*corev1.Pod{podNS1, podNS2},
		&served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	got1 := getBackendDirect(t, cl, "backend-a", "ns-1")
	if got1.Status.IndexParticipation == nil || got1.Status.IndexParticipation.PrefixCount != 2 {
		t.Fatalf("ns-1 backend-a participation = %+v, want prefixCount 2", got1.Status.IndexParticipation)
	}
	got2 := getBackendDirect(t, cl, "backend-a", "ns-2")
	if got2.Status.IndexParticipation == nil || got2.Status.IndexParticipation.PrefixCount != 5 {
		t.Fatalf("ns-2 backend-a participation = %+v, want prefixCount 5", got2.Status.IndexParticipation)
	}
}

// TestRefreshDeletedEnginePodSkipsAttribution: a replica reported in the
// snapshot whose engine pod no longer exists (drained pod, TTL not yet hit)
// must be silently skipped — other backends still update.
func TestRefreshDeletedEnginePodSkipsAttribution(t *testing.T) {
	cbA := cbFixture("backend-a", "default", map[string]string{"app": "vllm-a"})
	podA0 := enginePod("vllm-a-0", "default", map[string]string{"app": "vllm-a"})
	// vllm-a-1 reported in snapshot but no corresponding pod fixture.
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-a-0", Tenant: "default", PrefixCount: 2, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
			{ReplicaID: "vllm-a-1", Tenant: "default", PrefixCount: 99, LastEventAt: time.Unix(1_700_000_999, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbA},
		[]*corev1.Pod{podA0},
		&served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	a := getBackendDirect(t, cl, "backend-a", "default")
	if a.Status.IndexParticipation == nil || a.Status.IndexParticipation.PrefixCount != 2 {
		t.Fatalf("backend-a should ignore missing-pod replica; got %+v", a.Status.IndexParticipation)
	}
}

// TestRefreshBackendWithNoEngineSelectorSkipped: a CacheBackend without an
// EngineSelector (or with empty MatchLabels) must NOT receive any attribution
// — otherwise a misconfigured backend would silently claim every replica in
// its namespace by vacuous truth.
func TestRefreshBackendWithNoEngineSelectorSkipped(t *testing.T) {
	cbNoSelector := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "no-selector", Namespace: "default"},
	}
	cbA := cbFixture("backend-a", "default", map[string]string{"app": "vllm-a"})
	podA := enginePod("vllm-a-0", "default", map[string]string{"app": "vllm-a"})
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-a-0", Tenant: "default", PrefixCount: 2, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbNoSelector, cbA},
		[]*corev1.Pod{podA},
		&served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	no := getBackendDirect(t, cl, "no-selector", "default")
	if no.Status.IndexParticipation != nil {
		t.Fatalf("backend with no EngineSelector must not be projected, got %+v", no.Status.IndexParticipation)
	}
	a := getBackendDirect(t, cl, "backend-a", "default")
	if a.Status.IndexParticipation == nil || a.Status.IndexParticipation.PrefixCount != 2 {
		t.Fatalf("backend-a participation = %+v, want prefixCount 2", a.Status.IndexParticipation)
	}
}

// TestRefreshNoChurnOnIdenticalSnapshot (no-churn invariant): two consecutive
// identical snapshots must produce exactly one CacheBackend status write —
// asserted via resource-version stability on the second tick.
func TestRefreshNoChurnOnIdenticalSnapshot(t *testing.T) {
	cbA := cbFixture("backend-a", "default", map[string]string{"app": "vllm-a"})
	podA := enginePod("vllm-a-0", "default", map[string]string{"app": "vllm-a"})
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-a-0", Tenant: "default", PrefixCount: 4, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbA},
		[]*corev1.Pod{podA},
		&served, &mu)
	defer srv.Close()
	ctx := context.Background()

	if err := p.refresh(ctx); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	rv1 := getBackendDirect(t, cl, "backend-a", "default").ResourceVersion
	if err := p.refresh(ctx); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	rv2 := getBackendDirect(t, cl, "backend-a", "default").ResourceVersion
	if rv1 != rv2 {
		t.Fatalf("identical snapshot wrote status (rv %s → %s)", rv1, rv2)
	}
}

// TestRefreshHitRateStaysNil: HitRate aggregation is intentionally deferred
// because the snapshot's per-replica HitRate has no presence bit — a real 0%
// hit rate is indistinguishable from "not reported". Until the stats-reporter
// follow-up adds a presence signal to the snapshot, status.indexParticipation
// .hitRate stays nil regardless of replica values.
func TestRefreshHitRateStaysNil(t *testing.T) {
	cbA := cbFixture("backend-a", "default", map[string]string{"app": "vllm-a"})
	podA0 := enginePod("vllm-a-0", "default", map[string]string{"app": "vllm-a"})
	podA1 := enginePod("vllm-a-1", "default", map[string]string{"app": "vllm-a"})
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-a-0", Tenant: "default", PrefixCount: 2, HitRate: 0.75, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
			{ReplicaID: "vllm-a-1", Tenant: "default", PrefixCount: 3, HitRate: 0.85, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbA},
		[]*corev1.Pod{podA0, podA1},
		&served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	a := getBackendDirect(t, cl, "backend-a", "default")
	if a.Status.IndexParticipation == nil {
		t.Fatal("backend-a participation should be populated (prefixCount + lastEventAt)")
	}
	if a.Status.IndexParticipation.HitRate != nil {
		t.Fatalf("HitRate should be nil while the snapshot lacks a presence bit, got %q", *a.Status.IndexParticipation.HitRate)
	}
}

// TestRefreshScrapeFailureDoesNotClearParticipation (fail-soft): once
// indexParticipation is published, a failing /snapshot scrape must NOT
// clear it. Tested by seeding a status, then closing the server.
func TestRefreshScrapeFailureDoesNotClearParticipation(t *testing.T) {
	cbA := cbFixture("backend-a", "default", map[string]string{"app": "vllm-a"})
	podA := enginePod("vllm-a-0", "default", map[string]string{"app": "vllm-a"})
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-a-0", Tenant: "default", PrefixCount: 7, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbA},
		[]*corev1.Pod{podA},
		&served, &mu)

	// Publish the initial projection, then take the server down so the next
	// tick fails the scrape.
	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	before := getBackendDirect(t, cl, "backend-a", "default")
	if before.Status.IndexParticipation == nil || before.Status.IndexParticipation.PrefixCount != 7 {
		t.Fatalf("seed participation = %+v", before.Status.IndexParticipation)
	}
	srv.Close()

	if err := p.refresh(context.Background()); err == nil {
		t.Fatal("expected scrape error after server close")
	}
	after := getBackendDirect(t, cl, "backend-a", "default")
	if after.Status.IndexParticipation == nil || after.Status.IndexParticipation.PrefixCount != 7 {
		t.Fatalf("participation cleared on scrape failure: %+v", after.Status.IndexParticipation)
	}
}

// TestRefreshOverlappingSelectorsAttributeToBoth: two CacheBackends with
// overlapping EngineSelector that both match the same engine pod each get
// the replica's contribution — the operator can intentionally overlap
// selectors and each backend's status reflects what it sees.
func TestRefreshOverlappingSelectorsAttributeToBoth(t *testing.T) {
	cbStrict := cbFixture("strict", "default", map[string]string{"app": "vllm", "model": "llama"})
	cbLoose := cbFixture("loose", "default", map[string]string{"app": "vllm"})
	podMatch := enginePod("vllm-0", "default", map[string]string{"app": "vllm", "model": "llama"})
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-0", Tenant: "default", PrefixCount: 4, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbStrict, cbLoose},
		[]*corev1.Pod{podMatch},
		&served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	for _, name := range []string{"strict", "loose"} {
		got := getBackendDirect(t, cl, name, "default")
		if got.Status.IndexParticipation == nil || got.Status.IndexParticipation.PrefixCount != 4 {
			t.Fatalf("%s participation = %+v, want prefixCount 4", name, got.Status.IndexParticipation)
		}
	}
}

func TestRefreshCreatesSingletonEvenWhenServerDown(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheIndex{}).
		Build()

	// Unreachable snapshot endpoint (connection refused), short timeout.
	p := &CacheIndexPoller{
		Client:      cl,
		SnapshotURL: "http://127.0.0.1:1/snapshot",
		HTTPClient:  &http.Client{Timeout: time.Second},
		Name:        "cluster-default",
	}

	// refresh reports the scrape error...
	if err := p.refresh(context.Background()); err == nil {
		t.Fatal("expected an error when the snapshot endpoint is unreachable")
	}
	// ...but the singleton CR must still have been created (empty status).
	var ci cachev1alpha1.CacheIndex
	if err := cl.Get(context.Background(), types.NamespacedName{Name: "cluster-default"}, &ci); err != nil {
		t.Fatalf("singleton CacheIndex should exist even when the server is down: %v", err)
	}
}

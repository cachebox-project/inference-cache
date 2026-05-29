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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

// buildPollerWithBackends spins up a fake client + httptest server and returns
// a poller wired to both. The served Snapshot is read under the supplied mutex.
func buildPollerWithBackends(t *testing.T, backends []*cachev1alpha1.CacheBackend, served *index.Snapshot, mu *sync.Mutex) (*CacheIndexPoller, client.Client, *httptest.Server) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	builder := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheIndex{}, &cachev1alpha1.CacheBackend{})
	for _, cb := range backends {
		builder = builder.WithObjects(cb)
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

func cbFixture(name, ns string) *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
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

// TestRefreshProjectsParticipationPerBackend (happy path): two CacheBackends,
// a snapshot with replicas owned by each → indexParticipation reflects the
// per-backend sum/max.
func TestRefreshProjectsParticipationPerBackend(t *testing.T) {
	cbA := cbFixture("backend-a", "default")
	cbB := cbFixture("backend-b", "default")

	t1 := time.Unix(1_700_000_000, 0).UTC()
	t2 := time.Unix(1_700_000_500, 0).UTC()
	t3 := time.Unix(1_700_000_100, 0).UTC()

	var mu sync.Mutex
	served := index.Snapshot{
		TotalPrefixes: 6,
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "backend-a-0", PrefixCount: 2, LastEventAt: t1},
			{ReplicaID: "backend-a-1", PrefixCount: 3, LastEventAt: t2},
			{ReplicaID: "backend-b-0", PrefixCount: 1, LastEventAt: t3},
		},
	}
	p, cl, srv := buildPollerWithBackends(t, []*cachev1alpha1.CacheBackend{cbA, cbB}, &served, &mu)
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

// TestRefreshNoEventsForBackendKeepsZeroParticipation: a CacheBackend with no
// matching replicas must NOT have indexParticipation written (no error and no
// fabricated zero state — the field stays nil so the printer column shows the
// CRD default until a real signal arrives).
func TestRefreshNoEventsForBackendKeepsZeroParticipation(t *testing.T) {
	cbA := cbFixture("backend-a", "default")
	cbQuiet := cbFixture("backend-quiet", "default")
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "backend-a-0", PrefixCount: 1, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithBackends(t, []*cachev1alpha1.CacheBackend{cbA, cbQuiet}, &served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	q := getBackendDirect(t, cl, "backend-quiet", "default")
	if q.Status.IndexParticipation != nil {
		t.Fatalf("backend-quiet should have no indexParticipation, got %+v", q.Status.IndexParticipation)
	}
	a := getBackendDirect(t, cl, "backend-a", "default")
	if a.Status.IndexParticipation == nil || a.Status.IndexParticipation.PrefixCount != 1 {
		t.Fatalf("backend-a participation = %+v, want prefixCount 1", a.Status.IndexParticipation)
	}
}

// TestRefreshIgnoresUnownedReplicas: a snapshot replica whose id doesn't
// prefix-match any backend must be silently dropped — no panic, and other
// backends still update.
func TestRefreshIgnoresUnownedReplicas(t *testing.T) {
	cbA := cbFixture("backend-a", "default")
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "backend-a-0", PrefixCount: 2, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
			{ReplicaID: "orphan-7", PrefixCount: 99, LastEventAt: time.Unix(1_700_000_999, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithBackends(t, []*cachev1alpha1.CacheBackend{cbA}, &served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	a := getBackendDirect(t, cl, "backend-a", "default")
	if a.Status.IndexParticipation == nil || a.Status.IndexParticipation.PrefixCount != 2 {
		t.Fatalf("backend-a should ignore orphan replica; got participation %+v", a.Status.IndexParticipation)
	}
}

// TestRefreshNoChurnOnIdenticalSnapshot (no-churn invariant): two consecutive
// identical snapshots must produce exactly one CacheBackend status write —
// asserted via resource-version stability on the second tick.
func TestRefreshNoChurnOnIdenticalSnapshot(t *testing.T) {
	cbA := cbFixture("backend-a", "default")
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "backend-a-0", PrefixCount: 4, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithBackends(t, []*cachev1alpha1.CacheBackend{cbA}, &served, &mu)
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

// TestRefreshHitRateNilPassthrough: snapshot replicas all report HitRate=0
// (the "no value yet" signal until the stats reporter ships) →
// indexParticipation.HitRate stays nil. Don't fabricate a 0.
func TestRefreshHitRateNilPassthrough(t *testing.T) {
	cbA := cbFixture("backend-a", "default")
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "backend-a-0", PrefixCount: 2, HitRate: 0, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
			{ReplicaID: "backend-a-1", PrefixCount: 3, HitRate: 0, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithBackends(t, []*cachev1alpha1.CacheBackend{cbA}, &served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	a := getBackendDirect(t, cl, "backend-a", "default")
	if a.Status.IndexParticipation == nil {
		t.Fatal("backend-a participation should be populated (prefixCount + lastEventAt)")
	}
	if a.Status.IndexParticipation.HitRate != nil {
		t.Fatalf("HitRate should be nil while the stats reporter is unshipped, got %q", *a.Status.IndexParticipation.HitRate)
	}
}

// TestRefreshScrapeFailureDoesNotClearParticipation (fail-soft): once
// indexParticipation is published, a failing /snapshot scrape must NOT
// clear it. Tested by seeding a status, then closing the server.
func TestRefreshScrapeFailureDoesNotClearParticipation(t *testing.T) {
	cbA := cbFixture("backend-a", "default")
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "backend-a-0", PrefixCount: 7, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithBackends(t, []*cachev1alpha1.CacheBackend{cbA}, &served, &mu)

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

// TestRefreshLongestPrefixWinsOnAmbiguousNames: backends "cb" and "cb-a"
// — a replica named "cb-a-0" must be attributed to "cb-a", not "cb",
// otherwise the strings.HasPrefix matcher's longest-first ordering is broken.
func TestRefreshLongestPrefixWinsOnAmbiguousNames(t *testing.T) {
	cbShort := cbFixture("cb", "default")
	cbLong := cbFixture("cb-a", "default")
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "cb-a-0", PrefixCount: 3, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithBackends(t, []*cachev1alpha1.CacheBackend{cbShort, cbLong}, &served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	short := getBackendDirect(t, cl, "cb", "default")
	if short.Status.IndexParticipation != nil {
		t.Fatalf("short-name backend should not steal replica from cb-a, got %+v", short.Status.IndexParticipation)
	}
	long := getBackendDirect(t, cl, "cb-a", "default")
	if long.Status.IndexParticipation == nil || long.Status.IndexParticipation.PrefixCount != 3 {
		t.Fatalf("cb-a participation = %+v, want prefixCount 3", long.Status.IndexParticipation)
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

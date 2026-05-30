package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

	got, err := fetchSnapshot(context.Background(), srv.Client(), srv.URL, "")
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

	if _, err := fetchSnapshot(context.Background(), srv.Client(), srv.URL, ""); err == nil {
		t.Fatal("expected error on non-200 snapshot response")
	}
}

// TestFetchSnapshotSendsBearerToken proves the scrape carries the SA token in
// the Authorization header — that's what the server's TokenReview middleware
// looks at. Pins the over-the-wire contract between the poller and the
// auth-gated /snapshot listener.
func TestFetchSnapshotSendsBearerToken(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(index.Snapshot{TotalPrefixes: 1})
	}))
	defer srv.Close()

	if _, err := fetchSnapshot(context.Background(), srv.Client(), srv.URL, "test-token"); err != nil {
		t.Fatalf("fetchSnapshot: %v", err)
	}
	if gotAuth != "Bearer test-token" {
		t.Fatalf("Authorization header = %q, want %q", gotAuth, "Bearer test-token")
	}
}

// TestBearerToken_ReadsAndTrimsFile checks that the poller re-reads its
// projected SA token from disk and trims trailing whitespace — the projected
// token kubelet writes ends with a newline.
func TestBearerToken_ReadsAndTrimsFile(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/token"
	if err := os.WriteFile(path, []byte("a-real-token\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	p := &CacheIndexPoller{BearerTokenPath: path}
	got, err := p.bearerToken()
	if err != nil {
		t.Fatalf("bearerToken() unexpected error: %v", err)
	}
	if got != "a-real-token" {
		t.Fatalf("bearerToken() = %q, want %q", got, "a-real-token")
	}

	// Missing path → ("", nil): treated as "no token configured" so a
	// local out-of-cluster run still proceeds (server rejects 401, poller
	// stays running). This must NOT be conflated with a real read error.
	p.BearerTokenPath = dir + "/does-not-exist"
	got, err = p.bearerToken()
	if err != nil {
		t.Fatalf("bearerToken() on missing file: unexpected error %v", err)
	}
	if got != "" {
		t.Fatalf("bearerToken() on missing file = %q, want \"\"", got)
	}
}

// TestBearerToken_UnreadableFileReturnsError pins the should-fix semantics:
// a real read failure (file present but unreadable) surfaces as an error so
// the operator's log shows the path + cause, instead of silently degrading
// to an unauth scrape that the server rejects as a generic 401.
func TestBearerToken_UnreadableFileReturnsError(t *testing.T) {
	// chmod-0 is a portable way to make a present file unreadable for the
	// running user (unless we're root — skip then).
	if os.Geteuid() == 0 {
		t.Skip("running as root; chmod 0 does not deny read")
	}
	dir := t.TempDir()
	path := dir + "/token"
	if err := os.WriteFile(path, []byte("ignored"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	if err := os.Chmod(path, 0); err != nil {
		t.Fatalf("chmod token: %v", err)
	}

	p := &CacheIndexPoller{BearerTokenPath: path}
	got, err := p.bearerToken()
	if err == nil {
		t.Fatalf("bearerToken() on unreadable file: got %q + nil, want error", got)
	}
	if got != "" {
		t.Fatalf("bearerToken() on unreadable file = %q, want \"\"", got)
	}
	if !strings.Contains(err.Error(), path) {
		t.Fatalf("bearerToken() error %q does not mention path %q", err, path)
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

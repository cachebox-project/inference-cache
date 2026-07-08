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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/prometheus/client_golang/prometheus/testutil"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
	"github.com/cachebox-project/inference-cache/pkg/index"
)

func TestBuildCacheIndexStatus(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	snap := index.Snapshot{
		TotalPrefixes: 5,
		HotPrefixes:   0,
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "r1", Tenant: "ns-a", CacheMemoryBytes: 100, HitRate: 0.8, Pressure: 0.5, LastUpdate: now, StatsReported: true},
		},
		Tenants: []index.TenantSnapshot{
			// MemoryUsed is non-zero here on purpose: it simulates an older /
			// skewed server still reporting the deprecated, double-counted
			// per-tenant memory. The controller must DISCARD it (hard-zero),
			// asserted below.
			{TenantID: "t1", IndexEntries: 5, HitRate: 0.8, HitRateReported: true, MemoryUsed: 777},
		},
	}

	st := buildCacheIndexStatus(snap, "http://server/snapshot", now)

	if st.Prefixes.Summary.Total != 5 || st.Prefixes.Summary.Hot != 0 {
		t.Fatalf("prefixes = %+v, want {Total:5 Hot:0}", st.Prefixes)
	}
	if st.ObservedServer != "http://server/snapshot" {
		t.Fatalf("observedServer = %q", st.ObservedServer)
	}
	if len(st.Replicas) != 1 || st.Replicas[0].ID != "r1" || st.Replicas[0].Tenant != "ns-a" ||
		st.Replicas[0].HitRate == nil || *st.Replicas[0].HitRate != "0.8" || st.Replicas[0].Pressure != "0.5" {
		t.Fatalf("replica = %+v (hitRate=%v), want id r1 tenant ns-a hitRate 0.8 pressure 0.5", st.Replicas[0], derefStr(st.Replicas[0].HitRate))
	}
	if len(st.Tenants) != 1 || st.Tenants[0].ID != "t1" ||
		st.Tenants[0].IndexEntries == nil || *st.Tenants[0].IndexEntries != 5 ||
		st.Tenants[0].HitRate == nil || *st.Tenants[0].HitRate != "0.8" {
		t.Fatalf("tenant = %+v (indexEntries=%v hitRate=%v), want id t1 indexEntries 5 hitRate 0.8", st.Tenants[0], st.Tenants[0].IndexEntries, derefStr(st.Tenants[0].HitRate))
	}
	// Deprecated memoryUsed must be hard-zeroed regardless of what the snapshot
	// carried (skew-compat: an older server may still report a non-zero,
	// double-counted value — the controller is authoritative for keeping it 0).
	if st.Tenants[0].MemoryUsed != 0 {
		t.Fatalf("tenant MemoryUsed = %d, want 0 (controller must discard the snapshot's deprecated value)", st.Tenants[0].MemoryUsed)
	}
	// The per-tenant indexEntries sum to prefixes.summary.total (single tenant here).
	var sum int64
	for _, tn := range st.Tenants {
		if tn.IndexEntries != nil {
			sum += *tn.IndexEntries
		}
	}
	if sum != int64(st.Prefixes.Summary.Total) {
		t.Fatalf("Σ tenants[].indexEntries = %d, want == prefixes.summary.total %d", sum, st.Prefixes.Summary.Total)
	}
}

// derefStr is a test helper that renders a *string for failure messages,
// showing "<nil>" for the unreported sentinel.
func derefStr(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

// TestBuildCacheIndexStatusFiltersPrefixOnlyAndPicksWinner asserts the two
// guards on the cluster-wide CacheIndex.status.replicas:
//   - A row with no stats yet (LastUpdate zero) is dropped, so the v1alpha1
//     surface doesn't fabricate hitRate/pressure/memory zeros.
//   - On an id collision across tenants, the lexicographically-later tenant
//     wins deterministically (preserves listMapKey=id uniqueness).
func TestBuildCacheIndexStatusFiltersPrefixOnlyAndPicksWinner(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	snap := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-0", Tenant: "ns-a", CacheMemoryBytes: 100, LastUpdate: now, StatsReported: true},
			{ReplicaID: "vllm-0", Tenant: "ns-b", CacheMemoryBytes: 200, LastUpdate: now, StatsReported: true},
			{ReplicaID: "prefix-only", Tenant: "ns-a", PrefixCount: 5},
		},
	}
	st := buildCacheIndexStatus(snap, "http://server/snapshot", now)
	if len(st.Replicas) != 1 {
		t.Fatalf("replicas = %+v, want 1 row (prefix-only filtered, collision deduped)", st.Replicas)
	}
	got := st.Replicas[0]
	if got.ID != "vllm-0" || got.Tenant != "ns-b" || got.CacheMemoryBytes != 200 {
		t.Fatalf("collision winner = %+v, want id vllm-0 tenant ns-b memory 200 (lex-later tenant wins)", got)
	}
}

// TestBuildCacheIndexStatusHitRateNilWhenUnreported pins the harmonized
// pointer-ness: the cluster-aggregate HitRate (*string) stays nil when the
// stats reporter has not emitted yet, so a not-yet-reported replica / tenant is
// distinguishable from an observed 0% hit rate — matching the per-backend
// CacheBackend.status.indexParticipation.hitRate and per-tenant
// CacheTenant.status.indexEntries surfaces. IndexEntries (*int64) is always
// present on an emitted tenant row (a real observed count), never a fabricated
// nil.
func TestBuildCacheIndexStatusHitRateNilWhenUnreported(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	snap := index.Snapshot{
		TotalPrefixes: 4,
		Replicas: []index.ReplicaSnapshot{
			// Emitted row (non-zero LastUpdate) but the stats reporter has not
			// pinged: HitRate must stay nil, not "0".
			{ReplicaID: "r-unreported", Tenant: "ns-a", LastUpdate: now, StatsReported: false},
			// Stats-bearing replica: HitRate present.
			{ReplicaID: "r-reported", Tenant: "ns-a", HitRate: 0, LastUpdate: now, StatsReported: true},
		},
		Tenants: []index.TenantSnapshot{
			// Tenant with index entries but no reported stats: HitRate nil,
			// IndexEntries present (a real observed 0-vs-N count).
			{TenantID: "t-unreported", IndexEntries: 4, HitRate: 0, HitRateReported: false},
			// Tenant whose replicas reported a real 0% mean: HitRate present as
			// "0", distinct from the nil above.
			{TenantID: "t-reported", IndexEntries: 0, HitRate: 0, HitRateReported: true},
		},
	}

	st := buildCacheIndexStatus(snap, "http://server/snapshot", now)

	byReplica := map[string]cachev1alpha1.ReplicaCacheStatus{}
	for _, r := range st.Replicas {
		byReplica[r.ID] = r
	}
	if r := byReplica["r-unreported"]; r.HitRate != nil {
		t.Fatalf("r-unreported HitRate = %q, want nil (stats reporter has not pinged)", *r.HitRate)
	}
	if r := byReplica["r-reported"]; r.HitRate == nil || *r.HitRate != "0" {
		t.Fatalf("r-reported HitRate = %v, want a real \"0\" (observed 0%%, distinct from unreported nil)", derefStr(r.HitRate))
	}

	byTenant := map[string]cachev1alpha1.TenantCacheStatus{}
	for _, tn := range st.Tenants {
		byTenant[tn.ID] = tn
	}
	tu := byTenant["t-unreported"]
	if tu.HitRate != nil {
		t.Fatalf("t-unreported HitRate = %q, want nil (no replica reported stats)", *tu.HitRate)
	}
	if tu.IndexEntries == nil || *tu.IndexEntries != 4 {
		t.Fatalf("t-unreported IndexEntries = %v, want a present 4 (observed count, never fabricated nil)", tu.IndexEntries)
	}
	tr := byTenant["t-reported"]
	if tr.HitRate == nil || *tr.HitRate != "0" {
		t.Fatalf("t-reported HitRate = %v, want a real \"0\" (observed 0%% mean)", derefStr(tr.HitRate))
	}
	if tr.IndexEntries == nil || *tr.IndexEntries != 0 {
		t.Fatalf("t-reported IndexEntries = %v, want a present 0 (observed zero, not nil)", tr.IndexEntries)
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
		Replicas: []cachev1alpha1.ReplicaCacheStatus{{ID: "r1", CacheMemoryBytes: 100, HitRate: ptrTo("0.8")}},
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
	served := index.Snapshot{TotalPrefixes: 3, Replicas: []index.ReplicaSnapshot{{ReplicaID: "r1", CacheMemoryBytes: 100, HitRate: 0.8, LastUpdate: time.Unix(1_700_000_000, 0)}}}
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
	served = index.Snapshot{TotalPrefixes: 9, Replicas: []index.ReplicaSnapshot{{ReplicaID: "r1", CacheMemoryBytes: 500, HitRate: 0.9, LastUpdate: time.Unix(1_700_000_100, 0)}}}
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

// enginePodInjectedBy builds an engine Pod with the webhook's "injected-by"
// annotation stamped to <ns>/<backendName>. The poller treats this as the
// authoritative attribution signal, ignoring the EngineSelector fallback.
func enginePodInjectedBy(name, ns, backendNS, backendName string, labels map[string]string) *corev1.Pod {
	p := enginePod(name, ns, labels)
	p.Annotations = map[string]string{
		podwebhook.AnnotationInjectedBy: backendNS + "/" + backendName,
	}
	return p
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

// TestRefreshHitRateStaysNil: per-backend indexParticipation.hitRate stays nil
// even when the snapshot's replicas carry a hit rate AND the StatsReported
// presence bit. The cluster-aggregate CacheIndex projection consumes that bit,
// but the per-backend path aggregates many replicas onto one backend with no
// defined backend-level hit-rate reduction, so backend hit-rate aggregation is
// deliberately deferred to a follow-up; a fabricated value would mislead
// operators. This pins that deferral so a future change to emit it is a
// deliberate, tested one.
func TestRefreshHitRateStaysNil(t *testing.T) {
	cbA := cbFixture("backend-a", "default", map[string]string{"app": "vllm-a"})
	podA0 := enginePod("vllm-a-0", "default", map[string]string{"app": "vllm-a"})
	podA1 := enginePod("vllm-a-1", "default", map[string]string{"app": "vllm-a"})
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-a-0", Tenant: "default", PrefixCount: 2, HitRate: 0.75, StatsReported: true, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
			{ReplicaID: "vllm-a-1", Tenant: "default", PrefixCount: 3, HitRate: 0.85, StatsReported: true, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
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

// TestRefreshT2HitRatePresence: the tier-2 (external offload) hit-rate is
// presence-aware via the external query-token count. Three backends cover the
// three states an operator must be able to tell apart:
//   - healthy: queried + reloads served       -> "0.75"
//   - broken : queried but zero reloads served -> "0"  (silent-degradation signal)
//   - cold   : never queried                   -> nil  (must NOT read as 0)
func TestRefreshT2HitRatePresence(t *testing.T) {
	cbH := cbFixture("t2-healthy", "default", map[string]string{"app": "vllm-h"})
	cbB := cbFixture("t2-broken", "default", map[string]string{"app": "vllm-b"})
	cbC := cbFixture("t2-cold", "default", map[string]string{"app": "vllm-c"})
	podH := enginePod("vllm-h-0", "default", map[string]string{"app": "vllm-h"})
	podB := enginePod("vllm-b-0", "default", map[string]string{"app": "vllm-b"})
	podC := enginePod("vllm-c-0", "default", map[string]string{"app": "vllm-c"})
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-h-0", Tenant: "default", PrefixCount: 1, T2HitTokens: 750, T2QueryTokens: 1000, LastUpdate: time.Unix(1_700_000_000, 0).UTC()},
			{ReplicaID: "vllm-b-0", Tenant: "default", PrefixCount: 1, T2HitTokens: 0, T2QueryTokens: 500, LastUpdate: time.Unix(1_700_000_000, 0).UTC()},
			{ReplicaID: "vllm-c-0", Tenant: "default", PrefixCount: 1, T2HitTokens: 0, T2QueryTokens: 0, LastUpdate: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbH, cbB, cbC},
		[]*corev1.Pod{podH, podB, podC},
		&served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	assertT2 := func(name string, wantNil bool, want string) {
		t.Helper()
		ip := getBackendDirect(t, cl, name, "default").Status.IndexParticipation
		if ip == nil {
			t.Fatalf("%s: indexParticipation nil", name)
		}
		switch {
		case wantNil && ip.T2HitRate != nil:
			t.Fatalf("%s: t2HitRate = %q, want nil (tier-2 not exercised)", name, *ip.T2HitRate)
		case !wantNil && (ip.T2HitRate == nil || *ip.T2HitRate != want):
			t.Fatalf("%s: t2HitRate = %v, want %q", name, ip.T2HitRate, want)
		}
	}
	assertT2("t2-healthy", false, "0.75")
	assertT2("t2-broken", false, "0")
	assertT2("t2-cold", true, "")
}

// TestRefreshT2HitRateGauge: the poller mirrors t2HitRate onto the
// inferencecache_backend_t2_hit_rate gauge (the Alertmanager surface), present
// only for exercised backends, and prunes a series when its backend drains so a
// stale 0 can't trip a false alert.
func TestRefreshT2HitRateGauge(t *testing.T) {
	resetBackendT2HitRateForTest()
	defer resetBackendT2HitRateForTest() // leave global gauge state clean for the next metric test
	cbH := cbFixture("t2-h", "default", map[string]string{"app": "vh"})
	cbB := cbFixture("t2-b", "default", map[string]string{"app": "vb"})
	cbC := cbFixture("t2-c", "default", map[string]string{"app": "vc"})
	podH := enginePod("vh-0", "default", map[string]string{"app": "vh"})
	podB := enginePod("vb-0", "default", map[string]string{"app": "vb"})
	podC := enginePod("vc-0", "default", map[string]string{"app": "vc"})
	var mu sync.Mutex
	served := index.Snapshot{Replicas: []index.ReplicaSnapshot{
		{ReplicaID: "vh-0", Tenant: "default", PrefixCount: 1, T2HitTokens: 750, T2QueryTokens: 1000, LastUpdate: time.Unix(1_700_000_000, 0).UTC()},
		{ReplicaID: "vb-0", Tenant: "default", PrefixCount: 1, T2HitTokens: 0, T2QueryTokens: 500, LastUpdate: time.Unix(1_700_000_000, 0).UTC()},
		{ReplicaID: "vc-0", Tenant: "default", PrefixCount: 1, T2HitTokens: 0, T2QueryTokens: 0, LastUpdate: time.Unix(1_700_000_000, 0).UTC()},
	}}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbH, cbB, cbC},
		[]*corev1.Pod{podH, podB, podC}, &served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	// CollectAndCompare gathers what the poller actually exported and — unlike
	// WithLabelValues — never creates the child it reads, so a series that
	// production failed to emit is caught here, not silently materialized at 0.
	// It pins the EXACT set: healthy 0.75, broken 0, cold (never queried) absent.
	wantSteady := `# HELP inferencecache_backend_t2_hit_rate Tier-2 (external offload) reload hit-rate per CacheBackend in [0,1]; the series is present only once the tier is exercised, and 0 means it is wired but serving zero reloads.
# TYPE inferencecache_backend_t2_hit_rate gauge
inferencecache_backend_t2_hit_rate{backend="default/t2-b"} 0
inferencecache_backend_t2_hit_rate{backend="default/t2-h"} 0.75
`
	if err := testutil.CollectAndCompare(backendT2HitRate, strings.NewReader(wantSteady)); err != nil {
		t.Fatalf("steady-state gauge mismatch (want t2-h=0.75, t2-b=0, t2-c absent): %v", err)
	}

	// The query-token COUNTER (the activity signal BackendT2Degraded rate()s)
	// accumulates only positive per-poll deltas; the FIRST observation establishes
	// the baseline at 0 so a pre-existing cumulative cannot spike rate().
	wantSteadyQ := `# HELP inferencecache_backend_t2_query_tokens_total Monotonic count of tier-2 (external offload) query tokens observed per CacheBackend (positive per-poll deltas of the engine's cumulative; drops from replica/tenant churn or restart are clamped out). Use rate() to gate on tier-2 activity.
# TYPE inferencecache_backend_t2_query_tokens_total counter
inferencecache_backend_t2_query_tokens_total{backend="default/t2-b"} 0
inferencecache_backend_t2_query_tokens_total{backend="default/t2-h"} 0
`
	if err := testutil.CollectAndCompare(backendT2QueryTokensTotal, strings.NewReader(wantSteadyQ)); err != nil {
		t.Fatalf("steady-state query-token counter mismatch (first obs -> baseline 0 for t2-h/t2-b, t2-c absent): %v", err)
	}

	// The broken backend's replica leaves the index snapshot (drains). Because the
	// rate is cumulative, idleness alone would NOT prune it — drop-out does. Its
	// series must be pruned, not left at a stale 0.
	mu.Lock()
	served.Replicas = []index.ReplicaSnapshot{
		{ReplicaID: "vh-0", Tenant: "default", PrefixCount: 1, T2HitTokens: 1200, T2QueryTokens: 1500, LastUpdate: time.Unix(1_700_000_100, 0).UTC()},
	}
	mu.Unlock()
	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh 2: %v", err)
	}
	wantDrained := `# HELP inferencecache_backend_t2_hit_rate Tier-2 (external offload) reload hit-rate per CacheBackend in [0,1]; the series is present only once the tier is exercised, and 0 means it is wired but serving zero reloads.
# TYPE inferencecache_backend_t2_hit_rate gauge
inferencecache_backend_t2_hit_rate{backend="default/t2-h"} 0.8
`
	if err := testutil.CollectAndCompare(backendT2HitRate, strings.NewReader(wantDrained)); err != nil {
		t.Fatalf("after drain, gauge mismatch (want t2-b pruned, t2-h=0.8): %v", err)
	}
	// vh-0's query tokens grew 1000 -> 1500, so the counter accumulated the +500
	// delta; t2-b drained and was pruned.
	wantDrainedQ := `# HELP inferencecache_backend_t2_query_tokens_total Monotonic count of tier-2 (external offload) query tokens observed per CacheBackend (positive per-poll deltas of the engine's cumulative; drops from replica/tenant churn or restart are clamped out). Use rate() to gate on tier-2 activity.
# TYPE inferencecache_backend_t2_query_tokens_total counter
inferencecache_backend_t2_query_tokens_total{backend="default/t2-h"} 500
`
	if err := testutil.CollectAndCompare(backendT2QueryTokensTotal, strings.NewReader(wantDrainedQ)); err != nil {
		t.Fatalf("after drain, query-token counter mismatch (want t2-h=500 accumulated, t2-b pruned): %v", err)
	}

	// Deleting every backend -> empty list -> all gauge series pruned. The
	// empty-list early return must still reconcile, else a stale 0 lingers and
	// keeps alerting after the fleet is gone.
	for _, cb := range []*cachev1alpha1.CacheBackend{cbH, cbB, cbC} {
		if err := cl.Delete(context.Background(), cb); err != nil {
			t.Fatalf("delete backend %s: %v", cb.Name, err)
		}
	}
	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh 3: %v", err)
	}
	if n := testutil.CollectAndCount(backendT2HitRate); n != 0 {
		t.Fatalf("after all backends deleted, series = %d, want 0 (empty-list path must prune)", n)
	}
	if n := testutil.CollectAndCount(backendT2QueryTokensTotal); n != 0 {
		t.Fatalf("after all backends deleted, query-token series = %d, want 0", n)
	}
}

// TestReconcileBackendT2HitRateSeries pins the stale-series lifecycle (for BOTH
// per-backend tier-2 metrics) the false-alert guarantee depends on: a DELETED backend's series is
// pruned, a DRAINED (present, not-exercised, not-tainted) backend's series is
// pruned, and a TAINTED namespace's series is PRESERVED (a transient API error
// must not drop a series and silently clear an alert).
func TestReconcileBackendT2HitRateSeries(t *testing.T) {
	resetBackendT2HitRateForTest()
	defer resetBackendT2HitRateForTest() // leave global gauge state clean for the next metric test
	kA, kB, kD := t2Key{"ns", "a"}, t2Key{"ns", "b"}, t2Key{"ns", "d"}
	// set seeds BOTH per-backend tier-2 series for a key, so the lifecycle helper
	// can be checked to prune them together (it owns both).
	set := func(k t2Key, v float64) {
		backendT2HitRate.WithLabelValues(k.label()).Set(v)
		backendT2QueryTokensTotal.WithLabelValues(k.label()).Add(1)
	}
	keys := func(ks ...t2Key) map[t2Key]struct{} {
		m := map[t2Key]struct{}{}
		for _, k := range ks {
			m[k] = struct{}{}
		}
		return m
	}
	// assertBoth pins that the helper keeps backendT2HitRate and
	// backendT2QueryTokensTotal in lockstep — a regression that pruned one but not
	// the other would leave a stale series that still trips (or masks) an alert.
	assertBoth := func(tick string, want int) {
		t.Helper()
		if n := testutil.CollectAndCount(backendT2HitRate); n != want {
			t.Fatalf("%s hit-rate series = %d, want %d", tick, n, want)
		}
		if n := testutil.CollectAndCount(backendT2QueryTokensTotal); n != want {
			t.Fatalf("%s query-token series = %d, want %d (helper must prune both metrics)", tick, n, want)
		}
	}

	// Tick 1: A, B, D all exercised + present -> all tracked, none pruned.
	set(kA, 0.9)
	set(kB, 0)
	set(kD, 0.5)
	reconcileBackendT2HitRateSeries(keys(kA, kB, kD), keys(kA, kB, kD), nil)
	assertBoth("tick1", 3)

	// Tick 2: A re-exercised; B present but its namespace is TAINTED (must be
	// preserved); D is DELETED (absent from present -> pruned even under taint).
	set(kA, 0.8)
	reconcileBackendT2HitRateSeries(keys(kA), keys(kA, kB), map[string]struct{}{"ns": {}})
	assertBoth("tick2 (D pruned, B taint-preserved)", 2)

	// Tick 3: ns no longer tainted; B present but not exercised (drained) -> pruned.
	set(kA, 0.8)
	reconcileBackendT2HitRateSeries(keys(kA), keys(kA, kB), nil)
	assertBoth("tick3 (B drained)", 1)
}

// TestT2QueryDelta pins the clamp-and-baseline logic the BackendT2Degraded
// activity gate depends on: the monotonic query counter adds only positive
// per-tick deltas, treats the first observation as a baseline (no spike from the
// pre-existing cumulative), and clamps a drop (replica/tenant churn or engine
// restart) to 0 — never negative.
func TestT2QueryDelta(t *testing.T) {
	resetBackendT2HitRateForTest()
	defer resetBackendT2HitRateForTest()
	k := t2Key{"ns", "x"}
	if d := t2QueryDelta(k, 1000); d != 0 {
		t.Fatalf("first-observation delta = %d, want 0 (baseline, no spike)", d)
	}
	if d := t2QueryDelta(k, 1500); d != 500 {
		t.Fatalf("growth delta = %d, want 500", d)
	}
	if d := t2QueryDelta(k, 1400); d != 0 {
		t.Fatalf("drop delta = %d, want 0 (clamped — churn/restart must not go negative)", d)
	}
	if d := t2QueryDelta(k, 1400); d != 0 {
		t.Fatalf("flat delta = %d, want 0", d)
	}
	if d := t2QueryDelta(k, 1600); d != 200 {
		t.Fatalf("resumed-growth delta = %d, want 200 (from the post-drop baseline 1400)", d)
	}
}

// TestRefreshT2HitRateCumulativeAfterRegression documents the intentional
// lifetime-cumulative semantics: a backend that served reloads and then regresses
// to zero reloads under continuing queries keeps a NON-ZERO hit-rate, so the
// `== 0` T2Degraded condition / BackendT2Degraded alert (which target a tier that
// never served a reload) do not trip. A mid-life regression is caught instead by
// the windowed per-pod LMCacheT2NoHits alert.
func TestRefreshT2HitRateCumulativeAfterRegression(t *testing.T) {
	resetBackendT2HitRateForTest()
	defer resetBackendT2HitRateForTest()
	cb := cbFixture("t2-r", "default", map[string]string{"app": "vr"})
	pod := enginePod("vr-0", "default", map[string]string{"app": "vr"})
	var mu sync.Mutex
	served := index.Snapshot{Replicas: []index.ReplicaSnapshot{
		{ReplicaID: "vr-0", Tenant: "default", PrefixCount: 1, T2HitTokens: 750, T2QueryTokens: 1000, LastUpdate: time.Unix(1_700_000_000, 0).UTC()},
	}}
	p, _, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cb}, []*corev1.Pod{pod}, &served, &mu)
	defer srv.Close()
	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh 1: %v", err)
	}
	// Healthy: 750/1000 = 0.75. Now queries climb (1000 -> 5000) while hits stay
	// flat at 750 — the tier stopped serving reloads.
	mu.Lock()
	served.Replicas = []index.ReplicaSnapshot{
		{ReplicaID: "vr-0", Tenant: "default", PrefixCount: 1, T2HitTokens: 750, T2QueryTokens: 5000, LastUpdate: time.Unix(1_700_000_100, 0).UTC()},
	}
	mu.Unlock()
	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh 2: %v", err)
	}
	// The cumulative ratio decays (750/5000 = 0.15) but stays > 0, so the `== 0`
	// surfaces do NOT fire on this mid-life regression. Intentional.
	want := `# HELP inferencecache_backend_t2_hit_rate Tier-2 (external offload) reload hit-rate per CacheBackend in [0,1]; the series is present only once the tier is exercised, and 0 means it is wired but serving zero reloads.
# TYPE inferencecache_backend_t2_hit_rate gauge
inferencecache_backend_t2_hit_rate{backend="default/t2-r"} 0.15
`
	if err := testutil.CollectAndCompare(backendT2HitRate, strings.NewReader(want)); err != nil {
		t.Fatalf("late-regression hit-rate must stay > 0 (cumulative 750/5000 = 0.15): %v", err)
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

// TestRefreshOverlappingSelectorsFirstNameWins: two CacheBackends with
// overlapping EngineSelector that both match the same engine pod — the
// poller must attribute only to the deterministic "first by name" backend,
// mirroring the webhook's one-pod-one-backend wiring rule. Attributing to
// both would tell operators a backend is contributing when its engine was
// actually wired to the other backend's endpoint.
func TestRefreshOverlappingSelectorsFirstNameWins(t *testing.T) {
	cbAlpha := cbFixture("alpha", "default", map[string]string{"app": "vllm"})
	cbBeta := cbFixture("beta", "default", map[string]string{"app": "vllm", "model": "llama"})
	podMatch := enginePod("vllm-0", "default", map[string]string{"app": "vllm", "model": "llama"})
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-0", Tenant: "default", PrefixCount: 4, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbAlpha, cbBeta},
		[]*corev1.Pod{podMatch},
		&served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	alpha := getBackendDirect(t, cl, "alpha", "default")
	if alpha.Status.IndexParticipation == nil || alpha.Status.IndexParticipation.PrefixCount != 4 {
		t.Fatalf("alpha (first by name) should win, got %+v", alpha.Status.IndexParticipation)
	}
	beta := getBackendDirect(t, cl, "beta", "default")
	if beta.Status.IndexParticipation == nil || beta.Status.IndexParticipation.PrefixCount != 0 {
		t.Fatalf("beta should get zero-state, got %+v", beta.Status.IndexParticipation)
	}
}

// TestRefreshAnnotationOwnedBackendWithNoSelector: a pod's injected-by
// annotation points at a CacheBackend that itself has no EngineSelector
// (e.g. its selector was cleared after admission). The poller must NOT
// panic dereferencing perBackend and must attribute the replica to the
// annotation-named backend regardless of selector state.
func TestRefreshAnnotationOwnedBackendWithNoSelector(t *testing.T) {
	cbNoSelector := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: "default"},
	}
	cbOther := cbFixture("other", "default", map[string]string{"app": "vllm"})
	pod := enginePodInjectedBy("vllm-0", "default", "default", "owner", map[string]string{"app": "vllm"})
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-0", Tenant: "default", PrefixCount: 3, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbNoSelector, cbOther},
		[]*corev1.Pod{pod},
		&served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	owner := getBackendDirect(t, cl, "owner", "default")
	if owner.Status.IndexParticipation == nil || owner.Status.IndexParticipation.PrefixCount != 3 {
		t.Fatalf("annotation-owned backend should be attributed even without a selector, got %+v", owner.Status.IndexParticipation)
	}
	other := getBackendDirect(t, cl, "other", "default")
	if other.Status.IndexParticipation == nil || other.Status.IndexParticipation.PrefixCount != 0 {
		t.Fatalf("non-owner backend should publish zero-state, got %+v", other.Status.IndexParticipation)
	}
}

// TestRefreshAnnotationOverridesSelectorMatch: when the engine pod carries
// the webhook's `inferencecache.io/injected-by` annotation, that signal
// wins over the EngineSelector fallback. The labels in this test would
// have selected backend "alpha" (sorted first), but the annotation points
// at "beta" — beta MUST get the attribution, alpha MUST stay at zero.
func TestRefreshAnnotationOverridesSelectorMatch(t *testing.T) {
	cbAlpha := cbFixture("alpha", "default", map[string]string{"app": "vllm"})
	cbBeta := cbFixture("beta", "default", map[string]string{"app": "vllm"})
	podMatch := enginePodInjectedBy("vllm-0", "default", "default", "beta", map[string]string{"app": "vllm"})
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-0", Tenant: "default", PrefixCount: 6, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbAlpha, cbBeta},
		[]*corev1.Pod{podMatch},
		&served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	alpha := getBackendDirect(t, cl, "alpha", "default")
	if alpha.Status.IndexParticipation == nil || alpha.Status.IndexParticipation.PrefixCount != 0 {
		t.Fatalf("alpha should NOT get attribution (annotation points at beta), got %+v", alpha.Status.IndexParticipation)
	}
	beta := getBackendDirect(t, cl, "beta", "default")
	if beta.Status.IndexParticipation == nil || beta.Status.IndexParticipation.PrefixCount != 6 {
		t.Fatalf("beta should get attribution per annotation, got %+v", beta.Status.IndexParticipation)
	}
}

// TestRefreshPodLookupErrorPreservesPriorStatus: a transient apiserver
// error during the per-replica pod lookup must NOT cause the poller to
// publish a false drain. The namespace is "tainted" for the tick and
// every CacheBackend in it keeps its prior status until a clean tick
// can recompute attribution. This is the soft-state guarantee Codex
// flagged: skipping replicas after a Get failure used to under-count
// the backend's prefixCount and overwrite a real positive value with 0.
func TestRefreshPodLookupErrorPreservesPriorStatus(t *testing.T) {
	cb := cbFixture("backend", "default", map[string]string{"app": "vllm"})
	pod := enginePod("vllm-0", "default", map[string]string{"app": "vllm"})

	scheme := runtime.NewScheme()
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add cache scheme: %v", err)
	}
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}

	// First refresh: clean client, publishes a positive participation.
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-0", Tenant: "default", PrefixCount: 9, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	cleanBuilder := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheIndex{}, &cachev1alpha1.CacheBackend{}).
		WithObjects(cb, pod)
	cl := cleanBuilder.Build()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		_ = json.NewEncoder(w).Encode(served)
	}))
	defer srv.Close()
	p := &CacheIndexPoller{Client: cl, SnapshotURL: srv.URL, HTTPClient: srv.Client(), Name: "cluster-default"}
	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	got := getBackendDirect(t, cl, "backend", "default")
	if got.Status.IndexParticipation == nil || got.Status.IndexParticipation.PrefixCount != 9 {
		t.Fatalf("seed participation = %+v, want prefixCount 9", got.Status.IndexParticipation)
	}

	// Rebuild the poller with a client that fails Pod Get with a non-
	// NotFound error. Carry over the status we just published into the
	// new client by including the read-back CacheBackend as a fixture.
	failingCl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheIndex{}, &cachev1alpha1.CacheBackend{}).
		WithObjects(got, pod).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Pod); ok {
					return apierrors.NewServiceUnavailable("apiserver flake")
				}
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	failingPoller := &CacheIndexPoller{Client: failingCl, SnapshotURL: srv.URL, HTTPClient: srv.Client(), Name: "cluster-default"}

	if err := failingPoller.refresh(context.Background()); err != nil {
		t.Fatalf("refresh under failing pod Get: %v", err)
	}
	after := getBackendDirect(t, failingCl, "backend", "default")
	if after.Status.IndexParticipation == nil || after.Status.IndexParticipation.PrefixCount != 9 {
		t.Fatalf("transient pod-Get error must preserve prior participation; got %+v", after.Status.IndexParticipation)
	}
}

// TestRefreshUsesRealisticSidecarIdentityShape is the regression guard
// against the original bug Codex caught: the kvevent-subscriber sidecar
// derives replica_id from the engine POD NAME (not the CacheBackend name),
// so a CacheBackend "cache" selecting an engine Deployment "vllm" sees
// replicas like "vllm-7d9c8b6f4-abcd" — NOT "cache-0". This test uses the
// realistic shape so any future change that re-introduces a name-prefix
// matcher (or any other assumption tying replica_id to the CacheBackend
// name) will fail loudly here.
func TestRefreshUsesRealisticSidecarIdentityShape(t *testing.T) {
	// CacheBackend's name has NO relationship to the engine Deployment name.
	cache := cbFixture("cache", "default", map[string]string{"app": "vllm"})
	// Pod name shaped like a real Deployment-managed ReplicaSet pod.
	pod := enginePod("vllm-7d9c8b6f4-abcd", "default", map[string]string{"app": "vllm"})
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-7d9c8b6f4-abcd", Tenant: "default", PrefixCount: 12, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cache},
		[]*corev1.Pod{pod},
		&served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	got := getBackendDirect(t, cl, "cache", "default")
	if got.Status.IndexParticipation == nil || got.Status.IndexParticipation.PrefixCount != 12 {
		t.Fatalf("CacheBackend 'cache' should be attributed via EngineSelector despite replica_id 'vllm-7d9c8b6f4-abcd' not starting with 'cache-'; got %+v",
			got.Status.IndexParticipation)
	}
}

// TestRefreshAnnotationPointsAtMissingBackend: the pod's injected-by
// annotation names a CacheBackend that no longer exists (likely just
// deleted). The poller MUST NOT fall back to selector matching — the
// annotation reflects an explicit operator decision. Skipping leaves
// the cluster-wide CacheIndex as the source of truth for that data.
func TestRefreshAnnotationPointsAtMissingBackend(t *testing.T) {
	// Selector-matched backend "other" exists; the annotation names a
	// non-existent "gone". Without the no-fallback rule, "other" would
	// silently inherit attribution that was explicitly assigned away.
	other := cbFixture("other", "default", map[string]string{"app": "vllm"})
	pod := enginePodInjectedBy("vllm-0", "default", "default", "gone", map[string]string{"app": "vllm"})
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-0", Tenant: "default", PrefixCount: 3, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{other},
		[]*corev1.Pod{pod},
		&served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	got := getBackendDirect(t, cl, "other", "default")
	if got.Status.IndexParticipation == nil || got.Status.IndexParticipation.PrefixCount != 0 {
		t.Fatalf("annotation pointing at missing backend must not fall back to selector match, got %+v",
			got.Status.IndexParticipation)
	}
}

// TestRefreshAnnotationInWrongNamespaceFallsBack: an injected-by annotation
// that references a CacheBackend in a DIFFERENT namespace from the pod is
// ignored (cross-namespace attribution would be misleading and would let a
// pod in ns-A poison status of a backend in ns-B). The poller falls back
// to in-namespace selector matching.
func TestRefreshAnnotationInWrongNamespaceFallsBack(t *testing.T) {
	cbLocal := cbFixture("local", "ns-pod", map[string]string{"app": "vllm"})
	cbForeign := cbFixture("foreign", "ns-other", map[string]string{"app": "vllm"})
	// Pod in ns-pod, annotation points at ns-other/foreign — cross-namespace.
	pod := enginePodInjectedBy("vllm-0", "ns-pod", "ns-other", "foreign", map[string]string{"app": "vllm"})
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-0", Tenant: "ns-pod", PrefixCount: 4, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cbLocal, cbForeign},
		[]*corev1.Pod{pod},
		&served, &mu)
	defer srv.Close()

	if err := p.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	local := getBackendDirect(t, cl, "local", "ns-pod")
	if local.Status.IndexParticipation == nil || local.Status.IndexParticipation.PrefixCount != 4 {
		t.Fatalf("cross-namespace annotation should be ignored; ns-pod selector fallback must attribute to local, got %+v",
			local.Status.IndexParticipation)
	}
	foreign := getBackendDirect(t, cl, "foreign", "ns-other")
	// foreign has a selector so it still gets the standard zero-state
	// write, but the cross-namespace annotation must not promote its
	// prefixCount.
	if foreign.Status.IndexParticipation == nil || foreign.Status.IndexParticipation.PrefixCount != 0 {
		t.Fatalf("cross-namespace annotation must not attribute to foreign backend, got %+v", foreign.Status.IndexParticipation)
	}
}

// TestRefreshSelectorClearedAfterPublishingDrains: a backend with a
// selector publishes participation, then has its EngineSelector cleared
// (operator edit). The next tick must reset prefixCount to 0 — stale
// positive state after a selector removal would tell operators the
// backend is contributing when it has no claim on any pod anymore.
func TestRefreshSelectorClearedAfterPublishingDrains(t *testing.T) {
	cb := cbFixture("backend", "default", map[string]string{"app": "vllm"})
	pod := enginePod("vllm-0", "default", map[string]string{"app": "vllm"})
	var mu sync.Mutex
	served := index.Snapshot{
		Replicas: []index.ReplicaSnapshot{
			{ReplicaID: "vllm-0", Tenant: "default", PrefixCount: 8, LastEventAt: time.Unix(1_700_000_000, 0).UTC()},
		},
	}
	p, cl, srv := buildPollerWithFixtures(t,
		[]*cachev1alpha1.CacheBackend{cb},
		[]*corev1.Pod{pod},
		&served, &mu)
	defer srv.Close()
	ctx := context.Background()

	if err := p.refresh(ctx); err != nil {
		t.Fatalf("first refresh: %v", err)
	}
	got := getBackendDirect(t, cl, "backend", "default")
	if got.Status.IndexParticipation == nil || got.Status.IndexParticipation.PrefixCount != 8 {
		t.Fatalf("seed participation = %+v, want prefixCount 8", got.Status.IndexParticipation)
	}

	// Operator clears the EngineSelector.
	got.Spec.EngineSelector = nil
	if err := cl.Update(ctx, got); err != nil {
		t.Fatalf("update backend (clear selector): %v", err)
	}

	if err := p.refresh(ctx); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	got = getBackendDirect(t, cl, "backend", "default")
	if got.Status.IndexParticipation == nil || got.Status.IndexParticipation.PrefixCount != 0 {
		t.Fatalf("post-selector-clear participation = %+v, want prefixCount 0", got.Status.IndexParticipation)
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

func tenantCond(ct cachev1alpha1.CacheTenant, condType string) (metav1.Condition, bool) {
	for _, c := range ct.Status.Conditions {
		if c.Type == condType {
			return c, true
		}
	}
	return metav1.Condition{}, false
}

// TestReconcileTenantStatusesShadowedDuplicate pins the duplicate-tenantID
// contract: when two CacheTenants declare the same spec.tenantID, the
// control-plane reconciler enforces only the first by (namespace, name), so the
// status writer must mark the OTHER as a shadowed duplicate rather than claim
// its (non-effective) budget is enforced.
func TestReconcileTenantStatusesShadowedDuplicate(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	effBudget := int64(5)
	dupBudget := int64(100)
	// Same tenantID "shared"; alpha < beta by name → alpha is effective.
	effective := &cachev1alpha1.CacheTenant{
		ObjectMeta: metav1.ObjectMeta{Name: "alpha", Namespace: "team"},
		Spec: cachev1alpha1.CacheTenantSpec{
			TenantID: "shared",
			Quota:    &cachev1alpha1.CacheTenantQuotaSpec{MaxIndexEntries: &effBudget},
		},
	}
	shadowed := &cachev1alpha1.CacheTenant{
		ObjectMeta: metav1.ObjectMeta{Name: "beta", Namespace: "team"},
		Spec: cachev1alpha1.CacheTenantSpec{
			TenantID: "shared",
			Quota:    &cachev1alpha1.CacheTenantQuotaSpec{MaxIndexEntries: &dupBudget},
		},
	}
	// A duplicate with NO quota of its own must ALSO be flagged: the tenantID is
	// enforced by alpha, so reporting Ready=True/NoQuota would mislead.
	shadowedNoQuota := &cachev1alpha1.CacheTenant{
		ObjectMeta: metav1.ObjectMeta{Name: "gamma", Namespace: "team"},
		Spec:       cachev1alpha1.CacheTenantSpec{TenantID: "shared"},
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheTenant{}).
		WithObjects(effective, shadowed, shadowedNoQuota).
		Build()
	p := &CacheIndexPoller{Client: cl}
	ctx := context.Background()

	// 3 distinct prefixes for "shared": under the effective budget (5).
	snap := index.Snapshot{Tenants: []index.TenantSnapshot{{TenantID: "shared", IndexEntries: 3}}}
	if err := p.reconcileTenantStatuses(ctx, snap); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	get := func(name string) cachev1alpha1.CacheTenant {
		var ct cachev1alpha1.CacheTenant
		if err := cl.Get(ctx, types.NamespacedName{Name: name, Namespace: "team"}, &ct); err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		return ct
	}

	// Effective CR: Ready=True, QuotaExceeded=False against its own budget.
	eff := get("alpha")
	if c, ok := tenantCond(eff, tenantConditionReady); !ok || c.Status != metav1.ConditionTrue || c.Reason != "Observed" {
		t.Fatalf("effective Ready = %+v, want True/Observed", c)
	}
	if c, _ := tenantCond(eff, tenantConditionQuotaExceeded); c.Status != metav1.ConditionFalse || c.Reason != "WithinBudget" {
		t.Fatalf("effective QuotaExceeded = %+v, want False/WithinBudget", c)
	}

	// Shadowed CR: Ready=False/DuplicateTenantID, QuotaExceeded=False/NotEffective —
	// its 100-entry budget is NOT presented as enforced.
	dup := get("beta")
	if c, ok := tenantCond(dup, tenantConditionReady); !ok || c.Status != metav1.ConditionFalse || c.Reason != "DuplicateTenantID" {
		t.Fatalf("shadowed Ready = %+v, want False/DuplicateTenantID", c)
	}
	if c, _ := tenantCond(dup, tenantConditionQuotaExceeded); c.Status != metav1.ConditionFalse || c.Reason != "NotEffective" {
		t.Fatalf("shadowed QuotaExceeded = %+v, want False/NotEffective", c)
	}

	// No-quota duplicate is shadowed too (not Ready=True/NoQuota).
	dupNQ := get("gamma")
	if c, ok := tenantCond(dupNQ, tenantConditionReady); !ok || c.Status != metav1.ConditionFalse || c.Reason != "DuplicateTenantID" {
		t.Fatalf("no-quota duplicate Ready = %+v, want False/DuplicateTenantID", c)
	}
}

func TestReconcileTenantStatusesProjectsAndFlapsQuota(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	maxEntries := int64(3)
	ct := &cachev1alpha1.CacheTenant{
		ObjectMeta: metav1.ObjectMeta{Name: "ct", Namespace: "team-a"},
		Spec: cachev1alpha1.CacheTenantSpec{
			TenantID: "team-vision", // matched by tenantID, NOT metadata.name
			Quota:    &cachev1alpha1.CacheTenantQuotaSpec{MaxIndexEntries: &maxEntries},
		},
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheTenant{}).
		WithObjects(ct).
		Build()
	p := &CacheIndexPoller{Client: cl}
	ctx := context.Background()
	get := func() cachev1alpha1.CacheTenant {
		var out cachev1alpha1.CacheTenant
		if err := cl.Get(ctx, types.NamespacedName{Name: "ct", Namespace: "team-a"}, &out); err != nil {
			t.Fatalf("get CacheTenant: %v", err)
		}
		return out
	}

	// Observed under budget: Ready=True, QuotaExceeded=False, indexEntries=2.
	under := index.Snapshot{Tenants: []index.TenantSnapshot{{TenantID: "team-vision", IndexEntries: 2}}}
	if err := p.reconcileTenantStatuses(ctx, under); err != nil {
		t.Fatalf("reconcile (under): %v", err)
	}
	got := get()
	if got.Status.IndexEntries == nil || *got.Status.IndexEntries != 2 {
		t.Fatalf("indexEntries = %v, want 2", got.Status.IndexEntries)
	}
	if c, ok := tenantCond(got, tenantConditionReady); !ok || c.Status != metav1.ConditionTrue {
		t.Fatalf("Ready = %+v, want True", c)
	}
	if c, ok := tenantCond(got, tenantConditionQuotaExceeded); !ok || c.Status != metav1.ConditionFalse {
		t.Fatalf("QuotaExceeded = %+v, want False", c)
	}

	// Observed over budget: QuotaExceeded flaps True (OverEntryBudget).
	over := index.Snapshot{Tenants: []index.TenantSnapshot{{TenantID: "team-vision", IndexEntries: 5}}}
	if err := p.reconcileTenantStatuses(ctx, over); err != nil {
		t.Fatalf("reconcile (over): %v", err)
	}
	got = get()
	if c, ok := tenantCond(got, tenantConditionQuotaExceeded); !ok || c.Status != metav1.ConditionTrue || c.Reason != "OverEntryBudget" {
		t.Fatalf("QuotaExceeded = %+v, want True/OverEntryBudget", c)
	}

	// Back under budget: QuotaExceeded resets to False.
	if err := p.reconcileTenantStatuses(ctx, under); err != nil {
		t.Fatalf("reconcile (recover): %v", err)
	}
	if c, _ := tenantCond(get(), tenantConditionQuotaExceeded); c.Status != metav1.ConditionFalse {
		t.Fatalf("QuotaExceeded = %+v, want False after dropping under budget", c)
	}
}

func TestReconcileTenantStatusesAbsentTenantObservedAsZero(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := cachev1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	maxEntries := int64(5)
	ct := &cachev1alpha1.CacheTenant{
		ObjectMeta: metav1.ObjectMeta{Name: "ct", Namespace: "team-a"},
		Spec: cachev1alpha1.CacheTenantSpec{
			TenantID: "team-quiet",
			Quota:    &cachev1alpha1.CacheTenantQuotaSpec{MaxIndexEntries: &maxEntries},
		},
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&cachev1alpha1.CacheTenant{}).
		WithObjects(ct).
		Build()
	p := &CacheIndexPoller{Client: cl}
	ctx := context.Background()

	// A successful scrape with no row for team-quiet means it currently holds
	// zero prefixes — an observed 0, not "unknown".
	if err := p.reconcileTenantStatuses(ctx, index.Snapshot{}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	var got cachev1alpha1.CacheTenant
	if err := cl.Get(ctx, types.NamespacedName{Name: "ct", Namespace: "team-a"}, &got); err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status.IndexEntries == nil || *got.Status.IndexEntries != 0 {
		t.Fatalf("indexEntries = %v, want 0 (observed zero, not nil)", got.Status.IndexEntries)
	}
	if c, ok := tenantCond(got, tenantConditionReady); !ok || c.Status != metav1.ConditionTrue {
		t.Fatalf("Ready = %+v, want True (we have a live reading)", c)
	}
	// 0 ≤ budget → not exceeded.
	if c, ok := tenantCond(got, tenantConditionQuotaExceeded); !ok || c.Status != metav1.ConditionFalse {
		t.Fatalf("QuotaExceeded = %+v, want False", c)
	}
}

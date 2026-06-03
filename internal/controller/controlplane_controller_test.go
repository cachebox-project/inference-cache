package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	cacheserver "github.com/cachebox-project/inference-cache/pkg/server"
)

// pushRecorder records every PolicySnapshot received over HTTP so the test
// can assert on the body the controller would send to the real server.
type pushRecorder struct {
	mu        sync.Mutex
	snapshots []cacheserver.PolicySnapshot
	method    string
	authz     string // last Authorization header observed
}

func (p *pushRecorder) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_ = r.Body.Close()
		var snap cacheserver.PolicySnapshot
		if err := json.Unmarshal(body, &snap); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		p.mu.Lock()
		p.snapshots = append(p.snapshots, snap)
		p.method = r.Method
		p.authz = r.Header.Get("Authorization")
		p.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}
}

func (p *pushRecorder) lastAuthz() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.authz
}

func (p *pushRecorder) latest() (cacheserver.PolicySnapshot, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.snapshots) == 0 {
		return cacheserver.PolicySnapshot{}, false
	}
	return p.snapshots[len(p.snapshots)-1], true
}

func newPolicyTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := cachev1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	return s
}

func ttlPtr(d time.Duration) *metav1.Duration { x := metav1.Duration{Duration: d}; return &x }
func i32Ptr(v int32) *int32                   { return &v }
func i64Ptr(v int64) *int64                   { return &v }

// TestPushSnapshotFlattensPoliciesAndTenants proves the single reconciler emits
// one combined snapshot from BOTH CR types: policies keyed by namespace, tenant
// quotas keyed by spec.tenantID. Tenants without an enforceable budget are
// omitted (fail open); a budget of 0 is kept.
func TestPushSnapshotFlattensPoliciesAndTenants(t *testing.T) {
	rec := &pushRecorder{}
	srv := httptest.NewServer(rec.handler())
	defer srv.Close()

	cl := fake.NewClientBuilder().
		WithScheme(newPolicyTestScheme(t)).
		WithObjects(
			&cachev1alpha1.CachePolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "team-a"},
				Spec:       cachev1alpha1.CachePolicySpec{EvictionTTL: ttlPtr(time.Hour)},
			},
			// Enforceable: tenantID team-a, budget 1000.
			&cachev1alpha1.CacheTenant{
				ObjectMeta: metav1.ObjectMeta{Name: "ct-a", Namespace: "team-a"},
				Spec: cachev1alpha1.CacheTenantSpec{
					TenantID:      "team-a",
					IsolationMode: cachev1alpha1.CacheTenantIsolationModeFairness,
					Quota:         &cachev1alpha1.CacheTenantQuotaSpec{MaxIndexEntries: i64Ptr(1000)},
				},
			},
			// No quota → omitted from the snapshot (server leaves it unbounded).
			&cachev1alpha1.CacheTenant{
				ObjectMeta: metav1.ObjectMeta{Name: "ct-b", Namespace: "team-b"},
				Spec:       cachev1alpha1.CacheTenantSpec{TenantID: "team-b"},
			},
			// Budget 0 is a valid enforceable cap → kept.
			&cachev1alpha1.CacheTenant{
				ObjectMeta: metav1.ObjectMeta{Name: "ct-c", Namespace: "team-c"},
				Spec: cachev1alpha1.CacheTenantSpec{
					TenantID: "team-c",
					Quota:    &cachev1alpha1.CacheTenantQuotaSpec{MaxIndexEntries: i64Ptr(0)},
				},
			},
		).
		Build()

	r := &ControlPlaneReconciler{Client: cl, ServerPolicyURL: srv.URL, HTTPClient: srv.Client()}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	snap, ok := rec.latest()
	if !ok {
		t.Fatal("expected a push")
	}
	if snap.Version != cacheserver.PolicyPropagationVersion {
		t.Fatalf("version = %d, want %d", snap.Version, cacheserver.PolicyPropagationVersion)
	}
	if len(snap.Policies) != 1 || snap.Policies[0].Namespace != "team-a" {
		t.Fatalf("policies = %+v, want one for team-a", snap.Policies)
	}
	if len(snap.Tenants) != 2 {
		t.Fatalf("tenants = %+v, want 2 (team-a, team-c); team-b omitted", snap.Tenants)
	}
	// Sorted by tenantID: team-a then team-c.
	if snap.Tenants[0].TenantID != "team-a" || snap.Tenants[0].MaxIndexEntries != 1000 {
		t.Fatalf("tenants[0] = %+v, want team-a/1000", snap.Tenants[0])
	}
	if snap.Tenants[0].IsolationMode != "Fairness" {
		t.Fatalf("tenants[0].IsolationMode = %q, want Fairness", snap.Tenants[0].IsolationMode)
	}
	if snap.Tenants[1].TenantID != "team-c" || snap.Tenants[1].MaxIndexEntries != 0 {
		t.Fatalf("tenants[1] = %+v, want team-c/0", snap.Tenants[1])
	}
	for _, tn := range snap.Tenants {
		if tn.TenantID == "team-b" {
			t.Fatal("team-b has no quota and must be omitted")
		}
	}
}

func TestPushSnapshotIncludesAllPolicies(t *testing.T) {
	rec := &pushRecorder{}
	srv := httptest.NewServer(rec.handler())
	defer srv.Close()

	cl := fake.NewClientBuilder().
		WithScheme(newPolicyTestScheme(t)).
		WithObjects(
			&cachev1alpha1.CachePolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "team-a"},
				Spec: cachev1alpha1.CachePolicySpec{
					EvictionTTL:         ttlPtr(15 * time.Minute),
					MinimumPrefixTokens: i32Ptr(32),
					LookupTimeoutMs:     i32Ptr(25),
				},
			},
			&cachev1alpha1.CachePolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "team-b"},
				Spec:       cachev1alpha1.CachePolicySpec{EvictionTTL: ttlPtr(time.Hour)},
			},
		).
		Build()

	r := &ControlPlaneReconciler{
		Client:          cl,
		ServerPolicyURL: srv.URL,
		HTTPClient:      srv.Client(),
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	snap, ok := rec.latest()
	if !ok {
		t.Fatal("expected at least one push")
	}
	if snap.Version != cacheserver.PolicyPropagationVersion {
		t.Fatalf("snapshot version = %d, want %d", snap.Version, cacheserver.PolicyPropagationVersion)
	}
	if rec.method != http.MethodPost {
		t.Fatalf("HTTP method = %q, want POST", rec.method)
	}
	if len(snap.Policies) != 2 {
		t.Fatalf("expected 2 policies, got %d (%+v)", len(snap.Policies), snap.Policies)
	}
	// Sorted by namespace — team-a first.
	if snap.Policies[0].Namespace != "team-a" || snap.Policies[1].Namespace != "team-b" {
		t.Fatalf("policies not sorted by namespace: %+v", snap.Policies)
	}
	got := snap.Policies[0]
	if got.EvictionTTL != 15*time.Minute || got.MinimumPrefixTokens != 32 || got.LookupTimeoutMs != 25 {
		t.Fatalf("team-a resolved policy = %+v", got)
	}
}

func TestPushSnapshotReflectsDeletions(t *testing.T) {
	rec := &pushRecorder{}
	srv := httptest.NewServer(rec.handler())
	defer srv.Close()

	cp := &cachev1alpha1.CachePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "team-a"},
		Spec:       cachev1alpha1.CachePolicySpec{EvictionTTL: ttlPtr(time.Hour)},
	}
	cl := fake.NewClientBuilder().
		WithScheme(newPolicyTestScheme(t)).
		WithObjects(cp).
		Build()
	r := &ControlPlaneReconciler{
		Client: cl, ServerPolicyURL: srv.URL, HTTPClient: srv.Client(),
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if snap, _ := rec.latest(); len(snap.Policies) != 1 {
		t.Fatalf("first push should contain 1 policy, got %d", len(snap.Policies))
	}

	if err := cl.Delete(context.Background(), cp); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	// Replace-on-write: the next push must contain the empty list so the
	// server reverts that namespace to defaults.
	snap, _ := rec.latest()
	if len(snap.Policies) != 0 {
		t.Fatalf("after delete, snapshot should be empty; got %+v", snap.Policies)
	}
}

// TestPushSnapshotDeduplicatesByFirstNameWhenMultiplePoliciesShareNamespace
// pins the conflict-resolution rule: when several CachePolicies share a
// namespace, the entry with the alphabetically smallest name wins. The CRD
// does not enforce a singleton per namespace, so a deterministic tiebreak
// here keeps the effective policy independent of apiserver list ordering.
func TestPushSnapshotDeduplicatesByFirstNameWhenMultiplePoliciesShareNamespace(t *testing.T) {
	rec := &pushRecorder{}
	srv := httptest.NewServer(rec.handler())
	defer srv.Close()

	cl := fake.NewClientBuilder().
		WithScheme(newPolicyTestScheme(t)).
		WithObjects(
			// "z-pol" sorts last alphabetically — should LOSE.
			&cachev1alpha1.CachePolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "z-pol", Namespace: "team-a"},
				Spec:       cachev1alpha1.CachePolicySpec{MinimumPrefixTokens: i32Ptr(999)},
			},
			// "a-pol" sorts first — should WIN.
			&cachev1alpha1.CachePolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "a-pol", Namespace: "team-a"},
				Spec:       cachev1alpha1.CachePolicySpec{MinimumPrefixTokens: i32Ptr(16)},
			},
		).
		Build()
	r := &ControlPlaneReconciler{
		Client: cl, ServerPolicyURL: srv.URL, HTTPClient: srv.Client(),
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	snap, _ := rec.latest()
	if len(snap.Policies) != 1 {
		t.Fatalf("expected exactly 1 deduped entry for team-a, got %d (%+v)", len(snap.Policies), snap.Policies)
	}
	if snap.Policies[0].MinimumPrefixTokens != 16 {
		t.Fatalf("dedup picked the wrong CR: %+v (expected the a-pol value 16)", snap.Policies[0])
	}
}

// TestPushSnapshotSendsBearerToken pins the client-side contract: when
// BearerTokenPath points at a tmpfile (kubelet-shape), every POST carries
// `Authorization: Bearer <token>`. Mirror of the CacheIndexPoller's token
// handling.
func TestPushSnapshotSendsBearerToken(t *testing.T) {
	rec := &pushRecorder{}
	srv := httptest.NewServer(rec.handler())
	defer srv.Close()

	tokenPath := filepath.Join(t.TempDir(), "sa-token")
	// kubelet's projected token has a trailing newline; bearerToken() trims it.
	if err := os.WriteFile(tokenPath, []byte("test-sa-bearer\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	cl := fake.NewClientBuilder().
		WithScheme(newPolicyTestScheme(t)).
		WithObjects(&cachev1alpha1.CachePolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "team-a"},
		}).
		Build()
	r := &ControlPlaneReconciler{
		Client:          cl,
		ServerPolicyURL: srv.URL,
		HTTPClient:      srv.Client(),
		BearerTokenPath: tokenPath,
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := rec.lastAuthz(); got != "Bearer test-sa-bearer" {
		t.Fatalf("Authorization = %q, want %q", got, "Bearer test-sa-bearer")
	}
}

// TestPushSnapshotBearerTokenUnreadableFile pins the third bearerToken
// branch the CacheIndexPoller's symmetric test covers: a token file that
// EXISTS but cannot be read (permissions / IO error). The reconciler's
// bearerToken() must surface the underlying error so the operator's log
// shows the path + cause, instead of silently degrading to an unauth push
// the server rejects as a generic 401. Mirror of
// TestBearerToken_UnreadableFileReturnsError on the poller side.
func TestPushSnapshotBearerTokenUnreadableFile(t *testing.T) {
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

	r := &ControlPlaneReconciler{BearerTokenPath: path}
	got, err := r.bearerToken()
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

// TestPushSnapshotNoBearerWhenTokenFileMissing pins the fail-soft posture
// the controller inherits from the CacheIndexPoller: a missing token file
// (local-dev out-of-cluster path) does NOT error out the push — the request
// goes out unauthenticated and the server's auth posture decides what
// happens. The server's 401 surfaces as a normal failing tick (covered by
// TestPushSnapshotPropagatesNon2xxAsError); here we just pin that no
// Authorization header is sent when the file isn't there.
func TestPushSnapshotNoBearerWhenTokenFileMissing(t *testing.T) {
	rec := &pushRecorder{}
	srv := httptest.NewServer(rec.handler())
	defer srv.Close()

	cl := fake.NewClientBuilder().
		WithScheme(newPolicyTestScheme(t)).
		WithObjects(&cachev1alpha1.CachePolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "team-a"},
		}).
		Build()
	r := &ControlPlaneReconciler{
		Client:          cl,
		ServerPolicyURL: srv.URL,
		HTTPClient:      srv.Client(),
		BearerTokenPath: filepath.Join(t.TempDir(), "does-not-exist"),
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := rec.lastAuthz(); got != "" {
		t.Fatalf("expected no Authorization header when token file missing, got %q", got)
	}
}

func TestPushSnapshotPropagatesNon2xxAsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	r := &ControlPlaneReconciler{
		Client:          fake.NewClientBuilder().WithScheme(newPolicyTestScheme(t)).Build(),
		ServerPolicyURL: srv.URL,
		HTTPClient:      srv.Client(),
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err == nil {
		t.Fatal("expected an error when the server returns 500")
	}
}

// TestPushSnapshotSerializesConcurrentPushes proves that two concurrent
// reconciles never overlap inside pushSnapshot, so an older List + POST
// pair can't sandwich and overwrite a newer one. The handler asserts
// no in-flight push at entry; if pushMu were missing the handler would
// observe a concurrent call and fail the test.
func TestPushSnapshotSerializesConcurrentPushes(t *testing.T) {
	var (
		inflight   sync.Mutex
		concurrent bool
	)
	handler := func(w http.ResponseWriter, _ *http.Request) {
		// TryLock approximates "is another push currently inside the
		// handler?" without blocking. A false here means another push
		// holds the mutex, which means the two pushes were interleaved.
		if !inflight.TryLock() {
			concurrent = true
			w.WriteHeader(http.StatusNoContent)
			return
		}
		// Hold the handler "open" long enough that a concurrent push,
		// were one possible, would land while we're inside.
		time.Sleep(20 * time.Millisecond)
		inflight.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}
	srv := httptest.NewServer(http.HandlerFunc(handler))
	defer srv.Close()

	cl := fake.NewClientBuilder().
		WithScheme(newPolicyTestScheme(t)).
		WithObjects(&cachev1alpha1.CachePolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "team-a"},
		}).
		Build()
	r := &ControlPlaneReconciler{
		Client: cl, ServerPolicyURL: srv.URL, HTTPClient: srv.Client(),
	}

	const n = 4
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, _ = r.Reconcile(context.Background(), ctrl.Request{})
		}()
	}
	wg.Wait()

	if concurrent {
		t.Fatal("two pushSnapshot calls overlapped — pushMu is not serializing them")
	}
}

func TestPushSnapshotRoundTripsThroughServerPolicyStore(t *testing.T) {
	// Stand up the real /policy handler and assert the body the reconciler
	// sends successfully Replace()s the store — this guards against a
	// schema drift between the reconciler's marshal and the server's decode.
	store := cacheserver.NewPolicyStore()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Re-use the package's handler directly via a small adapter:
		// the package exports policyHandler symbolically via Service.New, but
		// here we re-create the same shape by wrapping the store.
		policyHandlerForTest(store).ServeHTTP(w, req)
	}))
	defer srv.Close()

	cl := fake.NewClientBuilder().
		WithScheme(newPolicyTestScheme(t)).
		WithObjects(&cachev1alpha1.CachePolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "team-a"},
			Spec: cachev1alpha1.CachePolicySpec{
				EvictionTTL:         ttlPtr(12 * time.Minute),
				MinimumPrefixTokens: i32Ptr(64),
				LookupTimeoutMs:     i32Ptr(10),
			},
		}).
		Build()
	r := &ControlPlaneReconciler{
		Client: cl, ServerPolicyURL: srv.URL, HTTPClient: srv.Client(),
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	p, ok := store.Lookup("team-a")
	if !ok {
		t.Fatal("server store should hold team-a after a successful push")
	}
	if p.EvictionTTL != 12*time.Minute || p.MinimumPrefixTokens != 64 || p.LookupTimeoutMs != 10 {
		t.Fatalf("round-trip mismatch: %+v", p)
	}
	if d := store.TTL("team-a"); d != 12*time.Minute {
		t.Fatalf("store.TTL = %v", d)
	}
}

// TestReconcilerFlattensEvictionAlgorithm pins the spec.eviction flattening:
// the upper-case CRD enum becomes the lower-case wire form, and an unset field
// defaults to LRU (matching the kubebuilder default and the index default).
func TestReconcilerFlattensEvictionAlgorithm(t *testing.T) {
	store := cacheserver.NewPolicyStore()
	srv := httptest.NewServer(policyHandlerForTest(store))
	defer srv.Close()

	cl := fake.NewClientBuilder().
		WithScheme(newPolicyTestScheme(t)).
		WithObjects(
			&cachev1alpha1.CachePolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns-lfu"},
				Spec:       cachev1alpha1.CachePolicySpec{Eviction: cachev1alpha1.CachePolicyEvictionAlgorithmLFU},
			},
			&cachev1alpha1.CachePolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns-lru"},
				Spec:       cachev1alpha1.CachePolicySpec{Eviction: cachev1alpha1.CachePolicyEvictionAlgorithmLRU},
			},
			&cachev1alpha1.CachePolicy{
				ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns-unset"},
				Spec:       cachev1alpha1.CachePolicySpec{EvictionTTL: ttlPtr(time.Minute)}, // eviction omitted
			},
		).
		Build()
	r := &ControlPlaneReconciler{Client: cl, ServerPolicyURL: srv.URL, HTTPClient: srv.Client()}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	for ns, want := range map[string]string{"ns-lfu": "lfu", "ns-lru": "lru", "ns-unset": "lru"} {
		if got := store.Eviction(ns); got != want {
			t.Fatalf("Eviction(%s) = %q, want %q", ns, got, want)
		}
	}
}

// policyHandlerForTest wraps the server package's exported handler factory.
// Kept as a one-line helper so the call site reads naturally in the test.
func policyHandlerForTest(s *cacheserver.PolicyStore) http.HandlerFunc {
	return cacheserver.NewPolicyHTTPHandler(s)
}

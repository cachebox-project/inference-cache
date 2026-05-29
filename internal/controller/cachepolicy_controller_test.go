package controller

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
		p.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}
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

	r := &CachePolicyReconciler{
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
	r := &CachePolicyReconciler{
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
	r := &CachePolicyReconciler{
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

func TestPushSnapshotPropagatesNon2xxAsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	r := &CachePolicyReconciler{
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
	r := &CachePolicyReconciler{
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
	r := &CachePolicyReconciler{
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

// policyHandlerForTest wraps the server package's exported handler factory.
// Kept as a one-line helper so the call site reads naturally in the test.
func policyHandlerForTest(s *cacheserver.PolicyStore) http.HandlerFunc {
	return cacheserver.NewPolicyHTTPHandler(s)
}

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/test/bufconn"
	authnv1 "k8s.io/api/authentication/v1"

	"github.com/cachebox-project/inference-cache/pkg/index"
)

// fakeT2Prober is the Stage-C fake. ProbePutGet returns whatever err the
// fake was constructed with; recorded fields let the test assert what the
// probe orchestrator passed in.
type fakeT2Prober struct {
	err            error
	calls          int
	lastBackend    string
	lastPayloadLen int
}

func (f *fakeT2Prober) ProbePutGet(_ context.Context, backend string, payload []byte) error {
	f.calls++
	f.lastBackend = backend
	f.lastPayloadLen = len(payload)
	return f.err
}

func newProberForTest(t *testing.T, t2 T2Prober) (*Prober, *index.Index) {
	t.Helper()
	// Fresh index per test. Default options match the production binary
	// (DefaultTTL, DefaultMaxEntries) but with no policy/tenant/eviction
	// resolvers — the probe tenant has no policy by design, so the index
	// resolves to its global defaults for the probe scope.
	idx := index.New()
	idx.Start(t.Context())
	return NewProber(idx, t2), idx
}

// TestProbeHashIsDeterministic pins the round-trip invariant the probe
// relies on: same (backend, hashScheme) → same bytes. A regression here
// silently breaks Stage A (the ingested hash and the looked-up hash
// disagree, so the entry never lands).
func TestProbeHashIsDeterministic(t *testing.T) {
	a := ProbeHash("cb-1", "vllm")
	b := ProbeHash("cb-1", "vllm")
	if !bytes.Equal(a, b) {
		t.Fatalf("ProbeHash not deterministic: %x vs %x", a, b)
	}
	if len(a) != 32 {
		t.Fatalf("ProbeHash length = %d, want 32 (SHA-256)", len(a))
	}

	// Different backend → different bytes.
	if bytes.Equal(a, ProbeHash("cb-2", "vllm")) {
		t.Fatal("ProbeHash collided on different backend")
	}
	// Different scheme → different bytes (engine-domain isolation, mirrors
	// the index's scheme-disjoint indexing).
	if bytes.Equal(a, ProbeHash("cb-1", "sglang")) {
		t.Fatal("ProbeHash collided on different hash_scheme")
	}
}

// TestProbeReplicaIDReservedPrefix proves the synthesized replica id always
// starts with the reserved `__probe-` prefix. Real subscribers set replica_id
// to the pod name (which cannot start with `__`), so the prefix is the
// collision-free namespace that keeps probe + workload state disjoint.
func TestProbeReplicaIDReservedPrefix(t *testing.T) {
	got := ProbeReplicaID("my-backend")
	if !strings.HasPrefix(got, ProbeReplicaPrefix) {
		t.Fatalf("ProbeReplicaID = %q, want prefix %q", got, ProbeReplicaPrefix)
	}
	if got == ProbeReplicaPrefix {
		t.Fatalf("ProbeReplicaID(%q) returned bare prefix — backend suffix dropped", "my-backend")
	}
}

// TestProbeResultAllPassedZeroValueFailsClosed pins the predicate's
// fail-closed property: a zero-value ProbeResult{} (no stages run, or a
// partial JSON decode) MUST NOT report passed. "No information" is not the
// same as "every stage succeeded" — without this guard, the controller-
// wiring follow-up could flip FunctionalProbeOK True on an empty response.
func TestProbeResultAllPassedZeroValueFailsClosed(t *testing.T) {
	if (ProbeResult{}).AllPassed() {
		t.Fatal("ProbeResult{}.AllPassed() = true, want false — zero-value must not pass")
	}
	// A partially-populated result also fails: two stages ok + one
	// zero-value field is still "no information" for that stage.
	partial := ProbeResult{Subscriber: ProbeStageOK, Routing: ProbeStageOK}
	if partial.AllPassed() {
		t.Fatal("ProbeResult with zero-value T2 returned AllPassed=true; want false")
	}
	// The all-explicit-ok case still passes.
	all := ProbeResult{Subscriber: ProbeStageOK, Routing: ProbeStageOK, T2: ProbeStageSkipped}
	if !all.AllPassed() {
		t.Fatal("explicit ok/ok/skipped ProbeResult should report AllPassed=true")
	}
}

// TestProberRunHappyPathStageABCSkippedT2 covers the no-T2-prober default:
// Stage A passes (ingest landed), Stage B passes (LookupRoute returns
// PREFIX_MATCH), Stage C is Skipped because no T2Prober is wired. This is
// the Stage 1 production posture — no controller is wiring a real T2Prober
// yet.
func TestProberRunHappyPathStageABCSkippedT2(t *testing.T) {
	prober, _ := newProberForTest(t, nil)
	result := prober.Run(t.Context(), ProbeRequest{
		Backend:     "cb-happy",
		Model:       "llama-3-8b",
		HashScheme:  "vllm",
		BackendType: BackendTypeLMCache,
	})

	if result.Subscriber != ProbeStageOK {
		t.Errorf("Subscriber = %q, want %q", result.Subscriber, ProbeStageOK)
	}
	if result.Routing != ProbeStageOK {
		t.Errorf("Routing = %q, want %q", result.Routing, ProbeStageOK)
	}
	if result.T2 != ProbeStageSkipped {
		t.Errorf("T2 = %q, want %q when no T2Prober is wired", result.T2, ProbeStageSkipped)
	}
	if !result.AllPassed() {
		t.Fatalf("AllPassed() = false; result = %+v", result)
	}
	if len(result.Errors) != 0 {
		t.Errorf("expected no errors, got %+v", result.Errors)
	}
}

// TestProberRunStageCSkippedForNonLMCache proves the T2 stage is gated on
// spec.type=LMCache: a non-LMCache backend reports T2=skipped even when a
// real T2Prober is wired. Memory and External backends carry no tier-2
// client to drive, so running the probe against one is a NO-OP for stage C
// by design.
func TestProberRunStageCSkippedForNonLMCache(t *testing.T) {
	t2 := &fakeT2Prober{}
	prober, _ := newProberForTest(t, t2)
	result := prober.Run(t.Context(), ProbeRequest{
		Backend:     "cb-mem",
		Model:       "m",
		HashScheme:  "vllm",
		BackendType: "Memory", // not LMCache
	})
	if result.T2 != ProbeStageSkipped {
		t.Fatalf("T2 = %q, want %q for non-LMCache backend", result.T2, ProbeStageSkipped)
	}
	if t2.calls != 0 {
		t.Fatalf("T2Prober was called %d times for non-LMCache backend, want 0", t2.calls)
	}
}

// TestProberRunStageCTreatsEmptyBackendTypeAsLMCache pins the CRD-default
// alignment: an omitted BackendType must run Stage C (it matches the
// CacheBackend.spec.type defaulter, which writes LMCache). The opposite
// behavior — silently skipping Stage C on an empty field — would flip
// FunctionalProbeOK True on a backend that never proved its T2 round-trip.
func TestProberRunStageCTreatsEmptyBackendTypeAsLMCache(t *testing.T) {
	t2 := &fakeT2Prober{}
	prober, _ := newProberForTest(t, t2)
	result := prober.Run(t.Context(), ProbeRequest{
		Backend:    "cb-default",
		Model:      "m",
		HashScheme: "vllm",
		// BackendType deliberately omitted — controller may send empty if
		// the CR omitted spec.type (the kubebuilder defaulter writes LMCache,
		// but a hand-rolled request can still arrive empty).
	})
	if result.T2 != ProbeStageOK {
		t.Errorf("T2 = %q, want %q — empty BackendType must run Stage C (CRD default)", result.T2, ProbeStageOK)
	}
	if t2.calls != 1 {
		t.Errorf("T2Prober calls = %d, want 1 — empty BackendType silently skipped Stage C", t2.calls)
	}
}

// TestProberRunStageCRunsForLMCacheWithProber covers the positive Stage-C
// path: an LMCache backend + a working T2Prober yields T2=ok and exercises
// the prober. The payload length check proves the orchestrator actually
// passed the probe bytes to the prober (a refactor that drops the payload
// silently would still satisfy "calls > 0" but not "payload was non-empty").
func TestProberRunStageCRunsForLMCacheWithProber(t *testing.T) {
	t2 := &fakeT2Prober{}
	prober, _ := newProberForTest(t, t2)
	result := prober.Run(t.Context(), ProbeRequest{
		Backend:     "cb-lm",
		Model:       "m",
		HashScheme:  "vllm",
		BackendType: BackendTypeLMCache,
	})
	if result.T2 != ProbeStageOK {
		t.Errorf("T2 = %q, want %q", result.T2, ProbeStageOK)
	}
	if t2.calls != 1 {
		t.Errorf("T2Prober calls = %d, want 1", t2.calls)
	}
	if t2.lastBackend != "cb-lm" {
		t.Errorf("T2Prober saw backend = %q, want %q", t2.lastBackend, "cb-lm")
	}
	if t2.lastPayloadLen == 0 {
		t.Errorf("T2Prober payload was empty — orchestrator dropped the bytes")
	}
}

// TestProberRunStageAFailsWhenIngestNoOps simulates the silent-failure
// mode where the subscriber writes but nothing reaches the index. The
// probe must report Subscriber=failed AND skip Stage B (the routing layer
// can't be diagnosed when its input never arrived), so the controller
// surfaces the upstream stage rather than a cascade.
func TestProberRunStageAFailsWhenIngestNoOps(t *testing.T) {
	prober, _ := newProberForTest(t, nil)
	prober.ingestFn = func(index.Update) {} // simulate a write that never lands

	result := prober.Run(t.Context(), ProbeRequest{
		Backend: "cb-1", Model: "m", HashScheme: "vllm",
	})
	if result.Subscriber != ProbeStageFailed {
		t.Errorf("Subscriber = %q, want %q", result.Subscriber, ProbeStageFailed)
	}
	if result.Routing != ProbeStageSkipped {
		t.Errorf("Routing = %q, want %q (cascade from failed Stage A)", result.Routing, ProbeStageSkipped)
	}
	if result.T2 != ProbeStageSkipped {
		t.Errorf("T2 = %q, want %q (cascade from failed Stage A)", result.T2, ProbeStageSkipped)
	}
	if result.AllPassed() {
		t.Fatal("AllPassed() = true despite Subscriber failed")
	}
	if !stageErrorPresent(result.Errors, ProbeStageSubscriber) {
		t.Errorf("expected subscriber stage error, got %+v", result.Errors)
	}
}

// TestProberRunStageBFailsWhenRouteReturnsNoHint simulates the silent-failure
// mode where the entry IS in the index but LookupRoute returns NO_HINT
// (e.g., proxy ↔ server hash-encoding mismatch). Stage A still passes
// (direct Lookup finds the entry), so the failure is uniquely attributable
// to the routing layer. Stage C must be SKIPPED on a Stage-B failure so
// the controller's diagnostic pinpoints the routing layer instead of
// reporting a cascading T2 result that the operator then has to disentangle.
func TestProberRunStageBFailsWhenRouteReturnsNoHint(t *testing.T) {
	t2 := &fakeT2Prober{}
	prober, _ := newProberForTest(t, t2)
	prober.routeFn = func(index.LookupRequest) index.LookupResult {
		return index.LookupResult{Strategy: index.StrategyNone}
	}

	result := prober.Run(t.Context(), ProbeRequest{
		Backend: "cb-1", Model: "m", HashScheme: "vllm", BackendType: BackendTypeLMCache,
	})
	if result.Subscriber != ProbeStageOK {
		t.Errorf("Subscriber = %q, want %q — direct Lookup is unaffected by the routeFn override", result.Subscriber, ProbeStageOK)
	}
	if result.Routing != ProbeStageFailed {
		t.Errorf("Routing = %q, want %q", result.Routing, ProbeStageFailed)
	}
	if result.T2 != ProbeStageSkipped {
		t.Errorf("T2 = %q, want %q — must skip on Stage-B failure to avoid cascading diagnostic", result.T2, ProbeStageSkipped)
	}
	if t2.calls != 0 {
		t.Errorf("T2Prober was called %d times after Stage B failed; want 0 (cascade prevention)", t2.calls)
	}
	if result.AllPassed() {
		t.Fatal("AllPassed() = true despite Routing failed")
	}
	if !stageErrorPresent(result.Errors, ProbeStageRouting) {
		t.Errorf("expected routing stage error, got %+v", result.Errors)
	}
}

// TestProberRunStageBDistinguishesWrongReplicaFromWrongStrategy pins the
// per-failure-mode diagnostic the controller surfaces. When LookupRoute
// returns PREFIX_MATCH but for a DIFFERENT replica than the probe synthesized,
// the error message names the expected probe replica explicitly — the operator
// should see "not among the scored replicas" rather than the misleading
// "returned PREFIX_MATCH, expected PREFIX_MATCH" the original implementation
// produced.
func TestProberRunStageBDistinguishesWrongReplicaFromWrongStrategy(t *testing.T) {
	prober, _ := newProberForTest(t, nil)
	prober.routeFn = func(index.LookupRequest) index.LookupResult {
		return index.LookupResult{
			Strategy: index.StrategyPrefixMatch,
			Scores:   []index.ReplicaScore{{ReplicaID: "wrong-replica", MatchedTokens: 16}},
		}
	}

	result := prober.Run(t.Context(), ProbeRequest{
		Backend: "cb-1", Model: "m", HashScheme: "vllm",
	})
	if result.Routing != ProbeStageFailed {
		t.Fatalf("Routing = %q, want %q", result.Routing, ProbeStageFailed)
	}
	msg := stageErrorMessage(result.Errors, ProbeStageRouting)
	expectedReplica := ProbeReplicaID("cb-1")
	if !strings.Contains(msg, "not among the scored replicas") {
		t.Errorf("routing error %q should distinguish wrong-replica from wrong-strategy", msg)
	}
	if !strings.Contains(msg, expectedReplica) {
		t.Errorf("routing error %q should name the expected probe replica %q", msg, expectedReplica)
	}
}

// TestProberRunStageCDistinguishesPutFromGet covers the LMCache failure
// modes the operator condition message has to disambiguate: the put silently
// dropped vs the get returning nothing. A *T2ProbeError carries
// the phase, and the orchestrator must surface that phase verbatim in the
// stage error message.
func TestProberRunStageCDistinguishesPutFromGet(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		wantSubstr string
	}{
		{name: "put fail", err: &T2ProbeError{Phase: "put", Err: errors.New("connection refused")}, wantSubstr: "put failed: connection refused"},
		{name: "get fail", err: &T2ProbeError{Phase: "get", Err: errors.New("not found")}, wantSubstr: "get failed: not found"},
		{name: "plain error", err: errors.New("some lmcache fault"), wantSubstr: "some lmcache fault"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t2 := &fakeT2Prober{err: tc.err}
			prober, _ := newProberForTest(t, t2)
			result := prober.Run(t.Context(), ProbeRequest{
				Backend: "cb-1", Model: "m", HashScheme: "vllm",
				BackendType: BackendTypeLMCache,
			})
			if result.T2 != ProbeStageFailed {
				t.Fatalf("T2 = %q, want %q", result.T2, ProbeStageFailed)
			}
			if !stageErrorPresent(result.Errors, ProbeStageT2) {
				t.Fatalf("expected t2 stage error, got %+v", result.Errors)
			}
			msg := stageErrorMessage(result.Errors, ProbeStageT2)
			if !strings.Contains(msg, tc.wantSubstr) {
				t.Errorf("T2 error message %q does not contain %q", msg, tc.wantSubstr)
			}
		})
	}
}

// TestProberRunLeavesNoStateInIndex pins the cleanup invariant: a successful
// run must leave the index empty of probe entries (reserved tenant, replica),
// so reserved tenant residue can't leak into anything. Without the deferred
// cleanup the index would accumulate one probe replica per CacheBackend per
// reconcile pass, polluting both /snapshot and the entry-count metric.
func TestProberRunLeavesNoStateInIndex(t *testing.T) {
	prober, idx := newProberForTest(t, nil)
	_ = prober.Run(t.Context(), ProbeRequest{Backend: "cb-1", Model: "m", HashScheme: "vllm"})

	replicas, totalPrefixes := idx.CacheState(ProbeTenantID, "m")
	if totalPrefixes != 0 {
		t.Errorf("totalPrefixes after probe = %d, want 0 — cleanup did not run", totalPrefixes)
	}
	if len(replicas) != 0 {
		t.Errorf("replicas after probe = %+v, want empty — replica stats not cleared", replicas)
	}
}

// TestProberRunSerializesConcurrentProbesForSameBackend pins the per-
// (backend, model) mutex that prevents the race where one probe's cleanup
// wipes another's just-ingested entry mid-flight. Without it, two
// concurrent Runs for the same backend would surface as false subscriber
// or routing failures. With it, both Runs land successfully — sequenced —
// and the index is empty afterwards.
func TestProberRunSerializesConcurrentProbesForSameBackend(t *testing.T) {
	prober, idx := newProberForTest(t, nil)

	const concurrent = 8
	results := make(chan ProbeResult, concurrent)
	for i := 0; i < concurrent; i++ {
		go func() {
			results <- prober.Run(t.Context(), ProbeRequest{
				Backend: "cb-shared", Model: "m", HashScheme: "vllm",
			})
		}()
	}

	for i := 0; i < concurrent; i++ {
		r := <-results
		if !r.AllPassed() {
			t.Errorf("concurrent run %d: AllPassed() = false; result = %+v", i, r)
		}
	}

	// All Runs serialized correctly, so the final state is clean.
	_, totalPrefixes := idx.CacheState(ProbeTenantID, "m")
	if totalPrefixes != 0 {
		t.Errorf("after %d concurrent runs, totalPrefixes = %d, want 0", concurrent, totalPrefixes)
	}
}

// TestProberRunIsIdempotent confirms a re-run with the same input still
// succeeds and still leaves no state. The controller (Stage 2) re-runs the
// probe on every reconcile; idempotency keeps the index stable across that
// loop instead of growing or surfacing transient ok→failed flicker.
func TestProberRunIsIdempotent(t *testing.T) {
	prober, idx := newProberForTest(t, nil)
	for i := 0; i < 3; i++ {
		result := prober.Run(t.Context(), ProbeRequest{Backend: "cb-1", Model: "m", HashScheme: "vllm"})
		if !result.AllPassed() {
			t.Fatalf("iteration %d: AllPassed() = false; result = %+v", i, result)
		}
	}
	_, totalPrefixes := idx.CacheState(ProbeTenantID, "m")
	if totalPrefixes != 0 {
		t.Fatalf("after 3 idempotent runs, totalPrefixes = %d, want 0", totalPrefixes)
	}
}

// TestProberRunDoesNotEvictRealWorkloadOnFullIndex pins the blocking
// invariant the probe must never violate: on a near-full index (totalEntries
// already at MaxEntries), the probe's transient ingest MUST NOT trigger the
// global cap sweep. The defense is index.WithReservedTenants(ProbeTenantID):
// probe entries don't count toward maxEntries AND are excluded from the cap-
// sweep victim candidate set, so the cap sweep is effectively invisible to
// the probe path. Without WithReservedTenants the seeded real-workload entry
// would be evicted to make room for the probe's transient +1.
func TestProberRunDoesNotEvictRealWorkloadOnFullIndex(t *testing.T) {
	// Index sized exactly to one entry, with the probe tenant reserved.
	idx := index.New(
		index.WithMaxEntries(1),
		index.WithReservedTenants(ProbeTenantID),
	)
	idx.Start(t.Context())
	prober := NewProber(idx, nil)

	// Seed a real-workload entry that fills the entire cap.
	idx.Ingest(index.Update{
		ReplicaID: "real-replica", Model: "m", Tenant: "real-tenant", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 64}},
	})

	// Run the probe. With WithReservedTenants, the probe-tenant entry is
	// cap-invisible: enforceCapLocked sees effectiveTotal=1 (the real entry)
	// even though totalEntries is 2 momentarily. No eviction.
	result := prober.Run(t.Context(), ProbeRequest{
		Backend: "cb-1", Model: "m", HashScheme: "vllm",
	})
	if !result.AllPassed() {
		t.Fatalf("probe AllPassed = false on near-cap index; result = %+v", result)
	}

	// The real workload entry MUST survive. If the probe-tenant exemption is
	// broken, the seeded entry would have been evicted as the oldest victim.
	scores := idx.Lookup(index.LookupRequest{
		Tenant: "real-tenant", Model: "m", HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	if len(scores) != 1 || scores[0].ReplicaID != "real-replica" {
		t.Fatalf("real-workload entry evicted by probe ingest on a full index; scores = %+v", scores)
	}
}

// TestProberRunConcurrentWithRealWorkloadDoesNotEvict pins the second half
// of the "no real-workload eviction" invariant: the test above proves the
// probe's OWN write doesn't trigger the cap sweep; this test proves that a
// CONCURRENT real-workload Ingest, racing the probe, doesn't pick a real-
// workload entry as victim either. That requires the reserved-tenants
// option (excluding probe entries from cap accounting AND victim candidacy).
// Without that pairing, the probe's transient +1 entry would still push
// totalEntries over the cap from the concurrent ingest's perspective.
func TestProberRunConcurrentWithRealWorkloadDoesNotEvict(t *testing.T) {
	idx := index.New(
		index.WithMaxEntries(1),
		index.WithReservedTenants(ProbeTenantID),
	)
	idx.Start(t.Context())
	prober := NewProber(idx, nil)

	idx.Ingest(index.Update{
		ReplicaID: "real-replica", Model: "m", Tenant: "real-tenant", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 64}},
	})

	const concurrent = 16
	done := make(chan struct{}, concurrent*2)
	for i := 0; i < concurrent; i++ {
		go func() {
			_ = prober.Run(t.Context(), ProbeRequest{
				Backend: "cb-1", Model: "m", HashScheme: "vllm",
			})
			done <- struct{}{}
		}()
		go func() {
			// Concurrent real-workload re-ingest: same key as the seed, so the
			// upsert is a refresh — totalEntries stays the same but the cap
			// sweep still runs. Without WithReservedTenants, the sweep would
			// see (totalEntries=1 real + 1 probe) > cap=1 and pick a victim.
			idx.Ingest(index.Update{
				ReplicaID: "real-replica", Model: "m", Tenant: "real-tenant", HashScheme: "vllm",
				Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 64}},
			})
			done <- struct{}{}
		}()
	}
	for i := 0; i < concurrent*2; i++ {
		<-done
	}

	// The real workload entry must still be there.
	scores := idx.Lookup(index.LookupRequest{
		Tenant: "real-tenant", Model: "m", HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	if len(scores) != 1 || scores[0].ReplicaID != "real-replica" {
		t.Fatalf("real-workload entry evicted under concurrent probe + ingest; got scores = %+v", scores)
	}
}

// TestProberRunReservedTenantUsedRegardlessOfRequest pins the contract that
// the caller cannot supply a tenant: ProbeRequest carries no tenant field,
// so a malicious or buggy caller cannot probe under a real workload tenant
// (which would mask real entries during cleanup). The check ingests under
// the reserved tenant and asserts a real-tenant lookup is unaffected.
func TestProberRunReservedTenantUsedRegardlessOfRequest(t *testing.T) {
	prober, idx := newProberForTest(t, nil)

	// Seed a real-workload entry. The probe MUST NOT touch this.
	idx.Ingest(index.Update{
		ReplicaID: "real-replica", Model: "m", Tenant: "real-tenant", HashScheme: "vllm",
		Prefixes: []index.PrefixRef{{PrefixHash: []byte("p"), TokenCount: 64}},
	})

	_ = prober.Run(t.Context(), ProbeRequest{Backend: "cb-1", Model: "m", HashScheme: "vllm"})

	// The real workload entry must survive. If the probe somehow leaked into
	// real-tenant's scope, ALL_CLEARED cleanup would have removed it.
	scores := idx.Lookup(index.LookupRequest{
		Tenant: "real-tenant", Model: "m", HashScheme: "vllm", PrefixHash: []byte("p"),
	})
	if len(scores) != 1 || scores[0].ReplicaID != "real-replica" {
		t.Fatalf("real-tenant entry disturbed by probe; got scores = %+v", scores)
	}
}

// TestProbeHandlerHappyPath drives the HTTP handler with a valid request
// body and asserts the JSON response shape. This is the wire surface the
// controller (Stage 2) reads.
func TestProbeHandlerHappyPath(t *testing.T) {
	prober, _ := newProberForTest(t, nil)
	handler := NewProbeHTTPHandler(prober)

	body, err := json.Marshal(ProbeRequest{Backend: "cb-1", Model: "m", HashScheme: "vllm"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/probe", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	var result ProbeResult
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatalf("decode body: %v (body=%s)", err, rr.Body.String())
	}
	if result.Backend != "cb-1" {
		t.Errorf("Backend = %q, want %q", result.Backend, "cb-1")
	}
	if !result.AllPassed() {
		t.Errorf("AllPassed() = false; result = %+v", result)
	}
}

// TestProbeHandlerRejectsNonPost confirms the handler is POST-only.
// Mirrors /snapshot's GET-only and /policy's POST/PUT guard — uniform
// method discipline across the three controller-facing endpoints.
func TestProbeHandlerRejectsNonPost(t *testing.T) {
	prober, _ := newProberForTest(t, nil)
	handler := NewProbeHTTPHandler(prober)

	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req := httptest.NewRequest(method, "/probe", nil)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s returned %d, want 405", method, rr.Code)
		}
		if got := rr.Header().Get("Allow"); got != http.MethodPost {
			t.Errorf("%s Allow header = %q, want %q", method, got, http.MethodPost)
		}
	}
}

// TestProbeHandlerRejectsMalformedBody covers the decode-side failures:
// invalid JSON, missing required fields, unknown fields (DisallowUnknownFields
// guard).
func TestProbeHandlerRejectsMalformedBody(t *testing.T) {
	prober, _ := newProberForTest(t, nil)
	handler := NewProbeHTTPHandler(prober)

	cases := []struct {
		name string
		body string
	}{
		{"invalid json", "not-json"},
		{"missing backend", `{"model":"m","hashScheme":"vllm"}`},
		{"missing model", `{"backend":"cb-1","hashScheme":"vllm"}`},
		{"missing hashScheme", `{"backend":"cb-1","model":"m"}`},
		{"unknown field", `{"backend":"cb-1","model":"m","hashScheme":"vllm","tenant":"override"}`},
		// Trailing JSON value after the first object: rejected so a
		// crafted body can't smuggle a second decoded value past the
		// strictness implied by DisallowUnknownFields.
		{"trailing json object", `{"backend":"cb-1","model":"m","hashScheme":"vllm"} {"backend":"x","model":"y","hashScheme":"vllm"}`},
		// Trailing closing bracket — the case dec.More() alone misses.
		{"trailing bracket", `{"backend":"cb-1","model":"m","hashScheme":"vllm"}]`},
		// Trailing garbage that's neither a JSON value nor a bracket.
		{"trailing garbage", `{"backend":"cb-1","model":"m","hashScheme":"vllm"}garbage`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/probe", strings.NewReader(tc.body))
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			if rr.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400; body=%s", rr.Code, rr.Body.String())
			}
		})
	}
}

// TestProbeNotServedOnPublicListener mirrors the /snapshot and /policy
// safeguards: /probe MUST NOT be reachable on the kubelet/Prometheus
// listener — only on the dedicated snapshot listener (which is itself
// gated by NetworkPolicy + bearer auth in production).
func TestProbeNotServedOnPublicListener(t *testing.T) {
	_, publicURL, _, stop := startInProcessServerConnFull(t)
	defer stop()

	body := emptyProbeRequestBody(t)
	code := postJSON(t, publicURL+"/probe", "", body)
	if code != http.StatusNotFound {
		t.Fatalf("POST /probe on public listener returned %d, want 404", code)
	}
}

// TestProbeServedOnSnapshotListener is the positive wire path: a valid
// POST to /probe on the snapshot listener succeeds (no auth wiring in the
// test default, mirroring TestPolicyServedOnSnapshotListener).
func TestProbeServedOnSnapshotListener(t *testing.T) {
	_, _, snapshotURL, stop := startInProcessServerConnFull(t)
	defer stop()

	body := emptyProbeRequestBody(t)
	code := postJSON(t, snapshotURL+"/probe", "", body)
	if code != http.StatusOK {
		t.Fatalf("POST /probe on snapshot listener returned %d, want 200", code)
	}
}

// TestControllerAuth_ProbeRejectsUnauthenticated is the auth-wiring gate
// for /probe — symmetric with TestControllerAuth_RejectsUnauthenticated
// for /snapshot and /policy. A request without a bearer must 401; a
// request with the controller SA must succeed; both outcomes must register
// in the per-endpoint metric (inferencecache_probe_auth_total) so a
// dashboard can tell probe auth failures from snapshot or policy ones.
func TestControllerAuth_ProbeRejectsUnauthenticated(t *testing.T) {
	const sa = "system:serviceaccount:inference-cache-system:inference-cache-controller-manager"

	reviewer := fakeReviewerFunc(func(_ context.Context, tr *authnv1.TokenReview) (*authnv1.TokenReview, error) {
		if tr.Spec.Token == "good" {
			return &authnv1.TokenReview{Status: authnv1.TokenReviewStatus{
				Authenticated: true,
				User:          authnv1.UserInfo{Username: sa},
			}}, nil
		}
		return &authnv1.TokenReview{Status: authnv1.TokenReviewStatus{Authenticated: false}}, nil
	})

	const bufSize = 1024 * 1024
	grpcListener := bufconn.Listen(bufSize)
	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen public: %v", err)
	}
	snapshotListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen snapshot: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- New(WithControllerAuth(reviewer, sa, "")).Serve(ctx, grpcListener, httpListener, snapshotListener)
	}()
	defer func() {
		cancel()
		if err := <-errCh; err != nil {
			t.Errorf("serve shutdown: %v", err)
		}
	}()

	probeURL := "http://" + snapshotListener.Addr().String() + "/probe"
	body := emptyProbeRequestBody(t)
	if code := postJSON(t, probeURL, "", body); code != http.StatusUnauthorized {
		t.Fatalf("unauth POST /probe returned %d, want 401", code)
	}
	if code := postJSON(t, probeURL, "good", body); code != http.StatusOK {
		t.Fatalf("authed POST /probe returned %d, want 200", code)
	}

	publicURL := "http://" + httpListener.Addr().String()
	mcode, mbody := getString(t, publicURL+"/metrics")
	if mcode != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200", mcode)
	}
	for _, want := range []string{
		`inferencecache_probe_auth_total{result="unauth"} 1`,
		`inferencecache_probe_auth_total{result="ok"} 1`,
	} {
		if !strings.Contains(mbody, want) {
			t.Errorf("metrics missing %q; body:\n%s", want, mbody)
		}
	}
}

// emptyProbeRequestBody returns a minimal valid ProbeRequest body so wire
// tests don't bake JSON literals (and silently break when the schema adds
// required fields).
func emptyProbeRequestBody(t *testing.T) string {
	t.Helper()
	b, err := json.Marshal(ProbeRequest{Backend: "cb-1", Model: "m", HashScheme: "vllm"})
	if err != nil {
		t.Fatalf("marshal probe request: %v", err)
	}
	return string(b)
}

// postJSON issues a POST with the given body and optional bearer token,
// returning the HTTP status. Sibling of postPolicy — /probe and /policy are
// both POST endpoints so the helper is structurally identical.
func postJSON(t *testing.T, url, token, body string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	httpClient := http.Client{Timeout: 2 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("post %s: %v", url, err)
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	return resp.StatusCode
}

func stageErrorPresent(errs []ProbeStageError, stage string) bool {
	for _, e := range errs {
		if e.Stage == stage {
			return true
		}
	}
	return false
}

func stageErrorMessage(errs []ProbeStageError, stage string) string {
	for _, e := range errs {
		if e.Stage == stage {
			return e.Message
		}
	}
	return ""
}

package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/cachebox-project/inference-cache/pkg/index"
)

// Functional-probe machinery.
//
// The probe is a deterministic synthetic round-trip the controller drives at
// each CacheBackend reconcile, AFTER the existing first-KV-event baseline check
// (pods Up, Service endpoints, first KV event seen). It catches CACHE-PLANE-
// INTERNAL silent-failure modes a Ready=True backend was hiding:
//
//   - Index keying / scheme handling / policy gating regressions that drop
//     well-formed events on ingest (Stage A).
//   - Routing-layer regressions that return NO_HINT for entries that ARE in
//     the index (Stage B). This includes tenant_id propagation drift between
//     the gRPC handler and the index lookup, and any index-side hash-bytes
//     storage change that the probe's synthesis hasn't been updated to match.
//   - A tier-2 (LMCache) put/get cycle that silently 0-hits because the
//     engine-side client and server-side storage disagree on the wire format
//     (Stage C, when an LMCache T2Prober is wired).
//
// What the probe DOES NOT catch: a proxy↔subscriber wire-encoding
// disagreement at the workload edge. The probe synthesizes one set of bytes
// and uses them VERBATIM on both ingest and lookup, so a real proxy emitting
// one encoding while the subscriber reports another is invisible to this
// probe. That class of bug needs complementary surfaces — workload-side
// metrics on subscriber-emitted vs index-stored hashes, or an end-to-end
// smoke driving a real engine through both halves.
//
// The probe synthesizes its own state (a deterministic 16-token block under a
// reserved tenant_id) and round-trips it through the three stages: a SUBSCRIBER
// pipeline check (the synthesized event lands in the index), a ROUTING check
// (the index returns PREFIX_MATCH for the probe's hash), and a T2 check (a
// put/get cycle through the supplied T2Prober). Each stage reports ok / failed
// / skipped; failures name the stage so the controller can surface a
// stage-specific condition with an operator-actionable message.
//
// This file ships the server-side machinery and the HTTP /probe handler. The
// controller wiring — calling /probe from the CacheBackend reconciler and
// writing the FunctionalProbeOK condition — is a follow-up; the metric
// (inferencecache_backend_probe_result) ships with the controller wiring too.
// Internal types here that are not yet read by anything outside this file's
// tests are explicitly carved out (per the project's "no inert field" rule —
// wired today, or names the follow-up that will wire it).

// ProbeTenantID is the reserved tenant id every probe synthesizes its state
// under. Real workload tenants (CacheTenant.spec.tenantID) are MinLength=1 and
// arbitrary; the slash-delimited `inferencecache.io/probe` form is in the
// project-canonical `inferencecache.io/...` namespace so a real tenant cannot
// accidentally collide. The /probe HTTP handler does not accept a caller-
// supplied tenant — the reservation is enforced server-side, never trusted
// from the request — so a real workload cannot read or write probe entries by
// spoofing the tenant id.
const ProbeTenantID = "inferencecache.io/probe"

// ProbeReplicaPrefix is the literal prefix every probe replica id starts with.
// Real subscribers set replica_id = pod-name (see PROJECT_CONTEXT §"Replica
// identity convention"); pod names cannot start with "__", so the reserved
// `__probe-` prefix is collision-free. Cleanup keys on this prefix (and the
// probe tenant + backend-derived suffix) to wipe ONLY the probe's own state on
// each Run, never a real replica's entries.
const ProbeReplicaPrefix = "__probe-"

// ProbeTokenCount is the per-block token count carried by the synthesized
// BlockStored. 16 is the smallest unit a vLLM-class engine reports
// (one KV block) — small enough that the probe payload stays trivial in the
// index even if cleanup is somehow skipped, but non-zero so the LookupRoute
// ranker treats it as a real prefix hit.
const ProbeTokenCount = int32(16)

// ProbeStageResult is the per-stage outcome encoded in the JSON ProbeResult
// the controller reads. Strings (not enums) so the wire stays stable when
// new outcomes appear — same forward-compat reasoning as gRPC reason_code.
type ProbeStageResult string

// Possible per-stage outcomes. "skipped" applies to a stage the probe chose
// not to run (T2 on a non-LMCache backend, or any downstream stage when an
// upstream stage failed — running them would surface a cascade of false
// failures that masks the real one).
const (
	ProbeStageOK      ProbeStageResult = "ok"
	ProbeStageFailed  ProbeStageResult = "failed"
	ProbeStageSkipped ProbeStageResult = "skipped"
)

// Stage names appear verbatim in ProbeStageError.Stage and (Stage 2) in the
// inferencecache_backend_probe_result metric `stage` label.
const (
	ProbeStageSubscriber = "subscriber"
	ProbeStageRouting    = "routing"
	ProbeStageT2         = "t2"
)

// BackendTypeLMCache is the spec.type value that gates Stage C. The probe
// only runs the T2 put/get against LMCache-backed CacheBackends — Memory
// and External backends carry no tier-2 client to drive. The string MUST
// agree with the CacheBackend.spec.type enum value in api/v1alpha1; using
// the literal here (rather than importing the CRD types) keeps pkg/server
// dependency-free of the CRD package, matching the policy/tenant pattern in
// pkg/server/policy.go.
//
// An empty BackendType on a ProbeRequest is treated as LMCache to match the
// CacheBackend CRD's defaulter (spec.type defaults to LMCache via the
// kubebuilder marker). Operators who run the probe by hand against a non-
// LMCache backend must set BackendType explicitly; the controller-wiring
// follow-up always reads spec.type from the CR and never sends empty.
const BackendTypeLMCache = "LMCache"

// ProbeRequest carries the parameters the probe needs to synthesize a
// deterministic round-trip. The tenant_id is NOT a request field — it is
// always ProbeTenantID, fixed server-side.
//
// Backend uniquely identifies which CacheBackend the probe is running
// against AND is interpolated into the reserved replica id
// (__probe-<backend>) and the deterministic probe hash. To prevent
// same-name CacheBackends in different namespaces from colliding in the
// reserved replica id, callers MUST pass a globally-unique form — the
// canonical shape is `<namespace>/<name>` (matching K8s resource identity).
// The controller-wiring follow-up always sends `<namespace>/<name>`; the
// HTTP handler validates that the field is non-empty but does not enforce
// the slash format, since hand-invoked probes on a single-namespace
// install can use any unique string.
//
// Model + HashScheme pin the engine domain the synthesized state lives
// under (so a probe for the vllm adapter cannot collide with a probe for
// the sglang adapter on the same backend). BackendType decides whether
// Stage C runs.
type ProbeRequest struct {
	Backend     string `json:"backend"`
	Model       string `json:"model"`
	HashScheme  string `json:"hashScheme"`
	BackendType string `json:"backendType,omitempty"`
}

// ProbeResult is the per-stage outcome returned to the controller. The
// controller maps a stage's failed result onto the corresponding
// FunctionalProbeOK condition reason (controller-wiring follow-up):
//
//	subscriber failed → ProbeSubscriberFailed
//	routing    failed → ProbeRoutingFailed
//	t2         failed → ProbeT2Failed
//
// Errors carries a stage-keyed message so the operator-visible condition
// surfaces a concrete diagnostic, not just "something failed".
type ProbeResult struct {
	Backend    string            `json:"backend"`
	Subscriber ProbeStageResult  `json:"subscriber"`
	Routing    ProbeStageResult  `json:"routing"`
	T2         ProbeStageResult  `json:"t2"`
	Errors     []ProbeStageError `json:"errors,omitempty"`
}

// ProbeStageError names one stage's failure mode in operator-readable form.
// Stage is one of ProbeStageSubscriber / ProbeStageRouting / ProbeStageT2.
type ProbeStageError struct {
	Stage   string `json:"stage"`
	Message string `json:"message"`
}

// AllPassed reports whether every stage either passed or was intentionally
// skipped (every stage has an explicit ok|skipped value, none is failed and
// none is the zero-value empty string). The HTTP /probe handler ALWAYS
// returns the full ProbeResult as JSON (HTTP 200 — the call itself
// succeeded), and the caller reads AllPassed to flip the FunctionalProbeOK
// condition once the controller wiring follow-up lands. The predicate is
// exported so that follow-up doesn't have to re-derive the "passed?"
// definition from the per-stage fields.
//
// The zero-value-fails-closed property matters: a partially-decoded result
// (the JSON envelope was truncated, a future field was renamed, the caller
// constructed a ProbeResult{} without running anything) MUST NOT pass —
// "no information" is not the same as "all three stages passed."
func (r ProbeResult) AllPassed() bool {
	return stagePassed(r.Subscriber) && stagePassed(r.Routing) && stagePassed(r.T2)
}

// stagePassed returns true only for an explicit ok or skipped — empty
// string (zero value, unrecognized future outcome) AND failed both return
// false. Keeps AllPassed's three-stage check readable as one predicate.
func stagePassed(s ProbeStageResult) bool {
	return s == ProbeStageOK || s == ProbeStageSkipped
}

// T2Prober drives a put/get round trip against an external tier-2 backend
// (today: LMCache). It is the seam the controller-wiring follow-up fills
// with a real LMCache client; this file ships the interface + a test fake.
// Returning nil means the cycle round-tripped cleanly; returning an error
// means the stage failed. Wrap the error in *T2ProbeError to tell the
// controller which half (put vs get) broke — the operator-facing condition
// message names the half, which is what disambiguates the two LMCache
// failure modes.
type T2Prober interface {
	ProbePutGet(ctx context.Context, backend string, payload []byte) error
}

// T2ProbeError tells the orchestrator which half of the put/get cycle failed,
// so the ProbeStageError surfaces "put failed: ..." vs "get failed: ..." in
// the operator-visible condition message. Phase is "put" or "get".
type T2ProbeError struct {
	Phase string
	Err   error
}

func (e *T2ProbeError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return e.Phase + " failed"
	}
	return e.Phase + " failed: " + e.Err.Error()
}

func (e *T2ProbeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Prober is the server-side functional self-test orchestrator.
// It synthesizes a probe BlockStored, ingests it through the live index,
// drives a LookupRoute against the index for the same hash, optionally
// drives a tier-2 put/get through the T2Prober, then cleans up via a
// CacheEvent_ALL_CLEARED for its reserved replica id so the probe leaves
// the index empty of its own state.
//
// This file ships Prober with t2=nil — every backend reports T2=skipped until
// the controller wiring follow-up plumbs a real T2Prober through. The
// controller integration is the only consumer that's not yet present; the
// HTTP /probe handler + the tests in this package are the live callers today.
type Prober struct {
	index *index.Index
	t2    T2Prober
	now   func() time.Time

	// runMu serializes Run by backend so two concurrent probes for the same
	// CacheBackend cannot race through the shared reserved-replica state. A
	// per-(backend, model) mutex is the smallest scope that prevents the race
	// — probes for DIFFERENT backends synthesize under DIFFERENT __probe-
	// replica ids and never collide. Without this, two overlapping runs could
	// see one's cleanup wipe the other's just-ingested entry mid-flight and
	// report a false subscriber/routing failure. The controller-wiring
	// follow-up additionally rate-limits to ~once per backend per 30s, so
	// contention here is rare in practice — the lock is the correctness
	// backstop for the no-rate-limit case (tests, hand-invoked probes,
	// future fast reconcile cycles).
	runMuMap sync.Map // key: "{backend}\x00{model}" → *sync.Mutex

	// ingestFn is the stage-A index-write step. Defaults to p.index.Ingest;
	// tests override it to simulate a subscriber pipeline that drops events
	// (the bug surfaces this way — the writer thinks it sent, the index
	// never recorded).
	ingestFn func(index.Update)

	// routeFn is the stage-B routing step. Defaults to p.index.LookupRoute
	// (the same orchestrated ranking entry-point the gRPC handler runs
	// through s.lookupFn); tests override it to simulate a routing layer
	// that returns NO_HINT even though the entry is in the index (the
	// bug surfaces this way — index has the bytes, lookup reports nothing).
	routeFn func(index.LookupRequest) index.LookupResult
}

// NewProber wires a probe orchestrator to the live index + an optional
// T2Prober. Passing nil for t2 disables Stage C (the probe always reports
// T2=skipped); the controller-wiring follow-up passes a real implementation.
func NewProber(idx *index.Index, t2 T2Prober) *Prober {
	return &Prober{
		index:    idx,
		t2:       t2,
		now:      time.Now,
		ingestFn: idx.Ingest,
		routeFn:  idx.LookupRoute,
	}
}

// lockForRun returns the per-(backend, model) mutex Run takes while it
// synthesizes + cleans up, lazily creating it on first use. The lock scope
// matches the reserved replica id's (backend, model) tuple — probes for
// different (backend, model) tuples never contend with each other.
func (p *Prober) lockForRun(backend, model string) *sync.Mutex {
	key := backend + "\x00" + model
	if m, ok := p.runMuMap.Load(key); ok {
		return m.(*sync.Mutex)
	}
	m, _ := p.runMuMap.LoadOrStore(key, &sync.Mutex{})
	return m.(*sync.Mutex)
}

// ProbeReplicaID is the reserved replica id the probe synthesizes its state
// under for a given backend. Exposed so the controller (when the wiring
// follow-up lands) can construct the same id when asserting "no probe
// entries leaked into a real lookup" in envtest integration tests.
func ProbeReplicaID(backend string) string {
	return ProbeReplicaPrefix + backend
}

// ProbeHash returns the deterministic 32-byte SHA-256 of a canonical input
// keyed by (backend, hashScheme). Same inputs → same bytes; callers can
// re-derive the hash for assertions without re-running the probe.
//
// What the probe round-trip ACTUALLY catches:
// the probe synthesizes one set of bytes and uses them VERBATIM on both
// ingest and lookup, so a NO_HINT here points to a cache-plane-internal
// regression (index keying drift, scheme handling, policy gating, routing
// orchestration) — not a proxy/subscriber wire-encoding disagreement.
// A proxy↔subscriber encoding mismatch by definition involves bytes the
// PROXY produces vs bytes the SUBSCRIBER reports; this probe doesn't run
// the proxy, so it cannot reproduce that exact failure. Catching that
// class of bug requires complementary surfaces (workload-side metrics on
// the subscriber's emitted hashes vs. the index's stored hashes, or an
// end-to-end smoke that drives a real engine through both halves).
// The encoding note matters here because if future work changes the
// index's storage format to fix such a mismatch, the probe's hash
// synthesis must match the new format — otherwise the probe will round-
// trip cleanly while real workloads continue to NO_HINT.
func ProbeHash(backend, hashScheme string) []byte {
	h := sha256.New()
	// Domain-separated input so a future hashing change can't collide with
	// any other SHA-256 the project might compute over a similar shape.
	h.Write([]byte("inferencecache.io/probe/v1\n"))
	h.Write([]byte("hashScheme:"))
	h.Write([]byte(hashScheme))
	h.Write([]byte{'\n'})
	h.Write([]byte("backend:"))
	h.Write([]byte(backend))
	h.Write([]byte{'\n'})
	return h.Sum(nil)
}

// Run executes the three stages and returns the per-stage outcome. The
// caller's context bounds the T2 stage (Stage C); stages A and B are
// in-memory and complete in microseconds. Run is idempotent: invoking it
// again with the same request overwrites the prior synthesized state and
// runs cleanup again at the end, leaving the index empty of probe entries.
//
// The probe never mutates real workload state: it uses ProbeTenantID and a
// reserved __probe-<backend> replica id, and the cleanup step emits an
// EventAllCleared against that reserved replica so leftover entries from a
// successful run cannot leak into a real LookupRoute. A run that returns
// before cleanup (panic, ctx done) still leaves only reserved-tenant entries
// the index's TTL sweep will eventually reap — the reserved naming makes the
// residue invisible to real workload lookups regardless.
func (p *Prober) Run(ctx context.Context, req ProbeRequest) ProbeResult {
	replicaID := ProbeReplicaID(req.Backend)
	probeHash := ProbeHash(req.Backend, req.HashScheme)

	// Serialize probes for this (backend, model) so two overlapping calls cannot
	// race through the shared reserved-replica state — one's cleanup wiping the
	// other's just-ingested entry would otherwise surface as a false subscriber
	// or routing failure. The lock is per-(backend, model); probes for different
	// CacheBackends run in parallel without contention.
	mu := p.lockForRun(req.Backend, req.Model)
	mu.Lock()
	defer mu.Unlock()

	result := ProbeResult{Backend: req.Backend}

	// Cleanup ALWAYS runs — even on early return from a Stage-A failure or a
	// panic in Stage B/C — so a flaky probe can't leak reserved-tenant entries
	// past the call. ApplyEvent under a reserved tenant + replica id is a no-op
	// when nothing was synthesized (e.g., if HashScheme is empty and Stage A
	// dropped the ingest), so cleanup is safe regardless of how far Run got.
	defer p.cleanup(req.Model, replicaID)

	// Stage A — subscriber pipeline. Synthesize a BlockStored equivalent and
	// verify it actually lands in the index. We check via a DIRECT index.Lookup
	// (not the orchestrated routeFn) so this stage isolates the ingest+index-
	// write path: if the entry is there, Stage A passes regardless of any
	// routing-layer regression. Routing failures are stage B's responsibility.
	update := index.Update{
		ReplicaID:  replicaID,
		Model:      req.Model,
		Tenant:     ProbeTenantID,
		HashScheme: req.HashScheme,
		Timestamp:  p.now(),
		Prefixes: []index.PrefixRef{{
			BlockHashes:      [][]byte{probeHash},
			BlockTokenCounts: []int32{ProbeTokenCount},
		}},
		Stats: &index.ReplicaStats{
			ReplicaID:        replicaID,
			CacheMemoryBytes: 0,
			HitRate:          1.0,
			Pressure:         0,
		},
	}
	p.ingestFn(update)

	directReq := index.LookupRequest{
		Tenant:           ProbeTenantID,
		Model:            req.Model,
		HashScheme:       req.HashScheme,
		BlockHashes:      [][]byte{probeHash},
		BlockTokenCounts: []int32{ProbeTokenCount},
	}
	if !replicaInScores(p.index.Lookup(directReq), replicaID) {
		result.Subscriber = ProbeStageFailed
		result.Errors = append(result.Errors, ProbeStageError{
			Stage:   ProbeStageSubscriber,
			Message: "synthesized probe event did not land in the index — subscriber→index write path is broken",
		})
		// An entry that never landed cannot route, so Stage B is undefined;
		// skip it so the controller's condition pinpoints the upstream stage
		// instead of also flagging a routing failure that's just a cascade.
		result.Routing = ProbeStageSkipped
		result.T2 = ProbeStageSkipped
		return result
	}
	result.Subscriber = ProbeStageOK

	// Stage B — routing path. Run the orchestrated LookupRoute (PREFIX_MATCH
	// vs TENANT_HOT vs NO_HINT) against the same hash bytes. The probe synthesizes
	// matching scheme + tenant + model on both sides, so a NO_HINT here means the
	// routing/index layer itself is broken — exactly the encoding/tenant-mismatch
	// regression class the probe must catch.
	routeRes := p.routeFn(directReq)
	if routeRes.Strategy != index.StrategyPrefixMatch || !replicaInScores(routeRes.Scores, replicaID) {
		result.Routing = ProbeStageFailed
		result.Errors = append(result.Errors, ProbeStageError{
			Stage:   ProbeStageRouting,
			Message: fmt.Sprintf("LookupRoute returned %s, expected PREFIX_MATCH — possible tenant_id or hash encoding mismatch", reasonForStrategy(routeRes.Strategy)),
		})
	} else {
		result.Routing = ProbeStageOK
	}

	// Stage C — T2 cycle. Skip on non-LMCache backends (no tier-2 to test) and
	// when no prober is wired (Stage 1 default). Empty BackendType is treated as
	// LMCache to match the CacheBackend CRD's default (spec.type defaults to
	// LMCache via the kubebuilder marker); without this an omitted field would
	// silently turn a real LMCache probe into "skipped" and the controller's
	// FunctionalProbeOK condition would flip True on a backend that never
	// proved its T2 round-trip works. The payload is small and deterministic so
	// a successful round-trip is observable as a byte match on the receiving
	// side; the probe doesn't care about the payload's content, only that what
	// went in came out.
	runT2 := req.BackendType == BackendTypeLMCache || req.BackendType == ""
	if !runT2 || p.t2 == nil {
		result.T2 = ProbeStageSkipped
		return result
	}
	if err := p.t2.ProbePutGet(ctx, req.Backend, probePayload(probeHash)); err != nil {
		result.T2 = ProbeStageFailed
		result.Errors = append(result.Errors, ProbeStageError{
			Stage:   ProbeStageT2,
			Message: fmt.Sprintf("T2 put/get cycle failed: %s", t2ErrorMessage(err)),
		})
		return result
	}
	result.T2 = ProbeStageOK
	return result
}

// cleanup wipes the probe's synthesized entries for one (model, replicaID)
// scope under the reserved tenant. ApplyEvent(ALL_CLEARED) removes ALL prefix
// entries the named replica held in (tenant, model) — across schemes — so a
// probe that ran across multiple HashSchemes on the same backend (Stage 2
// could choose to probe both vllm and sglang adapters in turn) still leaves
// no residue. The TTL sweep is the defense-in-depth backstop for a Run that
// panics before defer fires.
func (p *Prober) cleanup(model, replicaID string) {
	p.index.ApplyEvent(index.Event{
		Type:      index.EventAllCleared,
		ReplicaID: replicaID,
		Model:     model,
		Tenant:    ProbeTenantID,
		Timestamp: p.now(),
	})
}

// replicaInScores returns true when at least one score matches the replicaID.
// Both Stage A's direct index.Lookup and Stage B's index.LookupRoute return
// []index.ReplicaScore, so the same predicate fits both stages.
func replicaInScores(scores []index.ReplicaScore, replicaID string) bool {
	for _, s := range scores {
		if s.ReplicaID == replicaID {
			return true
		}
	}
	return false
}

// probePayload is the bytes the T2 prober writes and reads back. Keyed on the
// probe hash so two backends running probes concurrently can't collide on the
// same put key, and so a real workload's KV bytes never accidentally match a
// probe payload. The probe doesn't inspect the bytes — only that what went
// in came out — so this is opaque from the probe's perspective.
func probePayload(probeHash []byte) []byte {
	// Length-prefixed shape so the T2 backend sees a stable, parseable payload
	// regardless of the SHA-256 output size — same defense-in-depth as the
	// probe-hash input domain separator.
	out := make([]byte, 0, len(probeHash)+len("probe-payload-v1\n"))
	out = append(out, "probe-payload-v1\n"...)
	out = append(out, probeHash...)
	return out
}

// t2ErrorMessage extracts the operator-readable detail from a T2 cycle error.
// A *T2ProbeError carries phase information so the message names "put" or
// "get" explicitly; any other error type is surfaced verbatim.
func t2ErrorMessage(err error) string {
	var t2Err *T2ProbeError
	if errors.As(err, &t2Err) {
		return t2Err.Error()
	}
	return err.Error()
}

// probeHandler returns the HTTP handler for the /probe endpoint backed by the
// supplied Prober. It is exposed so tests can mount the handler directly
// (same pattern as policyHandler), and so a Stage 2 controller test can
// stand up an in-process server with the exact same decode path the binary
// mounts. The handler is auth-agnostic; the auth + NetworkPolicy gating
// lives in server.New, where the same TokenReview-backed bearer middleware
// that protects /snapshot and /policy is also applied here. Body size is
// capped at 4 KiB to bound memory if a buggy caller sends a runaway request
// (the JSON envelope is ~hundreds of bytes).
func probeHandler(prober *Prober) http.HandlerFunc {
	const maxBytes = 4 << 10 // 4 KiB — comfortably above any realistic request
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "method not allowed\n", http.StatusMethodNotAllowed)
			return
		}
		body := http.MaxBytesReader(w, r.Body, maxBytes)
		defer func() { _ = body.Close() }()
		dec := json.NewDecoder(body)
		dec.DisallowUnknownFields()
		var req ProbeRequest
		if err := dec.Decode(&req); err != nil {
			http.Error(w, "decode probe request: "+err.Error()+"\n", http.StatusBadRequest)
			return
		}
		// Reject trailing tokens after the first JSON value so a body like
		// `{...} {...}` cannot silently slip through DisallowUnknownFields
		// (which only catches unknown KEYS within the first decoded value).
		// Mirrors the strictness implied by the field-level guard.
		if dec.More() {
			http.Error(w, "probe request body has trailing content after the first JSON value\n", http.StatusBadRequest)
			return
		}
		if req.Backend == "" || req.Model == "" || req.HashScheme == "" {
			http.Error(w, "probe request missing required field (backend, model, hashScheme)\n", http.StatusBadRequest)
			return
		}
		result := prober.Run(r.Context(), req)
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(result); err != nil {
			// The encoder writes the response status implicitly via WriteHeader
			// on the first byte; if encoding fails partway we can't recover the
			// status, so just log via http.Error which is a no-op once headers
			// are flushed. Mirrors the /snapshot handler's posture.
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

// NewProbeHTTPHandler returns the HTTP handler the controller mounts at
// /probe, backed by the supplied Prober. Exposed for the Stage 2 controller
// envtest path that wants to mount the exact same handler shape in-process.
func NewProbeHTTPHandler(prober *Prober) http.HandlerFunc {
	return probeHandler(prober)
}

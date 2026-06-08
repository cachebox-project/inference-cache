package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	cacheserver "github.com/cachebox-project/inference-cache/pkg/server"
)

// Functional-probe gate: the controller composes the in-process functional-
// probe result on TOP of evaluateKVEventReadiness. It answers the operator
// question that the existing gates do NOT — "is the cache plane round-trip
// ACTUALLY working for this backend?" — by driving the server-side /probe
// endpoint.
//
// Composition order on a managed CacheBackend's Reconcile:
//
//	managedReadiness     → Deployment-level ready/progressing
//	    ↓
//	evaluateKVEventReadiness → first-event readiness gate layered on top
//	    ↓
//	evaluateFunctionalProbe  → THIS file: probe gate layered on top
//	    ↓
//	patchStatus              → atomic write of all conditions
//
// The probe gate is itself rate-limited (per-CacheBackend, ~30s) and
// CASCADE-SAFE: if the upstream KV-event gate already says Ready=False the
// probe call is skipped — a broken upstream cannot be diagnosed by a
// downstream probe, and bombarding the server with probes for backends that
// aren't going to be Ready anyway just adds noise.
//
// An operator escape hatch (the inferencecache.io/skip-functional-probe
// annotation) bypasses the call AND flips FunctionalProbeOK=True so the
// Ready gate doesn't downgrade. Same per-CR shape as
// annotationRequireKVEvents — alpha soft-rollout knob, per-backend, not a
// global flag.

const (
	// conditionTypeFunctionalProbeOK is the new condition type the gate
	// publishes alongside Ready/Progressing/Degraded. True means the most
	// recent probe call succeeded across every stage the backend's
	// configuration runs (subscriber-equivalent ingest, routing, and T2 if
	// LMCache). False means a specific stage failed — the operator-visible
	// condition reason names the stage. Unknown means the controller could
	// not reach /probe at all (transport error, 5xx) — the existing Ready
	// verdict is preserved so a brief outage doesn't flap every backend.
	conditionTypeFunctionalProbeOK = "FunctionalProbeOK"

	// Functional-probe condition reasons. The first three mirror the stage
	// names ProbeResult.Errors[].Stage uses, so a sed-translation between
	// the JSON body and the operator-facing reason is straightforward.
	reasonProbeOK            = "ProbeOK"
	reasonProbeIngestFailed  = "ProbeIngestFailed"
	reasonProbeRoutingFailed = "ProbeRoutingFailed"
	reasonProbeT2Failed      = "ProbeT2Failed"
	reasonProbeBypassed      = "ProbeBypassed"
	reasonProbeError         = "ProbeError"

	// annotationSkipFunctionalProbe is the per-CR escape hatch. Set to
	// exactly "true" to opt this CacheBackend out of the gate — the probe
	// call is skipped and FunctionalProbeOK is published as True with
	// reason=ProbeBypassed. Any other value (or absence) leaves the gate
	// on. Matches annotationRequireKVEvents's per-CR alpha-knob shape.
	annotationSkipFunctionalProbe = "inferencecache.io/skip-functional-probe"

	// probeFixedModel is the synthetic model identifier the controller
	// sends in every probe call. CacheBackends don't have a "model" field
	// (a backend may serve many engines, each handling many models), and
	// the probe synthesizes its own state under the reserved tenant, so
	// the model only needs to be a deterministic stable string. Using a
	// well-known sentinel keeps probe-related index entries trivially
	// identifiable in any operator dump.
	probeFixedModel = "functional-self-test"

	// DefaultProbeRateLimit is the floor for "how recently did we probe
	// this backend?" — a second reconcile within the window skips the
	// probe call AND leaves the existing FunctionalProbeOK condition
	// untouched. Matches the ticket's "max once per CacheBackend per ~30s"
	// requirement. Tests override CacheBackendReconciler.ProbeRateLimit.
	DefaultProbeRateLimit = 30 * time.Second
)

// probeResultMetric is `inferencecache_backend_probe_result_total{backend, stage, result}` —
// the per-stage outcome counter the dashboards key off. Label cardinality is
// O(backends × 3 stages × 3 outcomes), well within Prometheus's comfort zone
// even on large fleets. Name carries the `_total` suffix per Prometheus
// counter naming convention (every other first-party counter in this repo
// follows the same shape — see docs/reference/metrics.md). Registered on the
// controller-runtime registry so it shares the controller binary's /metrics
// endpoint with controller-runtime's own controller_runtime_* series.
var probeResultMetric = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "inferencecache_backend_probe_result_total",
		Help: "Per-stage outcome of the CacheBackend functional probe. One increment per stage per probe call. backend=<namespace>/<name>; stage=ingest|routing|t2; result=ok|failed|skipped.",
	},
	[]string{"backend", "stage", "result"},
)

func init() {
	crmetrics.Registry.MustRegister(probeResultMetric)
}

// probeRateLimiter is the per-(namespace, name) "last successfully-called-at"
// map the reconciler uses to gate the probe call. *sync.Map is the right
// shape: each CacheBackend Reconcile is single-flighted by controller-
// runtime, but the reconciler may process many backends concurrently
// across its worker goroutines, so the rate-limit map needs to be lock-
// free across keys.
type probeRateLimiter struct {
	last sync.Map // key: types.NamespacedName.String() → time.Time
}

// lastCalled returns the most recently-recorded probe-call timestamp for
// key, or the zero time if no call has been recorded. The caller uses
// the result to (a) decide whether to issue a fresh probe call, and
// (b) schedule a requeue at the next-allowed time so a quiet backend
// stuck in failure state still recovers when no other watch events fire.
func (p *probeRateLimiter) lastCalled(key string) time.Time {
	if p == nil {
		return time.Time{}
	}
	v, ok := p.last.Load(key)
	if !ok {
		return time.Time{}
	}
	t, ok := v.(time.Time)
	if !ok {
		return time.Time{}
	}
	return t
}

// allow reports whether the caller should issue a probe call for this
// backend right now. Returns true when no prior probe has been recorded
// (lastCalled is zero) or the elapsed time since the last call is at
// least rateLimit.
func (p *probeRateLimiter) allow(key string, now time.Time, rateLimit time.Duration) bool {
	prev := p.lastCalled(key)
	if prev.IsZero() {
		return true
	}
	return now.Sub(prev) >= rateLimit
}

// markCalled records that a probe call landed (regardless of probe-stage
// outcome — a per-stage `failed` is still a successful CALL). HTTP-level
// errors do NOT record a timestamp so the next reconcile retries
// immediately. The reconciler invokes markCalled AFTER the status patch
// commits — see functionalProbeVerdict.commitMark — so a failed patch
// does not burn a rate-limit slot, and the next retry re-runs the probe.
func (p *probeRateLimiter) markCalled(key string, now time.Time) {
	if p == nil {
		return
	}
	p.last.Store(key, now)
}

// forget removes the recorded timestamp for a deleted CacheBackend so the
// per-(namespace, name) map doesn't accumulate stale entries forever on a
// long-running controller against a churning fleet. Called from the
// reconciler's NotFound branch — see CacheBackendReconciler.Reconcile.
func (p *probeRateLimiter) forget(key string) {
	if p == nil {
		return
	}
	p.last.Delete(key)
}

// functionalProbeVerdict is the gate's output, applied to the running
// kvReadiness verdict by the reconciler's updateManagedStatus. The struct
// carries everything the caller needs to (a) atomically write/remove the
// FunctionalProbeOK condition under the same status patch as the other
// conditions, (b) re-apply the Ready downgrade across rate-limited
// reconciles so a previously-failed probe's verdict isn't silently
// overwritten by the upstream KV gate, (c) requeue the backend at the
// next-allowed-probe time so a quiet failure recovers on its own, and
// (d) only burn a rate-limit slot AFTER the status patch persists.
type functionalProbeVerdict struct {
	// shouldWriteCondition reports whether the gate decided the
	// FunctionalProbeOK condition should be (re-)written this reconcile.
	// False when the gate decided to NO-OP (rate-limited with no
	// existing-False to inherit, upstream Ready=False, probe disabled
	// with no existing condition to clear) — the caller leaves any
	// existing condition alone.
	shouldWriteCondition bool

	// condition is the new FunctionalProbeOK to publish. Populated only
	// when shouldWriteCondition is true.
	condition metav1.Condition

	// removeCondition signals the caller to delete any existing
	// FunctionalProbeOK from the backend's conditions. Set when the
	// probe is structurally disabled (nil/empty ProbeClient) so a CR
	// previously gated by a probe that's now turned off doesn't carry
	// a stale operator-visible condition indefinitely.
	removeCondition bool

	// downgradeReady is set when the gate decided Ready should be False.
	// Two paths:
	//   - Fresh probe call returned a stage failure: downgrade with the
	//     stage-specific reason and the server's diagnostic.
	//   - Rate-limited reconcile AND the existing FunctionalProbeOK
	//     condition was False: re-apply the prior downgrade so the
	//     status patch doesn't overwrite Ready=False/Probe*Failed with
	//     the upstream KV-gate's Ready=True/KVEventsObserved.
	downgradeReady bool
	readyReason    string
	readyMessage   string

	// requeueAfter, when non-zero, asks the caller to schedule another
	// reconcile at most this duration in the future. Used by the rate-
	// limit path so a quiet backend stuck in failure state still
	// re-probes when the window expires — without it, recovery would
	// depend on incidental external watch events.
	requeueAfter time.Duration

	// commitMark is invoked by the caller AFTER the status patch persists
	// (i.e. patchStatus returns nil). When non-nil it records the rate-
	// limit timestamp for the call this verdict represents — so a failed
	// patch does NOT burn a rate-limit slot, and the next retry re-runs
	// the probe immediately. Nil when the gate skipped the actual probe
	// call (rate-limited, bypassed, disabled, etc.).
	commitMark func()
}

// evaluateFunctionalProbe layers the functional-probe gate on top of the
// KV-event-gate verdict. It is a pure function (no Kubernetes API calls; no
// state outside what's passed in) so the rate-limit + transport behavior
// can be exercised end-to-end in unit tests without standing up a real
// reconciler. The CacheBackend reconciler's updateManagedStatus invokes it
// with the running kvReadiness, an optional ProbeClient (nil disables the
// gate), and a rate-limit cache it owns.
//
// State machine:
//
//	upstream Ready=False (KV gate said no)? → no probe call, no condition write
//	external/unsupported backend (no managed workload)? → caller doesn't invoke this; out of scope
//	probe client disabled (nil or empty URL)?
//	    existing FunctionalProbeOK present? → emit removeCondition so the stale
//	                                          condition is dropped (probe is
//	                                          structurally off now)
//	    no existing condition?             → no-op
//	annotation inferencecache.io/skip-functional-probe=true? → write FunctionalProbeOK=True, reason=ProbeBypassed
//	rate-limited (last call within rateLimit)?
//	    existing FunctionalProbeOK=False? → re-apply Ready downgrade (no write);
//	                                       requeue at window expiry
//	    other existing values             → no condition write; requeue at window expiry
//	call /probe →
//	    AllPassed? → write FunctionalProbeOK=True, reason=ProbeOK; mark called
//	    per-stage failed? → write FunctionalProbeOK=False, reason=Probe<Stage>Failed; downgrade Ready; mark called
//	    HTTP error? → write FunctionalProbeOK=Unknown, reason=ProbeError; LEAVE Ready alone; DO NOT mark called (retry next reconcile)
func evaluateFunctionalProbe(
	ctx context.Context,
	backend *cachev1alpha1.CacheBackend,
	upstream kvReadiness,
	probe *ProbeClient,
	limiter *probeRateLimiter,
	rateLimit time.Duration,
	now time.Time,
) functionalProbeVerdict {
	// Probe disabled (nil client, empty URL): the gate is structurally
	// off. Don't publish a fresh condition — a FunctionalProbeOK on a
	// reconciler that never calls /probe would just be misleading. But a
	// previously-written condition would persist forever without our help,
	// so if one exists, ask the caller to remove it.
	//
	// This check runs BEFORE the upstream-not-Ready short-circuit on
	// purpose: a backend in RolloutInProgress / AwaitingFirstKVEvent /
	// ReplicasUnavailable is not "ready enough to probe", but if the
	// operator just disabled --server-probe-url, the stale condition
	// SHOULD still be cleaned up regardless of upstream readiness. The
	// alternative (cleanup gated on upstream Ready=True) leaves a stale
	// FunctionalProbeOK on every not-yet-ready backend until the upstream
	// gate flips, which can be arbitrarily long.
	if probe == nil || probe.ProbeURL == "" {
		if existing := meta.FindStatusCondition(backend.Status.Conditions, conditionTypeFunctionalProbeOK); existing != nil {
			return functionalProbeVerdict{removeCondition: true}
		}
		return functionalProbeVerdict{}
	}

	// Cascade prevention: the probe answers "is the cache plane round-trip
	// working for this backend?" but only when the rest of the readiness
	// chain has already said the backend is otherwise ready. A broken
	// upstream can't be diagnosed by a downstream probe, and probing every
	// not-yet-ready backend on every reconcile just adds load.
	if upstream.readyStatus != metav1.ConditionTrue {
		return functionalProbeVerdict{}
	}

	// Annotation bypass: per-CR escape hatch. The brief calls this out
	// explicitly as an operator emergency knob — keep it before the rate
	// limit so a backend the operator has explicitly opted out doesn't pin
	// the rate-limiter slot for nothing.
	if isProbeBypassed(backend) {
		return functionalProbeVerdict{
			shouldWriteCondition: true,
			condition: metav1.Condition{
				Type:               conditionTypeFunctionalProbeOK,
				Status:             metav1.ConditionTrue,
				Reason:             reasonProbeBypassed,
				Message:            "functional probe bypassed via " + annotationSkipFunctionalProbe + ": \"true\"",
				ObservedGeneration: backend.Generation,
			},
		}
	}

	// Rate limit. Multiple reconciles within the window must inherit the
	// existing condition value — controller-runtime's reconcile loop fires
	// freely on Watch events, so without this the probe would hammer the
	// server on every CR/Deployment/Pod tick. CRITICAL: when the existing
	// condition is False, re-apply the Ready downgrade so the status patch
	// that updateManagedStatus is about to issue doesn't silently overwrite
	// Ready=False/ProbeIngestFailed with Ready=True/KVEventsObserved from
	// the upstream KV gate. Also schedule a requeue at the window-expiry
	// boundary so a quiet failed backend re-probes when the limit lifts,
	// without waiting for an incidental watch event.
	key := backend.Namespace + "/" + backend.Name
	if !limiter.allow(key, now, rateLimit) {
		verdict := functionalProbeVerdict{
			requeueAfter: requeueUntilNextProbe(limiter.lastCalled(key), rateLimit, now),
		}
		existing := meta.FindStatusCondition(backend.Status.Conditions, conditionTypeFunctionalProbeOK)
		if existing != nil && existing.Status == metav1.ConditionFalse {
			verdict.downgradeReady = true
			verdict.readyReason = existing.Reason
			verdict.readyMessage = existing.Message
		}
		return verdict
	}

	// Call the probe. Note: backend = <namespace>/<name> per the wire
	// contract; hashScheme is derived from spec.integration.engine so the
	// probe lands under the same engine domain a real KV event from this
	// backend would; backendType is spec.type so the server's Stage C gate
	// fires correctly (T2 runs only for LMCache).
	req := cacheserver.ProbeRequest{
		Backend:     key,
		Model:       probeFixedModel,
		HashScheme:  probeHashSchemeForBackend(backend),
		BackendType: string(backend.Spec.Type),
	}
	result, err := probe.Run(ctx, req)
	switch {
	case errors.Is(err, ErrProbeClientDisabled):
		// Race: the client became disabled between the precondition check
		// above and the actual call. Treat the same as "no condition".
		return functionalProbeVerdict{}
	case err != nil:
		// HTTP-level failure — server unreachable, transport timeout, 5xx,
		// JSON decode failure. By default a transient outage publishes
		// FunctionalProbeOK=Unknown and DOES NOT downgrade Ready: a brief
		// server outage should not flip every backend Ready=False (that's
		// the noise mode the existing CacheIndex poller / policy pusher
		// already avoid). The Unknown status is the operator's signal to
		// investigate the probe path itself.
		//
		// EXCEPTION: if the existing FunctionalProbeOK was already False
		// from a prior known stage failure, the transient HTTP error must
		// NOT silently upgrade Ready to True OR overwrite the False
		// condition with Unknown (which would forget the original
		// failure reason and the cycle would erase the operator-visible
		// downgrade). Preserve the existing False condition AND re-apply
		// the Ready downgrade until a successful probe call proves
		// recovery — symmetric to the rate-limited path above.
		//
		// No commitMark — failed calls don't burn a rate-limit slot, so
		// the next reconcile retries immediately when the server comes
		// back. recordProbeResult is also skipped: a failed transport
		// isn't a per-stage outcome. A non-zero requeueAfter ensures that
		// "next reconcile" actually fires on a quiet backend without
		// relying on incidental external watch events; use the rate-limit
		// window as a sensible retry cadence.
		existing := meta.FindStatusCondition(backend.Status.Conditions, conditionTypeFunctionalProbeOK)
		if existing != nil && existing.Status == metav1.ConditionFalse {
			return functionalProbeVerdict{
				// Do NOT write a new condition — leave the existing False
				// in place so a sequence of HTTP errors doesn't fade the
				// failure to Unknown and then upgrade to True. The False
				// is sticky until a successful probe resolves it.
				downgradeReady: true,
				readyReason:    existing.Reason,
				readyMessage:   existing.Message,
				requeueAfter:   rateLimit,
			}
		}
		return functionalProbeVerdict{
			shouldWriteCondition: true,
			condition: metav1.Condition{
				Type:               conditionTypeFunctionalProbeOK,
				Status:             metav1.ConditionUnknown,
				Reason:             reasonProbeError,
				Message:            "probe call failed: " + truncateMessage(err.Error()),
				ObservedGeneration: backend.Generation,
			},
			requeueAfter: rateLimit,
		}
	}

	// Successful call. Build the verdict; commitMark fires AFTER the
	// status patch succeeds so a failed patch doesn't burn a rate-limit
	// slot. The metric records the per-stage outcome immediately because
	// it observes work we already DID — independent of whether the status
	// patch persists.
	recordProbeResult(key, result)
	commitMark := func() { limiter.markCalled(key, now) }

	if result.AllPassed() {
		return functionalProbeVerdict{
			shouldWriteCondition: true,
			condition: metav1.Condition{
				Type:               conditionTypeFunctionalProbeOK,
				Status:             metav1.ConditionTrue,
				Reason:             reasonProbeOK,
				Message:            "functional probe round-trip succeeded across all enabled stages",
				ObservedGeneration: backend.Generation,
			},
			requeueAfter: rateLimit,
			commitMark:   commitMark,
		}
	}

	// At least one stage failed. Find the stage that failed (cascade
	// prevention on the SERVER side guarantees only the first failed stage
	// is reported as failed; downstream stages are skipped, not failed),
	// pull its diagnostic message, and translate into Ready+condition.
	reason, message := stageReasonAndMessage(result)
	return functionalProbeVerdict{
		shouldWriteCondition: true,
		condition: metav1.Condition{
			Type:               conditionTypeFunctionalProbeOK,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            message,
			ObservedGeneration: backend.Generation,
		},
		downgradeReady: true,
		readyReason:    reason,
		readyMessage:   message,
		requeueAfter:   rateLimit,
		commitMark:     commitMark,
	}
}

// requeueUntilNextProbe returns the duration from now until the rate-limit
// window expires for a backend that was last called at lastAt. Returns
// rateLimit when lastAt is the zero time (defensive — the caller shouldn't
// reach this branch in that case). A non-positive remaining window
// degenerates to a tight 1s requeue so the next reconcile fires soon
// without busy-looping.
func requeueUntilNextProbe(lastAt time.Time, rateLimit time.Duration, now time.Time) time.Duration {
	if lastAt.IsZero() {
		return rateLimit
	}
	remaining := rateLimit - now.Sub(lastAt)
	if remaining <= 0 {
		return time.Second
	}
	return remaining
}

// stageReasonAndMessage translates a failed ProbeResult into the operator-
// facing reason + message pair. The probe's cascade-prevention guarantees
// at most one stage is `failed` (downstream stages report `skipped`), so
// the order of the checks doesn't matter for correctness; ordering them
// upstream→downstream just makes the result deterministic across releases
// even if cascade prevention is ever loosened.
func stageReasonAndMessage(result cacheserver.ProbeResult) (string, string) {
	switch {
	case result.Ingest == cacheserver.ProbeStageFailed:
		return reasonProbeIngestFailed, errorMessageForStage(result, cacheserver.ProbeStageIngest, "in-process index ingest path is broken")
	case result.Routing == cacheserver.ProbeStageFailed:
		return reasonProbeRoutingFailed, errorMessageForStage(result, cacheserver.ProbeStageRouting, "index routing/key-derivation regression")
	case result.T2 == cacheserver.ProbeStageFailed:
		return reasonProbeT2Failed, errorMessageForStage(result, cacheserver.ProbeStageT2, "tier-2 put/get cycle failed")
	}
	// Defensive: AllPassed already returned false but no individual stage
	// is `failed` — a future addition (a fourth stage) would hit this
	// branch until the switch is updated.
	return reasonProbeError, "probe reported a failure with no recognized stage"
}

// errorMessageForStage looks up the stage's diagnostic message in the
// ProbeResult.Errors slice; falls back to the supplied default when the
// server didn't populate one. The fallback is operator-actionable on its
// own.
func errorMessageForStage(result cacheserver.ProbeResult, stage string, fallback string) string {
	for _, e := range result.Errors {
		if e.Stage == stage && strings.TrimSpace(e.Message) != "" {
			return truncateMessage(e.Message)
		}
	}
	return fallback
}

// recordProbeResult emits one increment per stage on the probe-result
// metric. Stages that the server skipped report skipped=1; only stages
// the server actually exercised report ok/failed. Match the server's
// per-stage names so the metric's `stage` label aligns with the JSON
// body's stage names.
//
// Empty AND unknown stage values are coerced to "failed" — the alerting
// contract MUST match what `cacheserver.ProbeResult.AllPassed` /
// `stagePassed` consider non-passing. If a malformed `{}` body, a future
// server emitting a stage name this controller hasn't learned yet, or any
// other non-"ok"/non-"skipped" value reaches us, `AllPassed` returns false
// (the gate downgrades Ready with `ProbeError`), so the per-stage metric
// MUST also record "failed" — otherwise `ServerProbeFail` silently misses
// the regression even though the operator-facing condition surfaces it.
// The forward-compat info (the actual wire string) is preserved in the
// `FunctionalProbeOK` condition message, not in the metric label set
// (keeping metric cardinality bounded).
func recordProbeResult(backendKey string, result cacheserver.ProbeResult) {
	for _, s := range []struct {
		stage  string
		result cacheserver.ProbeStageResult
	}{
		{cacheserver.ProbeStageIngest, result.Ingest},
		{cacheserver.ProbeStageRouting, result.Routing},
		{cacheserver.ProbeStageT2, result.T2},
	} {
		switch s.result {
		case cacheserver.ProbeStageOK, cacheserver.ProbeStageSkipped, cacheserver.ProbeStageFailed:
			// Recognized value — emit verbatim.
		default:
			// Empty wire value OR a future/unknown outcome — coerce to
			// "failed" so the alert path is honest about the unknown state.
			s.result = cacheserver.ProbeStageFailed
		}
		probeResultMetric.WithLabelValues(backendKey, s.stage, string(s.result)).Inc()
	}
}

// isProbeBypassed returns true when the operator-set escape-hatch
// annotation requests the gate be bypassed for this backend. Only the
// exact string "true" counts; anything else (absent, empty, "1", "True")
// leaves the gate enabled. Conservative parsing — the annotation is an
// alpha knob and a forgiving parser would mis-fire on accidental edits.
func isProbeBypassed(backend *cachev1alpha1.CacheBackend) bool {
	if backend == nil {
		return false
	}
	return backend.Annotations[annotationSkipFunctionalProbe] == "true"
}

// probeHashSchemeForBackend derives the probe's hashScheme from a backend's
// engine setting so the synthesized probe state lives under the same engine
// domain real KV events from this backend would. The CacheBackend CRD's
// spec.integration.engine carries the runtime ID (canonical: "vllm" |
// "sglang"); an empty value falls back to "vllm" matching the CRD
// defaulter. Kept here next to the probe gate rather than in the runtime
// adapter package because the probe's identifier scheme is server-contract
// (not adapter behavior).
func probeHashSchemeForBackend(backend *cachev1alpha1.CacheBackend) string {
	if backend == nil || backend.Spec.Integration == nil || strings.TrimSpace(backend.Spec.Integration.Engine) == "" {
		return "vllm"
	}
	return strings.ToLower(strings.TrimSpace(backend.Spec.Integration.Engine))
}

// truncateMessage caps a diagnostic string at a length that fits cleanly in
// the operator-visible condition message. Conditions don't enforce a hard
// limit, but kubectl prints them on a single line and an unbounded server
// error (e.g. a Go stack trace) would be unreadable. 512 chars matches the
// existing message lengths emitted by the rest of the reconciler.
func truncateMessage(s string) string {
	const limit = 512
	if len(s) <= limit {
		return s
	}
	return s[:limit-3] + "..."
}

// downgradeReadyVerdict applies a probe-failure downgrade to a kvReadiness
// verdict. Mirrors the existing "stickiness" pattern in
// evaluateKVEventReadiness — the Ready condition gets the probe's reason
// and message, but Degraded is left alone (the Degraded condition narrates
// the managed-Deployment health; the probe diagnoses the cache plane, not
// the Deployment). Exposed as a helper so the call site stays compact and
// the formatting of the surfaced message stays consistent.
func downgradeReadyVerdict(upstream kvReadiness, verdict functionalProbeVerdict) kvReadiness {
	if !verdict.downgradeReady {
		return upstream
	}
	out := upstream
	out.readyStatus = metav1.ConditionFalse
	out.readyReason = verdict.readyReason
	out.readyMessage = fmt.Sprintf("functional probe failed: %s", verdict.readyMessage)
	return out
}

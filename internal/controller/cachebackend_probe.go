package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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

// probeResultMetric is `inferencecache_backend_probe_result{backend, stage, result}` —
// the per-stage outcome counter the dashboards key off. Label cardinality is
// O(backends × 3 stages × 3 outcomes), well within Prometheus's comfort zone
// even on large fleets. Registered on the controller-runtime registry so it
// shares the controller binary's /metrics endpoint with controller-runtime's
// own controller_runtime_* series; matches the convention every other
// controller-side metric in this binary follows.
var probeResultMetric = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "inferencecache_backend_probe_result",
		Help: "Per-stage outcome of the CacheBackend functional probe. One increment per stage per probe call. backend=<namespace>/<name>; stage=ingest|routing|t2; result=ok|failed|skipped.",
	},
	[]string{"backend", "stage", "result"},
)

func init() {
	crmetrics.Registry.MustRegister(probeResultMetric)
}

// probeRateLimiter is the per-(namespace, name) "last successfully-called-at"
// map the reconciler uses to gate the probe call. Exposed as a method on the
// reconciler (next file) but the type lives here so the gate code that reads
// it can keep its dependencies focused. *sync.Map is the right shape: each
// CacheBackend Reconcile is single-flighted by controller-runtime, but the
// reconciler may process many backends concurrently across its worker
// goroutines, so the rate-limit map needs to be lock-free across keys.
type probeRateLimiter struct {
	last sync.Map // key: types.NamespacedName.String() → time.Time
}

// allow reports whether the caller should issue a probe call for this
// backend right now. It returns true when no prior probe has been recorded,
// or when the elapsed time since the last recorded call is >= rateLimit.
// Updating the recorded "last probed at" timestamp is the caller's
// responsibility — the rate-limit only counts against successful calls (an
// HTTP error doesn't extend the window, so the next reconcile retries
// promptly instead of waiting out the full rate-limit window).
func (p *probeRateLimiter) allow(key string, now time.Time, rateLimit time.Duration) bool {
	if p == nil {
		return true
	}
	v, ok := p.last.Load(key)
	if !ok {
		return true
	}
	prev, ok := v.(time.Time)
	if !ok {
		return true
	}
	return now.Sub(prev) >= rateLimit
}

// markCalled records that a probe call landed (regardless of probe-stage
// outcome — a per-stage `failed` is still a successful CALL). HTTP-level
// errors do NOT record a timestamp so the next reconcile retries
// immediately.
func (p *probeRateLimiter) markCalled(key string, now time.Time) {
	if p == nil {
		return
	}
	p.last.Store(key, now)
}

// functionalProbeVerdict is the gate's output, applied to the running
// kvReadiness verdict by the reconciler's updateManagedStatus.
type functionalProbeVerdict struct {
	// shouldWriteCondition reports whether the gate decided the
	// FunctionalProbeOK condition should be (re-)written this reconcile.
	// False when the gate decided to NO-OP (probe disabled, rate-limited,
	// upstream Ready=False) — the caller leaves any existing condition
	// alone.
	shouldWriteCondition bool

	// condition is the new FunctionalProbeOK to publish. Populated only
	// when shouldWriteCondition is true.
	condition metav1.Condition

	// downgradeReady is set when a probe stage failed (result is `failed`
	// for ingest/routing/t2). The caller flips the inherited Ready
	// condition from True to False with the same reason+message — the
	// operator-visible Ready verdict points at the broken stage.
	downgradeReady bool
	readyReason    string
	readyMessage   string
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
//	probe client disabled (nil or empty URL)? → no condition write (probe not configured)
//	annotation inferencecache.io/skip-functional-probe=true? → write FunctionalProbeOK=True, reason=ProbeBypassed
//	rate-limited (last call within rateLimit)? → no condition write (preserve last result)
//	call /probe →
//	    AllPassed? → write FunctionalProbeOK=True, reason=ProbeOK
//	    per-stage failed? → write FunctionalProbeOK=False, reason=Probe<Stage>Failed; downgrade Ready
//	    HTTP error? → write FunctionalProbeOK=Unknown, reason=ProbeError; LEAVE Ready alone
func evaluateFunctionalProbe(
	ctx context.Context,
	backend *cachev1alpha1.CacheBackend,
	upstream kvReadiness,
	probe *ProbeClient,
	limiter *probeRateLimiter,
	rateLimit time.Duration,
	now time.Time,
) functionalProbeVerdict {
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

	// Probe disabled (nil client, empty URL): the gate is structurally
	// off. Don't write a condition — a published FunctionalProbeOK on a
	// reconciler that never calls /probe would just be a misleading lie.
	if probe == nil || probe.ProbeURL == "" {
		return functionalProbeVerdict{}
	}

	// Rate limit. Multiple reconciles within the window inherit the
	// existing condition value — controller-runtime's reconcile loop fires
	// freely on Watch events, so without this the probe would hammer the
	// server on every CR/Deployment/Pod tick.
	key := backend.Namespace + "/" + backend.Name
	if !limiter.allow(key, now, rateLimit) {
		return functionalProbeVerdict{}
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
		// JSON decode failure. Publish FunctionalProbeOK=Unknown with the
		// transport error in the message but DO NOT downgrade Ready: a
		// brief server outage should not flip every backend Ready=False
		// (that's the noise mode the existing CacheIndex poller / policy
		// pusher already avoid). The Unknown status is the operator's
		// signal to investigate the probe path itself.
		return functionalProbeVerdict{
			shouldWriteCondition: true,
			condition: metav1.Condition{
				Type:               conditionTypeFunctionalProbeOK,
				Status:             metav1.ConditionUnknown,
				Reason:             reasonProbeError,
				Message:            "probe call failed: " + truncateMessage(err.Error()),
				ObservedGeneration: backend.Generation,
			},
		}
	}

	// Successful call — record the timestamp so the rate-limit kicks in
	// for the next reconcile, then translate the per-stage outcome into
	// the condition and the metric.
	limiter.markCalled(key, now)
	recordProbeResult(key, result)

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
	}
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
func recordProbeResult(backendKey string, result cacheserver.ProbeResult) {
	for _, s := range []struct {
		stage  string
		result cacheserver.ProbeStageResult
	}{
		{cacheserver.ProbeStageIngest, result.Ingest},
		{cacheserver.ProbeStageRouting, result.Routing},
		{cacheserver.ProbeStageT2, result.T2},
	} {
		if s.result == "" {
			// Empty wire value: defensive — count as skipped so the metric
			// stays accurate at three increments per call.
			s.result = cacheserver.ProbeStageSkipped
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

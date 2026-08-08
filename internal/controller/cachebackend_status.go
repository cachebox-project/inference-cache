// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sort"
	"strings"
	"time"
)

// Status condition types published on a managed CacheBackend.
//
// Ready reports whether the managed backend workload is currently serving
// (gated by the KV-event readiness gate — see evaluateKVEventReadiness).
// Progressing reports whether the controller is still driving the live state
// toward the desired state (template render, child apply, rollout in flight,
// awaiting first KV event). Degraded reports a terminal unhealthy state.
// Ready + Progressing together tell a still-converging backend
// (Ready=False, Progressing=True) apart from a stuck/degraded one
// (Ready=False, Progressing=False); Degraded names the specific failure.
const (
	conditionTypeReady       = "Ready"
	conditionTypeProgressing = "Progressing"
	// Degraded is published alongside Ready. It is True only when the
	// backend is in a genuinely degraded terminal state (rolled out but
	// replicas unavailable, or the workload is Available but no KV events
	// observed within firstEventTimeout).
	conditionTypeDegraded = "Degraded"
)

// KV-event readiness gate.
const (
	// annotationRequireKVEvents opts a CacheBackend OUT of the KV-event
	// readiness gate when set exactly to "false". Absent or any other value
	// leaves the gate enabled (default-on). It is a per-CR annotation rather
	// than a spec field because it is an alpha soft-rollout knob meant to be
	// retired once the gate is trusted (a spec field is harder to retract),
	// and per-CR so one backend can opt out without affecting others.
	annotationRequireKVEvents = "inferencecache.io/require-kv-events"

	// defaultFirstEventTimeout is the fallback when
	// spec.observation.firstEventTimeout is unset and the API-server default
	// ("5m") was not applied (e.g. fake-client unit tests). Mirrors the
	// +kubebuilder:default on the field.
	defaultFirstEventTimeout = 5 * time.Minute

	// Ready/Degraded condition reasons set by the gate. These double as the
	// Event reasons emitted on the corresponding transitions.
	reasonAwaitingFirstKVEvent = "AwaitingFirstKVEvent"
	reasonKVEventsObserved     = "KVEventsObserved"
	reasonNoKVEventsObserved   = "NoKVEventsObserved"

	// reasonNotDegraded is the Degraded=False condition reason.
	reasonNotDegraded = "NotDegraded"
)

// T2Degraded — advisory tier-2 (external offload, e.g. LMCache) health,
// derived from status.indexParticipation.t2HitRate (written by the CacheIndex
// poller). Published only once the tier has been exercised; it NEVER gates
// Ready — tier-2 is an optimization, not a serving dependency (fail-open).
const (
	conditionTypeT2Degraded = "T2Degraded"
	// reasonT2ZeroHitRate: the tier was queried but served zero reloads — wired
	// but useless (a silently-degraded offload tier).
	reasonT2ZeroHitRate = "T2ZeroHitRate"
	// reasonT2Serving: the tier is serving reloads (hit-rate > 0).
	reasonT2Serving = "T2Serving"
)

// EngineCompatibility — advisory engine↔connector compatibility, derived from
// the live container state of the engine pods THIS backend injected cache
// config into. Published False/InjectedEngineCrashLooping only when an injected
// engine container is in CrashLoopBackOff after the cache plane wired a KV
// connector — the live observation, NOT a confirmed root cause. A structural
// engine↔connector incompatibility (canonically a hybrid-attention model that
// cannot run a KV connector) is a common cause, but a crash-loop can equally be
// a bad image, command, missing dependency/secret, or OOM. Like T2Degraded it
// is advisory: it NEVER gates Ready (the engine is operator-owned and the
// KV-event gate already drives Ready); it just names an otherwise-silent
// crash-loop and points at the likely cause. The reason doubles as the Event
// reason on the transition into it.
const (
	conditionTypeEngineCompatibility = "EngineCompatibility"
	// reasonInjectedEngineCrashLooping: an injected engine pod is crash-looping
	// after connector injection (a likely-but-unconfirmed connector
	// incompatibility, canonically a hybrid-attention model).
	reasonInjectedEngineCrashLooping = "InjectedEngineCrashLooping"
)

// Event reasons emitted on a CacheBackend.
//
// The cache is an optimization, never a serving dependency: BackendDegraded
// and BackendRecovered narrate transitions of the managed workload's
// availability so operators see backend readiness changes in
// `kubectl describe`. The FailClosedEnabled / FailOpenRestored pair
// narrates transitions of the spec.integration.failOpen toggle —
// explicitly fail-closed is loud because the cache then becomes a serving
// dependency.
const (
	eventReasonBackendDegraded         = "BackendDegraded"
	eventReasonBackendRecovered        = "BackendRecovered"
	eventReasonFailClosedEnabled       = "FailClosedEnabled"
	eventReasonFailOpenRestored        = "FailOpenRestored"
	eventReasonEngineSelectorUnmatched = "EngineSelectorUnmatched"
)

// Condition reasons published on a CacheBackend's Ready condition. Stable
// strings so consumers (the CacheIndex poller, the future readiness gate
// that watches lastEventAt, operator dashboards) can switch on reason
// instead of regexing the message.
const (
	// conditionReasonExternalEndpointAccepted is set when a CacheBackend uses
	// external remote-storage ownership and spec.remoteStorage.endpoint is
	// non-empty: admission accepted the operator-supplied endpoint and we trust
	// it without probing reachability. A future enhancement could degrade Ready
	// on a connection-probe failure, but that's out of scope for the structured
	// external binding path today (fail-soft, trust the operator).
	conditionReasonExternalEndpointAccepted = "ExternalEndpointAccepted"
	// conditionReasonExternalEndpointMissing is set defensively when an
	// externally owned CacheBackend has spec.remoteStorage.endpoint empty.
	// Admission rejects this at the validating webhook, so this branch covers
	// objects that bypassed current admission.
	conditionReasonExternalEndpointMissing = "ExternalEndpointMissing"
	// conditionReasonExternalEndpointInvalid is set defensively when an
	// externally owned CacheBackend has a non-empty
	// spec.remoteStorage.endpoint that fails the shared shape check (bad scheme,
	// no port, embedded whitespace, unbracketed IPv6, …). Current admission
	// rejects all of these; this defensive reason covers objects that bypassed
	// admission. Status reflects the gap loudly rather than advertising the
	// malformed value as Ready=True (which would let the pod webhook then inject
	// an LMCACHE_REMOTE_URL the engine connector refuses at startup — turning a
	// cache misconfiguration into a serving outage).
	conditionReasonExternalEndpointInvalid = "ExternalEndpointInvalid"
)

// updateManagedStatus derives the Ready + Progressing conditions from the Deployment and patches status only when it changes.
//
// applyOK is the convergence signal from reconcileManaged: when apply failed,
// the live Deployment we read may still reflect a *prior* CR generation, so
// advancing Status.ObservedGeneration to the current CR generation would tell
// clients the controller has caught up when it hasn't. The published
// observedGeneration therefore stays at its prior value until apply succeeds
// for the current generation; the Ready and Progressing conditions carry the
// same generation so callers can tell which spec the observation belongs to.
func (r *CacheBackendReconciler) updateManagedStatus(ctx context.Context, backend *cachev1alpha1.CacheBackend, endpoint string, dep *appsv1.Deployment, applyOK bool) (time.Duration, error) {
	now := time.Now()
	readyStatus, reason, message := managedReadiness(backend, dep)
	// Resolve the stable timeout anchor: the latched FirstAvailableAt, or — the
	// first time the workload is Available — now. Using a latched value (not the
	// live Deployment Available condition, which resets on a flap) keeps the
	// firstEventTimeout window monotonic so Degraded stays sticky.
	anchor := time.Time{}
	if backend.Status.FirstAvailableAt != nil {
		anchor = backend.Status.FirstAvailableAt.Time
	} else if readyStatus == metav1.ConditionTrue {
		anchor = now
	}
	// Layer the KV-event readiness gate on top of the Deployment-level readiness.
	// Only the workload-Available state is gated; every other Deployment state
	// passes through unchanged.
	// Managed path never bypasses stickiness: the sticky NoKVEventsObserved
	// contract (a raised firstEventTimeout after a breach can't un-Degrade) holds.
	gate := evaluateKVEventReadiness(backend, readyStatus, reason, message, anchor, now, false)
	// Layer the functional-probe gate on top of the
	// KV-event verdict. It only fires when the upstream gate would
	// otherwise say Ready=True — a backend that's already Ready=False for
	// some other reason can't be diagnosed by a downstream probe, and the
	// rate-limit caps a healthy-backend probe to ~once per 30s. The verdict
	// may downgrade Ready to False with a probe-specific reason; the
	// condition itself is published verbatim in the patchStatus closure
	// below so it lands atomically alongside Ready/Progressing/Degraded.
	probeVerdict := evaluateFunctionalProbe(ctx, backend, gate, r.ProbeClient, &r.probeLimiter, r.probeRateLimit(), now)
	gate = downgradeReadyVerdict(gate, probeVerdict)
	// Engine-kernel health gate (lmcache c_ops load). Reads the kernel-check
	// init-container status off matched engine pods; surfaces
	// EngineKernelsHealthy and, in strict mode, downgrades Ready. Uses the
	// uncached APIReader (no Pod informer), fail-soft on list errors.
	kernelReader := client.Reader(r.APIReader)
	if kernelReader == nil {
		kernelReader = r.Client
	}
	kernelPods, kernelListedOK := listMatchedEnginePods(ctx, kernelReader, backend)
	kernelVerdict := evaluateEngineKernelHealth(backend, gate, kernelPods, kernelListedOK)
	gate = downgradeKernelReadyVerdict(gate, kernelVerdict)
	progressingStatus, progressingReason, progressingMessage := progressingFromReady(gate.readyStatus, gate.readyReason, gate.readyMessage)
	publishedGen := backend.Status.ObservedGeneration
	if applyOK {
		publishedGen = backend.Generation
	}
	// Engine↔connector compatibility (advisory): detect an injected engine pod
	// stuck in CrashLoopBackOff (the hybrid-attention signature) so the
	// otherwise-silent failure surfaces. Done before the patch — it lists pods —
	// and feeds the EngineCompatibility condition in the closure below.
	engineCompatMsg, engineCompatObserved := r.detectEngineConnectorCrashLoop(ctx, backend)
	prevEngineIncompatible := meta.IsStatusConditionFalse(backend.Status.Conditions, conditionTypeEngineCompatibility)
	err := r.patchStatus(ctx, backend, func() {
		backend.Status.Endpoint = endpoint
		backend.Status.ObservedGeneration = publishedGen
		// Latch the first KV-event observation write-once. The poller can later
		// clear indexParticipation.lastEventAt on a drain, so this durable
		// marker is what keeps lastKVEventSeen true thereafter.
		if backend.Status.FirstKVEventObservedAt == nil {
			if at := currentLastEventAt(backend); at != nil {
				backend.Status.FirstKVEventObservedAt = at.DeepCopy()
			}
		}
		// Latch the first-Available time write-once — the stable, flap-immune
		// anchor for the firstEventTimeout window (see FirstAvailableAt godoc).
		if backend.Status.FirstAvailableAt == nil && readyStatus == metav1.ConditionTrue {
			t := metav1.NewTime(now)
			backend.Status.FirstAvailableAt = &t
		}
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeReady,
			Status:             gate.readyStatus,
			Reason:             gate.readyReason,
			Message:            gate.readyMessage,
			ObservedGeneration: publishedGen,
		})
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeDegraded,
			Status:             gate.degradedStatus,
			Reason:             gate.degradedReason,
			Message:            gate.degradedMessage,
			ObservedGeneration: publishedGen,
		})
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeProgressing,
			Status:             progressingStatus,
			Reason:             progressingReason,
			Message:            progressingMessage,
			ObservedGeneration: publishedGen,
		})
		// Functional-probe condition. The gate decides whether to write,
		// remove, or leave it alone this reconcile. meta.SetStatusCondition
		// / RemoveStatusCondition both honor the write-only-on-change
		// contract — same as the other three conditions above.
		switch {
		case probeVerdict.shouldWriteCondition:
			cond := probeVerdict.condition
			cond.ObservedGeneration = publishedGen
			meta.SetStatusCondition(&backend.Status.Conditions, cond)
		case probeVerdict.removeCondition:
			meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeFunctionalProbeOK)
		}
		// Engine-kernel-health condition (lmcache c_ops load). Same
		// write/remove/leave-alone contract as the functional-probe condition.
		switch {
		case kernelVerdict.shouldWriteCondition:
			kc := kernelVerdict.condition
			kc.ObservedGeneration = publishedGen
			meta.SetStatusCondition(&backend.Status.Conditions, kc)
		case kernelVerdict.removeCondition:
			meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeEngineKernelsHealthy)
		}
		// T2Degraded (advisory) — derived from the poller-written
		// status.indexParticipation.t2HitRate. Present only once tier-2 is
		// exercised: True when it served zero reloads, False otherwise. It does
		// NOT downgrade Ready (tier-2 is an optimization). formatRate renders an
		// exact 0.0 as "0", so the string compare below is exact.
		if ip := backend.Status.IndexParticipation; ip != nil && ip.T2HitRate != nil {
			t2Status, t2Reason, t2Msg := metav1.ConditionFalse, reasonT2Serving,
				"Tier-2 (external offload) cache is serving reloads (hitRate="+*ip.T2HitRate+")."
			if *ip.T2HitRate == "0" {
				t2Status, t2Reason, t2Msg = metav1.ConditionTrue, reasonT2ZeroHitRate,
					"Tier-2 (external offload) cache is wired but served zero reloads; check the remote server's availability/sizing and the engine/server version + hash-scheme compatibility."
			}
			meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
				Type:               conditionTypeT2Degraded,
				Status:             t2Status,
				Reason:             t2Reason,
				Message:            t2Msg,
				ObservedGeneration: publishedGen,
			})
		} else {
			meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeT2Degraded)
		}
		// EngineCompatibility (advisory) — present False only when an injected
		// engine pod is crash-looping after connector injection. Never gates
		// Ready. Only touched when the engine pods were actually observed: a
		// pod-list error must PRESERVE the prior verdict (clearing then
		// re-asserting on the next success would flap the event and briefly hide
		// an active incompatibility).
		if engineCompatObserved {
			if engineCompatMsg != "" {
				meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
					Type:               conditionTypeEngineCompatibility,
					Status:             metav1.ConditionFalse,
					Reason:             reasonInjectedEngineCrashLooping,
					Message:            engineCompatMsg,
					ObservedGeneration: publishedGen,
				})
			} else {
				meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeEngineCompatibility)
			}
		}
	})
	// Commit the rate-limit slot ONLY after the status patch succeeds. A
	// failed patch must not burn a window — the next reconcile retries
	// immediately and re-runs the probe call. (The gate sets commitMark
	// only on a successful probe call, so a rate-limited or HTTP-failed
	// reconcile is a no-op here.)
	if err == nil && probeVerdict.commitMark != nil {
		probeVerdict.commitMark()
	}
	// Narrate the engine-incompatibility transition once (absent/True → False).
	if err == nil && engineCompatObserved && engineCompatMsg != "" && !prevEngineIncompatible && r.Recorder != nil {
		r.Recorder.Eventf(backend, nil, corev1.EventTypeWarning,
			reasonInjectedEngineCrashLooping, reasonInjectedEngineCrashLooping, "%s", engineCompatMsg)
	}
	// Take the tighter of the KV-gate requeue and the probe-gate requeue
	// so a stuck-failing backend re-probes when its rate-limit window
	// expires even without an external watch event. A zero from either
	// side means "no requeue requested"; min() must therefore ignore the
	// zero so a non-zero half always wins.
	requeue := minNonZero(gate.requeueAfter, probeVerdict.requeueAfter)
	return requeue, err
}

// minNonZero returns the smaller of two durations, treating a zero on
// either side as "no value" so a non-zero half always wins. Used to merge
// the KV-event and functional-probe requeue requests.
func minNonZero(a, b time.Duration) time.Duration {
	switch {
	case a == 0:
		return b
	case b == 0:
		return a
	case a < b:
		return a
	default:
		return b
	}
}

// kvReadiness is the resolved readiness verdict after layering the KV-event
// gate on top of the Deployment-level readiness. readyStatus/readyReason/
// readyMessage drive Conditions[Ready]; degraded* drive Conditions[Degraded];
// requeueAfter is non-zero only inside the AwaitingFirstKVEvent window, where
// it schedules the automatic Degraded flip once firstEventTimeout elapses
// without an event.
type kvReadiness struct {
	readyStatus     metav1.ConditionStatus
	readyReason     string
	readyMessage    string
	degradedStatus  metav1.ConditionStatus
	degradedReason  string
	degradedMessage string
	requeueAfter    time.Duration
}

// evaluateKVEventReadiness layers the KV-event readiness gate on top of the
// Deployment-derived health. The signal it adds is whether at least one KV
// event has been observed for this backend (status.indexParticipation.
// lastEventAt, written by the CacheIndex poller from engine-pod reports).
//
// Motivation: a backend whose managed workload is up can still be silently
// useless to the cache plane — the inference engine may be serving HTTP while
// its ZMQ KV-event publisher is mis-configured or crashed, so nothing flows
// into the index and LookupRoute keeps returning NO_HINT. The managed
// Deployment's own readiness cannot see that; the first-KV-event signal can.
//
// State machine — the gate only refines the state once the managed
// cache-backend Deployment is Available (managedReadiness Ready=True). Every
// other Deployment state (rollout in progress, scaled to zero, replicas
// unavailable) passes through unchanged:
//
//	Workload Available? | event seen? | within timeout? | Ready | Degraded | reason
//	No                  | -           | -               | (passthrough deployment readiness)
//	Yes                 | No          | Yes             | False | False    | AwaitingFirstKVEvent
//	Yes                 | Yes         | -               | True  | False    | KVEventsObserved
//	Yes                 | No          | No              | False | True     | NoKVEventsObserved
//
// "Event seen" is "have we EVER seen an event for this backend" — a non-nil
// lastEventAt already present on the first reconcile counts (no transition
// through AwaitingFirstKVEvent is required). The gate is opt-out per CR via the
// inferencecache.io/require-kv-events: "false" annotation. External backends
// never reach this code path (reconcileExternal short-circuits dispatch), so
// they are unconditionally exempt.
//
// Timeout anchor: `anchor` is the caller-resolved start of the firstEventTimeout
// clock — the write-once status.firstAvailableAt latch (the time the workload
// was first observed Available). It is deliberately NOT the live Deployment's
// Available condition LastTransitionTime, which resets on an availability flap:
// a latched anchor keeps the elapsed window monotonic, so once a backend
// breaches the timeout (Degraded / NoKVEventsObserved) a later flap cannot move
// it back to AwaitingFirstKVEvent — it stays Degraded until an event arrives.
// (Once firstKVEventObservedAt is latched the gate is satisfied regardless of
// the anchor, since lastKVEventSeen short-circuits.)
// freshWindow forces the gate to ignore a persisted sticky NoKVEventsObserved
// reason and re-evaluate from the caller-resolved anchor. It is set only on a
// mode transition that legitimately restarts the first-event clock (a
// server-bearing → events-only flip, where reconcileEventsOnly re-anchors to
// the flip moment). Without it a backend that timed out under Offload would
// carry Ready=False/NoKVEventsObserved into events-only and stay stuck Degraded
// despite the fresh anchor — defeating the routing-preserving remediation the
// flip exists for. Steady-state reconciles pass false, so the sticky contract
// (an operator raising firstEventTimeout after a breach can't un-Degrade) holds.
func evaluateKVEventReadiness(backend *cachev1alpha1.CacheBackend, readyStatus metav1.ConditionStatus, reason, message string, anchor, now time.Time, freshWindow bool) kvReadiness {
	// Base verdict mirrors the Deployment-level readiness; the Degraded
	// condition tracks the deployment-level Ready=False/ReplicasUnavailable
	// shape so it is consistent on every path (including the opt-out and
	// not-yet-Available paths below).
	base := kvReadiness{
		readyStatus:  readyStatus,
		readyReason:  reason,
		readyMessage: message,
	}
	if readyStatus == metav1.ConditionFalse && reason == conditionReasonReplicasUnavailable {
		base.degradedStatus = metav1.ConditionTrue
		base.degradedReason = reason
		base.degradedMessage = message
	} else {
		base.degradedStatus = metav1.ConditionFalse
		base.degradedReason = reasonNotDegraded
		base.degradedMessage = "backend is not in a degraded state"
	}

	// Opt-out, or a Deployment that is not yet Available: nothing to gate.
	if !kvEventGateEnabled(backend) || readyStatus != metav1.ConditionTrue {
		return base
	}

	// Workload is Available. Have we ever seen a KV event for this backend?
	if lastKVEventSeen(backend) {
		return kvReadiness{
			readyStatus:     metav1.ConditionTrue,
			readyReason:     reasonKVEventsObserved,
			readyMessage:    "at least one KV event observed; cache is receiving engine state",
			degradedStatus:  metav1.ConditionFalse,
			degradedReason:  reasonNotDegraded,
			degradedMessage: "backend is not in a degraded state",
		}
	}

	// Sticky Degraded: once the timeout has been breached
	// (Conditions[Ready].Reason == NoKVEventsObserved), stay Degraded until an
	// event arrives — never recompute the window. This guards the case where an
	// operator INCREASES spec.observation.firstEventTimeout after the window
	// already elapsed, which would otherwise move the backend back to
	// AwaitingFirstKVEvent (hiding a known publisher outage for another window),
	// contradicting the documented "once Degraded, stays Degraded until an
	// event" contract. (Availability flaps are handled separately by the stable
	// firstAvailableAt anchor; this persisted-reason check survives a no-flap
	// timeout change, where the condition is not overwritten.) A freshWindow
	// caller (a mode transition that re-anchors the clock) bypasses stickiness
	// so the flip gets a genuinely fresh window rather than inheriting the old
	// mode's timed-out verdict.
	if !freshWindow && readyConditionReason(backend) == reasonNoKVEventsObserved {
		return kvReadiness{
			readyStatus:     metav1.ConditionFalse,
			readyReason:     reasonNoKVEventsObserved,
			readyMessage:    "no KV events observed before the first-event timeout; staying Degraded until an event arrives",
			degradedStatus:  metav1.ConditionTrue,
			degradedReason:  reasonNoKVEventsObserved,
			degradedMessage: "no KV events observed before the first-event timeout",
		}
	}

	// Available but no event yet — still inside the first-event window? The
	// anchor is the latched first-Available time (now on the very first
	// Available reconcile, before the latch is persisted); a zero anchor would
	// only ever delay the Degraded flip, never trigger it prematurely.
	timeout := firstEventTimeout(backend)
	if anchor.IsZero() {
		anchor = now
	}
	// The AwaitingFirstKVEvent / NoKVEventsObserved Ready+Degraded messages are
	// operator-facing, so they must describe the actual anchor: a managed
	// (Offload) backend gates on its workload becoming Available; an events-only
	// backend has no workload, so the clock starts when the backend is wired.
	// Only the wording differs by mode — the reasons stay identical.
	awaitingMessage := fmt.Sprintf("cache-backend workload is Available but no KV events observed yet; waiting up to %s for the engine to report state", timeout)
	noEventsMessage := fmt.Sprintf("no KV events observed within %s of the workload becoming Available; check that engine pods are attached and their --kv-events-config / ZMQ publisher is healthy", timeout)
	noEventsDegradedMessage := fmt.Sprintf("no KV events observed within %s of the cache-backend workload becoming Available", timeout)
	if backend.Spec.IsEventsOnly() {
		awaitingMessage = fmt.Sprintf("events-only backend is wired but no KV events observed yet; waiting up to %s for the engine to report state", timeout)
		noEventsMessage = fmt.Sprintf("no KV events observed within %s of the events-only backend being wired; check that engine pods are attached and their --kv-events-config / ZMQ publisher is healthy", timeout)
		noEventsDegradedMessage = fmt.Sprintf("no KV events observed within %s of the events-only backend being wired", timeout)
	}
	if elapsed := now.Sub(anchor); elapsed < timeout {
		return kvReadiness{
			readyStatus:     metav1.ConditionFalse,
			readyReason:     reasonAwaitingFirstKVEvent,
			readyMessage:    awaitingMessage,
			degradedStatus:  metav1.ConditionFalse,
			degradedReason:  reasonNotDegraded,
			degradedMessage: "backend is not in a degraded state",
			// Re-reconcile at the deadline so the Degraded flip fires
			// automatically without an external event. No padding is added:
			// RequeueAfter fires no earlier than the requested delay, so the
			// next reconcile observes elapsed >= timeout and flips Degraded —
			// honoring firstEventTimeout as the actual bound rather than
			// overshooting it (which would be visible for small timeouts).
			requeueAfter: timeout - elapsed,
		}
	}
	return kvReadiness{
		readyStatus:     metav1.ConditionFalse,
		readyReason:     reasonNoKVEventsObserved,
		readyMessage:    noEventsMessage,
		degradedStatus:  metav1.ConditionTrue,
		degradedReason:  reasonNoKVEventsObserved,
		degradedMessage: noEventsDegradedMessage,
	}
}

// kvEventGateEnabled reports whether the KV-event readiness gate applies to
// this backend. Default-on; only the exact annotation value "false" opts out.
func kvEventGateEnabled(backend *cachev1alpha1.CacheBackend) bool {
	return backend.Annotations[annotationRequireKVEvents] != "false"
}

// lastKVEventSeen reports whether at least one KV event has EVER been observed
// for this backend. The gate's contract is "ever observed", but the poller's
// status.indexParticipation.lastEventAt is only a current-view projection — it
// legitimately clears to nil when a backend's replicas drain (scale-down,
// prefixes TTL'd; see the CacheIndex poller's drain handling). Reading that
// alone would let a backend that already passed the gate regress to
// AwaitingFirstKVEvent → NoKVEventsObserved on a drain. So the durable
// status.firstKVEventObservedAt latch (written write-once below) is consulted
// too: once set it pins the gate satisfied, matching the "first-event startup
// probe, not a liveness check" scope.
func lastKVEventSeen(backend *cachev1alpha1.CacheBackend) bool {
	if backend.Status.FirstKVEventObservedAt != nil {
		return true
	}
	ip := backend.Status.IndexParticipation
	return ip != nil && ip.LastEventAt != nil
}

// currentLastEventAt returns the poller's current-view lastEventAt for the
// backend, or nil. Used to latch firstKVEventObservedAt write-once.
func currentLastEventAt(backend *cachev1alpha1.CacheBackend) *metav1.Time {
	if ip := backend.Status.IndexParticipation; ip != nil {
		return ip.LastEventAt
	}
	return nil
}

// firstEventTimeout resolves the effective first-event timeout, falling back
// to defaultFirstEventTimeout when the spec field is unset or non-positive
// (the API-server applies the 5m kubebuilder default in production; the
// fallback covers fake-client tests and defensively rejects a zero value).
func firstEventTimeout(backend *cachev1alpha1.CacheBackend) time.Duration {
	if timeout := backend.Spec.EffectiveFirstEventTimeout(); timeout != nil && timeout.Duration > 0 {
		return timeout.Duration
	}
	return defaultFirstEventTimeout
}

// Ready condition reasons published by managedReadiness. Stable strings so
// downstream consumers (transition-event predicates, the Progressing
// derivation, dashboards) can switch on reason instead of regexing the
// message.
const (
	conditionReasonBackendReady      = "BackendReady"
	conditionReasonScaledToZero      = "ScaledToZero"
	conditionReasonRolloutInProgress = "RolloutInProgress"
	// conditionReasonEventsOnlyActive is the base Ready reason for an
	// events-only (tier-1 routing) backend, which provisions no server and so
	// has no workload to gate Ready on. The KV-event readiness gate normally
	// overrides it (AwaitingFirstKVEvent → KVEventsObserved / NoKVEventsObserved
	// as events arrive or time out); it surfaces verbatim only when the gate is
	// opted out via inferencecache.io/require-kv-events: "false".
	conditionReasonEventsOnlyActive    = "EventsOnlyActive"
	conditionReasonHostOnlyActive      = "HostOnlyActive"
	conditionReasonReplicasUnavailable = "ReplicasUnavailable"
)

// managedReadiness maps the Deployment's rollout state to the Ready
// condition (status + reason + message). Ready=True requires the Deployment
// to have observed its current generation and to have enough updated +
// available replicas, so a stale rollout (e.g. mid image change) is never
// reported Ready.
//
// When the CacheBackend is autoscaled the HPA owns the desired replica count,
// so the comparison target is the live Deployment's spec.replicas (which the
// HPA writes) rather than the CacheBackend's spec.replicas (which is ignored
// in that mode). This keeps Ready accurate when an HPA decides to run more
// pods than spec.replicas, and avoids a false ScaledToZero when spec.replicas
// happens to be 0 with autoscaling configured.
func managedReadiness(backend *cachev1alpha1.CacheBackend, dep *appsv1.Deployment) (metav1.ConditionStatus, string, string) {
	want := desiredReplicas(backend, dep)

	// A backend scaled to zero is not serving; never report it Ready.
	if want == 0 {
		return metav1.ConditionFalse, conditionReasonScaledToZero, "backend scaled to zero replicas"
	}

	rolledOut := dep.Status.ObservedGeneration >= dep.Generation
	switch {
	case rolledOut && dep.Status.UpdatedReplicas >= want && dep.Status.AvailableReplicas >= want:
		return metav1.ConditionTrue, conditionReasonBackendReady,
			fmt.Sprintf("%d/%d replicas available", dep.Status.AvailableReplicas, want)
	case !rolledOut || dep.Status.UpdatedReplicas < want:
		return metav1.ConditionFalse, conditionReasonRolloutInProgress,
			fmt.Sprintf("%d/%d replicas updated", dep.Status.UpdatedReplicas, want)
	default:
		return metav1.ConditionFalse, conditionReasonReplicasUnavailable,
			fmt.Sprintf("%d/%d replicas available", dep.Status.AvailableReplicas, want)
	}
}

// progressingFromReady derives the Progressing condition from the Ready
// condition's outcome. A Ready=True backend has converged (Progressing=False,
// Reason=Synced). A Ready=False backend that's actively converging
// (RolloutInProgress, or the KV-event gate's AwaitingFirstKVEvent — the
// controller is still driving toward the Ready=True endpoint in both)
// flips Progressing=True; one that has reached a stable terminal state
// (ScaledToZero) or stable failure (ReplicasUnavailable / NoKVEventsObserved)
// is NOT progressing — no rollout is in motion.
func progressingFromReady(readyStatus metav1.ConditionStatus, reason, message string) (metav1.ConditionStatus, string, string) {
	if readyStatus == metav1.ConditionTrue {
		return metav1.ConditionFalse, "Synced", "rendered children match desired state"
	}
	switch reason {
	case conditionReasonRolloutInProgress, reasonAwaitingFirstKVEvent:
		return metav1.ConditionTrue, reason, message
	case conditionReasonReplicasUnavailable:
		return metav1.ConditionFalse, "Degraded", message
	default:
		return metav1.ConditionFalse, reason, message
	}
}

// desiredReplicas is the per-reconcile source of truth for "how many replicas
// should this backend be running". With autoscaling enabled the HPA writes
// spec.replicas on the Deployment, so the live value is authoritative; without
// it, the user's spec.replicas (default 1) wins.
//
// It applies the same singleton clamp the render path does (clampSingletonReplicas):
// readiness must expect the count actually DEPLOYED, not the CR's grandfathered
// spec.replicas. Without this, a singleton backend (SGLang Redis L2, or a
// host-network Mooncake master) whose spec.replicas was set to 3 before admission
// rejected it deploys one pod but expects three, and reports RolloutInProgress
// forever. spec.replicas 0 (disabled) is preserved.
func desiredReplicas(backend *cachev1alpha1.CacheBackend, dep *appsv1.Deployment) int32 {
	want := unclampedDesiredReplicas(backend, dep)
	if want > 1 && cacheServerIsSingleton(backend, &dep.Spec.Template.Spec) {
		return 1
	}
	return want
}

func unclampedDesiredReplicas(backend *cachev1alpha1.CacheBackend, dep *appsv1.Deployment) int32 {
	if backend.Spec.Autoscaling != nil {
		// First reconcile after an HPA spec is added may briefly see
		// dep.Spec.Replicas still set by the controller; the HPA will overwrite
		// it within one cycle. Until then, fall back to the controller value.
		if dep.Spec.Replicas != nil {
			return *dep.Spec.Replicas
		}
		// Fall through to the floor.
	}
	if backend.Spec.Replicas != nil {
		return *backend.Spec.Replicas
	}
	return 1
}

// patchStatus applies mutate to the backend's status and patches it only when
// it changes. The effective spec.integration.failOpen is always echoed to
// status.failOpen so operators can read the current mode from status alone,
// and so transition detection in [CacheBackendReconciler.emitTransitionEvents]
// has a stable previous-value baseline to compare against.
func (r *CacheBackendReconciler) patchStatus(ctx context.Context, backend *cachev1alpha1.CacheBackend, mutate func()) error {
	before := backend.DeepCopy()
	mutate()
	failOpen := cachev1alpha1.IntegrationFailOpen(backend.Spec.Integration)
	backend.Status.FailOpen = &failOpen
	if equality.Semantic.DeepEqual(before.Status, backend.Status) {
		return nil
	}
	if err := r.Status().Patch(ctx, backend, client.MergeFrom(before)); err != nil {
		// Roll back the in-memory mutation. emitTransitionEvents is called on
		// every Reconcile return and compares the pre-reconcile snapshot to
		// backend.Status; leaving the un-persisted mutation in place would
		// fire a Warning/Normal event for a transition the apiserver never
		// observed, and the same transition would fire again on the next
		// reconcile (when the patch retries) — producing duplicate / phantom
		// events under status-subresource conflict / RBAC / API failures.
		backend.Status = before.Status
		return fmt.Errorf("patch CacheBackend status %s/%s: %w", backend.Namespace, backend.Name, err)
	}
	return nil
}

type matchedEnginePodsRefresh struct {
	churn bool
}

// refreshMatchedEnginePods refreshes status.matchedEnginePods from the live
// pod-label set in the CacheBackend's namespace. Runs once per reconcile and
// only touches the matchedEnginePods sub-field (via Status().Patch with
// MergeFrom) so it coexists cleanly with the other status writers in this
// reconciler and with any future status writers (e.g. an index-participation
// projector) that touch different sub-fields.
//
// Cadence-by-reconcile, not real-time: counts via a single namespaced
// client.List with the engineSelector — there is no Pod watch, and pod
// births/deaths between reconciles are not reflected until the next pass.
// To keep the count from going indefinitely stale between unrelated
// reconcile triggers, the Reconcile path sets `result.RequeueAfter =
// matchedEnginePodsRequeueInterval` whenever the CR has a non-empty
// EngineSelector, giving the field a bounded staleness without paying
// for a Pod informer. The real-time per-pod signal lives on the engine
// pods themselves (the `InjectedByCacheBackend` Event the
// engine-pod-events controller emits on every annotated pod); this
// status field answers the cluster-wide "is anyone connected at all?"
// question.
//
// Selector resolution mirrors the mutating webhook's policy: a nil or
// empty MatchLabels matches nothing (a broad selector at admission time
// would silently claim every pod). A CR with no selector therefore
// reports no count — and a CR that previously had one and just lost it
// gets its prior value cleared so the printer column doesn't advertise a
// stale match for a CR that no longer claims engine pods.
//
// Fail-soft semantics:
//   - List error → log + skip the tick; keep the existing value.
//   - Status patch error → roll back the in-memory mutation so the rest of
//     the reconcile (transition events, log fields) sees only what the
//     apiserver actually persisted.
//
// Never returns an error: the matchedEnginePods refresh must not escalate
// a transient observation failure into a Reconcile error that retries the
// rest of the reconcile machinery unnecessarily.
func (r *CacheBackendReconciler) refreshMatchedEnginePods(ctx context.Context, backend *cachev1alpha1.CacheBackend) matchedEnginePodsRefresh {
	before := backend.DeepCopy()
	var out matchedEnginePodsRefresh
	selectorDiagnosticReliable := true

	sel := backend.Spec.EngineSelector
	if sel == nil || len(sel.MatchLabels) == 0 {
		if backend.Status.MatchedEnginePods == nil && backend.Status.EngineSelectorMessage == "" {
			return out
		}
		backend.Status.MatchedEnginePods = nil
		backend.Status.EngineSelectorMessage = ""
	} else {
		matcher := labels.SelectorFromSet(sel.MatchLabels)
		var pods corev1.PodList
		// Pin the pod read to the uncached APIReader so the controller-
		// runtime cache does NOT register a Pod informer on first use —
		// otherwise the manager would watch every pod cluster-wide just to
		// keep this snapshot count fresh, which the locked design
		// explicitly rejected. Fall back to the cached client only when
		// APIReader is unset (test wiring without a real APIReader); the
		// reconciler still functions, it just uses the cache.
		reader := client.Reader(r.APIReader)
		if reader == nil {
			reader = r.Client
		}
		if err := reader.List(ctx, &pods,
			client.InNamespace(backend.Namespace),
			client.MatchingLabelsSelector{Selector: matcher},
		); err != nil {
			log.FromContext(ctx).V(1).Info("matchedEnginePods refresh skipped: pod list failed",
				"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
			return out
		}
		count := int32(len(pods.Items))
		nonTerminalCount := nonTerminalPodCount(pods.Items)
		desired, desiredKnown, desiredReliable := r.desiredEngineReplicas(ctx, backend, matcher)
		selectorDiagnosticReliable = desiredReliable
		if desiredReliable && desiredKnown && desired != nonTerminalCount {
			out.churn = true
		}

		message := backend.Status.EngineSelectorMessage
		if count > 0 {
			message = ""
		} else if desiredReliable {
			if !desiredKnown || desired > 0 {
				message = engineSelectorUnmatchedMessage(sel.MatchLabels)
			} else {
				message = ""
			}
		}
		if backend.Status.MatchedEnginePods != nil &&
			*backend.Status.MatchedEnginePods == count &&
			backend.Status.EngineSelectorMessage == message {
			return out
		}
		backend.Status.MatchedEnginePods = &count
		backend.Status.EngineSelectorMessage = message
	}

	if err := r.Status().Patch(ctx, backend, client.MergeFrom(before)); err != nil {
		backend.Status.MatchedEnginePods = before.Status.MatchedEnginePods
		backend.Status.EngineSelectorMessage = before.Status.EngineSelectorMessage
		log.FromContext(ctx).V(1).Info("matchedEnginePods refresh: status patch failed",
			"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
		return out
	}

	if r.Recorder != nil && selectorDiagnosticReliable && backend.Status.MatchedEnginePods != nil &&
		*backend.Status.MatchedEnginePods == 0 && backend.Status.EngineSelectorMessage != "" &&
		(before.Status.MatchedEnginePods == nil || *before.Status.MatchedEnginePods > 0 ||
			before.Status.EngineSelectorMessage == "") {
		r.Recorder.Eventf(backend, nil, corev1.EventTypeNormal,
			eventReasonEngineSelectorUnmatched, eventReasonEngineSelectorUnmatched,
			"%s", backend.Status.EngineSelectorMessage)
	}
	return out
}

func (r *CacheBackendReconciler) desiredEngineReplicas(ctx context.Context, backend *cachev1alpha1.CacheBackend, matcher labels.Selector) (int32, bool, bool) {
	reader := client.Reader(r.APIReader)
	if reader == nil {
		reader = r.Client
	}
	var deps appsv1.DeploymentList
	if err := reader.List(ctx, &deps, client.InNamespace(backend.Namespace)); err != nil {
		log.FromContext(ctx).V(1).Info("matchedEnginePods desired-replica refresh skipped: deployment list failed",
			"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
		return 0, false, false
	}
	var desired int32
	var found bool
	for i := range deps.Items {
		dep := &deps.Items[i]
		if !matcher.Matches(labels.Set(dep.Spec.Template.Labels)) {
			continue
		}
		found = true
		replicas := int32(1)
		if dep.Spec.Replicas != nil {
			replicas = *dep.Spec.Replicas
		}
		desired += replicas
	}
	return desired, found, true
}

func nonTerminalPodCount(pods []corev1.Pod) int32 {
	var count int32
	for i := range pods {
		switch pods[i].Status.Phase {
		case corev1.PodSucceeded, corev1.PodFailed:
			continue
		default:
			count++
		}
	}
	return count
}

func engineSelectorUnmatchedMessage(matchLabels map[string]string) string {
	return fmt.Sprintf("spec.engineSelector.matchLabels={%s}; no Pods in namespace match", formatMatchLabels(matchLabels))
}

func formatMatchLabels(matchLabels map[string]string) string {
	keys := make([]string, 0, len(matchLabels))
	for k := range matchLabels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+":"+matchLabels[k])
	}
	return strings.Join(parts, ",")
}

// stateSnapshot captures the prior-status fields that drive transition events.
// degraded / ready are derived from the Ready condition (status + reason) so
// the transition logic doesn't need a separate phase enum on status. failOpen
// is the previously-echoed integration.failOpen (nil ⇒ never observed ⇒
// treated as the API default of true, so an initial apply with
// failOpen=false correctly fires the warning).
type stateSnapshot struct {
	// ready is whether Conditions[Ready].Status was True. Pairs with
	// degraded so emitTransitionEvents can narrate the Degraded → Ready
	// recovery transition.
	ready bool
	// degraded is whether Conditions[Degraded].Status was True (covers both
	// the deployment-level ReplicasUnavailable shape AND the KV-event-gate
	// NoKVEventsObserved shape). The readyReason check below splits the two
	// for suppression purposes.
	degraded bool
	failOpen bool
	// readyReason is the prior Conditions[Ready].Reason. The KV-event gate's
	// transitions (AwaitingFirstKVEvent / KVEventsObserved / NoKVEventsObserved)
	// ride on this reason, and the suppression for BackendDegraded /
	// BackendRecovered uses it to distinguish a deployment-caused Degraded
	// from a KV-event-caused one (which has its own dedicated Event).
	readyReason string
	// firstEventLatched is whether status.firstKVEventObservedAt was set. The
	// KVEventsObserved Event keys on the nil→set transition of this latch (not
	// the Ready reason) so it fires exactly once — on the TRUE first event —
	// rather than re-firing every time a rollout takes an already-event-seen
	// backend through RolloutInProgress and back to KVEventsObserved.
	firstEventLatched bool
}

// snapshotState captures the prior status values that drive transition events.
// Called at the top of Reconcile before any mutation so emitTransitionEvents
// has a stable baseline to compare the post-reconcile state against.
func snapshotState(cb *cachev1alpha1.CacheBackend) stateSnapshot {
	return stateSnapshot{
		ready:             isReady(cb),
		degraded:          isDegraded(cb),
		failOpen:          statusFailOpen(cb.Status.FailOpen),
		readyReason:       readyConditionReason(cb),
		firstEventLatched: cb.Status.FirstKVEventObservedAt != nil,
	}
}

// isReady reports whether the Ready condition is currently True.
func isReady(cb *cachev1alpha1.CacheBackend) bool {
	c := meta.FindStatusCondition(cb.Status.Conditions, conditionTypeReady)
	return c != nil && c.Status == metav1.ConditionTrue
}

// isDegraded reports whether Conditions[Degraded] is True — covering both
// the deployment-level ReplicasUnavailable shape and the KV-event-gate
// NoKVEventsObserved shape. BackendDegraded events narrate the former; the
// gate emits its own NoKVEventsObserved event for the latter, so the
// generic event is suppressed by the readyReason check below.
func isDegraded(cb *cachev1alpha1.CacheBackend) bool {
	c := meta.FindStatusCondition(cb.Status.Conditions, conditionTypeDegraded)
	return c != nil && c.Status == metav1.ConditionTrue
}

// readyConditionReason returns the current Conditions[Ready].Reason, or "" when
// the condition is absent.
func readyConditionReason(cb *cachev1alpha1.CacheBackend) string {
	if c := meta.FindStatusCondition(cb.Status.Conditions, conditionTypeReady); c != nil {
		return c.Reason
	}
	return ""
}

// statusFailOpen treats a missing status.failOpen as the API default (true).
// A first-time reconcile with spec.integration.failOpen=false is then correctly
// observed as a transition true→false and fires the FailClosedEnabled Warning.
func statusFailOpen(v *bool) bool {
	if v == nil {
		return true
	}
	return *v
}

// emitTransitionEvents emits Kubernetes Events on transitions of
// Conditions[Ready/Degraded], the KV-event readiness gate state, or the
// effective failOpen toggle. By design events fire only on transitions —
// never on steady state — so a Ready backend reconciling every few seconds
// does not flood the event stream.
//
//   - Entering Conditions[Degraded]=True → Warning BackendDegraded
//     (suppressed for the KV-event-gate flavor — readyReason=NoKVEventsObserved
//     — which emits its own NoKVEventsObserved event instead).
//   - Leaving Conditions[Degraded]=True for Ready=True → Normal
//     BackendRecovered (suppressed when recovering from the KV-event-gate
//     Degraded, which emits KVEventsObserved).
//   - KV-event gate (keyed on the Ready condition reason): Normal
//     AwaitingFirstKVEvent on first reaching it; Normal KVEventsObserved on the
//     first event observed; Warning NoKVEventsObserved on the timeout breach.
//   - failOpen flipped true → false → Warning FailClosedEnabled (the cache
//     becomes a serving dependency; advanced opt-in).
//   - failOpen flipped false → true → Normal FailOpenRestored.
func (r *CacheBackendReconciler) emitTransitionEvents(cb *cachev1alpha1.CacheBackend, before stateSnapshot) {
	if r.Recorder == nil {
		return
	}
	after := snapshotState(cb)

	// Generic Conditions[Degraded] transitions. The KV-event gate's Degraded
	// and Ready flavors carry their own, more specific events below, so
	// suppress the generic event when the transition is a gate flavor —
	// otherwise a KV-event-timeout Degraded would fire BOTH BackendDegraded
	// and NoKVEventsObserved for one transition.
	//
	// Suppression is keyed on the *prior* readyReason for recovery, not the
	// new one: a backend that already saw KV events, then degrades for
	// ReplicasUnavailable, then recovers, comes back with readyReason
	// KVEventsObserved — but that is an ordinary deployment recovery
	// (BackendRecovered), not a first-event observation, so we must not key
	// the suppression on the new reason.
	if !before.degraded && after.degraded &&
		after.readyReason != reasonNoKVEventsObserved {
		r.Recorder.Eventf(cb, nil, corev1.EventTypeWarning, eventReasonBackendDegraded, eventReasonBackendDegraded,
			"cache backend is degraded: %s", degradedMessage(cb))
	}
	if before.degraded && after.ready &&
		before.readyReason != reasonNoKVEventsObserved {
		r.Recorder.Eventf(cb, nil, corev1.EventTypeNormal, eventReasonBackendRecovered, eventReasonBackendRecovered,
			"cache backend recovered to Ready")
	}

	// KV-event readiness gate transitions, keyed on the Ready condition reason
	// so they fire once on entry into each gate state.
	if before.readyReason != reasonAwaitingFirstKVEvent && after.readyReason == reasonAwaitingFirstKVEvent {
		// Mode-aware lead-in: an events-only backend has no workload, so it
		// describes the wired backend rather than a non-existent Deployment.
		awaitingLead := "cache-backend workload is Available but no KV events observed yet"
		if cb.Spec.IsEventsOnly() {
			awaitingLead = "events-only backend is wired but no KV events observed yet"
		}
		r.Recorder.Eventf(cb, nil, corev1.EventTypeNormal, reasonAwaitingFirstKVEvent, reasonAwaitingFirstKVEvent,
			"%s; backend stays Ready=False/AwaitingFirstKVEvent until the first event (check that engine pods are attached and their --kv-events-config is healthy if none arrive)", awaitingLead)
	}
	// KVEventsObserved fires exactly once — on the nil→set transition of the
	// firstKVEventObservedAt latch, i.e. the TRUE first event. Keying on the
	// latch (not the Ready reason) means a later rollout that takes an
	// already-event-seen backend through RolloutInProgress and back to
	// KVEventsObserved does NOT re-fire "first KV event observed", and a
	// deployment recovery (ReplicasUnavailable → Ready, events never lost) emits
	// BackendRecovered above instead of a spurious KVEventsObserved.
	if !before.firstEventLatched && after.firstEventLatched {
		r.Recorder.Eventf(cb, nil, corev1.EventTypeNormal, reasonKVEventsObserved, reasonKVEventsObserved,
			"first KV event observed; backend is Ready")
	}
	if before.readyReason != reasonNoKVEventsObserved && after.readyReason == reasonNoKVEventsObserved {
		// Mode-aware anchor phrase: events-only has no workload — its window
		// starts when the backend is wired, not when a Deployment goes Available.
		anchorPhrase := "the workload becoming Available"
		if cb.Spec.IsEventsOnly() {
			anchorPhrase = "the events-only backend being wired"
		}
		r.Recorder.Eventf(cb, nil, corev1.EventTypeWarning, reasonNoKVEventsObserved, reasonNoKVEventsObserved,
			"no KV events observed within %s of %s; no engine pods are attached, or the engine's KV-event publisher is mis-configured (--kv-events-config / ZMQ bind)", firstEventTimeout(cb), anchorPhrase)
	}

	if before.failOpen && !after.failOpen {
		r.Recorder.Eventf(cb, nil, corev1.EventTypeWarning, eventReasonFailClosedEnabled, eventReasonFailClosedEnabled,
			"fail-closed mode enabled — cache is now a serving dependency; engine requests will fail when the cache is unreachable")
	}
	if !before.failOpen && after.failOpen {
		r.Recorder.Eventf(cb, nil, corev1.EventTypeNormal, eventReasonFailOpenRestored, eventReasonFailOpenRestored,
			"fail-open mode restored — cache is again an optimization, not a serving dependency")
	}
}

// degradedMessage surfaces the Ready=False condition's message (set by
// managedReadiness) so the BackendDegraded event names the failure mode
// (e.g. "1/3 replicas available") instead of just announcing the transition.
func degradedMessage(cb *cachev1alpha1.CacheBackend) string {
	if c := meta.FindStatusCondition(cb.Status.Conditions, conditionTypeReady); c != nil && c.Message != "" {
		return c.Message
	}
	return "backend workload not available"
}

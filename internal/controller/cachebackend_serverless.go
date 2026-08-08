// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
	"time"
)

// reconcileExternal mirrors an externally owned backend's configured endpoint
// to status and marks the backend Ready: there is no Service to wait on, so
// admission acceptance of spec.remoteStorage.endpoint is the only readiness
// signal the controller has. The Ready condition flips to True in lock step so
// the Ready printcolumn (kubectl get cb) reflects the accepted endpoint for
// externally owned resources that admission has already accepted.
//
// Three terminal states, each driven by the SAME shape rule the
// validating webhook applies on CREATE/UPDATE — so the reconciler is
// honest about CRs that were stored under a laxer rule set:
//
//   - external endpoint empty             → Ready=False/ExternalEndpointMissing
//   - endpoint malformed for its provider → Ready=False/ExternalEndpointInvalid
//   - endpoint valid for its provider     → Ready=True/ExternalEndpointAccepted
//
// The "invalid" branch matters because admission's shape rule tightened
// over the life of the CRD (added port-required, bracket-required-IPv6,
// no-embedded-whitespace, etc. as we learned what the engine connector
// rejects). A pre-existing stored CR carrying e.g. `https://...` or a
// portless host would otherwise be marked Ready=True/ExternalEndpointAccepted
// here; the pod webhook would then hand the value to a provider wire that
// cannot parse it, turning a cache misconfiguration into a serving outage.
// Publishing Ready=False with a specific reason names the gap, and the pod
// webhook short-circuits on the same check (returns no-injection, fail-open).
//
// External backends never enter the KV-event readiness gate, so the
// managed-only Degraded condition is cleared here. Two status fields are
// deliberately NOT reset: firstKVEventObservedAt (a monotonic write-once
// "ever observed a KV event" marker — clearing it would be ineffective
// anyway, since the preserved poller-owned lastEventAt would immediately
// re-satisfy the gate on a return to the managed path) and
// status.indexParticipation (owned by the CacheIndex poller, which converges
// it on its own; an External backend whose engine pods still report KV events
// legitimately keeps it).
func (r *CacheBackendReconciler) reconcileExternal(ctx context.Context, backend *cachev1alpha1.CacheBackend) error {
	// Wipe the in-memory cascade shadow + rate-limit timestamp
	// alongside the on-cluster status clearing below — see
	// clearServerInstanceLatchShadow for why a lingering shadow
	// across managed→External→managed would false-cascade.
	r.clearServerInstanceLatchShadow(backend)
	// Wipe the functional-probe rate-limit entry alongside removing
	// the FunctionalProbeOK condition below. Without this, a CR that
	// flips managed → External → managed within the 30s rate-limit
	// window would have a stale lastCalled timestamp on re-entry, so
	// the first reconcile under the managed path would skip the /probe
	// call (and, because we just removed the condition, find no prior
	// FunctionalProbeOK to re-apply downgrade off of) — Ready=True
	// would be published with no fresh probe verdict.
	r.probeLimiter.forget(client.ObjectKeyFromObject(backend).String())
	return r.patchStatus(ctx, backend, func() {
		// TrimSpace before every decision. Admission rejects a
		// whitespace-only endpoint at write time, but a pre-existing
		// CR in etcd from before admission was installed can still
		// carry one. Publishing the trimmed value as status.endpoint
		// means the pod webhook's `endpoint == ""` short-circuit
		// naturally catches whitespace too without a second TrimSpace
		// at the consumer.
		storage := backend.Spec.EffectiveRemoteStorage()
		endpoint := ""
		if storage != nil {
			endpoint = strings.TrimSpace(storage.Endpoint)
		}
		backend.Status.Endpoint = endpoint
		// Clear the cache-server-instance latch — External backends
		// have no controller-managed cache-server pods, and
		// cleanupOwnedWorkload above has just deleted any prior
		// managed Deployment. Leaving the latch set would expose a
		// stale UID to operators.
		backend.Status.ObservedServerInstance = ""
		backend.Status.ObservedGeneration = backend.Generation

		// Decide the Ready reason + message in one place so the
		// Progressing/Ready conditions stay in lockstep.
		var (
			readyStatus = metav1.ConditionFalse
			readyReason string
			readyMsg    string
		)
		switch {
		case endpoint == "":
			readyReason = conditionReasonExternalEndpointMissing
			readyMsg = "the external remote-storage endpoint is empty"
		default:
			// Use the same shape validator the admission webhook
			// uses; prefix the helper's message with the active API
			// field so an
			// operator running kubectl describe sees the same
			// shape complaint they would get on a fresh kubectl
			// apply.
			if err := backendadapter.ValidateExternalEndpoint(storage.Provider, endpoint); err != nil {
				readyReason = conditionReasonExternalEndpointInvalid
				readyMsg = "spec.remoteStorage." + err.Error()
				break
			}
			readyStatus = metav1.ConditionTrue
			readyReason = conditionReasonExternalEndpointAccepted
			readyMsg = "External endpoint accepted; controller does not provision cache pods for External backends"
		}

		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeProgressing,
			Status:             metav1.ConditionFalse,
			Reason:             readyReason,
			Message:            "External backends complete admission immediately",
			ObservedGeneration: backend.Generation,
		})
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeReady,
			Status:             readyStatus,
			Reason:             readyReason,
			Message:            readyMsg,
			ObservedGeneration: backend.Generation,
		})
		// Clear any Degraded condition left over from a prior managed state;
		// External readiness is the endpoint check above, not the KV gate.
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeDegraded)
		// FunctionalProbeOK doesn't apply to External backends — the probe
		// gate only fires inside updateManagedStatus, and the controller
		// doesn't drive any cache-plane round-trip for an external endpoint
		// (the gate only applies to managed backends). Clear any
		// FunctionalProbeOK left over from a prior managed state so the
		// External-mode CR doesn't surface a stale condition that no
		// reconcile path will ever update.
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeFunctionalProbeOK)
		// Same for EngineKernelsHealthy — it is published only from the managed
		// path, so clear any left over from a prior managed state (the docs
		// state External backends publish only Ready + Progressing).
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeEngineKernelsHealthy)
		// T2Degraded is a managed-only advisory; an External backend's
		// tier-2 (if any) is operator-managed and not evaluated here --
		// clear any left over from a prior managed state.
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeT2Degraded)
		// EngineCompatibility is likewise evaluated only by the managed and
		// host-only status paths. External storage still receives runtime-side
		// connector injection, but this reconcile path does not inspect those
		// engine pods, so clear any condition left over from a prior mode rather
		// than leave a stale warning the External contract never updates.
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeEngineCompatibility)
	})
}

// reconcileEventsOnly drives an events-only (tier-1 routing) backend: it
// provisions NO cache-server workload and publishes no endpoint, but still runs
// the KV-event readiness gate so Ready reflects "the engine is reporting state"
// exactly as a managed backend does. The kvevent-subscriber sidecar is injected
// engine-side by the (mode-aware) pod webhook, and status.indexParticipation is
// owned by the CacheIndex poller as usual — so routing, LookupRoute, and the
// per-backend index slice behave identically to a managed backend; only the
// offload tier (a server + KV connector) is absent.
//
// Like reconcileExternal it clears the in-memory cascade shadow and the
// functional-probe rate-limit entry (there is no server to cascade-restart or
// probe) and clears the managed-only FunctionalProbeOK / T2Degraded conditions.
// Events-only also clears EngineKernelsHealthy / EngineCompatibility because it
// loads no connector; host-only evaluates both engine-side diagnostics normally.
// The firstEventTimeout window is anchored on status.firstAvailableAt, latched
// here on the first reconcile — an events-only backend is "up" the moment it
// exists (no workload to wait on), so the gate starts its clock immediately and
// its Degraded flip stays sticky exactly as on the managed path. A backend
// flipping in from a server-bearing mode re-anchors the latch to the flip moment
// rather than inheriting that mode's (workload-)availability time.
func (r *CacheBackendReconciler) reconcileEventsOnly(ctx context.Context, backend *cachev1alpha1.CacheBackend) (ctrl.Result, error) {
	return r.reconcileServerless(ctx, backend, conditionReasonEventsOnlyActive,
		"events-only backend active; routing tier wired with no offload server")
}

func (r *CacheBackendReconciler) reconcileHostOnly(ctx context.Context, backend *cachev1alpha1.CacheBackend) (ctrl.Result, error) {
	return r.reconcileServerless(ctx, backend, conditionReasonHostOnlyActive,
		"host-only cache active; engine-local cache wired with no remote provider")
}

func (r *CacheBackendReconciler) reconcileServerless(ctx context.Context, backend *cachev1alpha1.CacheBackend, activeReason, activeMessage string) (ctrl.Result, error) {
	now := time.Now()
	// A backend flipping INTO a serverless mode (events-only or host-only) from a
	// server-bearing mode still carries that mode's status.endpoint /
	// observedServerInstance at the top of this reconcile. Serverless modes clear
	// both below, so a non-empty value here uniquely marks the first reconcile
	// after the flip.
	// On that transition any latched firstAvailableAt reflects the OLD mode's
	// availability (e.g. an Offload workload that went Available long ago), not the
	// new serverless mode's "up" moment, so reusing it as the firstEventTimeout
	// anchor could breach the window the instant we flip and strand the backend at
	// NoKVEventsObserved/Degraded. Re-anchor to now so the new mode gets a fresh
	// first-event window from when it took effect.
	transitionedFromServerMode := backend.Status.Endpoint != "" || backend.Status.ObservedServerInstance != ""
	// Base readiness is unconditionally True (no workload to gate on); the
	// KV-event gate layers AwaitingFirstKVEvent → KVEventsObserved /
	// NoKVEventsObserved on top, anchored on the firstAvailableAt latch.
	anchor := now
	if backend.Status.FirstAvailableAt != nil && !transitionedFromServerMode {
		anchor = backend.Status.FirstAvailableAt.Time
	}
	// On a server-bearing-to-serverless transition the clock is re-anchored to now,
	// so also bypass any sticky NoKVEventsObserved carried over from the prior
	// mode — otherwise the flip would inherit that timed-out verdict and stay
	// Degraded despite the fresh window.
	gate := evaluateKVEventReadiness(backend, metav1.ConditionTrue,
		activeReason,
		activeMessage,
		anchor, now, transitionedFromServerMode)
	kernelVerdict := kernelHealthVerdict{}
	engineCompatMsg := ""
	engineCompatObserved := false
	previousEngineIncompatible := false
	eventsOnly := backend.Spec.IsEventsOnly()
	if !eventsOnly {
		kernelReader := client.Reader(r.APIReader)
		if kernelReader == nil {
			kernelReader = r.Client
		}
		kernelPods, kernelListedOK := listMatchedEnginePods(ctx, kernelReader, backend)
		kernelVerdict = evaluateEngineKernelHealth(backend, gate, kernelPods, kernelListedOK)
		gate = downgradeKernelReadyVerdict(gate, kernelVerdict)
		engineCompatMsg, engineCompatObserved = r.detectEngineConnectorCrashLoop(ctx, backend)
		previousEngineIncompatible = meta.IsStatusConditionFalse(backend.Status.Conditions, conditionTypeEngineCompatibility)
	}
	progressingStatus, progressingReason, progressingMessage := progressingFromReady(gate.readyStatus, gate.readyReason, gate.readyMessage)

	// No server to cascade-restart or functionally probe — drop both in-memory
	// trackers so a later Offload re-entry inside their windows starts clean
	// (mirrors reconcileExternal).
	r.clearServerInstanceLatchShadow(backend)
	r.probeLimiter.forget(client.ObjectKeyFromObject(backend).String())

	err := r.patchStatus(ctx, backend, func() {
		// No provisioned server: no endpoint, no server-instance latch.
		// status.indexParticipation stays poller-owned.
		backend.Status.Endpoint = ""
		backend.Status.ObservedServerInstance = ""
		backend.Status.ObservedGeneration = backend.Generation
		// Latch the first KV-event observation + first-Available time write-once,
		// the same contract as updateManagedStatus (the gate reads both).
		if backend.Status.FirstKVEventObservedAt == nil {
			if at := currentLastEventAt(backend); at != nil {
				backend.Status.FirstKVEventObservedAt = at.DeepCopy()
			}
		}
		// Latch the first-Available time write-once, OR re-anchor it on the
		// server-mode→events-only transition (where the prior value is the old
		// mode's availability, not the events-only start — see above).
		if backend.Status.FirstAvailableAt == nil || transitionedFromServerMode {
			t := metav1.NewTime(now)
			backend.Status.FirstAvailableAt = &t
		}
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeReady,
			Status:             gate.readyStatus,
			Reason:             gate.readyReason,
			Message:            gate.readyMessage,
			ObservedGeneration: backend.Generation,
		})
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeDegraded,
			Status:             gate.degradedStatus,
			Reason:             gate.degradedReason,
			Message:            gate.degradedMessage,
			ObservedGeneration: backend.Generation,
		})
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeProgressing,
			Status:             progressingStatus,
			Reason:             progressingReason,
			Message:            progressingMessage,
			ObservedGeneration: backend.Generation,
		})
		// Managed-only advisories never apply to a server-less backend.
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeFunctionalProbeOK)
		if eventsOnly {
			// Events-only loads no LMCache connector, so the kernel-check init
			// container is never injected. Clear any prior Offload verdict.
			meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeEngineKernelsHealthy)
		} else {
			switch {
			case kernelVerdict.shouldWriteCondition:
				condition := kernelVerdict.condition
				condition.ObservedGeneration = backend.Generation
				meta.SetStatusCondition(&backend.Status.Conditions, condition)
			case kernelVerdict.removeCondition:
				meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeEngineKernelsHealthy)
			}
		}
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeT2Degraded)
		if eventsOnly {
			// Events-only injects no connector, so the advisory cannot apply.
			meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeEngineCompatibility)
		} else if engineCompatObserved {
			if engineCompatMsg != "" {
				meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
					Type:               conditionTypeEngineCompatibility,
					Status:             metav1.ConditionFalse,
					Reason:             reasonInjectedEngineCrashLooping,
					Message:            engineCompatMsg,
					ObservedGeneration: backend.Generation,
				})
			} else {
				meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeEngineCompatibility)
			}
		}
	})
	if err == nil && engineCompatObserved && engineCompatMsg != "" && !previousEngineIncompatible && r.Recorder != nil {
		r.Recorder.Eventf(backend, nil, corev1.EventTypeWarning,
			reasonInjectedEngineCrashLooping, reasonInjectedEngineCrashLooping, "%s", engineCompatMsg)
	}
	return ctrl.Result{RequeueAfter: gate.requeueAfter}, err
}

// reconcileUnmanaged sheds any previously owned workload and clears managed status
// for a backend this module no longer provisions (unsupported runtime/backend or
// deferred kind). The managed conditions are removed; firstKVEventObservedAt and
// status.indexParticipation are left as-is (see reconcileExternal's comment — the
// latch is a monotonic marker and indexParticipation is poller-owned). The
// firstAvailableAt gate anchor IS reset, so a later managed/events-only re-entry
// starts a fresh first-event window instead of reusing a pre-unmanaged
// availability time.
func (r *CacheBackendReconciler) reconcileUnmanaged(ctx context.Context, backend *cachev1alpha1.CacheBackend) error {
	if err := r.cleanupOwnedWorkload(ctx, backend); err != nil {
		return err
	}
	// Wipe the in-memory cascade shadow + rate-limit timestamp
	// alongside the on-cluster status clearing below.
	r.clearServerInstanceLatchShadow(backend)
	// Wipe the functional-probe rate-limit entry alongside removing
	// the FunctionalProbeOK condition below. Same reasoning as in
	// reconcileExternal — without this, a managed → Unmanaged →
	// managed cycle inside the 30s rate-limit window would suppress
	// the first /probe call on re-entry and publish Ready=True with
	// no fresh probe verdict.
	r.probeLimiter.forget(client.ObjectKeyFromObject(backend).String())
	return r.patchStatus(ctx, backend, func() {
		backend.Status.Endpoint = ""
		// Clear the cache-server-instance latch — cleanupOwnedWorkload
		// above has just deleted any prior managed Deployment and we
		// no longer provision one, so a retained UID would advertise
		// a stale identifier.
		backend.Status.ObservedServerInstance = ""
		backend.Status.ObservedGeneration = backend.Generation
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeReady)
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeProgressing)
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeDegraded)
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeFunctionalProbeOK)
		// EngineKernelsHealthy is a managed-path-only condition; clear any left
		// over so an unmanaged CR doesn't carry a stale kernel verdict.
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeEngineKernelsHealthy)
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeT2Degraded)
		// EngineCompatibility is a managed-only advisory; an Unmanaged backend
		// is no longer evaluated for injected engine-pod crash-loops, so clear
		// any left over from a prior managed state.
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeEngineCompatibility)
		// Reset the KV-event-gate timeout ANCHOR. Unlike firstKVEventObservedAt
		// (a monotonic observation marker, deliberately kept — see godoc),
		// firstAvailableAt records when the backend became "up" for the gate's
		// firstEventTimeout window. An unmanaged backend is not up in any sense,
		// so a stale anchor from a prior managed generation must not survive:
		// otherwise a later re-entry — in particular Offload→Unmanaged→EventsOnly,
		// which clears endpoint/observedServerInstance so the events-only
		// re-anchor heuristic can't see the transition — would reuse a
		// long-past availability time and breach the window on the first
		// events-only reconcile.
		backend.Status.FirstAvailableAt = nil
	})
}

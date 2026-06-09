package checks

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/cli/doctor"
)

const checkCacheBackendHealth = "CacheBackendHealth"

// CacheBackendHealth inspects each CacheBackend in scope across the dimensions
// the controller surfaces on status: Ready, matched engine pods, index
// participation (KV-event freshness), and endpoint reachability. Every failing
// axis emits its own finding so the operator sees exactly what is wrong; a
// backend that passes every applicable axis emits a single OK.
//
// Two health models coexist:
//   - Managed backends (the default): the controller provisions the cache
//     workload, matches engine pods by spec.engineSelector, and gates Ready on
//     observing a KV event. All axes apply.
//   - External backends (spec.type=External): the operator supplies the cache
//     endpoint; readiness is endpoint acceptance, NOT engine-pod matching or
//     KV-event observation (they never enter that gate — see PROJECT_CONTEXT).
//     For these, doctor checks only Ready + endpoint reachability, so a valid
//     External config is not spuriously flagged for "0 matched pods" or "no
//     index participation".
//
// The matched-engine-pod axis prefers the controller-written
// status.matchedEnginePods (its authoritative snapshot) but falls back to a
// live label match when status is absent — so doctor flags a selector mismatch
// even before the controller has reconciled, which is exactly the freshly-
// misconfigured state operators run doctor in. It fires only when the backend
// actually declares an engineSelector: a selectorless backend has nothing to
// mismatch.
//
// The index-participation axis keys off lastEventAt, not the prefix count: zero
// warm prefixes is a VALID state for an up-but-idle backend (PROJECT_CONTEXT),
// so doctor flags "engine not reporting" only when no KV event has ever been
// observed, and "engine stale" when events were seen but have stopped.
func CacheBackendHealth(ctx context.Context, c client.Client, ns string, now time.Time, staleWindow time.Duration, dial TCPDialer) []doctor.Finding {
	var backends cachev1alpha1.CacheBackendList
	if err := c.List(ctx, &backends, client.InNamespace(ns)); err != nil {
		return []doctor.Finding{listError(checkCacheBackendHealth, "CacheBackend", err)}
	}

	var findings []doctor.Finding
	for i := range backends.Items {
		cb := &backends.Items[i]
		ref := resourceRef("CacheBackend", cb.Namespace, cb.Name)
		healthy := true
		note := func(f doctor.Finding) {
			healthy = false
			findings = append(findings, f)
		}

		// Ready condition (all backends).
		if ready := findCondition(cb.Status.Conditions, conditionReady); ready == nil || ready.Status != metav1.ConditionTrue {
			note(doctor.Finding{
				Code: doctor.CodeBackendNotReady, Status: doctor.StatusWarn,
				Check: checkCacheBackendHealth, Resource: ref, Message: notReadyMessage(ready),
			})
		}

		managed := cb.Spec.Type != cachev1alpha1.CacheBackendTypeExternal
		if managed {
			// Matched engine pods — only meaningful when a selector is declared.
			if hasEngineSelector(cb) {
				count, err := matchedEnginePodCount(ctx, c, cb)
				switch {
				case err != nil:
					note(doctor.Finding{
						Code: doctor.CodeAPIReadFailed, Status: doctor.StatusFail,
						Check: checkCacheBackendHealth, Resource: ref,
						Message: fmt.Sprintf("could not determine matched engine pods (selector-mismatch check inconclusive): %v", err),
					})
				case count == 0:
					note(doctor.Finding{
						Code: doctor.CodeBackendSelectorMismatch, Status: doctor.StatusWarn,
						Check: checkCacheBackendHealth, Resource: ref,
						Message: "status.matchedEnginePods is 0 (LikelySelectorMismatch): spec.engineSelector matches no engine pods — the engine Deployment may be missing, scaled to zero, or its pod labels have drifted from the selector",
					})
				}
			}

			// Index participation: keyed off KV-event observation, not prefix count.
			if f, ok := participationFinding(cb.Status.IndexParticipation, cb.Status.FirstKVEventObservedAt, ref, now, staleWindow); ok {
				note(f)
			}

			// Functional self-test gate: the controller writes FunctionalProbeOK
			// only once the backend is otherwise eligible, so its absence is not a
			// problem (Ready/CB003 already cover the not-yet-ready state). A
			// present-but-not-True condition is the silent-failure signal the bare
			// Ready bit does not explain — surface its reason/message.
			if fp := findCondition(cb.Status.Conditions, conditionFunctionalProbeOK); fp != nil && fp.Status != metav1.ConditionTrue {
				note(doctor.Finding{
					Code: doctor.CodeBackendFunctionalProbeFailing, Status: doctor.StatusWarn,
					Check: checkCacheBackendHealth, Resource: ref,
					Message: fmt.Sprintf("FunctionalProbeOK=%s (reason %s): the controller's end-to-end functional self-test is failing for this backend — %s", fp.Status, fp.Reason, fp.Message),
				})
			}
		}

		// Endpoint presence + reachability (all backends).
		if f, ok := endpointFinding(ctx, cb, ref, dial); ok {
			note(f)
		}

		if healthy {
			findings = append(findings, doctor.Finding{
				Code: doctor.CodeBackendHealthy, Status: doctor.StatusOK,
				Check: checkCacheBackendHealth, Resource: ref, Message: healthyMessage(cb, managed, dial != nil),
			})
		}
	}
	return findings
}

func hasEngineSelector(cb *cachev1alpha1.CacheBackend) bool {
	return cb.Spec.EngineSelector != nil && len(cb.Spec.EngineSelector.MatchLabels) > 0
}

// participationFinding evaluates the index-participation axis using the right
// signal for each question:
//   - "has the engine EVER reported?" — answered by the durable
//     status.firstKVEventObservedAt latch (written write-once, never cleared).
//     Only its absence (with no live event either) means EngineNotReportingState.
//   - "are events still flowing?" — answered by the live lastEventAt, which the
//     CacheIndex poller legitimately CLEARS when a backend drains (scale-down,
//     prefixes TTL'd). A drained backend that already proved its publisher works
//     is healthy, so a cleared lastEventAt with firstKVEventObservedAt set emits
//     nothing — it is NOT reported as "never observed" (the bug of keying solely
//     off lastEventAt). Only a present-but-old lastEventAt is EngineStale.
//
// Zero warm prefixes is always a valid state and never drives a finding here.
func participationFinding(ip *cachev1alpha1.CacheBackendIndexParticipation, firstEventAt *metav1.Time, ref string, now time.Time, staleWindow time.Duration) (doctor.Finding, bool) {
	everObserved := firstEventAt != nil || (ip != nil && ip.LastEventAt != nil)
	if !everObserved {
		return doctor.Finding{
			Code: doctor.CodeBackendNotReportingState, Status: doctor.StatusWarn,
			Check: checkCacheBackendHealth, Resource: ref,
			Message: "no KV event ever observed for this backend (EngineNotReportingState): status.firstKVEventObservedAt and indexParticipation.lastEventAt are both unset — the engine's KV-event publisher may be silent or the subscriber sidecar absent",
		}, true
	}
	if ip != nil && ip.LastEventAt != nil && now.Sub(ip.LastEventAt.Time) > staleWindow {
		return doctor.Finding{
			Code: doctor.CodeBackendStale, Status: doctor.StatusWarn,
			Check: checkCacheBackendHealth, Resource: ref,
			Message: staleMessage(ip.LastEventAt, now, staleWindow),
		}, true
	}
	return doctor.Finding{}, false
}

// matchedEnginePodCount returns the controller-written status.matchedEnginePods
// when present, else a live label-match count against the backend's namespace.
// A pod-list failure is returned as an error (not silently coerced to 0) so the
// caller can report it as inconclusive rather than as a selector mismatch.
func matchedEnginePodCount(ctx context.Context, c client.Client, cb *cachev1alpha1.CacheBackend) (int, error) {
	if cb.Status.MatchedEnginePods != nil {
		return int(*cb.Status.MatchedEnginePods), nil
	}
	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(cb.Namespace)); err != nil {
		return 0, err
	}
	count := 0
	for i := range pods.Items {
		if selectorMatches(cb.Spec.EngineSelector.MatchLabels, pods.Items[i].Labels) {
			count++
		}
	}
	return count, nil
}

// endpointFinding reports an endpoint problem (missing, or present-but-
// unreachable) for a backend, returning ok=false when the endpoint is healthy
// (or when no dialer is configured to prove unreachability).
func endpointFinding(ctx context.Context, cb *cachev1alpha1.CacheBackend, ref string, dial TCPDialer) (doctor.Finding, bool) {
	if cb.Status.Endpoint == "" {
		return doctor.Finding{
			Code: doctor.CodeBackendEndpointUnreachable, Status: doctor.StatusWarn,
			Check: checkCacheBackendHealth, Resource: ref,
			Message: "status.endpoint is empty — clients have no address to reach this backend yet",
		}, true
	}
	if dial == nil {
		return doctor.Finding{}, false
	}
	if err := dial(ctx, cb.Status.Endpoint); err != nil {
		return doctor.Finding{
			Code: doctor.CodeBackendEndpointUnreachable, Status: doctor.StatusWarn,
			Check: checkCacheBackendHealth, Resource: ref,
			Message: fmt.Sprintf("status.endpoint %q is not reachable over TCP: %v", cb.Status.Endpoint, err),
		}, true
	}
	return doctor.Finding{}, false
}

func notReadyMessage(ready *metav1.Condition) string {
	if ready == nil {
		return "Ready condition is not set yet — the controller has not reconciled this backend, or it has never observed a KV event"
	}
	return fmt.Sprintf("Ready=%s (reason %s): %s", ready.Status, ready.Reason, ready.Message)
}

func staleMessage(lastEventAt *metav1.Time, now time.Time, staleWindow time.Duration) string {
	age := now.Sub(lastEventAt.Time).Round(time.Second)
	return fmt.Sprintf("status.indexParticipation.lastEventAt is %s old (EngineStale): exceeds the %s freshness window — KV events have stopped flowing", age, staleWindow)
}

func healthyMessage(cb *cachev1alpha1.CacheBackend, managed, dialed bool) string {
	// Only claim "reachable" when an actual TCP probe ran; with no dialer
	// (e.g. --config-only) doctor verified the endpoint is published, not that
	// it answers.
	endpoint := "endpoint present"
	if dialed {
		endpoint = "endpoint reachable"
	}
	if !managed {
		return "External backend: Ready, " + endpoint
	}
	return fmt.Sprintf("Ready, engine pods matched, %d prefix(es) indexed, %s", prefixCount(cb.Status.IndexParticipation), endpoint)
}

func prefixCount(ip *cachev1alpha1.CacheBackendIndexParticipation) int64 {
	if ip == nil {
		return 0
	}
	return ip.PrefixCount
}

// listError builds the standard FAIL finding for a Kubernetes List that errored
// (RBAC denial, apiserver unreachable). The check cannot draw a conclusion, so
// the safe report is a FAIL the operator can act on.
func listError(check, kind string, err error) doctor.Finding {
	return doctor.Finding{
		Code:     doctor.CodeAPIReadFailed,
		Status:   doctor.StatusFail,
		Check:    check,
		Resource: kind,
		Message:  fmt.Sprintf("could not list %s objects: %v", kind, err),
	}
}

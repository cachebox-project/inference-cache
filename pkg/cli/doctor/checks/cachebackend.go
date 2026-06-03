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
// participation (prefix count + event freshness), and endpoint reachability.
// Every failing axis emits its own finding so the operator sees exactly what is
// wrong; a backend that passes every axis emits a single OK.
//
// The matched-engine-pod axis prefers the controller-written
// status.matchedEnginePods (its authoritative snapshot) but falls back to a
// live label match when status is absent — so doctor flags a selector mismatch
// even before the controller has reconciled, which is exactly the freshly-
// misconfigured state operators run doctor in.
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

		// Ready condition.
		if ready := findCondition(cb.Status.Conditions, conditionReady); ready == nil || ready.Status != metav1.ConditionTrue {
			healthy = false
			findings = append(findings, doctor.Finding{
				Code:     doctor.CodeBackendNotReady,
				Status:   doctor.StatusWarn,
				Check:    checkCacheBackendHealth,
				Resource: ref,
				Message:  notReadyMessage(ready),
			})
		}

		// Matched engine pods.
		matched := matchedEnginePodCount(ctx, c, cb)
		if matched == 0 {
			healthy = false
			findings = append(findings, doctor.Finding{
				Code:     doctor.CodeBackendSelectorMismatch,
				Status:   doctor.StatusWarn,
				Check:    checkCacheBackendHealth,
				Resource: ref,
				Message:  "status.matchedEnginePods is 0 (LikelySelectorMismatch): spec.engineSelector matches no engine pods — the engine Deployment may be missing, scaled to zero, or its pod labels have drifted from the selector",
			})
		}

		// Index participation: prefix count + event freshness.
		ip := cb.Status.IndexParticipation
		if ip == nil || ip.PrefixCount == 0 {
			healthy = false
			findings = append(findings, doctor.Finding{
				Code:     doctor.CodeBackendNotReportingState,
				Status:   doctor.StatusWarn,
				Check:    checkCacheBackendHealth,
				Resource: ref,
				Message:  "status.indexParticipation.prefixCount is 0 (EngineNotReportingState): no warm prefixes attributed to this backend — the engine's KV-event publisher may be silent or the subscriber sidecar absent",
			})
		} else if ip.LastEventAt == nil || now.Sub(ip.LastEventAt.Time) > staleWindow {
			healthy = false
			findings = append(findings, doctor.Finding{
				Code:     doctor.CodeBackendStale,
				Status:   doctor.StatusWarn,
				Check:    checkCacheBackendHealth,
				Resource: ref,
				Message:  staleMessage(ip.LastEventAt, now, staleWindow),
			})
		}

		// Endpoint presence + reachability.
		if f, ok := endpointFinding(ctx, cb, ref, dial); ok {
			healthy = false
			findings = append(findings, f)
		}

		if healthy {
			findings = append(findings, doctor.Finding{
				Code:     doctor.CodeBackendHealthy,
				Status:   doctor.StatusOK,
				Check:    checkCacheBackendHealth,
				Resource: ref,
				Message:  fmt.Sprintf("Ready, %d matched engine pod(s), %d prefix(es) indexed, endpoint reachable", matched, prefixCount(ip)),
			})
		}
	}
	return findings
}

// matchedEnginePodCount returns the controller-written status.matchedEnginePods
// when present, else a live label-match count against the backend's namespace.
func matchedEnginePodCount(ctx context.Context, c client.Client, cb *cachev1alpha1.CacheBackend) int {
	if cb.Status.MatchedEnginePods != nil {
		return int(*cb.Status.MatchedEnginePods)
	}
	if cb.Spec.EngineSelector == nil || len(cb.Spec.EngineSelector.MatchLabels) == 0 {
		return 0
	}
	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(cb.Namespace)); err != nil {
		return 0
	}
	count := 0
	for i := range pods.Items {
		if selectorMatches(cb.Spec.EngineSelector.MatchLabels, pods.Items[i].Labels) {
			count++
		}
	}
	return count
}

// endpointFinding reports an endpoint problem (missing, or present-but-
// unreachable) for a backend, returning ok=false when the endpoint is healthy
// (or when no dialer is configured to prove unreachability).
func endpointFinding(ctx context.Context, cb *cachev1alpha1.CacheBackend, ref string, dial TCPDialer) (doctor.Finding, bool) {
	if cb.Status.Endpoint == "" {
		return doctor.Finding{
			Code:     doctor.CodeBackendEndpointUnreachable,
			Status:   doctor.StatusWarn,
			Check:    checkCacheBackendHealth,
			Resource: ref,
			Message:  "status.endpoint is empty — clients have no address to reach this backend yet",
		}, true
	}
	if dial == nil {
		return doctor.Finding{}, false
	}
	if err := dial(ctx, cb.Status.Endpoint); err != nil {
		return doctor.Finding{
			Code:     doctor.CodeBackendEndpointUnreachable,
			Status:   doctor.StatusWarn,
			Check:    checkCacheBackendHealth,
			Resource: ref,
			Message:  fmt.Sprintf("status.endpoint %q is not reachable over TCP: %v", cb.Status.Endpoint, err),
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
	if lastEventAt == nil {
		return fmt.Sprintf("status.indexParticipation.lastEventAt is unset (EngineStale): no KV event observed within the last %s", staleWindow)
	}
	age := now.Sub(lastEventAt.Time).Round(time.Second)
	return fmt.Sprintf("status.indexParticipation.lastEventAt is %s old (EngineStale): exceeds the %s freshness window — KV events have stopped flowing", age, staleWindow)
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

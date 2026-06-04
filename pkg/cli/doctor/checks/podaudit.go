package checks

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/cli/doctor"
)

const (
	checkEnginePodInjection = "EnginePodInjectionAudit"
	checkOrphanPods         = "OrphanPodCheck"
)

// EnginePodInjectionAudit finds pods that match some CacheBackend's
// engineSelector and verifies each carries an InjectedByCacheBackend Event —
// the controller's proof that the mutating Pod webhook actually wired the pod.
// A matched pod with no such Event is serving uncached (it likely lost the
// admission race against the reconciler, or was created before the backend
// existed) and gets a WARN.
func EnginePodInjectionAudit(ctx context.Context, c client.Client, ns string) []doctor.Finding {
	var backends cachev1alpha1.CacheBackendList
	if err := c.List(ctx, &backends, client.InNamespace(ns)); err != nil {
		return []doctor.Finding{listError(checkEnginePodInjection, "CacheBackend", err)}
	}
	// Collect selectors keyed by namespace — a pod is only claimed by a
	// CacheBackend in its own namespace (the webhook reads the owning backend
	// in-namespace). The backend UID is carried so the audit can re-validate the
	// injected-by-uid annotation the way the controller does.
	type sel struct {
		backend string
		uid     string
		labels  map[string]string
	}
	byNamespace := map[string][]sel{}
	for i := range backends.Items {
		cb := &backends.Items[i]
		if cb.Spec.EngineSelector == nil || len(cb.Spec.EngineSelector.MatchLabels) == 0 {
			continue
		}
		byNamespace[cb.Namespace] = append(byNamespace[cb.Namespace], sel{backend: cb.Name, uid: string(cb.UID), labels: cb.Spec.EngineSelector.MatchLabels})
	}
	// Match the pod webhook's documented tie-break for overlapping selectors:
	// the lexicographically-smallest CacheBackend name wins. Sorting here makes
	// the audit's "first match" agree with the backend that actually injected
	// the pod, rather than depending on List order.
	for ns := range byNamespace {
		sort.Slice(byNamespace[ns], func(i, j int) bool { return byNamespace[ns][i].backend < byNamespace[ns][j].backend })
	}

	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(ns)); err != nil {
		return []doctor.Finding{listError(checkEnginePodInjection, "Pod", err)}
	}

	var findings []doctor.Finding
	for i := range pods.Items {
		pod := &pods.Items[i]
		var matched *sel
		for j := range byNamespace[pod.Namespace] {
			if selectorMatches(byNamespace[pod.Namespace][j].labels, pod.Labels) {
				matched = &byNamespace[pod.Namespace][j]
				break
			}
		}
		if matched == nil {
			continue
		}
		ref := resourceRef("Pod", pod.Namespace, pod.Name)

		// Trust the durable inferencecache.io/injected-by annotation only when it
		// both NAMES the matched backend and carries an injected-by-uid matching
		// that backend's UID — the same consistency the controller requires,
		// which rejects a forged, stale, or internally-inconsistent annotation
		// pair. This outlives the GC-able Event.
		if owner := pod.Annotations[annotationInjectedBy]; owner != "" &&
			owner == pod.Namespace+"/"+matched.backend &&
			matched.uid != "" && pod.Annotations[annotationInjectedByUID] == matched.uid {
			findings = append(findings, doctor.Finding{
				Code:     doctor.CodeEnginePodInjected,
				Status:   doctor.StatusOK,
				Check:    checkEnginePodInjection,
				Resource: ref,
				Message:  fmt.Sprintf("engine pod is injected by CacheBackend %q (inferencecache.io/injected-by annotation validated against the backend UID)", owner),
			})
			continue
		}

		// Fall back to the controller-emitted Event, matched to THIS pod's UID so
		// an old Event left over from a same-named, recreated pod cannot mark a
		// freshly-uninjected pod as healthy. listEventsFor reads BOTH the legacy
		// core/v1 and the modern events.k8s.io/v1 APIs — the production
		// controller emits via the modern recorder (events.k8s.io/v1 with
		// Regarding), and reading only one API would silently miss real Events.
		events, err := listEventsFor(ctx, c, pod.Namespace, "Pod", pod.Name)
		if err != nil {
			findings = append(findings, listError(checkEnginePodInjection, "Event", err))
			continue
		}
		if hasInjectionEventForUID(events, pod.UID) {
			findings = append(findings, doctor.Finding{
				Code:     doctor.CodeEnginePodInjected,
				Status:   doctor.StatusOK,
				Check:    checkEnginePodInjection,
				Resource: ref,
				Message:  fmt.Sprintf("engine pod matched CacheBackend %q and carries an InjectedByCacheBackend Event for its current UID", matched.backend),
			})
		} else {
			findings = append(findings, doctor.Finding{
				Code:     doctor.CodeEnginePodNotInjected,
				Status:   doctor.StatusWarn,
				Check:    checkEnginePodInjection,
				Resource: ref,
				Message:  fmt.Sprintf("engine pod matches CacheBackend %q (engineSelector) but has no injection marker (no validated inferencecache.io/injected-by annotation and no InjectedByCacheBackend Event for its UID) — it may be running uncached; recreate it (e.g. kubectl rollout restart) so the mutating webhook re-evaluates", matched.backend),
			})
		}
	}
	return findings
}

// hasInjectionEventForUID reports whether any event is an InjectedByCacheBackend
// Event whose target UID matches the pod's current UID. Operates over the
// normalized event shape so a match in either the legacy core/v1 or the modern
// events.k8s.io/v1 API counts.
func hasInjectionEventForUID(events []normalizedEvent, podUID types.UID) bool {
	for i := range events {
		if events[i].Reason == eventInjectedByCacheBackend && events[i].UID == podUID {
			return true
		}
	}
	return false
}

// OrphanPods surfaces pods that recorded a NoMatchingCacheBackend Event within
// the orphan window — pods that expected cache injection but matched no
// CacheBackend, which usually means an operator misconfiguration (wrong labels,
// missing CacheBackend, wrong namespace).
//
// Forward-looking: no controller emits the NoMatchingCacheBackend Event yet
// (see eventNoMatchingCacheBackend), so this check is a no-op against today's
// clusters and only begins reporting once the engine-pod binding work wires the
// emitter. It is implemented now so the doctor's check set matches its spec and
// lights up automatically when the producer lands — and reads both the legacy
// core/v1 and modern events.k8s.io/v1 APIs so it works regardless of which
// recorder the future emitter uses.
func OrphanPods(ctx context.Context, c client.Client, ns string, now time.Time, window time.Duration) []doctor.Finding {
	events, err := listAllEvents(ctx, c, ns)
	if err != nil {
		return []doctor.Finding{listError(checkOrphanPods, "Event", err)}
	}
	cutoff := now.Add(-window)
	var findings []doctor.Finding
	for i := range events {
		e := events[i]
		if e.Reason != eventNoMatchingCacheBackend || e.Kind != "Pod" {
			continue
		}
		if e.When.Before(cutoff) {
			continue
		}
		findings = append(findings, doctor.Finding{
			Code:     doctor.CodeOrphanPod,
			Status:   doctor.StatusWarn,
			Check:    checkOrphanPods,
			Resource: resourceRef("Pod", e.Namespace, e.Name),
			Message:  fmt.Sprintf("pod recorded a NoMatchingCacheBackend Event (LikelyOperatorMisconfiguration): %s", e.Message),
		})
	}
	return findings
}

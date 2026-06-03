package checks

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
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
	// in-namespace).
	type sel struct {
		backend string
		labels  map[string]string
	}
	byNamespace := map[string][]sel{}
	for i := range backends.Items {
		cb := &backends.Items[i]
		if cb.Spec.EngineSelector == nil || len(cb.Spec.EngineSelector.MatchLabels) == 0 {
			continue
		}
		byNamespace[cb.Namespace] = append(byNamespace[cb.Namespace], sel{backend: cb.Name, labels: cb.Spec.EngineSelector.MatchLabels})
	}

	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace(ns)); err != nil {
		return []doctor.Finding{listError(checkEnginePodInjection, "Pod", err)}
	}

	var findings []doctor.Finding
	for i := range pods.Items {
		pod := &pods.Items[i]
		matchedBackend := ""
		for _, s := range byNamespace[pod.Namespace] {
			if selectorMatches(s.labels, pod.Labels) {
				matchedBackend = s.backend
				break
			}
		}
		if matchedBackend == "" {
			continue
		}
		ref := resourceRef("Pod", pod.Namespace, pod.Name)
		events, err := listEventsFor(ctx, c, pod.Namespace, "Pod", pod.Name)
		if err != nil {
			findings = append(findings, listError(checkEnginePodInjection, "Event", err))
			continue
		}
		if hasEventReason(events, eventInjectedByCacheBackend) {
			findings = append(findings, doctor.Finding{
				Code:     doctor.CodeEnginePodInjected,
				Status:   doctor.StatusOK,
				Check:    checkEnginePodInjection,
				Resource: ref,
				Message:  fmt.Sprintf("engine pod matched CacheBackend %q and carries the InjectedByCacheBackend Event", matchedBackend),
			})
		} else {
			findings = append(findings, doctor.Finding{
				Code:     doctor.CodeEnginePodNotInjected,
				Status:   doctor.StatusWarn,
				Check:    checkEnginePodInjection,
				Resource: ref,
				Message:  fmt.Sprintf("engine pod matches CacheBackend %q (engineSelector) but has no InjectedByCacheBackend Event — it may be running uncached; recreate it (e.g. kubectl rollout restart) so the mutating webhook re-evaluates", matchedBackend),
			})
		}
	}
	return findings
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
// lights up automatically when the producer lands.
func OrphanPods(ctx context.Context, c client.Client, ns string, now time.Time, window time.Duration) []doctor.Finding {
	var events corev1.EventList
	if err := c.List(ctx, &events, client.InNamespace(ns)); err != nil {
		return []doctor.Finding{listError(checkOrphanPods, "Event", err)}
	}
	cutoff := now.Add(-window)
	var findings []doctor.Finding
	for i := range events.Items {
		e := events.Items[i]
		if e.Reason != eventNoMatchingCacheBackend || e.InvolvedObject.Kind != "Pod" {
			continue
		}
		if eventTime(e).Before(cutoff) {
			continue
		}
		findings = append(findings, doctor.Finding{
			Code:     doctor.CodeOrphanPod,
			Status:   doctor.StatusWarn,
			Check:    checkOrphanPods,
			Resource: resourceRef("Pod", e.InvolvedObject.Namespace, e.InvolvedObject.Name),
			Message:  fmt.Sprintf("pod recorded a NoMatchingCacheBackend Event (LikelyOperatorMisconfiguration): %s", e.Message),
		})
	}
	return findings
}

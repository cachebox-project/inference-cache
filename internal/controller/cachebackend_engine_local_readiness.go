package controller

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

const (
	reasonEnginePodsReady             = "EnginePodsReady"
	reasonAwaitingEnginePods          = "AwaitingEnginePods"
	reasonEnginePodsRolloutInProgress = "EnginePodsRolloutInProgress"
	reasonEnginePodsUnavailable       = "EnginePodsUnavailable"
	reasonEnginePodsNotInjected       = "EnginePodsNotInjected"
	reasonEnginePodsInjectionMismatch = "EnginePodsInjectionMismatch"
	reasonAllEnginePodsSkipped        = "AllEnginePodsSkipped"
)

type engineLocalReadiness struct {
	readyStatus        metav1.ConditionStatus
	readyReason        string
	readyMessage       string
	progressingStatus  metav1.ConditionStatus
	progressingReason  string
	progressingMessage string
	degradedStatus     metav1.ConditionStatus
	degradedReason     string
	degradedMessage    string
}

// reconcileEngineLocal reports readiness from user-owned engine Pods. Dispatch
// currently scopes this stronger contract to native SGLang HiCache; other
// host-only combinations retain reconcileHostOnly's serverless contract.
// It never creates, patches, or restarts an engine workload. CacheBackend spec
// changes therefore remain Progressing until the workload owner replaces the
// stale-generation Pods and they pass CREATE admission again.
func (r *CacheBackendReconciler) reconcileEngineLocal(
	ctx context.Context,
	backend *cachev1alpha1.CacheBackend,
	adapter adapterruntime.KVCacheRuntimeAdapter,
) (ctrl.Result, error) {
	if err := r.cleanupOwnedWorkload(ctx, backend); err != nil {
		return ctrl.Result{}, err
	}

	selector := labels.SelectorFromSet(backend.Spec.EngineSelector.MatchLabels)
	var pods corev1.PodList
	reader := client.Reader(r.APIReader)
	if reader == nil {
		reader = r.Client
	}
	if err := reader.List(ctx, &pods,
		client.InNamespace(backend.Namespace),
		client.MatchingLabelsSelector{Selector: selector},
	); err != nil {
		// Readiness is observation-only. Preserve the last published verdict
		// when the live Pod set cannot be observed, and let controller-runtime
		// retry with backoff.
		return ctrl.Result{}, fmt.Errorf("list engine Pods for CacheBackend %s/%s: %w",
			backend.Namespace, backend.Name, err)
	}

	readiness := evaluateEngineLocalReadiness(backend, pods.Items, adapter)
	r.clearServerInstanceLatchShadow(backend)
	r.probeLimiter.forget(client.ObjectKeyFromObject(backend).String())

	err := r.patchStatus(ctx, backend, func() {
		backend.Status.Endpoint = ""
		backend.Status.ObservedServerInstance = ""
		backend.Status.ObservedGeneration = backend.Generation
		backend.Status.FirstAvailableAt = nil

		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeReady,
			Status:             readiness.readyStatus,
			Reason:             readiness.readyReason,
			Message:            readiness.readyMessage,
			ObservedGeneration: backend.Generation,
		})
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeProgressing,
			Status:             readiness.progressingStatus,
			Reason:             readiness.progressingReason,
			Message:            readiness.progressingMessage,
			ObservedGeneration: backend.Generation,
		})
		meta.SetStatusCondition(&backend.Status.Conditions, metav1.Condition{
			Type:               conditionTypeDegraded,
			Status:             readiness.degradedStatus,
			Reason:             readiness.degradedReason,
			Message:            readiness.degradedMessage,
			ObservedGeneration: backend.Generation,
		})

		// Server-backed health signals do not apply to native SGLang HiCache.
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeFunctionalProbeOK)
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeEngineKernelsHealthy)
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeT2Degraded)
		meta.RemoveStatusCondition(&backend.Status.Conditions, conditionTypeEngineCompatibility)
	})
	if err != nil {
		return ctrl.Result{}, err
	}
	if readiness.readyStatus != metav1.ConditionTrue {
		return ctrl.Result{RequeueAfter: r.matchedEnginePodsChurnRequeueInterval()}, nil
	}
	return ctrl.Result{}, nil
}

// evaluateEngineLocalReadiness applies the native SGLang HiCache readiness
// contract to one live selector result. Terminating and terminal Pods are
// historical and ignored. Explicitly skipped Pods opt out. Every remaining Pod
// must carry the current CacheBackend name/UID/generation receipt, already
// contain the engine config the adapter would inject for that CacheBackend, and
// be Kubernetes Ready.
func evaluateEngineLocalReadiness(
	backend *cachev1alpha1.CacheBackend,
	pods []corev1.Pod,
	adapter adapterruntime.KVCacheRuntimeAdapter,
) engineLocalReadiness {
	var (
		activeCount  int
		skipped      []string
		participants []corev1.Pod
	)
	for i := range pods {
		pod := &pods[i]
		if pod.DeletionTimestamp != nil ||
			pod.Status.Phase == corev1.PodSucceeded ||
			pod.Status.Phase == corev1.PodFailed {
			continue
		}
		activeCount++
		if podwebhook.SkipAnnotationOptsOut(pod.Annotations[podwebhook.AnnotationSkip]) {
			// AnnotationSkip is the authoritative public operator opt-out.
			// AnnotationInjectSkipped is webhook-written audit metadata, not an
			// authenticated receipt, so readiness deliberately does not require
			// it here. Both annotations are user-writable; failurePolicy=Ignore
			// means their presence cannot authenticate webhook execution.
			skipped = append(skipped, pod.Name)
			continue
		}
		participants = append(participants, *pod)
	}

	switch {
	case activeCount == 0:
		return engineLocalProgressing(reasonAwaitingEnginePods,
			"no active engine Pods match spec.engineSelector")
	case len(participants) == 0:
		return engineLocalReady(reasonAllEnginePodsSkipped,
			fmt.Sprintf("all %d active matching engine Pods explicitly skipped cache injection", activeCount))
	}

	wantBy := backend.Namespace + "/" + backend.Name
	wantUID := string(backend.UID)
	var missing, mismatched, stale, unconfigured, unavailable []string
	for i := range participants {
		pod := &participants[i]
		injectedBy := pod.Annotations[podwebhook.AnnotationInjectedBy]
		injectedUID := pod.Annotations[podwebhook.AnnotationInjectedByUID]
		injectedGeneration := pod.Annotations[podwebhook.AnnotationInjectedGeneration]
		generation, generationErr := strconv.ParseInt(injectedGeneration, 10, 64)

		switch {
		case injectedBy == "" || injectedUID == "" || injectedGeneration == "" || generationErr != nil || generation < 0:
			missing = append(missing, pod.Name)
		case injectedBy != wantBy || injectedUID != wantUID || generation > backend.Generation:
			mismatched = append(mismatched, pod.Name)
		case generation < backend.Generation:
			stale = append(stale, pod.Name)
		case !engineConfigConverged(adapter, pod, backend):
			unconfigured = append(unconfigured, pod.Name)
		case !podIsReady(pod):
			unavailable = append(unavailable, pod.Name)
		}
	}

	total := len(participants)
	switch {
	case len(missing) > 0:
		return engineLocalDegraded(reasonEnginePodsNotInjected,
			podDiagnostic("%d/%d engine Pods are missing a valid CacheBackend injection receipt and must be recreated: %s",
				missing, total))
	case len(mismatched) > 0:
		return engineLocalDegraded(reasonEnginePodsInjectionMismatch,
			podDiagnostic("%d/%d engine Pods carry an injection receipt for a different CacheBackend identity or future generation: %s",
				mismatched, total))
	case len(stale) > 0:
		return engineLocalProgressing(reasonEnginePodsRolloutInProgress,
			podDiagnostic("%d/%d engine Pods carry an older CacheBackend generation and must be rolled out: %s",
				stale, total))
	case len(unconfigured) > 0:
		return engineLocalDegraded(reasonEnginePodsNotInjected,
			podDiagnostic("%d/%d engine Pods do not contain the current CacheBackend engine configuration and must be recreated: %s",
				unconfigured, total))
	case len(unavailable) > 0:
		return engineLocalDegraded(reasonEnginePodsUnavailable,
			podDiagnostic("%d/%d current-generation engine Pods are not Ready: %s",
				unavailable, total))
	default:
		message := fmt.Sprintf("%d/%d engine Pods carry CacheBackend generation %d and are Ready",
			total, total, backend.Generation)
		if len(skipped) > 0 {
			message += fmt.Sprintf("; %d additional matching Pods explicitly skipped injection", len(skipped))
		}
		return engineLocalReady(reasonEnginePodsReady, message)
	}
}

// engineConfigConverged uses the Pod webhook's complete idempotent engine
// mutation contract as a read-only verifier. A converged PodSpec is unchanged
// when the current adapter configuration and engineOverrides are applied to an
// in-memory copy. This verifies the actual engine configuration instead of
// trusting user-writable receipt annotations as proof that the mutating
// webhook ran.
func engineConfigConverged(
	adapter adapterruntime.KVCacheRuntimeAdapter,
	pod *corev1.Pod,
	backend *cachev1alpha1.CacheBackend,
) bool {
	if adapter == nil {
		return false
	}
	want := pod.Spec.DeepCopy()
	if err := podwebhook.ApplyEngineConfigWithOverrides(adapter, want, nil, backend); err != nil {
		return false
	}
	return reflect.DeepEqual(*want, pod.Spec)
}

func engineLocalReady(reason, message string) engineLocalReadiness {
	return engineLocalReadiness{
		readyStatus:        metav1.ConditionTrue,
		readyReason:        reason,
		readyMessage:       message,
		progressingStatus:  metav1.ConditionFalse,
		progressingReason:  "Synced",
		progressingMessage: message,
		degradedStatus:     metav1.ConditionFalse,
		degradedReason:     reasonNotDegraded,
		degradedMessage:    "backend is not in a degraded state",
	}
}

func engineLocalProgressing(reason, message string) engineLocalReadiness {
	return engineLocalReadiness{
		readyStatus:        metav1.ConditionFalse,
		readyReason:        reason,
		readyMessage:       message,
		progressingStatus:  metav1.ConditionTrue,
		progressingReason:  reason,
		progressingMessage: message,
		degradedStatus:     metav1.ConditionFalse,
		degradedReason:     reasonNotDegraded,
		degradedMessage:    "backend is not in a degraded state",
	}
}

func engineLocalDegraded(reason, message string) engineLocalReadiness {
	return engineLocalReadiness{
		readyStatus:        metav1.ConditionFalse,
		readyReason:        reason,
		readyMessage:       message,
		progressingStatus:  metav1.ConditionFalse,
		progressingReason:  "Degraded",
		progressingMessage: message,
		degradedStatus:     metav1.ConditionTrue,
		degradedReason:     reason,
		degradedMessage:    message,
	}
}

func podDiagnostic(format string, podNames []string, total int) string {
	sort.Strings(podNames)
	return truncateMessage(fmt.Sprintf(format, len(podNames), total, strings.Join(podNames, ", ")))
}

package controller

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

// EngineKernelsHealthy gate: surfaces the engine-side native CUDA-kernel
// (lmcache c_ops) load health on the CacheBackend, read from the
// lmcache-kernel-check init container the pod webhook injects into matched
// engine pods. Default is fail-OPEN observability (report-only): the condition
// surfaces the problem but does NOT downgrade Ready, so a degraded kernel tier
// does not make the cache a serving dependency. The opt-in strict mode
// (annotation = "strict") makes the init container exit non-zero — the engine
// pod stays in Init and never serves — AND downgrades Ready here.
//
// IMPORTANT (green != fast): EngineKernelsHealthy=True means the native
// kernels LOADED (no torch fallback). It does NOT certify that T2 reload is
// fast under concurrency — that is validated only by a separate GPU
// concurrency canary. And it proves load-time linkage, not runtime
// executability ("lib present but driver too old" loads fine, fails at launch).
const (
	conditionTypeEngineKernelsHealthy = "EngineKernelsHealthy"

	reasonKernelsHealthy = "KernelsHealthy"
	// reasonKernelLoadFailed covers every way the native lmcache c_ops
	// kernels did not load: a libcudart/CUDA-runtime mismatch (the root
	// cause), a CPU/pure-python build with no compiled extension, or lmcache
	// not being importable at all. The specific cause is carried verbatim in
	// the condition .message (the detector's FAIL: line) — the reason stays
	// generic so it never over-claims a CUDA mismatch for a packaging error.
	reasonKernelLoadFailed   = "KernelLoadFailed"
	reasonKernelCheckError   = "KernelCheckError"
	reasonKernelCheckPending = "KernelCheckPending"

	// reasonEngineKernelDegraded is the Ready-condition reason used when a
	// strict-mode backend has a failing kernel check. The message names the
	// GPU-stranding cost so operators reclaim fast.
	reasonEngineKernelDegraded = "EngineKernelDegraded"
)

// kernelHealthVerdict mirrors functionalProbeVerdict's shape so the caller
// composes both gates uniformly in updateManagedStatus.
type kernelHealthVerdict struct {
	shouldWriteCondition bool
	condition            metav1.Condition
	removeCondition      bool
	downgradeReady       bool
	readyReason          string
	readyMessage         string
}

// evaluateEngineKernelHealth produces a verdict from the backend's matched
// engine pods' kernel-check init-container status. Pure function: the caller
// (updateManagedStatus) performs the pod List via listMatchedEnginePods and
// passes the result in. The gate is inactive (removeCondition / no-op) when no
// matched pod carries a kernel-check container (CPU backends, annotation=off,
// no engineSelector).
//
// listedOK reports whether the pod list actually succeeded. When it is false
// the caller couldn't observe the matched pods (a transient API/RBAC error),
// so the verdict is a strict no-op: a list failure must not be read as "no
// kernel-check pods" and clear a known EngineKernelsHealthy=False — that would
// hide the last-known kernel failure behind a transient blip.
func evaluateEngineKernelHealth(
	backend *cachev1alpha1.CacheBackend,
	upstream kvReadiness,
	pods []corev1.Pod,
	listedOK bool,
) kernelHealthVerdict {
	if !listedOK {
		return kernelHealthVerdict{}
	}
	cond, active := aggregateKernelHealth(backend, pods)
	if !active {
		if meta.FindStatusCondition(backend.Status.Conditions, conditionTypeEngineKernelsHealthy) != nil {
			return kernelHealthVerdict{removeCondition: true}
		}
		return kernelHealthVerdict{}
	}
	v := kernelHealthVerdict{shouldWriteCondition: true, condition: cond}

	// Strict-mode Ready downgrade — ONLY on a definite mismatch (False), never
	// on Pending/Error (transient/indeterminate). Unlike the functional-probe
	// gate, this does NOT require upstream Ready=True first: in strict mode a
	// failing init container holds the engine pod in Init, so it never emits a
	// KV event and the upstream gate would otherwise pin Ready to
	// AwaitingFirstKVEvent/NoKVEventsObserved — masking the actual root cause.
	// A confirmed KernelLoadFailed is a hard fact read straight off the pod, so
	// it takes precedence and surfaces EngineKernelDegraded as the Ready reason
	// at deploy time, which is the point of opting into strict. (The Degraded
	// condition still narrates the no-KV-events symptom separately.)
	if backend.Annotations[adapterruntime.AnnotationLMCacheKernelCheck] == adapterruntime.KernelCheckModeStrict &&
		cond.Status == metav1.ConditionFalse {
		v.downgradeReady = true
		v.readyReason = reasonEngineKernelDegraded
		v.readyMessage = "lmcache CUDA kernels failed to load on one or more engine pods; in strict mode those pods stay in Init holding their GPU reservation without serving — fix the engine image's lmcache/CUDA alignment or set " + adapterruntime.AnnotationLMCacheKernelCheck + "=report-only"
	}
	return v
}

// aggregateKernelHealth classifies the matched pods' kernel-check init
// statuses into a single condition. Returns active=false when no matched pod
// carries a kernel-check init container. Precedence: mismatch > error >
// pending > healthy — a single confirmed mismatch is the most actionable.
func aggregateKernelHealth(backend *cachev1alpha1.CacheBackend, pods []corev1.Pod) (metav1.Condition, bool) {
	var seen bool
	var failMsg string
	var nFail, nErr, nPending, nOK int
	for i := range pods {
		st := findKernelCheckStatus(&pods[i])
		if st == nil {
			continue
		}
		seen = true
		term := st.State.Terminated
		if term == nil && st.LastTerminationState.Terminated != nil {
			term = st.LastTerminationState.Terminated // CrashLoopBackOff: read last terminated
		}
		if term == nil {
			nPending++
			continue
		}
		msg := strings.TrimSpace(term.Message)
		switch {
		case strings.HasPrefix(msg, adapterruntime.KernelCheckMsgFailPrefix):
			nFail++
			if failMsg == "" {
				failMsg = msg
			}
		case msg == adapterruntime.KernelCheckMsgOK:
			nOK++
		default:
			nErr++
		}
	}
	if !seen {
		return metav1.Condition{}, false
	}
	mk := func(status metav1.ConditionStatus, reason, message string) metav1.Condition {
		return metav1.Condition{
			Type:               conditionTypeEngineKernelsHealthy,
			Status:             status,
			Reason:             reason,
			Message:            truncateMessage(message),
			ObservedGeneration: backend.Generation,
		}
	}
	switch {
	case nFail > 0:
		return mk(metav1.ConditionFalse, reasonKernelLoadFailed,
			fmt.Sprintf("native lmcache c_ops kernels failed to load on %d engine pod(s): %s", nFail, failMsg)), true
	case nErr > 0:
		return mk(metav1.ConditionUnknown, reasonKernelCheckError,
			fmt.Sprintf("kernel-check init container on %d engine pod(s) terminated without a recognized result message", nErr)), true
	case nPending > 0:
		return mk(metav1.ConditionUnknown, reasonKernelCheckPending,
			fmt.Sprintf("kernel-check init container has not completed on %d engine pod(s)", nPending)), true
	default:
		return mk(metav1.ConditionTrue, reasonKernelsHealthy,
			fmt.Sprintf("native lmcache c_ops kernels loaded on %d engine pod(s)", nOK)), true
	}
}

// findKernelCheckStatus returns the lmcache-kernel-check init-container status
// on pod, or nil if absent.
func findKernelCheckStatus(pod *corev1.Pod) *corev1.ContainerStatus {
	for i := range pod.Status.InitContainerStatuses {
		if pod.Status.InitContainerStatuses[i].Name == adapterruntime.LMCacheKernelCheckContainerName {
			return &pod.Status.InitContainerStatuses[i]
		}
	}
	return nil
}

// listMatchedEnginePods lists pods in the backend's namespace matching its
// engineSelector via the uncached reader (same read posture as
// refreshMatchedEnginePods — no Pod informer). The bool reports whether the
// observation succeeded: a no-selector backend returns (nil, true) — there are
// legitimately no engine pods — but a transient List error returns (nil,
// false) so the caller can distinguish "no pods" from "couldn't look" and
// avoid clearing a known condition on a blip.
func listMatchedEnginePods(ctx context.Context, reader client.Reader, backend *cachev1alpha1.CacheBackend) ([]corev1.Pod, bool) {
	sel := backend.Spec.EngineSelector
	if sel == nil || len(sel.MatchLabels) == 0 || reader == nil {
		return nil, true
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods,
		client.InNamespace(backend.Namespace),
		client.MatchingLabelsSelector{Selector: labels.SelectorFromSet(sel.MatchLabels)},
	); err != nil {
		log.FromContext(ctx).V(1).Info("kernel-health: pod list failed (fail-soft)",
			"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
		return nil, false
	}
	// Scope attribution to pods THIS backend actually wired — match the
	// webhook-stamped injected-by + injected-by-uid pair, not the engineSelector
	// alone. With overlapping selectors (or a hand-authored kernel-check
	// container), a pod wired to a DIFFERENT CacheBackend could otherwise drive
	// this backend's verdict and, in strict mode, downgrade its Ready. This is
	// the same ownership signal the cache-server-restart cascade uses.
	wantInjectedBy := backend.Namespace + "/" + backend.Name
	wantInjectedByUID := string(backend.UID)
	owned := make([]corev1.Pod, 0, len(pods.Items))
	for i := range pods.Items {
		ann := pods.Items[i].Annotations
		if ann[podwebhook.AnnotationInjectedBy] != wantInjectedBy {
			continue
		}
		if wantInjectedByUID == "" || ann[podwebhook.AnnotationInjectedByUID] != wantInjectedByUID {
			continue
		}
		owned = append(owned, pods.Items[i])
	}
	return owned, true
}

// downgradeKernelReadyVerdict applies a strict-mode kernel downgrade to the
// running readiness verdict. Mirrors downgradeReadyVerdict (probe gate).
func downgradeKernelReadyVerdict(upstream kvReadiness, v kernelHealthVerdict) kvReadiness {
	if !v.downgradeReady {
		return upstream
	}
	out := upstream
	out.readyStatus = metav1.ConditionFalse
	out.readyReason = v.readyReason
	out.readyMessage = v.readyMessage
	return out
}

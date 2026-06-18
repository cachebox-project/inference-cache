package controller

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

func podWithKernelStatus(state corev1.ContainerState) corev1.Pod {
	return corev1.Pod{
		Spec: corev1.PodSpec{InitContainers: []corev1.Container{{
			Name: adapterruntime.LMCacheKernelCheckContainerName,
		}}},
		Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{{
			Name:  adapterruntime.LMCacheKernelCheckContainerName,
			State: state,
		}}},
	}
}

// specOnlyKernelPod has the kernel-check init container in its spec but no
// observed status yet — a just-created / unscheduled pod.
func specOnlyKernelPod() corev1.Pod {
	return corev1.Pod{Spec: corev1.PodSpec{InitContainers: []corev1.Container{{
		Name: adapterruntime.LMCacheKernelCheckContainerName,
	}}}}
}

// strictPodWithKernelStatus is a pod whose kernel-check init container was
// admitted in STRICT mode (KERNEL_CHECK_STRICT=1 on its spec env), plus the
// given terminated status. The strict-ness lives on the pod, not the CR.
func strictPodWithKernelStatus(state corev1.ContainerState) corev1.Pod {
	p := podWithKernelStatus(state)
	p.Spec.InitContainers = []corev1.Container{{
		Name: adapterruntime.LMCacheKernelCheckContainerName,
		Env:  []corev1.EnvVar{{Name: adapterruntime.EnvKernelCheckStrict, Value: "1"}},
	}}
	return p
}

func termed(code int32, msg string) corev1.ContainerState {
	return corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: code, Message: msg}}
}

func setStaleKernelCondition(cb *cachev1alpha1.CacheBackend) {
	meta.SetStatusCondition(&cb.Status.Conditions, metav1.Condition{
		Type: conditionTypeEngineKernelsHealthy, Status: metav1.ConditionTrue, Reason: reasonKernelsHealthy,
	})
}

func TestAggregateKernelHealth(t *testing.T) {
	cb := &cachev1alpha1.CacheBackend{ObjectMeta: metav1.ObjectMeta{Name: "cb", Namespace: "ns"}}
	cases := []struct {
		name       string
		pods       []corev1.Pod
		wantStatus metav1.ConditionStatus
		wantReason string
		wantActive bool
	}{
		{"all ok", []corev1.Pod{podWithKernelStatus(termed(0, "OK"))}, metav1.ConditionTrue, reasonKernelsHealthy, true},
		{"one fail", []corev1.Pod{
			podWithKernelStatus(termed(0, "OK")),
			podWithKernelStatus(termed(0, "FAIL: ImportError: libcudart.so.13")),
		}, metav1.ConditionFalse, reasonKernelLoadFailed, true},
		{"strict crashloop fail via lastState", []corev1.Pod{{
			Spec: corev1.PodSpec{InitContainers: []corev1.Container{{Name: adapterruntime.LMCacheKernelCheckContainerName}}},
			Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{{
				Name:                 adapterruntime.LMCacheKernelCheckContainerName,
				State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
				LastTerminationState: termed(1, "FAIL: ImportError: libcudart.so.13"),
			}}},
		}}, metav1.ConditionFalse, reasonKernelLoadFailed, true},
		{"garbage message is error not mismatch", []corev1.Pod{podWithKernelStatus(termed(127, ""))}, metav1.ConditionUnknown, reasonKernelCheckError, true},
		{"running, no terminated status => pending", []corev1.Pod{podWithKernelStatus(corev1.ContainerState{Running: &corev1.ContainerStateRunning{}})}, metav1.ConditionUnknown, reasonKernelCheckPending, true},
		{"spec present but status not observed yet => pending", []corev1.Pod{specOnlyKernelPod()}, metav1.ConditionUnknown, reasonKernelCheckPending, true},
		{"no kernel-check container in spec => inactive", []corev1.Pod{{Status: corev1.PodStatus{}}}, "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cond, active, _ := aggregateKernelHealth(cb, tc.pods)
			if active != tc.wantActive {
				t.Fatalf("active = %v, want %v", active, tc.wantActive)
			}
			if !active {
				return
			}
			if cond.Status != tc.wantStatus {
				t.Errorf("status = %q, want %q", cond.Status, tc.wantStatus)
			}
			if cond.Reason != tc.wantReason {
				t.Errorf("reason = %q, want %q", cond.Reason, tc.wantReason)
			}
		})
	}
}

func TestAggregateKernelHealthErrorIncludesExitDetail(t *testing.T) {
	// A KernelCheckError (terminated without our OK/FAIL: message — e.g. OOMKilled)
	// must carry the exit code / reason / raw message so the condition is
	// actionable, not just "unrecognized".
	cb := &cachev1alpha1.CacheBackend{ObjectMeta: metav1.ObjectMeta{Name: "cb", Namespace: "ns"}}
	pod := podWithKernelStatus(corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
		ExitCode: 137, Reason: "OOMKilled",
	}})
	cond, active, _ := aggregateKernelHealth(cb, []corev1.Pod{pod})
	if !active || cond.Reason != reasonKernelCheckError {
		t.Fatalf("want active KernelCheckError; got active=%v reason=%q", active, cond.Reason)
	}
	if !strings.Contains(cond.Message, "exit 137") || !strings.Contains(cond.Message, "OOMKilled") {
		t.Errorf("KernelCheckError message must include the exit code + reason; got %q", cond.Message)
	}
}

func TestEvaluateEngineKernelHealthStrictDowngradesReady(t *testing.T) {
	// No strict annotation on the CR — the downgrade is driven by the POD's
	// admitted strict mode (its init container's KERNEL_CHECK_STRICT=1).
	cb := &cachev1alpha1.CacheBackend{ObjectMeta: metav1.ObjectMeta{Name: "cb", Namespace: "ns"}}
	up := kvReadiness{readyStatus: metav1.ConditionTrue, readyReason: "KVEventsObserved"}
	v := evaluateEngineKernelHealth(cb, up, []corev1.Pod{strictPodWithKernelStatus(termed(1, "FAIL: ImportError: libcudart.so.13"))}, true)
	if !v.downgradeReady || v.readyReason != reasonEngineKernelDegraded {
		t.Fatalf("strict-admitted pod mismatch must downgrade Ready with EngineKernelDegraded; got downgrade=%v reason=%q", v.downgradeReady, v.readyReason)
	}
	out := downgradeKernelReadyVerdict(up, v)
	if out.readyStatus != metav1.ConditionFalse {
		t.Errorf("downgraded readyStatus = %q, want False", out.readyStatus)
	}
}

func TestEvaluateEngineKernelHealthStrictDowngradesEvenBeforeFirstKVEvent(t *testing.T) {
	// Deploy-time path: a strict init container fails BEFORE any KV event, so
	// upstream is still AwaitingFirstKVEvent (Ready=False). A confirmed
	// mismatch must still downgrade to EngineKernelDegraded — not be masked by
	// the KV-event-waiting reason it causes.
	cb := &cachev1alpha1.CacheBackend{ObjectMeta: metav1.ObjectMeta{Name: "cb", Namespace: "ns"}}
	upstreamNotReady := kvReadiness{readyStatus: metav1.ConditionFalse, readyReason: "AwaitingFirstKVEvent"}
	v := evaluateEngineKernelHealth(cb, upstreamNotReady, []corev1.Pod{strictPodWithKernelStatus(termed(1, "FAIL: ImportError: libcudart.so.13"))}, true)
	if !v.downgradeReady || v.readyReason != reasonEngineKernelDegraded {
		t.Fatalf("strict mismatch must downgrade Ready to EngineKernelDegraded even pre-first-KV-event; got downgrade=%v reason=%q", v.downgradeReady, v.readyReason)
	}
	out := downgradeKernelReadyVerdict(upstreamNotReady, v)
	if out.readyStatus != metav1.ConditionFalse || out.readyReason != reasonEngineKernelDegraded {
		t.Errorf("downgraded verdict = %q/%q, want False/EngineKernelDegraded", out.readyStatus, out.readyReason)
	}
}

func TestEvaluateEngineKernelHealthAnnotationStrictButReportOnlyPodDoesNotDowngrade(t *testing.T) {
	// The CR annotation says strict, but the pod was admitted report-only (no
	// KERNEL_CHECK_STRICT env) — e.g. the operator flipped the annotation after
	// the pod was already running. Pod truth wins: do NOT downgrade Ready (that
	// pod is actually serving), though the condition still surfaces False.
	cb := &cachev1alpha1.CacheBackend{ObjectMeta: metav1.ObjectMeta{Name: "cb", Namespace: "ns",
		Annotations: map[string]string{adapterruntime.AnnotationLMCacheKernelCheck: adapterruntime.KernelCheckModeStrict}}}
	up := kvReadiness{readyStatus: metav1.ConditionTrue}
	v := evaluateEngineKernelHealth(cb, up, []corev1.Pod{podWithKernelStatus(termed(0, "FAIL: ImportError: libcudart.so.13"))}, true)
	if v.downgradeReady {
		t.Error("a report-only-admitted pod must not downgrade Ready even when the CR annotation now says strict")
	}
	if !v.shouldWriteCondition || v.condition.Status != metav1.ConditionFalse {
		t.Errorf("the condition must still surface False; got write=%v status=%q", v.shouldWriteCondition, v.condition.Status)
	}
}

func TestEvaluateEngineKernelHealthReportOnlyDoesNotDowngrade(t *testing.T) {
	cb := &cachev1alpha1.CacheBackend{ObjectMeta: metav1.ObjectMeta{Name: "cb", Namespace: "ns"}} // no strict annotation
	up := kvReadiness{readyStatus: metav1.ConditionTrue}
	v := evaluateEngineKernelHealth(cb, up, []corev1.Pod{podWithKernelStatus(termed(0, "FAIL: ImportError: libcudart.so.13"))}, true)
	if v.downgradeReady {
		t.Error("report-only (default) must NOT downgrade Ready")
	}
	if !v.shouldWriteCondition || v.condition.Status != metav1.ConditionFalse {
		t.Errorf("report-only must still surface EngineKernelsHealthy=False; got write=%v status=%q", v.shouldWriteCondition, v.condition.Status)
	}
}

func TestEvaluateEngineKernelHealthInactiveRemovesStaleCondition(t *testing.T) {
	cb := &cachev1alpha1.CacheBackend{ObjectMeta: metav1.ObjectMeta{Name: "cb", Namespace: "ns"}}
	setStaleKernelCondition(cb) // sets a stale EngineKernelsHealthy condition; see helper below
	v := evaluateEngineKernelHealth(cb, kvReadiness{readyStatus: metav1.ConditionTrue}, []corev1.Pod{{Status: corev1.PodStatus{}}}, true)
	if !v.removeCondition {
		t.Error("inactive gate with an existing condition must request removeCondition")
	}
}

func TestListMatchedEnginePodsScopesByInjectedBy(t *testing.T) {
	scheme := newScheme(t)
	backend := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "mine", Namespace: "ns", UID: "uid-mine"},
		Spec: cachev1alpha1.CacheBackendSpec{
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{MatchLabels: map[string]string{"app": "x"}},
		},
	}
	mkPod := func(name, injBy, injUID string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "ns", Labels: map[string]string{"app": "x"},
			Annotations: map[string]string{
				podwebhook.AnnotationInjectedBy:    injBy,
				podwebhook.AnnotationInjectedByUID: injUID,
			},
		}}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		mkPod("mine-pod", "ns/mine", "uid-mine"),    // wired by THIS backend
		mkPod("other-pod", "ns/other", "uid-other"), // overlapping selector, different backend
		mkPod("forged-pod", "ns/mine", "wrong-uid"), // forged injected-by, wrong UID
	).Build()

	pods, ok := listMatchedEnginePods(context.Background(), c, backend)
	if !ok {
		t.Fatal("listOK should be true on a successful list")
	}
	if len(pods) != 1 || pods[0].Name != "mine-pod" {
		names := make([]string, len(pods))
		for i := range pods {
			names[i] = pods[i].Name
		}
		t.Fatalf("expected only [mine-pod] (injected-by + UID scoped); got %v", names)
	}
}

func TestEvaluateEngineKernelHealthListErrorPreservesCondition(t *testing.T) {
	// A transient pod-list failure (listedOK=false) must NOT be read as
	// "no kernel-check pods" and clear a known EngineKernelsHealthy=False.
	cb := &cachev1alpha1.CacheBackend{ObjectMeta: metav1.ObjectMeta{Name: "cb", Namespace: "ns"}}
	setStaleKernelCondition(cb)
	v := evaluateEngineKernelHealth(cb, kvReadiness{readyStatus: metav1.ConditionTrue}, nil, false)
	if v.removeCondition {
		t.Error("a list error must NOT remove the existing condition")
	}
	if v.shouldWriteCondition {
		t.Error("a list error must NOT write a new condition")
	}
	if v.downgradeReady {
		t.Error("a list error must NOT change Ready")
	}
}

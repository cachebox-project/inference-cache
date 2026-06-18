package controller

import (
	"context"
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
	return corev1.Pod{Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{{
		Name:  adapterruntime.LMCacheKernelCheckContainerName,
		State: state,
	}}}}
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
		{"strict crashloop fail via lastState", []corev1.Pod{{Status: corev1.PodStatus{InitContainerStatuses: []corev1.ContainerStatus{{
			Name:                 adapterruntime.LMCacheKernelCheckContainerName,
			State:                corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}},
			LastTerminationState: termed(1, "FAIL: ImportError: libcudart.so.13"),
		}}}}}, metav1.ConditionFalse, reasonKernelLoadFailed, true},
		{"garbage message is error not mismatch", []corev1.Pod{podWithKernelStatus(termed(127, ""))}, metav1.ConditionUnknown, reasonKernelCheckError, true},
		{"pending", []corev1.Pod{podWithKernelStatus(corev1.ContainerState{Running: &corev1.ContainerStateRunning{}})}, metav1.ConditionUnknown, reasonKernelCheckPending, true},
		{"no kernel-check container => inactive", []corev1.Pod{{Status: corev1.PodStatus{}}}, "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cond, active := aggregateKernelHealth(cb, tc.pods)
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

func TestEvaluateEngineKernelHealthStrictDowngradesReady(t *testing.T) {
	cb := &cachev1alpha1.CacheBackend{ObjectMeta: metav1.ObjectMeta{Name: "cb", Namespace: "ns",
		Annotations: map[string]string{adapterruntime.AnnotationLMCacheKernelCheck: adapterruntime.KernelCheckModeStrict}}}
	up := kvReadiness{readyStatus: metav1.ConditionTrue, readyReason: "KVEventsObserved"}
	v := evaluateEngineKernelHealth(cb, up, []corev1.Pod{podWithKernelStatus(termed(1, "FAIL: ImportError: libcudart.so.13"))}, true)
	if !v.downgradeReady || v.readyReason != reasonEngineKernelDegraded {
		t.Fatalf("strict mismatch must downgrade Ready with EngineKernelDegraded; got downgrade=%v reason=%q", v.downgradeReady, v.readyReason)
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
	cb := &cachev1alpha1.CacheBackend{ObjectMeta: metav1.ObjectMeta{Name: "cb", Namespace: "ns",
		Annotations: map[string]string{adapterruntime.AnnotationLMCacheKernelCheck: adapterruntime.KernelCheckModeStrict}}}
	upstreamNotReady := kvReadiness{readyStatus: metav1.ConditionFalse, readyReason: "AwaitingFirstKVEvent"}
	v := evaluateEngineKernelHealth(cb, upstreamNotReady, []corev1.Pod{podWithKernelStatus(termed(1, "FAIL: ImportError: libcudart.so.13"))}, true)
	if !v.downgradeReady || v.readyReason != reasonEngineKernelDegraded {
		t.Fatalf("strict mismatch must downgrade Ready to EngineKernelDegraded even pre-first-KV-event; got downgrade=%v reason=%q", v.downgradeReady, v.readyReason)
	}
	out := downgradeKernelReadyVerdict(upstreamNotReady, v)
	if out.readyStatus != metav1.ConditionFalse || out.readyReason != reasonEngineKernelDegraded {
		t.Errorf("downgraded verdict = %q/%q, want False/EngineKernelDegraded", out.readyStatus, out.readyReason)
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

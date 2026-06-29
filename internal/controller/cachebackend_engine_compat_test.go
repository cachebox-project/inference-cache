package controller

import (
	"context"
	"strings"
	"testing"

	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
	corev1 "k8s.io/api/core/v1"
)

// enginePodWithContainerState builds a label-matched engine pod with the given
// injected-by stamp and a single container in the given waiting reason. An
// empty waitingReason marks the container Running instead.
func enginePodWithContainerState(name, ns string, lbls map[string]string, injectedBy, containerName, waitingReason string) *corev1.Pod {
	p := engineLikePod(name, ns, lbls)
	if injectedBy != "" {
		p.Annotations = map[string]string{podwebhook.AnnotationInjectedBy: injectedBy}
	}
	cs := corev1.ContainerStatus{Name: containerName}
	if waitingReason != "" {
		cs.State.Waiting = &corev1.ContainerStateWaiting{Reason: waitingReason}
	} else {
		cs.State.Running = &corev1.ContainerStateRunning{}
	}
	p.Status.ContainerStatuses = []corev1.ContainerStatus{cs}
	return p
}

func TestDetectEngineConnectorCrashLoop(t *testing.T) {
	scheme := newScheme(t)
	const ns, name = "ns1", "cache"
	injectedBy := ns + "/" + name

	tests := []struct {
		desc    string
		pod     *corev1.Pod
		wantMsg bool
	}{
		{
			desc:    "injected engine in CrashLoopBackOff is flagged",
			pod:     enginePodWithContainerState("e1", ns, matchedSelector, injectedBy, "vllm", crashLoopBackOffReason),
			wantMsg: true,
		},
		{
			desc:    "injected engine running is not flagged",
			pod:     enginePodWithContainerState("e1", ns, matchedSelector, injectedBy, "vllm", ""),
			wantMsg: false,
		},
		{
			desc:    "non-injected pod crash-looping is not ours to flag",
			pod:     enginePodWithContainerState("e1", ns, matchedSelector, "", "vllm", crashLoopBackOffReason),
			wantMsg: false,
		},
		{
			desc:    "pod stamped for a different backend is not flagged",
			pod:     enginePodWithContainerState("e1", ns, matchedSelector, ns+"/other", "vllm", crashLoopBackOffReason),
			wantMsg: false,
		},
		{
			desc:    "ImagePullBackOff is not the connector-incompatibility signature",
			pod:     enginePodWithContainerState("e1", ns, matchedSelector, injectedBy, "vllm", "ImagePullBackOff"),
			wantMsg: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			cb := lmcacheBackendWithSelector(name, ns, matchedSelector)
			r := newReconciler(scheme, cb, tc.pod)
			msg := r.detectEngineConnectorCrashLoop(context.Background(), cb)
			switch {
			case tc.wantMsg && msg == "":
				t.Fatalf("want a diagnostic message, got empty")
			case !tc.wantMsg && msg != "":
				t.Fatalf("want empty, got: %s", msg)
			case tc.wantMsg && !strings.Contains(msg, "CrashLoopBackOff"):
				t.Fatalf("diagnostic must name CrashLoopBackOff, got: %s", msg)
			}
		})
	}
}

// A backend with no engineSelector short-circuits before listing pods.
func TestDetectEngineConnectorCrashLoopNoSelector(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackend("cache", "ns1") // no EngineSelector configured
	r := newReconciler(scheme, cb)
	if msg := r.detectEngineConnectorCrashLoop(context.Background(), cb); msg != "" {
		t.Fatalf("no-selector backend must return empty, got: %s", msg)
	}
}

package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

const testBackendUID = "be-uid-123"

// ctrState is a (container name, kubelet waiting reason) pair for a synthetic
// pod status. An empty waitingReason marks the container Running.
type ctrState struct {
	name          string
	waitingReason string
}

// enginePodWithContainers builds a namespace-resident pod stamped with the
// given injected-by + injected-by-uid annotations (when injectedBy is set) and
// one container status per ctrState.
func enginePodWithContainers(name, ns string, lbls map[string]string, injectedBy, injectedByUID string, cs ...ctrState) *corev1.Pod {
	p := engineLikePod(name, ns, lbls)
	if injectedBy != "" {
		p.Annotations = map[string]string{podwebhook.AnnotationInjectedBy: injectedBy}
		if injectedByUID != "" {
			p.Annotations[podwebhook.AnnotationInjectedByUID] = injectedByUID
		}
	}
	for _, c := range cs {
		st := corev1.ContainerStatus{Name: c.name}
		if c.waitingReason != "" {
			st.State.Waiting = &corev1.ContainerStateWaiting{Reason: c.waitingReason}
		} else {
			st.State.Running = &corev1.ContainerStateRunning{}
		}
		p.Status.ContainerStatuses = append(p.Status.ContainerStatuses, st)
	}
	return p
}

func TestDetectEngineConnectorCrashLoop(t *testing.T) {
	scheme := newScheme(t)
	const ns, name = "ns1", "cache"
	injectedBy := ns + "/" + name
	sidecar := adapterruntime.SubscriberContainerName
	clbo := func(n string) ctrState { return ctrState{n, crashLoopBackOffReason} }
	run := func(n string) ctrState { return ctrState{n, ""} }

	tests := []struct {
		desc    string
		pod     *corev1.Pod
		wantMsg bool
	}{
		{"injected engine in CrashLoopBackOff is flagged",
			enginePodWithContainers("e1", ns, matchedSelector, injectedBy, testBackendUID, clbo("vllm")), true},
		{"injected engine running is not flagged",
			enginePodWithContainers("e1", ns, matchedSelector, injectedBy, testBackendUID, run("vllm")), false},
		{"non-injected pod crash-looping is not ours to flag",
			enginePodWithContainers("e1", ns, matchedSelector, "", "", clbo("vllm")), false},
		{"injected-by with a mismatched UID is rejected (forgery / recreated CR)",
			enginePodWithContainers("e1", ns, matchedSelector, injectedBy, "stale-uid", clbo("vllm")), false},
		{"injected-by with no UID stamp is rejected",
			enginePodWithContainers("e1", ns, matchedSelector, injectedBy, "", clbo("vllm")), false},
		{"a crashing injected sidecar (engine healthy) is not the connector signature",
			enginePodWithContainers("e1", ns, matchedSelector, injectedBy, testBackendUID, run("vllm"), clbo(sidecar)), false},
		{"an unrelated crash-looping sidecar (mesh proxy) with a healthy engine is not flagged",
			enginePodWithContainers("e1", ns, matchedSelector, injectedBy, testBackendUID, run("vllm"), clbo("istio-proxy")), false},
		{"engine crash-looping with a healthy sidecar is flagged",
			enginePodWithContainers("e1", ns, matchedSelector, injectedBy, testBackendUID, clbo("vllm"), run(sidecar)), true},
		{"injected engine named other than the adapter default (subscriber appended) is still flagged",
			enginePodWithContainers("e1", ns, matchedSelector, injectedBy, testBackendUID, clbo("my-engine"), run(sidecar)), true},
		{"ImagePullBackOff is not the connector-incompatibility signature",
			enginePodWithContainers("e1", ns, matchedSelector, injectedBy, testBackendUID, ctrState{"vllm", "ImagePullBackOff"}), false},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			cb := lmcacheBackendWithSelector(name, ns, matchedSelector)
			cb.UID = testBackendUID
			r := newReconciler(scheme, cb, tc.pod)
			msg, observed := r.detectEngineConnectorCrashLoop(context.Background(), cb)
			if !observed {
				t.Fatalf("observed=false on a healthy fake client (the pod list should succeed)")
			}
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

// A backend with no injected pods is observed but clean (no condition).
func TestDetectEngineConnectorCrashLoopNoInjectedPods(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackendWithSelector("cache", "ns1", matchedSelector)
	cb.UID = testBackendUID
	r := newReconciler(scheme, cb) // no pods at all
	if msg, observed := r.detectEngineConnectorCrashLoop(context.Background(), cb); !observed || msg != "" {
		t.Fatalf("want observed=true, msg empty; got observed=%v msg=%q", observed, msg)
	}
}

// A pod-list failure must report observed=false (live state unknown) with an
// empty diagnostic, so the caller PRESERVES any existing EngineCompatibility
// condition rather than clearing it. (Preservation itself is asserted at the
// controller level in TestUpdateManagedStatusPreservesEngineCompatibilityOnListError.)
func TestDetectEngineConnectorCrashLoopListErrorIsUnobserved(t *testing.T) {
	scheme := newScheme(t)
	cb := lmcacheBackendWithSelector("cache", "ns1", matchedSelector)
	cb.UID = testBackendUID
	listErr := errors.New("synthetic pod-list failure")
	funcs := interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				return listErr
			}
			return c.List(ctx, list, opts...)
		},
	}
	// Seed the crash-looping pod so the ONLY reason the detector returns
	// nothing is the injected list error (not an empty cluster).
	pod := enginePodWithContainers("e1", "ns1", matchedSelector, "ns1/cache", testBackendUID,
		ctrState{"vllm", crashLoopBackOffReason})
	r := newReconcilerWithInterceptor(scheme, funcs, cb, pod)
	msg, observed := r.detectEngineConnectorCrashLoop(context.Background(), cb)
	if observed {
		t.Fatalf("observed=true on a pod-list failure; want observed=false (live state unknown)")
	}
	if msg != "" {
		t.Fatalf("diagnostic = %q on a pod-list failure; want empty", msg)
	}
}

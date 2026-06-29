package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
)

// crashLoopBackOffReason is the kubelet waiting reason for a container that has
// failed repeatedly and is now being backed off — the signature of a hard init
// failure, distinct from a transient first-boot restart.
const crashLoopBackOffReason = "CrashLoopBackOff"

// detectEngineConnectorCrashLoop returns an operator-facing diagnostic when an
// engine pod THIS backend injected cache config into is stuck in
// CrashLoopBackOff — the live signature of a structural engine↔connector
// incompatibility — and "" otherwise.
//
// The canonical cause is a hybrid-attention model (Qwen3.6/Next gated-DeltaNet,
// Mamba/Jamba, KDA, Falcon-H, Granite-hybrid, …): vLLM disables its hybrid
// KV-cache manager the moment ANY KV connector (LMCache, Mooncake, NIXL) is
// wired, then fails KV-spec unification at engine init. The engine crash-loops
// with zero operator-facing signal — the operator has to read engine logs to
// learn the CacheBackend config is structurally incompatible with the model.
// Surfacing it as a condition turns that silent failure loud; otherwise the
// KV-event readiness gate only reports a generic "no KV events observed" once
// firstEventTimeout expires, which does not name the cause.
//
// Best-effort and connector-agnostic: a missing selector or a pod-list error
// returns "" (no diagnostic, no churn). Only pods carrying THIS backend's
// inferencecache.io/injected-by stamp are considered — we flag crash-loops
// downstream of our own injection, never an operator's unrelated crash-loop.
func (r *CacheBackendReconciler) detectEngineConnectorCrashLoop(ctx context.Context, backend *cachev1alpha1.CacheBackend) string {
	sel := backend.Spec.EngineSelector
	if sel == nil || len(sel.MatchLabels) == 0 {
		return ""
	}
	reader := client.Reader(r.APIReader)
	if reader == nil {
		reader = r.Client
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods,
		client.InNamespace(backend.Namespace),
		client.MatchingLabelsSelector{Selector: labels.SelectorFromSet(sel.MatchLabels)},
	); err != nil {
		log.FromContext(ctx).V(1).Info("engine-compatibility check skipped: pod list failed",
			"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
		return ""
	}
	wantInjectedBy := backend.Namespace + "/" + backend.Name
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Annotations[podwebhook.AnnotationInjectedBy] != wantInjectedBy {
			continue
		}
		for j := range p.Status.ContainerStatuses {
			cs := &p.Status.ContainerStatuses[j]
			if cs.State.Waiting != nil && cs.State.Waiting.Reason == crashLoopBackOffReason {
				return fmt.Sprintf("container %q in engine pod %q is in CrashLoopBackOff after the cache plane injected a KV connector — "+
					"if this is a hybrid-attention model (Qwen3.6/Next gated-DeltaNet, Mamba/Jamba, KDA, Falcon-H, Granite-hybrid, …), "+
					"vLLM cannot run a KV connector alongside its hybrid KV-cache manager (KV-spec unification fails at init); remove "+
					"the connector or run the engine events-only via the inferencecache.io/skip-inject annotation until an events-only "+
					"integration mode ships.", cs.Name, p.Name)
			}
		}
	}
	return ""
}

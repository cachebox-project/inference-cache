package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	podwebhook "github.com/cachebox-project/inference-cache/internal/webhook/pod"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

// crashLoopBackOffReason is the kubelet waiting reason for a container that has
// failed repeatedly and is now being backed off — the signature of a hard init
// failure, distinct from a transient first-boot restart.
const crashLoopBackOffReason = "CrashLoopBackOff"

// detectEngineConnectorCrashLoop reports whether an engine pod THIS backend
// injected a KV connector into has an ENGINE container stuck in
// CrashLoopBackOff — the live signature of a structural engine↔connector
// incompatibility — and whether the engine pods could be observed at all.
//
// The canonical cause is a hybrid-attention model (Qwen3.6/Next gated-DeltaNet,
// Mamba/Jamba, KDA, Falcon-H, Granite-hybrid, …): vLLM disables its hybrid
// KV-cache manager the moment ANY KV connector (LMCache, Mooncake, NIXL) is
// wired, then fails KV-spec unification at engine init. The engine crash-loops
// with zero operator-facing signal — the operator has to read engine logs to
// learn the CacheBackend config is structurally incompatible. Surfacing it as a
// condition turns that silent failure loud (the KV-event readiness gate only
// reports a generic "no KV events observed" once firstEventTimeout expires,
// which does not name the cause).
//
// Returns (diagnostic, observed):
//   - observed == false — the pod list failed, so the live state is unknown;
//     the caller MUST preserve any existing EngineCompatibility condition rather
//     than clear it (clearing then re-asserting on the next success would flap
//     the Warning event and briefly hide an active incompatibility).
//   - observed == true, diagnostic != "" — an injected engine is crash-looping.
//   - observed == true, diagnostic == "" — observed, nothing incompatible.
//
// Pods are matched by the inferencecache.io/injected-by + injected-by-uid
// annotation PAIR, not the current spec.engineSelector: an already-injected pod
// keeps its connector after the selector is removed or its labels drift, so the
// sticky pods that can still crash-loop must be found by the wiring stamp.
// Requiring the matching UID rejects a forged annotation and a survivor of a
// deleted/recreated CR of the same name — the same hardening the restart
// cascade applies. Connector-agnostic; only our own injection's fallout.
func (r *CacheBackendReconciler) detectEngineConnectorCrashLoop(ctx context.Context, backend *cachev1alpha1.CacheBackend) (string, bool) {
	reader := client.Reader(r.APIReader)
	if reader == nil {
		reader = r.Client
	}
	var pods corev1.PodList
	if err := reader.List(ctx, &pods, client.InNamespace(backend.Namespace)); err != nil {
		log.FromContext(ctx).V(1).Info("engine-compatibility check skipped: pod list failed",
			"namespace", backend.Namespace, "name", backend.Name, "error", err.Error())
		return "", false
	}
	wantInjectedBy := backend.Namespace + "/" + backend.Name
	wantInjectedByUID := string(backend.UID)
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Annotations[podwebhook.AnnotationInjectedBy] != wantInjectedBy {
			continue
		}
		if wantInjectedByUID == "" || p.Annotations[podwebhook.AnnotationInjectedByUID] != wantInjectedByUID {
			continue
		}
		for j := range p.Status.ContainerStatuses {
			cs := &p.Status.ContainerStatuses[j]
			// Skip OUR injected sidecar: a crashing kvevent-subscriber is a
			// different failure (and a different fix) than the engine being
			// connector-incompatible. Only an engine container's crash-loop is
			// the hybrid-attention signature.
			if cs.Name == adapterruntime.SubscriberContainerName {
				continue
			}
			if cs.State.Waiting != nil && cs.State.Waiting.Reason == crashLoopBackOffReason {
				return fmt.Sprintf("container %q in engine pod %q is in CrashLoopBackOff after the cache plane injected a KV connector — "+
					"if this is a hybrid-attention model (Qwen3.6/Next gated-DeltaNet, Mamba/Jamba, KDA, Falcon-H, Granite-hybrid, …), "+
					"vLLM cannot run a KV connector alongside its hybrid KV-cache manager (KV-spec unification fails at init); remove "+
					"the connector or run the engine events-only via the inferencecache.io/skip-inject annotation until an events-only "+
					"integration mode ships.", cs.Name, p.Name), true
			}
		}
	}
	return "", true
}

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

// detectEngineConnectorCrashLoop reports whether the ENGINE container of a pod
// THIS backend injected a KV connector into is stuck in CrashLoopBackOff — the
// live OBSERVATION the EngineCompatibility condition surfaces, not a confirmed
// root cause — and whether the engine pods could be observed at all.
//
// A common cause is a hybrid-attention model (Qwen3.6/Next gated-DeltaNet,
// Mamba/Jamba, KDA, Falcon-H, Granite-hybrid, …): vLLM disables its hybrid
// KV-cache manager the moment ANY KV connector (LMCache, Mooncake, NIXL) is
// wired, then fails KV-spec unification at engine init. But a crash-loop is a
// generic kubelet state — a bad image, command, missing dependency/secret, or
// OOM produces the same signature — so the diagnostic hedges the cause and tells
// the operator to confirm via engine logs. Surfacing it as a condition turns an
// otherwise-silent crash-loop loud (the KV-event readiness gate only reports a
// generic "no KV events observed" once firstEventTimeout expires, which does not
// name a likely cause or point at the fix).
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
// cascade applies.
//
// The crash-loop check is scoped to the single ENGINE container the connector
// was injected into (the adapter-declared engine container name), NOT every
// container on the pod. A service-mesh proxy, a user logging sidecar, or our own
// kvevent-subscriber crash-looping is a different failure with a different fix —
// only the engine container's crash-loop is the connector-incompatibility
// signature. Connector-agnostic; only our own injection's fallout.
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
	engineContainer := r.engineContainerName(backend)
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Annotations[podwebhook.AnnotationInjectedBy] != wantInjectedBy {
			continue
		}
		if wantInjectedByUID == "" || p.Annotations[podwebhook.AnnotationInjectedByUID] != wantInjectedByUID {
			continue
		}
		cs := engineContainerStatus(p, engineContainer)
		if cs == nil {
			continue
		}
		if cs.State.Waiting != nil && cs.State.Waiting.Reason == crashLoopBackOffReason {
			return fmt.Sprintf("container %q in engine pod %q is in CrashLoopBackOff after the cache plane injected a KV connector. "+
				"A structural engine↔connector incompatibility is a common cause — hybrid-attention models (Qwen3.6/Next "+
				"gated-DeltaNet, Mamba/Jamba, KDA, Falcon-H, Granite-hybrid, …) disable vLLM's hybrid KV-cache manager the moment "+
				"a KV connector loads (KV-spec unification fails at init) — but a crash-loop can also be a bad image, command, "+
				"missing dependency/secret, or OOM, so check the engine logs to confirm. If it is the connector, remove it — note "+
				"the inferencecache.io/skip-inject annotation opts the pod out of cache wiring entirely (the "+
				"kvevent-subscriber included), so on its own it stops routing too and is not a routing-preserving workaround.", cs.Name, p.Name), true
		}
	}
	return "", true
}

// engineContainerName resolves the runtime adapter for the backend and returns
// the name of the engine container the adapter injects the KV connector into
// (e.g. "vllm"). Returns "" when no adapter wires this (runtime, backend) pair
// or the adapter declares no canonical engine container (the reference adapter)
// — in either case no connector is injected, so there is nothing to diagnose.
func (r *CacheBackendReconciler) engineContainerName(backend *cachev1alpha1.CacheBackend) string {
	registry := r.Registry
	if registry == nil {
		registry = adapterruntime.DefaultRegistry()
	}
	adapter, err := registry.Select(adapterruntime.ResolveRuntimeID(backend), backend)
	if err != nil {
		return ""
	}
	return adapter.EngineContainerName()
}

// engineContainerStatus returns the status of the engine container the connector
// was injected into, mirroring the webhook's overrideTargetIndex targeting:
// match by the adapter-declared engine container name, and — only when that name
// is set but absent from the pod — fall back to the sole container of a
// single-container pod. Returns nil when the engine container cannot be
// identified, so an unrelated crash-looping sidecar (service-mesh proxy, user
// logging sidecar, kvevent-subscriber) is never misread as the connector
// incompatibility signature.
func engineContainerStatus(pod *corev1.Pod, engineContainerName string) *corev1.ContainerStatus {
	if engineContainerName == "" {
		return nil
	}
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].Name == engineContainerName {
			return &pod.Status.ContainerStatuses[i]
		}
	}
	if len(pod.Spec.Containers) == 1 && len(pod.Status.ContainerStatuses) == 1 {
		return &pod.Status.ContainerStatuses[0]
	}
	return nil
}

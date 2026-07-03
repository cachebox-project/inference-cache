package runtime

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/adapters/runtime/internal/enginewire"
)

// This file holds the engine-agnostic LMCache rendering shared by every runtime
// adapter that fronts a managed LMCache backend — the vLLM+LMCache adapter
// (vllm_lmcache.go) and the SGLang+LMCache adapter (pkg/adapters/runtime/sglang)
// today. The standalone lmcache-server is engine-agnostic at the server level
// (engines connect via lm://<svc>:<port> regardless of engine family), and the
// kvevent-subscriber sidecar binary is one binary parameterised per engine, so
// both adapters render identical objects modulo the engine-specific knobs
// (container name, hash scheme, ZMQ port). Centralising the render keeps the
// two adapters from drifting; the engine-side wire differs and stays in each
// adapter / the internal enginewire package.

// LMCache standalone-server canonical defaults. These template a standalone
// LMCache server pod that engines connect to via LMCACHE_REMOTE_URL=lm://<svc>:<port>,
// matching the upstream "share KV across instances" topology
// (https://docs.lmcache.ai/getting_started/quickstart/share_kv_cache.html and
// the LMCache Dockerfile.standalone in
// https://github.com/LMCache/LMCache/tree/dev/docker). Defaults are overridable
// via CacheBackend.Spec.BackendConfig so a real deployment can pin to digests
// without a code change.
const (
	// defaultLMCacheServerImage is the upstream standalone LMCache server
	// image, pinned to a specific version rather than a floating :latest.
	//
	// Why pin off :latest: the lmcache-server and the lmcache *client*
	// compiled into the engine communicate over a versioned wire protocol. A
	// floating :latest can drift to a server build whose protocol no longer
	// matches the engine's client; the mismatch disables tier-2 offload
	// silently (stores fail, 0 hits, no surfaced error). Pinning removes that
	// silent-drift risk and makes renders reproducible.
	//
	// This is NOT auto-aligned with the engine's client: IC has no source of
	// truth for the engine image's lmcache client version (it is operator-
	// supplied or pip-installed at runtime). Operators MUST keep the version
	// here (or their backendConfig.serverImage override) wire-compatible with
	// their engine's lmcache client — see the "LMCache server / client version
	// alignment" section in docs/design/cachebackend-api.md.
	//
	// Overridable via backendConfig.serverImage (production should pin to a
	// digest there).
	//
	// TODO: wire-test and digest-pin before production. v0.4.7 is version-
	// aligned (it exists upstream and matches the lmcache 0.4.7 client used in
	// validation), but the standalone server image itself was not independently
	// wire-tested here — confirm against a tested build and prefer an @sha256:
	// digest. Do not substitute an invented digest.
	defaultLMCacheServerImage = "lmcache/standalone:v0.4.7"
	// defaultLMCacheServerPort is the canonical lm:// port the LMCache docs use
	// for the standalone server.
	defaultLMCacheServerPort = int32(65432)
	// defaultLMCacheServerHost is the bind address inside the pod.
	defaultLMCacheServerHost = "0.0.0.0"
	// defaultLMCacheServerStorage is the LMCache server storage device; "cpu"
	// (the default, in-memory) is the only widely-supported option today.
	defaultLMCacheServerStorage = "cpu"
	// defaultLMCacheServerPortName is the named container port other parts of
	// the system can address by name without hard-coding the integer.
	defaultLMCacheServerPortName = "lmcache"

	// BackendConfig override keys. Keep them short, kebab-free, JSON-friendly
	// since they round-trip through CacheBackend.Spec.BackendConfig (a
	// map[string]string).
	// cfgKeyServerImage is the BackendConfig key that overrides the
	// lmcache-server container image. The name is deliberately distinct from
	// the legacy "image" key (which addressed the all-in-one vLLM container the
	// previous reconciler rendered) so an existing CR carrying
	// `backendConfig.image: vllm/vllm-openai:...` does not silently render an
	// lmcache-server pod with the wrong image — the legacy key is now ignored
	// and the lmcache-server falls back to its default image.
	cfgKeyServerImage   = "serverImage"
	cfgKeyServerCommand = "serverCommand"
)

// Shared kvevent-subscriber sidecar defaults. Vendor-neutral; production should
// set the image to a digest-pinned reference and the policy-server address to
// the in-cluster Service DNS the operator's server actually exposes.
const (
	// SubscriberContainerName is the well-known name for the kvevent-subscriber
	// sidecar the LMCache-fronting adapters render. Webhook callers use it to
	// short-circuit re-admission (skip the append if a container by this name
	// is already present), and operators can `kubectl logs <pod> -c
	// kvevent-subscriber` without guessing.
	SubscriberContainerName = "kvevent-subscriber"
	// DefaultSubscriberImage is the well-known dev-tag the Makefile's
	// subscriber-image target emits; operators pass it (or a production-pinned
	// digest) to the controller's --kvevent-subscriber-image flag to enable
	// auto-attach.
	//
	// Auto-attach is opt-in by design: when no image is configured the adapter
	// returns no sidecar at all. A pulled-but-unavailable image would put the
	// sidecar container into ImagePullBackOff, which keeps the engine pod from
	// going Ready and would violate the "cache is an optimisation, never a
	// serving dependency" posture. Defaulting off keeps the default install
	// safe — operators turn auto-attach on when they're ready to ship a
	// subscriber image alongside the controller.
	DefaultSubscriberImage = "ghcr.io/cachebox-project/inference-cache-subscriber:dev"
	// DefaultPolicyServerGRPCAddress is the in-cluster Service DNS the
	// kvevent-subscriber sidecar dials by default. Mirrors the assumption the
	// controller's HTTP poller already makes about the policy server's
	// Deployment landing in the inference-cache-system namespace.
	DefaultPolicyServerGRPCAddress = "inference-cache-server.inference-cache-system.svc.cluster.local:9090"

	// modelBackendConfigKey is the BackendConfig key the adapters read the
	// served model id from when rendering the subscriber sidecar. Mirrors the
	// key the reconciler canary already writes (`backendConfig.model: <served
	// model>`); kept as a constant so a future rename ripples through one place.
	modelBackendConfigKey = "model"
)

// ResolveLMCacheServer renders the standalone LMCache server's container set and
// the Service's port set for a managed LMCache backend. The reconciler owns
// ObjectMeta, the Service Selector, the workload kind (Deployment vs
// StatefulSet), and owner references — all of which depend on the CacheBackend
// identity, not on the adapter. Returning only PodSpec.Containers /
// PodSpec.Volumes and Service.Spec.Ports / Service.Spec.Type keeps the seam
// clean: an adapter rendering identical containers for two CacheBackends in
// different namespaces never has to learn about names.
//
// Shared by the vLLM+LMCache and SGLang+LMCache adapters — the lmcache-server
// is engine-agnostic (the engine connects to lm://<svc>:<port> regardless of
// engine family), so the render does not depend on the runtime.
func ResolveLMCacheServer(cache *cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	if cache == nil {
		return nil, nil, fmt.Errorf("resolve cache server: cache is nil")
	}
	cfg := cache.Spec.BackendConfig
	image := enginewire.ConfigOr(cfg, cfgKeyServerImage, defaultLMCacheServerImage)

	command, args := serverCommand(cfg)
	container := corev1.Container{
		Name:            "lmcache-server",
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         command,
		Args:            args,
		Ports: []corev1.ContainerPort{
			{Name: defaultLMCacheServerPortName, ContainerPort: defaultLMCacheServerPort, Protocol: corev1.ProtocolTCP},
		},
		// A TCP-socket readiness probe on the lm:// port gates AvailableReplicas
		// (and therefore the CacheBackend's Ready condition, via managedReadiness)
		// on the LMCache server actually accepting connections — otherwise
		// status could flip Ready before the server is reachable.
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(defaultLMCacheServerPortName)},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
			FailureThreshold:    6,
		},
		// Container resources come from spec.resources (CRD-defaulted to a
		// 4Gi request / 8Gi memory limit so every CacheBackend is bounded
		// by the cgroup rather than OOM-killed under T2 write load). When
		// autoscaling is set, the helper additionally fills in a CPU
		// request fallback so a CPU-utilization HPA has a denominator —
		// never overwriting an operator-supplied CPU request.
		Resources: defaultServerResources(cache),
	}

	pod := &corev1.PodSpec{
		Containers: []corev1.Container{container},
	}

	svc := &corev1.Service{
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{
					Name:       defaultLMCacheServerPortName,
					Port:       defaultLMCacheServerPort,
					TargetPort: intstr.FromString(defaultLMCacheServerPortName),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
	return pod, svc, nil
}

// defaultServerResources resolves the Container.Resources block for the
// lmcache-server. spec.resources (CRD-defaulted to a 4Gi memory request /
// 8Gi memory limit) is the operator-owned baseline and is passed through
// verbatim. When spec.autoscaling is set, the helper additionally fills in
// a CPU request fallback (250m) so a CPU-utilization HPA has a denominator
// — the fallback never overwrites an operator-supplied CPU request. The
// returned ResourceRequirements is a fresh value so callers never alias
// into the CR's spec; mutating the result MUST NOT propagate back into the
// informer-cached object.
func defaultServerResources(cache *cachev1alpha1.CacheBackend) corev1.ResourceRequirements {
	var out corev1.ResourceRequirements
	if cache != nil && cache.Spec.Resources != nil {
		out = *cache.Spec.Resources.DeepCopy()
	}
	if cache == nil || cache.Spec.Autoscaling == nil {
		return out
	}
	// nil-safe init: spec.resources may have been omitted (or carried
	// only Limits), so Requests can be nil here even though we are
	// about to write into it for the HPA fallback below.
	if out.Requests == nil {
		out.Requests = corev1.ResourceList{}
	}
	// CPU-only fallback: the autoscaling spec drives a
	// targetCPUUtilizationPercent HPA, which needs a *positive* CPU
	// request as the denominator. The admission validator admits
	// requests.cpu: "0" (zero is a valid kubelet shape — "no
	// guaranteed minimum"), but with autoscaling it gives the HPA a
	// zero denominator and breaks utilization math; so we treat
	// present-but-non-positive identically to absent and replace it
	// with the fallback. A positive operator-supplied value survives
	// untouched.
	//
	// Memory is NOT auto-filled — spec.resources (carrying the
	// CRD-stamped memory default) is the canonical source for memory,
	// and synthesising a memory request here would override an
	// operator-supplied limits-only shape.
	cpu, hasCPU := out.Requests[corev1.ResourceCPU]
	if !hasCPU || cpu.Sign() <= 0 {
		out.Requests[corev1.ResourceCPU] = resource.MustParse("250m")
	}
	return out
}

// serverCommand returns the LMCache server command + args, with a single
// BackendConfig override hook (cfgKeyServerCommand) for users who want to
// switch to the newer `python3 -m lmcache.v1.multiprocess.server` form once
// it stabilises. The default targets the older `lmcache_server <host> <port>
// <storage>` form because it has a documented port (65432) and arg layout.
func serverCommand(cfg map[string]string) (command, args []string) {
	if raw := enginewire.ConfigOr(cfg, cfgKeyServerCommand, ""); raw != "" {
		fields := strings.Fields(raw)
		if len(fields) > 0 {
			return []string{fields[0]}, fields[1:]
		}
	}
	return []string{"lmcache_server"}, []string{
		defaultLMCacheServerHost,
		fmt.Sprintf("%d", defaultLMCacheServerPort),
		defaultLMCacheServerStorage,
	}
}

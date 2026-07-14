package runtime

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/adapters/runtime/internal/enginewire"
)

// Redis L2 render for the SGLang LMCache MP-mode data plane.
//
// SGLang drives LMCache in multiprocess (MP) mode: the engine attaches to a
// node-local MP worker, and the worker offloads its shared/cross-node tier to an
// `--l2-adapter`. Unlike the vLLM `lm://` path, `lm://` is not a valid MP
// `--l2-adapter` type, so the SGLang pair cannot reuse [ResolveLMCacheServer].
// Redis (the `resp` adapter) is the shared L2: a network-addressable store that
// fits the one-Service, engines-anywhere model exactly (a ClusterIP Service, no
// hostNetwork/mesh — the opposite of Mooncake), it maps onto one Deployment +
// Service, and it is proven end-to-end. The heavier tiers (`s3`, `mooncake_store`)
// are durability/bandwidth opt-ins for a future backendConfig knob, not the
// simple default. See docs/design/sglang-lmcache-mp-mode.md.
//
// This render is the shared-store half of the MP data plane; the engine-side wire
// (config-file + MP-worker sidecar pointed at this Redis) is injected by the
// (sglang, LMCache) adapter's engine path. It is factored out and unit-tested on
// its own so the store render is reviewed independently of the engine wire.
const (
	// defaultRedisImage is the upstream Redis image for the managed L2 store. A
	// major.minor-alpine tag is more stable than :7 / :latest but still mutable
	// within its patch line, so it is a sane default, NOT a reproducible pin:
	// production MUST pin an exact release or @sha256 digest via
	// backendConfig.redisImage. This redis:7 line is what validation exercised
	// against the pinned lmcache MP worker.
	defaultRedisImage = "docker.io/library/redis:7.4-alpine"
	// defaultRedisPort is the canonical Redis port the `resp` L2 adapter dials.
	defaultRedisPort = int32(6379)
	// defaultRedisPortName is the named container/service port so callers address
	// it by name without hard-coding the integer.
	defaultRedisPortName = "redis"

	// redisMaxmemoryDefaultBytes is the memory sizing assumed when spec.resources
	// carries none (pre-defaulting paths); the derived --maxmemory is a fraction
	// of it. Matches the CRD's 8Gi memory default.
	redisMaxmemoryDefaultBytes = int64(8) * 1024 * 1024 * 1024 // 8Gi

	// cfgKeyRedisImage overrides the Redis image (production should pin a digest).
	cfgKeyRedisImage = "redisImage"
)

// ResolveRedisL2Server renders the managed Redis L2 store's container set and the
// Service's port set for the SGLang LMCache MP-mode data plane, mirroring the seam
// [ResolveLMCacheServer] uses: the reconciler owns ObjectMeta, the Service
// Selector, the workload kind, and owner references (all CacheBackend-identity
// dependent), so this returns only PodSpec.Containers and Service.Spec
// Ports/Type.
//
// The Redis backend is a **singleton** cache: it holds no replicated state, so
// running more than one pod behind the Service would shard requests across
// independent key spaces and silently partition the L2. This render only
// describes the pod + service shape; the single-replica invariant MUST be
// enforced by the reconciler/admission (reject a multi-replica (sglang, LMCache)
// CacheBackend), added alongside the wiring that consumes this render — before it
// provisions anything.
//
// Security posture matches the lm:// lmcache-server this replaces: an
// unauthenticated, non-TLS ClusterIP holding KV blocks, trusted to the in-cluster
// network — any pod that can reach the Service can read, overwrite, or flush
// cached KV. Hardening (a NetworkPolicy scoping access to engine pods, Redis AUTH,
// or TLS) is a follow-up carried at the same posture as the existing server.
func ResolveRedisL2Server(cache *cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	if cache == nil {
		return nil, nil, fmt.Errorf("resolve redis L2: cache is nil")
	}
	cfg := cache.Spec.BackendConfig
	image := enginewire.ConfigOr(cfg, cfgKeyRedisImage, defaultRedisImage)

	container := corev1.Container{
		Name:            "redis-l2",
		Image:           image,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Command:         []string{"redis-server"},
		// --save "" + --appendonly no: the L2 is an ephemeral cache tier, not a
		// database — disable RDB/AOF persistence so a restart starts cold (the KV
		// is soft state; loss is a cache miss, never a wrong answer) and no PVC is
		// implied. --maxmemory + allkeys-lru bound the cache to the derived budget
		// so the cgroup does not OOM-kill it under write load.
		Args: []string{
			"--save", "",
			"--appendonly", "no",
			"--maxmemory", fmt.Sprintf("%d", redisMaxmemoryBytes(cache)),
			"--maxmemory-policy", "allkeys-lru",
		},
		Ports: []corev1.ContainerPort{
			{Name: defaultRedisPortName, ContainerPort: defaultRedisPort, Protocol: corev1.ProtocolTCP},
		},
		// TCP-socket readiness on :6379 gates AvailableReplicas (and the
		// CacheBackend Ready condition) on Redis actually accepting connections,
		// so status never flips Ready before the L2 is reachable.
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString(defaultRedisPortName)},
			},
			InitialDelaySeconds: 3,
			PeriodSeconds:       10,
			FailureThreshold:    6,
		},
		// Reuse the shared server-resources helper: spec.resources (CRD-defaulted)
		// is the operator-owned baseline, plus the CPU-request fallback when
		// autoscaling is set. The memory limit here is also what --maxmemory is
		// derived from, so the two stay consistent.
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
					Name:       defaultRedisPortName,
					Port:       defaultRedisPort,
					TargetPort: intstr.FromString(defaultRedisPortName),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
	return pod, svc, nil
}

// redisMaxmemoryBytes derives Redis's `--maxmemory` LRU budget as 80% of the pod's
// memory sizing: the memory limit, falling back to the request, then to
// redisMaxmemoryDefaultBytes when neither is set. Deriving it from the same
// spec.resources the container carries — and staying strictly below it (80%) —
// keeps the budget inside the cgroup with headroom, so LRU eviction reclaims space
// before the OOM killer ever fires.
func redisMaxmemoryBytes(cache *cachev1alpha1.CacheBackend) int64 {
	base := redisMaxmemoryDefaultBytes
	if cache != nil && cache.Spec.Resources != nil {
		res := cache.Spec.Resources
		if q, ok := res.Limits[corev1.ResourceMemory]; ok && q.Value() > 0 {
			base = q.Value()
		} else if q, ok := res.Requests[corev1.ResourceMemory]; ok && q.Value() > 0 {
			base = q.Value()
		}
	}
	// 80%, computed base - base/5: overflow-safe (base/5 and the subtraction cannot
	// overflow for a non-negative int64) and inherently positive for base >= 1, so
	// it never rounds to --maxmemory 0 (= unlimited in Redis) and needs no floor
	// that could itself exceed a small limit. 80% is <= base (strictly < for the
	// only realistic range, base >= 5 bytes), leaving cgroup headroom so LRU
	// eviction — not the OOM killer — reclaims space.
	return base - base/5
}

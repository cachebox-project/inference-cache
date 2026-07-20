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
	// defaultRedisImage is the upstream Redis image for the managed L2 store, and
	// is tracked in docs/reference-stack/VERSIONS.md alongside the other stack
	// pins. A major.minor-alpine tag is more stable than :7 / :latest but still
	// mutable within its patch line, so it is a sane default, NOT a reproducible
	// pin: production MUST pin an exact release or @sha256 digest via
	// backendConfig.redisImage, per the image-pin policy in
	// docs/design/sglang-lmcache-mp-mode.md. This redis:7 line is what validation
	// exercised against the pinned lmcache MP worker. Redis needs no lmcache
	// version alignment (the MP worker speaks RESP), so this pin moves
	// independently of the engine/lmcache tuple.
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
		// No Command: the official Redis image's ENTRYPOINT (docker-entrypoint.sh)
		// must run. When it sees `redis-server` as the first argument while running as
		// root, it chowns the data dir and re-execs Redis as the unprivileged `redis`
		// user (gosu). Setting Command would OVERRIDE that entrypoint and run Redis as
		// root. So `redis-server` leads Args instead: args replace the image CMD but
		// leave the ENTRYPOINT — and its privilege drop — intact.
		// --save "" + --appendonly no: the L2 is an ephemeral cache tier, not a
		// database — disable RDB/AOF persistence so a restart starts cold (the KV
		// is soft state; loss is a cache miss, never a wrong answer) and no PVC is
		// implied. --maxmemory + allkeys-lru bound the cache to the derived budget
		// so the cgroup does not OOM-kill it under write load. --protected-mode no:
		// the worker reaches Redis over the ClusterIP — a non-loopback client — so
		// protected mode (which would accept the TCP connection, passing the
		// readiness probe, but reject actual commands) must be off. This is rendered
		// explicitly rather than relying on the image's default, and is consistent
		// with the documented in-cluster-trust security posture.
		Args: []string{
			"redis-server",
			"--save", "",
			"--appendonly", "no",
			"--protected-mode", "no",
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

// redisMaxmemoryBytes derives Redis's `--maxmemory` LRU budget as ~80% of the pod's
// memory LIMIT, falling back to redisMaxmemoryDefaultBytes when no positive limit is
// set. It deliberately does NOT size off a memory request: a request is a scheduling
// floor, not a ceiling, so a request-only pod has no cgroup bound for the budget to
// stay under — basing an OOM-avoidance budget on it would be a false comfort. This
// matches the accepted design (docs/design/sglang-lmcache-mp-mode.md: `--maxmemory`
// "from the pod's memory limit (with headroom), falling back to an explicit bounded
// default"). Sizing off the limit and staying below it (80%) keeps the dataset budget
// inside the cgroup with headroom, so LRU eviction has room to reclaim space and
// REDUCES the risk of an OOM kill. It cannot eliminate it: `--maxmemory` bounds the
// dataset, not Redis's total RSS (fragmentation, client/output buffers, replication
// backlog all live outside it), so a pathological workload can still exceed the cgroup.
func redisMaxmemoryBytes(cache *cachev1alpha1.CacheBackend) int64 {
	base := redisMaxmemoryDefaultBytes
	if cache != nil && cache.Spec.Resources != nil {
		if q, ok := cache.Spec.Resources.Limits[corev1.ResourceMemory]; ok && q.Value() > 0 {
			base = q.Value()
		}
	}
	// ~80%, computed base - base/5. Integer division truncates base/5 DOWN, so the
	// result rounds slightly UP of exact 80% (exact only when base is divisible by 5;
	// e.g. base=173 → 139, vs 138.4 exact). A larger budget means marginally LESS
	// headroom, not more — negligible at realistic limits, and it never reaches base
	// (see below), so the direction is safe. Overflow-safe (base/5 and
	// the subtraction cannot overflow for a non-negative int64) and inherently
	// positive for base >= 1, so it never rounds to --maxmemory 0 (= unlimited in
	// Redis) and needs no floor that could itself exceed a small limit. The result is
	// <= base (strictly < for the only realistic range, base >= 5 bytes), leaving
	// cgroup headroom so LRU eviction has room to run before RSS reaches the limit.
	return base - base/5
}

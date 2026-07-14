package runtime

import (
	"strconv"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// argVal returns the value following flag in an args slice (two-arg form), or ""
// if flag is absent or trails with no value.
func argVal(args []string, flag string) (string, bool) {
	for i, a := range args {
		if a == flag {
			if i+1 < len(args) {
				return args[i+1], true
			}
			return "", true
		}
	}
	return "", false
}

func withMemory(cb *cachev1alpha1.CacheBackend, limit, request string) *cachev1alpha1.CacheBackend {
	rr := &corev1.ResourceRequirements{}
	if limit != "" {
		rr.Limits = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(limit)}
	}
	if request != "" {
		rr.Requests = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(request)}
	}
	cb.Spec.Resources = rr
	return cb
}

func TestResolveRedisL2Server(t *testing.T) {
	cb := newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "sglang")
	pod, svc, err := ResolveRedisL2Server(cb)
	if err != nil {
		t.Fatalf("ResolveRedisL2Server: %v", err)
	}
	if pod == nil || svc == nil {
		t.Fatalf("ResolveRedisL2Server returned nil pod or svc")
	}
	if len(pod.Containers) != 1 {
		t.Fatalf("want 1 container, got %d", len(pod.Containers))
	}
	c := pod.Containers[0]
	if c.Name != "redis-l2" {
		t.Errorf("container name = %q, want redis-l2", c.Name)
	}
	if c.Image != defaultRedisImage {
		t.Errorf("image = %q, want default %q", c.Image, defaultRedisImage)
	}
	if len(c.Command) != 1 || c.Command[0] != "redis-server" {
		t.Errorf("command = %v, want [redis-server]", c.Command)
	}
	// Ephemeral-cache posture: persistence off, bounded LRU.
	if v, ok := argVal(c.Args, "--save"); !ok || v != "" {
		t.Errorf("--save = %q (ok=%v), want persistence disabled (empty)", v, ok)
	}
	if v, ok := argVal(c.Args, "--appendonly"); !ok || v != "no" {
		t.Errorf("--appendonly = %q, want no", v)
	}
	if v, ok := argVal(c.Args, "--maxmemory-policy"); !ok || v != "allkeys-lru" {
		t.Errorf("--maxmemory-policy = %q, want allkeys-lru", v)
	}
	if v, ok := argVal(c.Args, "--maxmemory"); !ok {
		t.Errorf("--maxmemory not set")
	} else if n, perr := strconv.ParseInt(v, 10, 64); perr != nil || n <= 0 {
		t.Errorf("--maxmemory = %q, want a positive integer (0 = unlimited in Redis; else allkeys-lru is a no-op → OOM)", v)
	}
	// Port + readiness.
	if len(c.Ports) != 1 || c.Ports[0].ContainerPort != defaultRedisPort {
		t.Errorf("ports = %v, want one :%d", c.Ports, defaultRedisPort)
	}
	if c.ReadinessProbe == nil || c.ReadinessProbe.TCPSocket == nil {
		t.Fatalf("want a TCP-socket readiness probe on the redis port")
	}
	if got := c.ReadinessProbe.TCPSocket.Port.StrVal; got != defaultRedisPortName {
		t.Errorf("readiness probe port = %q, want %q", got, defaultRedisPortName)
	}
	// Service: adapter fills Type + Ports only.
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("service type = %q, want ClusterIP", svc.Spec.Type)
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != defaultRedisPort {
		t.Errorf("service ports = %v, want one :%d", svc.Spec.Ports, defaultRedisPort)
	}
}

func TestResolveRedisL2ServerImageOverride(t *testing.T) {
	cb := newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "sglang")
	cb.Spec.BackendConfig = map[string]string{cfgKeyRedisImage: "registry.example/redis@sha256:deadbeef"}
	pod, _, err := ResolveRedisL2Server(cb)
	if err != nil {
		t.Fatalf("ResolveRedisL2Server: %v", err)
	}
	if got := pod.Containers[0].Image; got != "registry.example/redis@sha256:deadbeef" {
		t.Errorf("image = %q, want the backendConfig override", got)
	}
}

func TestResolveRedisL2ServerMaxmemory(t *testing.T) {
	// 80% via base/10*8 (overflow-safe), 1Mi positivity floor. Precomputed:
	// 8Gi->6871947672, 4Gi->3435973832, 100Mi->83886080, 1 byte->1Mi floor.
	cases := []struct {
		name           string
		limit, request string
		wantMaxmemory  int64
	}{
		{"limit 8Gi -> 80%", "8Gi", "", 6871947672},
		{"limit 4Gi wins over request -> 80% of limit", "4Gi", "2Gi", 3435973832},
		{"request-only 4Gi -> 80% of request", "", "4Gi", 3435973832},
		{"no sizing -> 80% of 8Gi default", "", "", 6871947672},
		{"tiny limit 100Mi -> 80%, in-bounds (no over-limit floor)", "100Mi", "", 83886080},
		{"sub-byte limit -> 1Mi positivity floor, never 0/unlimited", "1", "", 1048576},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cb := newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "sglang")
			if tc.limit != "" || tc.request != "" {
				withMemory(cb, tc.limit, tc.request)
			}
			if got := redisMaxmemoryBytes(cb); got != tc.wantMaxmemory {
				t.Errorf("redisMaxmemoryBytes = %d, want %d", got, tc.wantMaxmemory)
			}
		})
	}
}

// TestResolveRedisL2ServerMaxmemoryFitsLimit asserts the rendered --maxmemory
// stays strictly below the container's memory limit (headroom for Redis overhead),
// so LRU eviction — not the OOM killer — reclaims space. Regression guard for the
// old 256Mi floor that could exceed a small limit.
func TestResolveRedisL2ServerMaxmemoryFitsLimit(t *testing.T) {
	for _, limit := range []string{"100Mi", "512Mi", "8Gi"} {
		t.Run(limit, func(t *testing.T) {
			cb := withMemory(newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "sglang"), limit, "")
			pod, _, err := ResolveRedisL2Server(cb)
			if err != nil {
				t.Fatalf("ResolveRedisL2Server: %v", err)
			}
			v, ok := argVal(pod.Containers[0].Args, "--maxmemory")
			if !ok {
				t.Fatalf("--maxmemory not set")
			}
			mm, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				t.Fatalf("--maxmemory = %q, not an integer: %v", v, err)
			}
			lq := resource.MustParse(limit)
			limitBytes := lq.Value()
			if mm >= limitBytes {
				t.Errorf("--maxmemory %d >= container memory limit %d (%s) — Redis OOMs before evicting", mm, limitBytes, limit)
			}
		})
	}
}

func TestResolveRedisL2ServerNilCache(t *testing.T) {
	if _, _, err := ResolveRedisL2Server(nil); err == nil {
		t.Fatalf("ResolveRedisL2Server(nil) = nil error, want an error")
	}
}

// TestResolveRedisL2ServerStaysClusterIP bounds the blast radius: the managed L2
// stays a plain in-cluster virtual IP — no hostNetwork, no NodePort — so it fits
// the engines-anywhere model (the whole reason Redis is the default L2, not the
// Mooncake mesh).
func TestResolveRedisL2ServerStaysClusterIP(t *testing.T) {
	pod, svc, err := ResolveRedisL2Server(newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "sglang"))
	if err != nil {
		t.Fatalf("ResolveRedisL2Server: %v", err)
	}
	if pod.HostNetwork {
		t.Errorf("pod.HostNetwork = true, want false (Redis L2 must not need host networking)")
	}
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Errorf("service type = %q, want ClusterIP", svc.Spec.Type)
	}
}

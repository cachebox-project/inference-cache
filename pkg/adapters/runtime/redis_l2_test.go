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
	if v, ok := argVal(c.Args, "--protected-mode"); !ok || v != "no" {
		t.Errorf("--protected-mode = %q, want no (Redis is reached over a non-loopback ClusterIP)", v)
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
	// Remaining intentional fields: pull policy, service target/protocol, probe timing.
	if c.ImagePullPolicy != corev1.PullIfNotPresent {
		t.Errorf("imagePullPolicy = %q, want IfNotPresent", c.ImagePullPolicy)
	}
	if p := svc.Spec.Ports[0]; p.TargetPort.StrVal != defaultRedisPortName || p.Protocol != corev1.ProtocolTCP {
		t.Errorf("service port target/proto = %v/%v, want %s/TCP", p.TargetPort, p.Protocol, defaultRedisPortName)
	}
	if rp := c.ReadinessProbe; rp.InitialDelaySeconds != 3 || rp.PeriodSeconds != 10 || rp.FailureThreshold != 6 {
		t.Errorf("readiness timing = %d/%d/%d, want 3/10/6", rp.InitialDelaySeconds, rp.PeriodSeconds, rp.FailureThreshold)
	}
}

func TestResolveRedisL2ServerResourceContract(t *testing.T) {
	// The renderer's resource contract, asserted on the surface its consumer uses
	// (the rendered container) rather than only on the shared helper: spec.resources
	// is the operator-owned baseline and passes through; autoscaling adds the
	// CPU-request fallback the HPA needs as a utilization denominator; and the
	// rendered resources must not ALIAS the CR — a caller mutating the pod it got
	// back would otherwise be writing into the CacheBackend's spec.
	t.Run("spec.resources passes through", func(t *testing.T) {
		cb := withMemory(newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "sglang"), "3Gi", "1Gi")
		pod, _, err := ResolveRedisL2Server(cb)
		if err != nil {
			t.Fatalf("ResolveRedisL2Server: %v", err)
		}
		got := pod.Containers[0].Resources
		if q := got.Limits[corev1.ResourceMemory]; q.String() != "3Gi" {
			t.Errorf("limits.memory = %q, want the operator's 3Gi", q.String())
		}
		if q := got.Requests[corev1.ResourceMemory]; q.String() != "1Gi" {
			t.Errorf("requests.memory = %q, want the operator's 1Gi", q.String())
		}
	})

	t.Run("autoscaling adds the CPU-request fallback", func(t *testing.T) {
		cb := withMemory(newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "sglang"), "3Gi", "1Gi")
		cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 3}
		pod, _, err := ResolveRedisL2Server(cb)
		if err != nil {
			t.Fatalf("ResolveRedisL2Server: %v", err)
		}
		cpu := pod.Containers[0].Resources.Requests[corev1.ResourceCPU]
		if cpu.IsZero() {
			t.Fatalf("requests.cpu is zero/absent under autoscaling — a targetCPUUtilization HPA would divide by zero: %+v", pod.Containers[0].Resources)
		}
	})

	t.Run("rendered resources do not alias the CR", func(t *testing.T) {
		cb := withMemory(newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "sglang"), "3Gi", "1Gi")
		pod, _, err := ResolveRedisL2Server(cb)
		if err != nil {
			t.Fatalf("ResolveRedisL2Server: %v", err)
		}
		// Mutate what the renderer handed back; the CR must be untouched.
		pod.Containers[0].Resources.Limits[corev1.ResourceMemory] = resource.MustParse("99Gi")
		if q := cb.Spec.Resources.Limits[corev1.ResourceMemory]; q.String() != "3Gi" {
			t.Fatalf("mutating the rendered pod wrote through to the CacheBackend: spec.resources.limits.memory = %q, want 3Gi", q.String())
		}
	})
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
	// 80% via base - base/5 (overflow-safe, inherently positive, no floor).
	// Precomputed: 8Gi->6871947674, 4Gi->3435973837, 100Mi->83886080, 1 byte->1.
	cases := []struct {
		name           string
		limit, request string
		wantMaxmemory  int64
	}{
		{"limit 8Gi -> 80%", "8Gi", "", 6871947674},
		{"limit 4Gi wins over request -> 80% of limit", "4Gi", "2Gi", 3435973837},
		// A request is a scheduling floor, not a ceiling, so it is NOT a sizing
		// source: a request-only block falls back to the bounded default, exactly as
		// a resource-less spec does. (Matches the design: limit-or-fixed-default.)
		{"request-only 4Gi -> ignored, 80% of 8Gi default", "", "4Gi", 6871947674},
		{"no sizing -> 80% of 8Gi default", "", "", 6871947674},
		{"limit 100Mi -> 80%, in-bounds", "100Mi", "", 83886080},
		{"one-byte limit -> stays positive (never 0/unlimited), never exceeds base", "1", "", 1},
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
// removed floor that could exceed a small limit. Covers realistic limits (>= a few
// MiB); the degenerate sub-5-byte range — where integer 80% cannot be both
// positive AND strictly below the limit, and a Redis pod cannot run anyway — is
// asserted (as equal-to-base, still positive) in the maxmemory table, not here.
func TestResolveRedisL2ServerMaxmemoryFitsLimit(t *testing.T) {
	cases := []struct{ name, limit, request string }{
		{"limit 100Mi", "100Mi", ""},
		{"limit 8Gi", "8Gi", ""},
		{"limit+request", "8Gi", "4Gi"},
		{"request-only (no limit)", "", "4Gi"},
		{"no resources", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cb := newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "sglang")
			if tc.limit != "" || tc.request != "" {
				withMemory(cb, tc.limit, tc.request)
			}
			pod, _, err := ResolveRedisL2Server(cb)
			if err != nil {
				t.Fatalf("ResolveRedisL2Server: %v", err)
			}
			c := pod.Containers[0]
			v, ok := argVal(c.Args, "--maxmemory")
			if !ok {
				t.Fatalf("--maxmemory not set")
			}
			mm, perr := strconv.ParseInt(v, 10, 64)
			if perr != nil || mm <= 0 {
				t.Fatalf("--maxmemory = %q, want a positive integer", v)
			}
			// When the RENDERED container carries a memory limit, --maxmemory must
			// stay strictly below it. Compare against the rendered limit (not the
			// test input) so this also catches drift between redisMaxmemoryBytes and
			// defaultServerResources. request-only / no-resources shapes carry no
			// limit — there is nothing to exceed, so only positivity is asserted.
			if lim, has := c.Resources.Limits[corev1.ResourceMemory]; has {
				if limBytes := lim.Value(); mm >= limBytes {
					t.Errorf("--maxmemory %d >= rendered memory limit %d — Redis OOMs before evicting", mm, limBytes)
				}
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

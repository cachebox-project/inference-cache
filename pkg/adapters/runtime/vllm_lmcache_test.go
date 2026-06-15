package runtime

import (
	"flag"
	"fmt"
	"io"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

func newLMCacheBackend(cfg map[string]string) *cachev1alpha1.CacheBackend {
	cb := newCacheBackend(cachev1alpha1.CacheBackendTypeLMCache, "vllm")
	cb.Spec.BackendConfig = cfg
	return cb
}

func TestVLLMLMCacheSupports(t *testing.T) {
	a := NewVLLMLMCacheAdapter()

	cases := []struct {
		name    string
		runtime RuntimeID
		cache   *cachev1alpha1.CacheBackend
		want    bool
	}{
		{"vllm+lmcache", RuntimeVLLM, newLMCacheBackend(nil), true},
		{"vllm+external", RuntimeVLLM, newCacheBackend(cachev1alpha1.CacheBackendTypeExternal, "vllm"), false},
		{"vllm+mooncake", RuntimeVLLM, newCacheBackend(cachev1alpha1.CacheBackendTypeMooncake, "vllm"), false},
		{"sglang+lmcache", RuntimeSGLang, newLMCacheBackend(nil), false},
		{"reference+lmcache", RuntimeReference, newLMCacheBackend(nil), false},
		{"nil cache", RuntimeVLLM, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := a.Supports(tc.runtime, tc.cache); got != tc.want {
				t.Fatalf("Supports(%q, %+v) = %v, want %v", tc.runtime, tc.cache, got, tc.want)
			}
		})
	}
}

// resolvePod unwraps ResolveCacheServer for tests that only assert on the
// rendered pod, failing on error or a nil result. Tests that need the Service
// or DataVolume call ResolveCacheServer directly.
func resolvePod(t *testing.T, a KVCacheRuntimeAdapter, cb *cachev1alpha1.CacheBackend) *corev1.PodSpec {
	t.Helper()
	resolved, err := a.ResolveCacheServer(cb)
	if err != nil {
		t.Fatalf("ResolveCacheServer: %v", err)
	}
	if resolved == nil || resolved.PodSpec == nil {
		t.Fatalf("ResolveCacheServer returned nil resolved/pod")
	}
	return resolved.PodSpec
}

func TestVLLMLMCacheResolveCacheServer(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)

	resolved, err := a.ResolveCacheServer(cb)
	if err != nil {
		t.Fatalf("ResolveCacheServer: %v", err)
	}
	if resolved == nil || resolved.PodSpec == nil || resolved.Service == nil {
		t.Fatalf("ResolveCacheServer returned nil resolved/pod/svc")
	}
	pod, svc := resolved.PodSpec, resolved.Service

	// An ephemeral backend (no spec.storage.pvc) declares no data volume and
	// keeps the in-memory "cpu" storage device — status quo preserved.
	if resolved.DataVolume != nil {
		t.Fatalf("DataVolume = %+v, want nil for an ephemeral backend", resolved.DataVolume)
	}

	if len(pod.Containers) != 1 {
		t.Fatalf("containers = %d, want 1", len(pod.Containers))
	}
	c := pod.Containers[0]
	if c.Name != "lmcache-server" {
		t.Fatalf("container name = %q, want lmcache-server", c.Name)
	}
	if c.Image != "lmcache/standalone:v0.4.7" {
		t.Fatalf("container image = %q, want lmcache/standalone:v0.4.7 default", c.Image)
	}
	if len(c.Command) != 1 || c.Command[0] != "lmcache_server" {
		t.Fatalf("command = %v, want [lmcache_server]", c.Command)
	}
	wantArgs := []string{"0.0.0.0", "65432", "cpu"}
	if len(c.Args) != len(wantArgs) {
		t.Fatalf("args = %v, want %v", c.Args, wantArgs)
	}
	for i, want := range wantArgs {
		if c.Args[i] != want {
			t.Fatalf("args[%d] = %q, want %q", i, c.Args[i], want)
		}
	}
	if len(c.Ports) != 1 || c.Ports[0].ContainerPort != 65432 {
		t.Fatalf("ports = %v, want a single 65432 port", c.Ports)
	}

	// Service spec: adapter fills Type + Ports only — ObjectMeta and Selector
	// are the reconciler's responsibility (see KVCacheRuntimeAdapter docs).
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Fatalf("svc.Spec.Type = %q, want ClusterIP", svc.Spec.Type)
	}
	if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Port != 65432 {
		t.Fatalf("svc.Spec.Ports = %v, want a single 65432 port", svc.Spec.Ports)
	}
	if svc.Spec.Selector != nil {
		t.Fatalf("svc.Spec.Selector = %v, want nil (reconciler owns the selector)", svc.Spec.Selector)
	}
	if svc.Name != "" || svc.Namespace != "" {
		t.Fatalf("svc ObjectMeta = %q/%q, want empty (reconciler owns ObjectMeta)", svc.Namespace, svc.Name)
	}
}

func TestVLLMLMCacheResolveCacheServerHasReadinessProbe(t *testing.T) {
	// Without a readiness probe on the lm:// port, AvailableReplicas (and
	// therefore the CacheBackend's Ready condition) can flip True before
	// the server is actually serving — making status optimistic. The
	// adapter must render a TCP probe targeting the named lmcache port so
	// Ready waits on the real accept loop.
	a := NewVLLMLMCacheAdapter()
	pod := resolvePod(t, a, newLMCacheBackend(nil))
	probe := pod.Containers[0].ReadinessProbe
	if probe == nil {
		t.Fatalf("ReadinessProbe is nil; want a TCP probe so Ready waits on the actual accept loop")
	}
	if probe.TCPSocket == nil {
		t.Fatalf("ReadinessProbe.TCPSocket is nil; want a TCP-socket probe")
	}
	if probe.TCPSocket.Port.StrVal != "lmcache" {
		t.Fatalf("probe targets %q, want named port \"lmcache\"", probe.TCPSocket.Port.StrVal)
	}
}

func TestVLLMLMCacheResolveCacheServerNoRequestsForRawNilResourcesNoAutoscaling(t *testing.T) {
	// Renderer baseline for the RAW-STRUCT path (no apiserver in the
	// loop): spec.resources is nil and spec.autoscaling is nil, so the
	// adapter renders zero Requests on the container. On a live cluster
	// the same minimal CacheBackend arrives at the reconciler with the
	// CRD-stamped memory default already applied to spec.resources — the
	// reconciler-against-real-apiserver behavior is asserted end-to-end
	// in TestIntegrationCacheBackendResources/DefaultStampsMemoryLimits…
	// This test pins the no-default-stamp invariant the unit-test path
	// relies on so future contributors don't accidentally inject defaults
	// in the renderer itself.
	a := NewVLLMLMCacheAdapter()
	pod := resolvePod(t, a, newLMCacheBackend(nil))
	if len(pod.Containers[0].Resources.Requests) != 0 {
		t.Fatalf("container Requests = %v, want empty when spec.resources is nil and autoscaling is unset (raw-struct path)", pod.Containers[0].Resources.Requests)
	}
}

func TestVLLMLMCacheResolveCacheServerHasCPURequestWhenAutoscaled(t *testing.T) {
	// A targetCPUUtilizationPercent HPA needs the pod's CPU request
	// as the utilization denominator, so without one the autoscaler
	// never gets a usable metric. The adapter must therefore declare
	// a CPU request on the lmcache-server container when spec.autoscaling
	// is set. Memory is NOT auto-filled — spec.resources is the
	// canonical source (and on the apiserver path the CRD-stamped
	// default carries it); synthesising a second memory request here
	// would silently override an operator-supplied limits-only shape.
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 3}
	pod := resolvePod(t, a, cb)
	reqs := pod.Containers[0].Resources.Requests
	cpu, hasCPU := reqs[corev1.ResourceCPU]
	if !hasCPU || cpu.IsZero() {
		t.Fatalf("container Resources.Requests missing a CPU request under autoscaling: %v", reqs)
	}
	if _, hasMem := reqs[corev1.ResourceMemory]; hasMem {
		t.Fatalf("container Resources.Requests[memory] = %v, want unset (memory is not auto-filled — spec.resources is the canonical source)", reqs[corev1.ResourceMemory])
	}
}

func TestVLLMLMCacheResolveCacheServerAutoscalingPreservesLimitsOnlyResources(t *testing.T) {
	// Operator-supplied limits-only spec.resources combined with
	// autoscaling MUST surface as: limits intact, requests carry only
	// the HPA CPU fallback (no synthesised memory request). The
	// previous behavior synthesised a 1Gi memory request whenever
	// memory was absent under autoscaling, which silently overrode
	// the operator's "limit-only" intent — that is the gap this
	// test pins shut.
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 3}
	cb.Spec.Resources = &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("8Gi"),
		},
	}
	pod := resolvePod(t, a, cb)
	got := pod.Containers[0].Resources

	wantLim := resource.MustParse("8Gi")
	if mem := got.Limits[corev1.ResourceMemory]; mem.Cmp(wantLim) != 0 {
		t.Fatalf("Limits[memory] = %v, want operator-supplied %v", mem.String(), wantLim.String())
	}
	if _, hasMem := got.Requests[corev1.ResourceMemory]; hasMem {
		t.Fatalf("Requests[memory] = %v, want unset (operator declared limits-only)", got.Requests[corev1.ResourceMemory])
	}
	if cpu, hasCPU := got.Requests[corev1.ResourceCPU]; !hasCPU || cpu.IsZero() {
		t.Fatalf("Requests[cpu] = %v, want HPA fallback", cpu)
	}
}

func TestVLLMLMCacheResolveCacheServerHonorsSpecResources(t *testing.T) {
	// spec.resources is the operator-owned knob for the lmcache-server
	// container's Resources. When set the adapter MUST pass it through
	// verbatim (modulo the autoscaling CPU fallback covered in a separate
	// test) — the CRD-schema default supplies memory limits to every
	// CacheBackend so the cache-server pod is bounded by the cgroup limit
	// rather than OOM-killed by the kubelet under T2 load.
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	cb.Spec.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("4Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("8Gi"),
		},
	}
	pod := resolvePod(t, a, cb)
	got := pod.Containers[0].Resources

	wantReqMem := resource.MustParse("4Gi")
	if mem := got.Requests[corev1.ResourceMemory]; mem.Cmp(wantReqMem) != 0 {
		t.Fatalf("Requests[memory] = %v, want %v", mem.String(), wantReqMem.String())
	}
	wantLimMem := resource.MustParse("8Gi")
	if mem := got.Limits[corev1.ResourceMemory]; mem.Cmp(wantLimMem) != 0 {
		t.Fatalf("Limits[memory] = %v, want %v", mem.String(), wantLimMem.String())
	}
	if _, ok := got.Requests[corev1.ResourceCPU]; ok {
		t.Fatalf("Requests[cpu] = %v, want unset (no autoscaling, operator did not opt in)", got.Requests[corev1.ResourceCPU])
	}
}

func TestVLLMLMCacheResolveCacheServerSpecResourcesNotMutated(t *testing.T) {
	// The adapter must not mutate the CacheBackend's spec.resources in
	// place — controllers reconcile against an informer-cached object,
	// and a write through the pointer would propagate back to every
	// subsequent reader on the same shared cache.
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 3}
	cb.Spec.Resources = &corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("8Gi"),
		},
	}
	_ = resolvePod(t, a, cb)

	if cb.Spec.Resources.Requests != nil {
		t.Fatalf("spec.resources.requests = %v, want nil (adapter mutated the spec)", cb.Spec.Resources.Requests)
	}
}

func TestVLLMLMCacheResolveCacheServerEmptySpecResourcesIsRespected(t *testing.T) {
	// An operator who explicitly supplies `spec.resources: {}` is
	// suppressing the CRD-default memory budget. The adapter MUST honor
	// the empty struct as "no Resources" rather than synthesising a
	// fallback — otherwise the documented suppress-the-default workflow
	// silently re-introduces limits the operator deliberately omitted.
	// (No autoscaling here either: the autoscaling-fallback test pins
	// the orthogonal HPA-CPU behavior.)
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	cb.Spec.Resources = &corev1.ResourceRequirements{}
	pod := resolvePod(t, a, cb)
	got := pod.Containers[0].Resources
	if len(got.Requests) != 0 {
		t.Fatalf("Requests = %v, want empty when spec.resources is {} (operator suppressed default)", got.Requests)
	}
	if len(got.Limits) != 0 {
		t.Fatalf("Limits = %v, want empty when spec.resources is {} (operator suppressed default)", got.Limits)
	}
}

func TestVLLMLMCacheResolveCacheServerAutoscalingFillsMissingCPU(t *testing.T) {
	// When spec.resources is set but omits a CPU request, autoscaling
	// must still get a CPU-request denominator filled in by the adapter
	// — otherwise the operator's memory-only spec.resources silently
	// breaks the HPA metric path. The adapter MUST NOT overwrite a
	// CPU request the operator did supply.
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 3}
	cb.Spec.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("4Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("8Gi"),
		},
	}
	pod := resolvePod(t, a, cb)
	reqs := pod.Containers[0].Resources.Requests
	cpu, hasCPU := reqs[corev1.ResourceCPU]
	if !hasCPU || cpu.IsZero() {
		t.Fatalf("Requests[cpu] = %v, want a non-zero CPU fallback for HPA", cpu)
	}
	// Operator-supplied memory must survive the autoscaling merge.
	wantMem := resource.MustParse("4Gi")
	if mem := reqs[corev1.ResourceMemory]; mem.Cmp(wantMem) != 0 {
		t.Fatalf("Requests[memory] = %v, want operator-supplied %v", mem.String(), wantMem.String())
	}
}

func TestVLLMLMCacheResolveCacheServerAutoscalingReplacesZeroCPU(t *testing.T) {
	// The admission webhook admits `requests.cpu: "0"` (zero is a
	// valid kubelet shape — explicit "no guaranteed minimum"), but
	// paired with autoscaling it gives the HPA a zero denominator
	// and breaks utilization math. The renderer's fallback contract
	// is "the HPA always has a usable CPU denominator under
	// autoscaling", so a zero (or otherwise non-positive)
	// operator-supplied CPU request MUST be replaced with the 250m
	// fallback at render time. A POSITIVE operator-supplied value
	// still survives — that case is pinned by
	// TestVLLMLMCacheResolveCacheServerAutoscalingRespectsOperatorCPU.
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 3}
	cb.Spec.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("0")},
	}
	pod := resolvePod(t, a, cb)
	cpu := pod.Containers[0].Resources.Requests[corev1.ResourceCPU]
	if cpu.Sign() <= 0 {
		t.Fatalf("Requests[cpu] = %v, want non-zero HPA fallback (operator wrote 0)", cpu)
	}
}

func TestVLLMLMCacheResolveCacheServerAutoscalingRespectsOperatorCPU(t *testing.T) {
	// If the operator already supplied a CPU request, the autoscaling
	// fallback MUST NOT overwrite it — the operator's value is
	// authoritative for HPA-utilization math, and a silent overwrite
	// would surprise users tuning the denominator.
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 3}
	cb.Spec.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("750m"),
		},
	}
	pod := resolvePod(t, a, cb)
	wantCPU := resource.MustParse("750m")
	if cpu := pod.Containers[0].Resources.Requests[corev1.ResourceCPU]; cpu.Cmp(wantCPU) != 0 {
		t.Fatalf("Requests[cpu] = %v, want operator-supplied %v", cpu.String(), wantCPU.String())
	}
}

func TestVLLMLMCacheResolveCacheServerImageOverride(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(map[string]string{"serverImage": "registry.example.com/lmcache:pinned"})

	pod := resolvePod(t, a, cb)
	if got := pod.Containers[0].Image; got != "registry.example.com/lmcache:pinned" {
		t.Fatalf("container image = %q, want overridden", got)
	}
}

func TestVLLMLMCacheResolveCacheServerCommandOverride(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(map[string]string{
		"serverCommand": "python3 -m lmcache.v1.multiprocess.server --cpu-buffer-size 60",
	})

	pod := resolvePod(t, a, cb)
	c := pod.Containers[0]
	if len(c.Command) != 1 || c.Command[0] != "python3" {
		t.Fatalf("command = %v, want [python3]", c.Command)
	}
	wantArgs := []string{"-m", "lmcache.v1.multiprocess.server", "--cpu-buffer-size", "60"}
	if len(c.Args) != len(wantArgs) {
		t.Fatalf("args = %v, want %v", c.Args, wantArgs)
	}
	for i, want := range wantArgs {
		if c.Args[i] != want {
			t.Fatalf("args[%d] = %q, want %q", i, c.Args[i], want)
		}
	}
}

func TestVLLMLMCacheResolveCacheServerNilCache(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	if _, err := a.ResolveCacheServer(nil); err == nil {
		t.Fatalf("ResolveCacheServer(nil) returned no error")
	}
}

func TestVLLMLMCacheResolveCacheServerPersistentDeclaresDataVolume(t *testing.T) {
	// When spec.storage.pvc is set, the adapter declares a DataVolume so the
	// reconciler can provision + mount a PVC. The adapter declares (name +
	// mount path) but does NOT add the volume/mount itself — the reconciler
	// owns the PVC name and the merge.
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	cb.Spec.Storage = &cachev1alpha1.CacheBackendStorageSpec{PVC: &cachev1alpha1.CacheBackendPVCSpec{}}

	resolved, err := a.ResolveCacheServer(cb)
	if err != nil {
		t.Fatalf("ResolveCacheServer: %v", err)
	}
	if resolved.DataVolume == nil {
		t.Fatalf("DataVolume = nil, want non-nil when spec.storage.pvc is set")
	}
	if resolved.DataVolume.VolumeName == "" {
		t.Fatalf("DataVolume.VolumeName is empty; want a stable volume name")
	}
	if !strings.HasPrefix(resolved.DataVolume.MountPath, "/") {
		t.Fatalf("DataVolume.MountPath = %q, want an absolute in-container path", resolved.DataVolume.MountPath)
	}
	if len(resolved.PodSpec.Volumes) != 0 {
		t.Fatalf("adapter rendered pod volumes %v; the reconciler owns volume mounting", resolved.PodSpec.Volumes)
	}

	// Deferred device-switch invariant: the lmcache-server command is unchanged
	// when persistence is requested — the storage device stays "cpu" (positional
	// arg 3 of the legacy command). Switching it to a disk-backed device that
	// spills to the mount path is a separate follow-up. Assert the FULL arg set
	// (not just arg[2] when len==3) so a reshape or drop of the args fails this
	// canary instead of silently passing.
	c := resolved.PodSpec.Containers[0]
	wantArgs := []string{"0.0.0.0", "65432", "cpu"}
	if len(c.Args) != len(wantArgs) {
		t.Fatalf("server args = %v, want %v (storage device must stay cpu; disk-backed device is a separate follow-up)", c.Args, wantArgs)
	}
	for i, w := range wantArgs {
		if c.Args[i] != w {
			t.Fatalf("server args[%d] = %q, want %q (disk-backed device is a separate follow-up)", i, c.Args[i], w)
		}
	}
}

func TestVLLMLMCacheInjectEngineConfig(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	pod := &corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name: EngineContainerName,
				Args: []string{"--enable-prefix-caching", "--max-model-len", "8192"},
				Env: []corev1.EnvVar{
					{Name: "HF_TOKEN", Value: "secret-token"},
				},
			},
			{
				Name: "sidecar",
				Env:  []corev1.EnvVar{{Name: "SIDECAR_VAR", Value: "untouched"}},
			},
		},
	}

	if err := a.InjectEngineConfig(pod, "cache.ns1.svc.cluster.local:65432", cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}

	engine := pod.Containers[0]
	url, ok := lookupEnv(engine.Env, EnvLMCacheRemoteURL)
	if !ok || url != "lm://cache.ns1.svc.cluster.local:65432" {
		t.Fatalf("%s = (%q, %v), want lm://cache.ns1.svc.cluster.local:65432", EnvLMCacheRemoteURL, url, ok)
	}
	if v, _ := lookupEnv(engine.Env, EnvLMCacheRemoteSerde); v != "naive" {
		t.Fatalf("%s = %q, want naive (CPU-safe default)", EnvLMCacheRemoteSerde, v)
	}
	if v, _ := lookupEnv(engine.Env, EnvLMCacheChunkSize); v != "256" {
		t.Fatalf("%s = %q, want 256", EnvLMCacheChunkSize, v)
	}
	if v, _ := lookupEnv(engine.Env, EnvLMCacheLocalCPU); v != "False" {
		t.Fatalf("%s = %q, want False (remote-only by default)", EnvLMCacheLocalCPU, v)
	}
	if v, _ := lookupEnv(engine.Env, EnvVLLMUseV1); v != "1" {
		t.Fatalf("%s = %q, want 1", EnvVLLMUseV1, v)
	}
	// Existing env on the engine container is preserved.
	if v, _ := lookupEnv(engine.Env, "HF_TOKEN"); v != "secret-token" {
		t.Fatalf("HF_TOKEN was clobbered: got %q, want secret-token", v)
	}
	// Existing args are preserved + the connector arg pair is appended.
	if !containsArg(engine.Args, "--enable-prefix-caching") {
		t.Fatalf("--enable-prefix-caching was dropped: %v", engine.Args)
	}
	wantTransfer := kvTransferConfig(cachev1alpha1.CacheBackendIntegrationRoleReadWrite)
	if !containsArgPair(engine.Args, defaultEngineKVTransferConfigArg, wantTransfer) {
		t.Fatalf("connector args missing %s %s: %v", defaultEngineKVTransferConfigArg, wantTransfer, engine.Args)
	}

	// Sidecar is not the engine container, so it should be untouched.
	sidecar := pod.Containers[1]
	if _, ok := lookupEnv(sidecar.Env, EnvLMCacheRemoteURL); ok {
		t.Fatalf("sidecar got LMCache env injected; the adapter should target only the engine container")
	}
	if v, _ := lookupEnv(sidecar.Env, "SIDECAR_VAR"); v != "untouched" {
		t.Fatalf("SIDECAR_VAR was clobbered: got %q", v)
	}
}

func TestVLLMLMCacheInjectEngineConfigSingleContainerPodAcceptsAnyName(t *testing.T) {
	// A pod with exactly one container is accepted as the engine even when
	// the container is not named "vllm" — there's no sidecar to crash.
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "engine"}}}

	if err := a.InjectEngineConfig(pod, "cache.ns1.svc.cluster.local:65432", cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	if _, ok := lookupEnv(pod.Containers[0].Env, EnvLMCacheRemoteURL); !ok {
		t.Fatalf("single-container pod missing %s; should have been treated as the engine", EnvLMCacheRemoteURL)
	}
}

func TestVLLMLMCacheInjectEngineConfigMultiContainerWithoutVLLMNameErrors(t *testing.T) {
	// A multi-container pod with no container named "vllm" must be
	// rejected: blindly mutating every container would inject vLLM-only
	// flags onto sidecars and crash them.
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "engine", Env: []corev1.EnvVar{{Name: "EXISTING", Value: "x"}}},
		{Name: "sidecar", Env: []corev1.EnvVar{{Name: "SIDECAR_VAR", Value: "untouched"}}},
	}}

	err := a.InjectEngineConfig(pod, "cache.ns1.svc.cluster.local:65432", cb)
	if err == nil {
		t.Fatalf("expected an error for multi-container pod without a vllm-named container")
	}
	// Containers must come back untouched — no partial-mutation footprint.
	for _, c := range pod.Containers {
		if _, ok := lookupEnv(c.Env, EnvLMCacheRemoteURL); ok {
			t.Fatalf("container %q got %s injected before the error: %v", c.Name, EnvLMCacheRemoteURL, c.Env)
		}
	}
}

func TestVLLMLMCacheInjectEngineConfigIdempotent(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: EngineContainerName}}}

	if err := a.InjectEngineConfig(pod, "first.svc:65432", cb); err != nil {
		t.Fatalf("first InjectEngineConfig: %v", err)
	}
	if err := a.InjectEngineConfig(pod, "second.svc:65432", cb); err != nil {
		t.Fatalf("second InjectEngineConfig: %v", err)
	}

	envs := pod.Containers[0].Env
	urlMatches := 0
	for _, e := range envs {
		if e.Name == EnvLMCacheRemoteURL {
			urlMatches++
			if e.Value != "lm://second.svc:65432" {
				t.Fatalf("idempotent inject did not update value: got %q", e.Value)
			}
		}
	}
	if urlMatches != 1 {
		t.Fatalf("expected exactly 1 %s entry after second inject, got %d", EnvLMCacheRemoteURL, urlMatches)
	}

	// Args: the connector arg pair must appear exactly once.
	wantTransfer := kvTransferConfig(cachev1alpha1.CacheBackendIntegrationRoleReadWrite)
	flagCount := 0
	valueCount := 0
	for _, a := range pod.Containers[0].Args {
		if a == defaultEngineKVTransferConfigArg {
			flagCount++
		}
		if a == wantTransfer {
			valueCount++
		}
	}
	if flagCount != 1 || valueCount != 1 {
		t.Fatalf("connector arg pair count = (flag %d, value %d), want (1, 1)", flagCount, valueCount)
	}
}

func TestVLLMLMCacheInjectEngineConfigFailOpen(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	trueVal, falseVal := true, false
	cases := []struct {
		name     string
		failOpen *bool
		want     string
	}{
		{"default (unset → true)", nil, "true"},
		{"explicit true", &trueVal, "true"},
		{"explicit false", &falseVal, "false"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cb := newLMCacheBackend(nil)
			cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
				Engine:   "vllm",
				FailOpen: tc.failOpen,
			}
			pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: EngineContainerName}}}
			if err := a.InjectEngineConfig(pod, "x.svc:65432", cb); err != nil {
				t.Fatalf("InjectEngineConfig: %v", err)
			}
			if v, _ := lookupEnv(pod.Containers[0].Env, EnvInferenceCacheFailOpen); v != tc.want {
				t.Fatalf("%s = %q, want %q", EnvInferenceCacheFailOpen, v, tc.want)
			}
		})
	}
}

func TestVLLMLMCacheInjectEngineConfigRoleMapping(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cases := []struct {
		role        cachev1alpha1.CacheBackendIntegrationRole
		wantKVRole  string
		description string
	}{
		{cachev1alpha1.CacheBackendIntegrationRoleReadOnly, "kv_consumer", "ReadOnly → kv_consumer"},
		{cachev1alpha1.CacheBackendIntegrationRoleWriteOnly, "kv_producer", "WriteOnly → kv_producer"},
		{cachev1alpha1.CacheBackendIntegrationRoleReadWrite, "kv_both", "ReadWrite → kv_both"},
		{"", "kv_both", "unset → kv_both (default)"},
	}
	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			cb := newLMCacheBackend(nil)
			cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
				Engine: "vllm",
				Role:   tc.role,
			}
			pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: EngineContainerName}}}
			if err := a.InjectEngineConfig(pod, "x.svc:65432", cb); err != nil {
				t.Fatalf("InjectEngineConfig: %v", err)
			}
			wantValue := fmt.Sprintf(`{"kv_connector":"LMCacheConnectorV1","kv_role":%q}`, tc.wantKVRole)
			if !containsArgPair(pod.Containers[0].Args, defaultEngineKVTransferConfigArg, wantValue) {
				t.Fatalf("Args = %v, want pair (%s, %s)", pod.Containers[0].Args, defaultEngineKVTransferConfigArg, wantValue)
			}
		})
	}
}

func TestVLLMLMCacheInjectEngineConfigConfigOverrides(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(map[string]string{
		"chunkSize":   "512",
		"remoteSerde": "cachegen",
		"localCPU":    "True",
		"maxLocalCPU": "40",
	})
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: EngineContainerName}}}
	if err := a.InjectEngineConfig(pod, "x.svc:65432", cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	checks := map[string]string{
		EnvLMCacheChunkSize:   "512",
		EnvLMCacheRemoteSerde: "cachegen",
		EnvLMCacheLocalCPU:    "True",
		EnvLMCacheMaxLocalCPU: "40",
	}
	for name, want := range checks {
		if v, _ := lookupEnv(pod.Containers[0].Env, name); v != want {
			t.Fatalf("%s = %q, want %q (BackendConfig override)", name, v, want)
		}
	}
}

func TestVLLMLMCacheInjectEngineConfigPassesThroughLMScheme(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: EngineContainerName}}}
	// A caller that already prefixed lm:// must not produce lm://lm://.
	if err := a.InjectEngineConfig(pod, "lm://already.scheme:65432", cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	url, _ := lookupEnv(pod.Containers[0].Env, EnvLMCacheRemoteURL)
	if url != "lm://already.scheme:65432" {
		t.Fatalf("%s = %q, want pass-through (no double prefix)", EnvLMCacheRemoteURL, url)
	}
	if strings.HasPrefix(url, "lm://lm://") {
		t.Fatalf("double lm:// prefix in %q", url)
	}
}

func TestVLLMLMCacheInjectEngineConfigBadInput(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	good := &corev1.PodSpec{Containers: []corev1.Container{{Name: EngineContainerName}}}
	cases := []struct {
		name string
		fn   func() error
	}{
		{"nil pod", func() error { return a.InjectEngineConfig(nil, "x.svc:65432", cb) }},
		{"nil cache", func() error { return a.InjectEngineConfig(good, "x.svc:65432", nil) }},
		{"empty endpoint", func() error { return a.InjectEngineConfig(good, "", cb) }},
		{"no containers", func() error { return a.InjectEngineConfig(&corev1.PodSpec{}, "x.svc:65432", cb) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.fn(); err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

func TestVLLMLMCacheInjectRouterConfigIsNoop(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "router", Env: []corev1.EnvVar{{Name: "EXISTING", Value: "x"}}}}}
	if err := a.InjectRouterConfig(pod, "x.svc:65432", cb); err != nil {
		t.Fatalf("InjectRouterConfig: %v", err)
	}
	// LMCache has no router; the pod must come back untouched (existing env kept,
	// no LMCache env added).
	if len(pod.Containers[0].Env) != 1 || pod.Containers[0].Env[0].Name != "EXISTING" {
		t.Fatalf("InjectRouterConfig modified container env: %v", pod.Containers[0].Env)
	}
}

func TestVLLMLMCacheInjectRouterConfigTrulyNoopsOnBadInput(t *testing.T) {
	// The KVCacheRuntimeAdapter contract says backends without a router
	// component should return nil without touching pod. The LMCache adapter
	// must honour that even for nil/empty inputs so callers can blindly
	// invoke InjectRouterConfig on every adapter without branching.
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	good := &corev1.PodSpec{Containers: []corev1.Container{{Name: "router"}}}
	cases := []struct {
		name string
		fn   func() error
	}{
		{"nil pod", func() error { return a.InjectRouterConfig(nil, "x", cb) }},
		{"nil cache", func() error { return a.InjectRouterConfig(good, "x", nil) }},
		{"empty endpoint", func() error { return a.InjectRouterConfig(good, "", cb) }},
		{"no containers", func() error { return a.InjectRouterConfig(&corev1.PodSpec{}, "x", cb) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.fn(); err != nil {
				t.Fatalf("InjectRouterConfig %s returned %v, want nil (router-less backend is a no-op)", tc.name, err)
			}
		})
	}
}

// TestVLLMLMCacheReservedArgs pins the reserved-arg list — the args the
// validating webhook will hard-reject from spec.integration.engineOverrides.
// Reservation IS the contract: changing what's reserved without also
// adjusting the documented override surface in docs/design/cachebackend-api.md
// is a contract change, so this test is intentionally exact.
func TestVLLMLMCacheReservedArgs(t *testing.T) {
	got := vllmLMCacheAdapter{}.ReservedArgs()
	want := []string{defaultEngineKVTransferConfigArg}
	if !equalStrSlice(got, want) {
		t.Fatalf("ReservedArgs = %v, want %v", got, want)
	}
}

// TestVLLMLMCacheReservedEnv pins the reserved-env list. Three env names
// are reserved here (the integration strictly requires them): the resolved
// remote URL, the v1-codepath selector, and the spec.integration.failOpen
// mirror. Known tunables (LMCACHE_CHUNK_SIZE / LMCACHE_REMOTE_SERDE /
// LMCACHE_LOCAL_CPU / LMCACHE_MAX_LOCAL_CPU_SIZE) are NOT reserved, and the
// test also asserts they are absent. (--kv-transfer-config is reserved on
// the args side, covered by TestVLLMLMCacheReservedArgs.)
func TestVLLMLMCacheReservedEnv(t *testing.T) {
	got := vllmLMCacheAdapter{}.ReservedEnv()
	want := []string{EnvLMCacheRemoteURL, EnvVLLMUseV1, EnvInferenceCacheFailOpen}
	if !equalStrSlice(got, want) {
		t.Fatalf("ReservedEnv = %v, want %v", got, want)
	}
	// Negative-control: documented tunables MUST NOT appear in the
	// reserved set, or admission would block legitimate operator overrides
	// the design explicitly supports.
	tunable := map[string]bool{
		EnvLMCacheChunkSize:   true,
		EnvLMCacheRemoteSerde: true,
		EnvLMCacheLocalCPU:    true,
		EnvLMCacheMaxLocalCPU: true,
	}
	for _, name := range got {
		if tunable[name] {
			t.Errorf("env %q is documented as tunable and MUST NOT be reserved", name)
		}
	}
}

// TestVLLMLMCacheEngineContainerName confirms the adapter exposes its
// canonical container name to the pod webhook so the override merge lands on
// the same container [InjectEngineConfig] modified.
func TestVLLMLMCacheEngineContainerName(t *testing.T) {
	if got := (vllmLMCacheAdapter{}).EngineContainerName(); got != EngineContainerName {
		t.Fatalf("EngineContainerName = %q, want %q", got, EngineContainerName)
	}
}

func TestDefaultRegistryResolvesVLLMLMCache(t *testing.T) {
	r := DefaultRegistry()
	if r.Len() == 0 {
		t.Fatalf("DefaultRegistry has no adapters")
	}
	got, err := r.Select(RuntimeVLLM, newLMCacheBackend(nil))
	if err != nil {
		t.Fatalf("Select(vllm, LMCache): %v", err)
	}
	if _, ok := got.(vllmLMCacheAdapter); !ok {
		t.Fatalf("Select returned %T, want vllmLMCacheAdapter", got)
	}
}

func TestUpsertArgPairAppendsAndReplaces(t *testing.T) {
	// Append when missing.
	got := upsertArgPair([]string{"--keep"}, "--flag", "v1")
	want := []string{"--keep", "--flag", "v1"}
	if !equalStrSlice(got, want) {
		t.Fatalf("upsertArgPair append = %v, want %v", got, want)
	}
	// Replace value when flag already present (two-arg form).
	got = upsertArgPair([]string{"--flag", "old", "--other"}, "--flag", "new")
	want = []string{"--flag", "new", "--other"}
	if !equalStrSlice(got, want) {
		t.Fatalf("upsertArgPair replace = %v, want %v", got, want)
	}
	// Trailing flag with no value: append the value.
	got = upsertArgPair([]string{"--flag"}, "--flag", "v")
	want = []string{"--flag", "v"}
	if !equalStrSlice(got, want) {
		t.Fatalf("upsertArgPair trailing-flag = %v, want %v", got, want)
	}
	// Equals form: a single `--flag=old` entry must be replaced in place
	// with the two-arg form, not have a second `--flag new` appended.
	got = upsertArgPair([]string{"--flag=old", "--other"}, "--flag", "new")
	want = []string{"--flag", "new", "--other"}
	if !equalStrSlice(got, want) {
		t.Fatalf("upsertArgPair equals-form replace = %v, want %v", got, want)
	}
	// Idempotence across forms: the equals form gets normalised to the
	// two-arg form, and a second upsert collapses to a single entry.
	got = upsertArgPair([]string{"--flag=v1"}, "--flag", "v1")
	got = upsertArgPair(got, "--flag", "v2")
	want = []string{"--flag", "v2"}
	if !equalStrSlice(got, want) {
		t.Fatalf("upsertArgPair equals-then-two-arg idempotence = %v, want %v", got, want)
	}
}

func TestVLLMLMCacheObservationSidecarShape(t *testing.T) {
	// Auto-attach is opt-in: the operator passes the subscriber image via
	// the controller flag. WithSubscriberImage here mirrors the production
	// wiring. Without it ObservationSidecar would return nil (see
	// TestVLLMLMCacheObservationSidecarSkipsWithoutImage).
	a := NewVLLMLMCacheAdapter(WithSubscriberImage(DefaultSubscriberImage))
	cb := newLMCacheBackend(map[string]string{"model": "Qwen/Qwen2.5-0.5B-Instruct"})
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "engine-a", Namespace: "engines"},
	}

	c, err := a.ObservationSidecar(cb, pod)
	if err != nil {
		t.Fatalf("ObservationSidecar: %v", err)
	}
	if c == nil {
		t.Fatalf("ObservationSidecar returned nil for vLLM+LMCache with a model + image set")
	}
	if c.Name != SubscriberContainerName {
		t.Fatalf("container name = %q, want %q", c.Name, SubscriberContainerName)
	}
	if c.Image != DefaultSubscriberImage {
		t.Fatalf("container image = %q, want %q", c.Image, DefaultSubscriberImage)
	}
	// Downward-API env vars carry the pod's name/namespace at start time —
	// vital because pod.Name is empty at admission for generateName pods.
	if !envHasFieldRef(c.Env, "POD_NAME", "metadata.name") {
		t.Fatalf("env missing POD_NAME via downward API: %v", c.Env)
	}
	if !envHasFieldRef(c.Env, "POD_NAMESPACE", "metadata.namespace") {
		t.Fatalf("env missing POD_NAMESPACE via downward API: %v", c.Env)
	}
	wantArgFragments := []string{
		"--engine-endpoint=tcp://127.0.0.1:5557",
		"--server=" + DefaultPolicyServerGRPCAddress,
		"--replica-id=$(POD_NAME)",
		"--tenant-id=$(POD_NAMESPACE)",
		"--model-id=Qwen/Qwen2.5-0.5B-Instruct",
		"--hash-scheme=vllm",
		// Required for vLLM+LMCache: LMCache is an L2 tier that retains
		// blocks after the engine evicts them from GPU. Forwarding vLLM's
		// per-block BlockRemoved as PREFIX_EVICTED would drop a routing
		// hint the replica can still cheaply serve from L2 — the
		// cache-stress 0-PREFIX_MATCH regression. Pinning the arg here
		// keeps a future adapter edit from silently re-enabling the
		// eviction forward and re-introducing the bug.
		"--ignore-block-removed=true",
	}
	for _, want := range wantArgFragments {
		if !containsArg(c.Args, want) {
			t.Fatalf("subscriber args missing %q; args = %v", want, c.Args)
		}
	}
	if c.SecurityContext == nil || c.SecurityContext.RunAsNonRoot == nil || !*c.SecurityContext.RunAsNonRoot {
		t.Fatalf("SecurityContext must run non-root; got %+v", c.SecurityContext)
	}
	if c.SecurityContext.Capabilities == nil || len(c.SecurityContext.Capabilities.Drop) == 0 {
		t.Fatalf("SecurityContext must drop ALL capabilities; got %+v", c.SecurityContext.Capabilities)
	}
}

func TestVLLMLMCacheObservationSidecarHonoursOptions(t *testing.T) {
	a := NewVLLMLMCacheAdapter(
		WithSubscriberImage("registry.example.com/subscriber:pinned"),
		WithPolicyServerGRPCAddress("ic-server.custom-ns.svc.cluster.local:9090"),
	)
	cb := newLMCacheBackend(map[string]string{"model": "MyOrg/MyModel"})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "engine-z", Namespace: "engines"}}

	c, err := a.ObservationSidecar(cb, pod)
	if err != nil || c == nil {
		t.Fatalf("ObservationSidecar: (%v, %v)", c, err)
	}
	if c.Image != "registry.example.com/subscriber:pinned" {
		t.Fatalf("image override ignored: got %q", c.Image)
	}
	if !containsArg(c.Args, "--server=ic-server.custom-ns.svc.cluster.local:9090") {
		t.Fatalf("server address override ignored; args = %v", c.Args)
	}
}

func TestVLLMLMCacheObservationSidecarSkipsWithoutModel(t *testing.T) {
	// BackendConfig["model"] is the documented source of --model-id. Without
	// it the subscriber binary would refuse to start (model-id is a required
	// flag), so the adapter returns (nil, nil) to skip the append. The next
	// admission picks up the sidecar once the operator sets the field.
	a := NewVLLMLMCacheAdapter(WithSubscriberImage(DefaultSubscriberImage))
	cb := newLMCacheBackend(nil)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "engine-a"}}

	c, err := a.ObservationSidecar(cb, pod)
	if err != nil {
		t.Fatalf("ObservationSidecar: %v", err)
	}
	if c != nil {
		t.Fatalf("expected nil sidecar when backendConfig.model is unset, got %+v", c)
	}
}

func TestVLLMLMCacheObservationSidecarSkipsWithoutImage(t *testing.T) {
	// Default install opts OUT of auto-attach: when the controller flag
	// --kvevent-subscriber-image is unset, the adapter returns no sidecar
	// at all — even when backendConfig.model is set — so an operator that
	// hasn't yet shipped a subscriber image can't end up with engine pods
	// stuck in ImagePullBackOff. Opt-in by passing WithSubscriberImage.
	a := NewVLLMLMCacheAdapter() // no image configured
	cb := newLMCacheBackend(map[string]string{"model": "MyOrg/MyModel"})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "engine-a"}}

	c, err := a.ObservationSidecar(cb, pod)
	if err != nil {
		t.Fatalf("ObservationSidecar: %v", err)
	}
	if c != nil {
		t.Fatalf("expected nil sidecar when subscriber image is unconfigured, got %+v", c)
	}
}

func TestVLLMLMCacheObservationSidecarArgsParseAgainstSubscriberFlagSet(t *testing.T) {
	// Regression: the Go flag package exits on unknown flags, so a sidecar
	// arg that the kvevent-subscriber binary doesn't recognise crashes the
	// container at startup and the engine pod silently fails to report
	// cache state. This test parses the rendered args through a FlagSet
	// mirroring the subscriber binary's flag surface and asserts they
	// parse cleanly. Keep the flag set in sync with
	// cmd/kvevent-subscriber/main.go — adding a flag to the sidecar's args
	// before the binary learns it is what this guard exists to catch.
	a := NewVLLMLMCacheAdapter(WithSubscriberImage(DefaultSubscriberImage))
	cb := newLMCacheBackend(map[string]string{"model": "Qwen/Qwen2.5-0.5B-Instruct"})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "engine-a", Namespace: "engines"}}

	c, err := a.ObservationSidecar(cb, pod)
	if err != nil || c == nil {
		t.Fatalf("ObservationSidecar: (%v, %v)", c, err)
	}

	fs := flag.NewFlagSet("kvevent-subscriber", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	// Subset of cmd/kvevent-subscriber/main.go's flag surface (the
	// event-path flags every shipped subscriber accepts). Stats-path
	// flags are intentionally absent here AND in the rendered args; both
	// land in the same follow-up.
	fs.String("engine-endpoint", "", "")
	fs.String("topic", "", "")
	fs.String("server", "", "")
	fs.String("replica-id", "", "")
	fs.String("model-id", "", "")
	fs.String("tenant-id", "", "")
	fs.String("hash-scheme", "", "")
	fs.Duration("window", 0, "")
	fs.Bool("ignore-block-removed", false, "")

	if err := fs.Parse(c.Args); err != nil {
		t.Fatalf("rendered sidecar args rejected by subscriber FlagSet: %v\nargs = %v", err, c.Args)
	}
	// Belt-and-suspenders: parse a control case that should fail so the
	// FlagSet isn't silently accepting unknown flags (rules out the test
	// being a tautology if someone passes the wrong FlagSet mode).
	if err := fs.Parse(append(c.Args, "--definitely-not-a-real-flag=x")); err == nil {
		t.Fatalf("control: FlagSet must reject unknown flag --definitely-not-a-real-flag")
	}
}

func TestVLLMLMCacheObservationSidecarBadInput(t *testing.T) {
	a := NewVLLMLMCacheAdapter(WithSubscriberImage(DefaultSubscriberImage))
	cb := newLMCacheBackend(map[string]string{"model": "m"})
	cases := []struct {
		name string
		cb   *cachev1alpha1.CacheBackend
		pod  *corev1.Pod
	}{
		{"nil cache", nil, &corev1.Pod{}},
		{"nil pod", cb, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := a.ObservationSidecar(tc.cb, tc.pod); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

// envHasFieldRef returns true if env contains an entry named name backed by
// a fieldRef whose FieldPath matches the given path. Used to assert the
// downward-API env the subscriber needs to resolve $(POD_NAME) /
// $(POD_NAMESPACE) at container start.
func envHasFieldRef(env []corev1.EnvVar, name, path string) bool {
	for _, e := range env {
		if e.Name == name && e.ValueFrom != nil && e.ValueFrom.FieldRef != nil && e.ValueFrom.FieldRef.FieldPath == path {
			return true
		}
	}
	return false
}

func TestReferenceAdapterObservationSidecarIsNil(t *testing.T) {
	a := NewReferenceAdapter()
	cb := newCacheBackend(cachev1alpha1.CacheBackendTypeExternal, "")
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ref-pod"}}
	c, err := a.ObservationSidecar(cb, pod)
	if err != nil {
		t.Fatalf("ObservationSidecar: %v", err)
	}
	if c != nil {
		t.Fatalf("reference adapter ObservationSidecar must return nil, got %+v", c)
	}
}

func containsArg(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

func containsArgPair(args []string, flag, value string) bool {
	for i, a := range args {
		if a == flag && i+1 < len(args) && args[i+1] == value {
			return true
		}
	}
	return false
}

func equalStrSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

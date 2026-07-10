package runtime

import (
	"flag"
	"io"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// wantMooncakeRemoteURL is the LMCACHE_REMOTE_URL the Mooncake adapter must
// inject for a bare host:port endpoint — the mooncakestore:// analog of lm://.
const wantMooncakeRemoteURL = "mooncakestore://cache.ns1.svc.cluster.local:50051"

func newMooncakeBackend(cfg map[string]string) *cachev1alpha1.CacheBackend {
	cb := newCacheBackend(cachev1alpha1.CacheBackendTypeMooncake, "vllm")
	cb.Spec.BackendConfig = cfg
	return cb
}

func TestVLLMMooncakeSupports(t *testing.T) {
	a := NewVLLMMooncakeAdapter()
	cases := []struct {
		name    string
		runtime RuntimeID
		cache   *cachev1alpha1.CacheBackend
		want    bool
	}{
		{"vllm+mooncake", RuntimeVLLM, newMooncakeBackend(nil), true},
		{"vllm+lmcache", RuntimeVLLM, newLMCacheBackend(nil), false},
		{"vllm+external", RuntimeVLLM, newCacheBackend(cachev1alpha1.CacheBackendTypeExternal, "vllm"), false},
		{"sglang+mooncake", RuntimeSGLang, newMooncakeBackend(nil), false},
		{"nil cache", RuntimeVLLM, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := a.Supports(tc.runtime, tc.cache); got != tc.want {
				t.Fatalf("Supports(%s, %v) = %v, want %v", tc.runtime, tc.cache, got, tc.want)
			}
		})
	}
}

func TestVLLMMooncakeSupportedPairs(t *testing.T) {
	a := NewVLLMMooncakeAdapter().(PairLister)
	pairs := a.SupportedPairs()
	if len(pairs) != 1 {
		t.Fatalf("SupportedPairs len = %d, want 1: %v", len(pairs), pairs)
	}
	want := SupportedPair{Runtime: RuntimeVLLM, Backend: cachev1alpha1.CacheBackendTypeMooncake}
	if pairs[0] != want {
		t.Fatalf("SupportedPairs[0] = %v, want %v", pairs[0], want)
	}
}

func TestVLLMMooncakeResolveCacheServer(t *testing.T) {
	a := NewVLLMMooncakeAdapter()
	cb := newMooncakeBackend(nil)

	pod, svc, err := a.ResolveCacheServer(cb)
	if err != nil {
		t.Fatalf("ResolveCacheServer: %v", err)
	}
	if pod == nil || svc == nil {
		t.Fatalf("ResolveCacheServer returned (pod=%v, svc=%v); want both non-nil", pod, svc)
	}
	if len(pod.Containers) != 1 {
		t.Fatalf("pod containers = %d, want 1", len(pod.Containers))
	}
	c := pod.Containers[0]
	if c.Name != mooncakeMasterContainerName {
		t.Fatalf("container name = %q, want %q", c.Name, mooncakeMasterContainerName)
	}
	if c.Image != defaultMooncakeMasterImage {
		t.Fatalf("image = %q, want %q", c.Image, defaultMooncakeMasterImage)
	}
	if len(c.Command) != 1 || c.Command[0] != "mooncake_master" {
		t.Fatalf("command = %v, want [mooncake_master]", c.Command)
	}
	for _, want := range []string{
		"--rpc_port=50051",
		"--metrics_port=9003",
		"--enable_http_metadata_server=true",
		"--http_metadata_server_host=0.0.0.0",
		"--http_metadata_server_port=8080",
	} {
		if !containsArg(c.Args, want) {
			t.Fatalf("master args missing %q; args = %v", want, c.Args)
		}
	}

	// The RPC port MUST be the FIRST container port AND the FIRST Service port:
	// the reconciler's serviceEndpoint helper formats status.endpoint from the
	// Service's first port, and the engine must dial the master's RPC port
	// (mooncakestore://), not the metadata port.
	if len(c.Ports) == 0 || c.Ports[0].Name != mooncakeRPCPortName || c.Ports[0].ContainerPort != defaultMooncakeMasterRPCPort {
		t.Fatalf("first container port = %+v, want %s/%d", c.Ports, mooncakeRPCPortName, defaultMooncakeMasterRPCPort)
	}
	if !hasContainerPort(c.Ports, mooncakeMetadataPortName, defaultMooncakeMetadataPort) {
		t.Fatalf("metadata container port missing; ports = %+v", c.Ports)
	}
	if !hasContainerPort(c.Ports, mooncakeMetricsPortName, defaultMooncakeMetricsPort) {
		t.Fatalf("metrics container port missing; ports = %+v", c.Ports)
	}

	if c.ReadinessProbe == nil || c.ReadinessProbe.TCPSocket == nil ||
		c.ReadinessProbe.TCPSocket.Port.StrVal != mooncakeRPCPortName {
		t.Fatalf("readiness probe = %+v, want TCPSocket on %q", c.ReadinessProbe, mooncakeRPCPortName)
	}

	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Fatalf("service type = %q, want ClusterIP", svc.Spec.Type)
	}
	if len(svc.Spec.Ports) == 0 || svc.Spec.Ports[0].Name != mooncakeRPCPortName ||
		svc.Spec.Ports[0].Port != defaultMooncakeMasterRPCPort {
		t.Fatalf("first service port = %+v, want %s/%d (serviceEndpoint uses Ports[0])",
			svc.Spec.Ports, mooncakeRPCPortName, defaultMooncakeMasterRPCPort)
	}
	if svc.Spec.Ports[0].TargetPort.StrVal != mooncakeRPCPortName {
		t.Fatalf("first service targetPort = %v, want %q", svc.Spec.Ports[0].TargetPort, mooncakeRPCPortName)
	}
}

// TestVLLMMooncakeResolveCacheServerHostNetworkAndHeadless pins the two properties
// Mooncake's peer-to-peer transfer engine depends on. Without BOTH, the backend
// reconciles Ready and transfers zero KV — validated on a real cluster, where the
// master's key count never left 0.
//
//   - hostNetwork: the master on :50051 only returns a directory pointer; the
//     engine then dials a real node IP on a dynamically negotiated port. CNI
//     overlay pod IPs are not reachable for that mesh.
//   - headless Service: a virtual ClusterIP forwards only the ports declared on
//     it, stranding those dynamic ports. clusterIP=None makes the Service DNS name
//     (which serviceEndpoint publishes into status.endpoint) resolve straight to
//     the master's node IP with every port reachable.
func TestVLLMMooncakeResolveCacheServerHostNetworkAndHeadless(t *testing.T) {
	a := NewVLLMMooncakeAdapter()
	pod, svc, err := a.ResolveCacheServer(newMooncakeBackend(nil))
	if err != nil {
		t.Fatalf("ResolveCacheServer: %v", err)
	}
	if !pod.HostNetwork {
		t.Fatal("pod.HostNetwork = false; mooncake's transfer engine cannot run on overlay pod IPs")
	}
	if pod.DNSPolicy != corev1.DNSClusterFirstWithHostNet {
		t.Fatalf("pod.DNSPolicy = %q, want %q (a hostNetwork pod must keep cluster DNS)",
			pod.DNSPolicy, corev1.DNSClusterFirstWithHostNet)
	}
	if svc.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Fatalf("svc.Spec.ClusterIP = %q, want %q (headless)", svc.Spec.ClusterIP, corev1.ClusterIPNone)
	}
	// Headless is still Type=ClusterIP; the type must not have drifted.
	if svc.Spec.Type != corev1.ServiceTypeClusterIP {
		t.Fatalf("svc.Spec.Type = %q, want %q", svc.Spec.Type, corev1.ServiceTypeClusterIP)
	}
}

func hasContainerPort(ports []corev1.ContainerPort, name string, port int32) bool {
	for _, p := range ports {
		if p.Name == name && p.ContainerPort == port {
			return true
		}
	}
	return false
}

func TestVLLMMooncakeResolveCacheServerImageOverride(t *testing.T) {
	a := NewVLLMMooncakeAdapter()
	cb := newMooncakeBackend(map[string]string{cfgKeyServerImage: "registry.example.com/mooncake@sha256:abc"})
	pod := resolvePod(t, a, cb)
	if got := pod.Containers[0].Image; got != "registry.example.com/mooncake@sha256:abc" {
		t.Fatalf("image override ignored: got %q", got)
	}
}

// TestVLLMMooncakeDefaultImageFullyQualified guards against a regression to a
// bare short name in defaultMooncakeMasterImage. A CRI-O node without short-name
// resolution configured rejects short names ("short-name … did not resolve to an
// alias"), so the default MUST carry an explicit registry host (e.g.
// docker.io/...). containerd resolves short names by default, but a
// fully-qualified reference is safe on both.
func TestVLLMMooncakeDefaultImageFullyQualified(t *testing.T) {
	registry, rest, ok := strings.Cut(defaultMooncakeMasterImage, "/")
	if !ok {
		t.Fatalf("default image %q has no registry host (no %q separator)", defaultMooncakeMasterImage, "/")
	}
	// A reference is fully qualified when the segment before the first slash is a
	// registry host: it contains a '.' or ':' (host[:port]) or is "localhost".
	if !strings.ContainsAny(registry, ".:") && registry != "localhost" {
		t.Fatalf("default image %q is a short name (registry segment %q is not a host, path %q); "+
			"CRI-O without short-name resolution configured rejects it — fully-qualify it (e.g. docker.io/...)",
			defaultMooncakeMasterImage, registry, rest)
	}
}

func TestVLLMMooncakeResolveCacheServerCommandOverride(t *testing.T) {
	a := NewVLLMMooncakeAdapter()
	// Use a non-port flag for the override: the docs/godoc warn operators NOT
	// to change the pinned RPC/metadata ports via serverCommand (the Service +
	// status.endpoint are fixed to them), so the test must not normalize that
	// footgun. A verbosity flag is a harmless, representative override.
	cb := newMooncakeBackend(map[string]string{cfgKeyServerCommand: "mooncake_master --v=1"})
	pod := resolvePod(t, a, cb)
	c := pod.Containers[0]
	if len(c.Command) != 1 || c.Command[0] != "mooncake_master" {
		t.Fatalf("command = %v, want [mooncake_master]", c.Command)
	}
	if len(c.Args) != 1 || c.Args[0] != "--v=1" {
		t.Fatalf("args = %v, want [--v=1]", c.Args)
	}
}

func TestVLLMMooncakeResolveCacheServerNilCache(t *testing.T) {
	a := NewVLLMMooncakeAdapter()
	if _, _, err := a.ResolveCacheServer(nil); err == nil {
		t.Fatalf("expected error for nil cache")
	}
}

func TestVLLMMooncakeResolveCacheServerHonorsSpecResources(t *testing.T) {
	a := NewVLLMMooncakeAdapter()
	cb := newMooncakeBackend(nil)
	cb.Spec.Resources = &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("16Gi")},
		Limits:   corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("32Gi")},
	}
	pod := resolvePod(t, a, cb)
	got := pod.Containers[0].Resources
	if got.Requests.Memory().String() != "16Gi" || got.Limits.Memory().String() != "32Gi" {
		t.Fatalf("resources = %+v, want requests=16Gi limits=32Gi", got)
	}
}

// TestVLLMMooncakeInjectEngineConfigHostNetworkIsOptIn pins the opt-in contract for
// the one mutation that changes a customer pod's security posture.
//
// Mooncake's transfer engine is a peer-to-peer mesh, so an engine on an overlay pod
// IP cannot reach the master and the backend moves zero KV. But hostNetwork is a
// privilege, and mutating webhooks run BEFORE Pod Security validation: injecting it
// unasked would turn a working engine pod into one a "restricted" namespace rejects,
// with an error naming Pod Security rather than this controller. So the engine only
// moves when the operator asks, via spec.integration.engineHostNetwork.
func TestVLLMMooncakeInjectEngineConfigHostNetworkIsOptIn(t *testing.T) {
	a := NewVLLMMooncakeAdapter()
	enginePod := func() *corev1.PodSpec {
		return &corev1.PodSpec{Containers: []corev1.Container{{Name: EngineContainerName}}}
	}

	t.Run("NotInjectedByDefault", func(t *testing.T) {
		pod := enginePod()
		if err := a.InjectEngineConfig(pod, "cache.ns.svc.cluster.local:50051", newMooncakeBackend(nil)); err != nil {
			t.Fatalf("InjectEngineConfig: %v", err)
		}
		if pod.HostNetwork {
			t.Fatal("engine pod moved onto hostNetwork with no opt-in; a restricted namespace would then reject it")
		}
		if pod.DNSPolicy != "" {
			t.Fatalf("dnsPolicy = %q, want unset when hostNetwork was not requested", pod.DNSPolicy)
		}
	})

	t.Run("InjectedWhenOperatorOptsIn", func(t *testing.T) {
		cb := newMooncakeBackend(nil)
		if cb.Spec.Integration == nil {
			cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
		}
		cb.Spec.Integration.EngineHostNetwork = true

		pod := enginePod()
		if err := a.InjectEngineConfig(pod, "cache.ns.svc.cluster.local:50051", cb); err != nil {
			t.Fatalf("InjectEngineConfig: %v", err)
		}
		if !pod.HostNetwork {
			t.Fatal("engine pod not moved onto hostNetwork despite the opt-in; it cannot reach the mesh from an overlay IP")
		}
		if pod.DNSPolicy != corev1.DNSClusterFirstWithHostNet {
			t.Fatalf("dnsPolicy = %q, want %q (the master's Service name must still resolve)",
				pod.DNSPolicy, corev1.DNSClusterFirstWithHostNet)
		}
	})
}

// TestVLLMLMCacheInjectEngineConfigNeverTouchesHostNetwork bounds the blast radius:
// the opt-in field is Mooncake-only (admission rejects it elsewhere), and the
// LMCache wire must never move a customer's engine onto the host network.
func TestVLLMLMCacheInjectEngineConfigNeverTouchesHostNetwork(t *testing.T) {
	a := NewVLLMLMCacheAdapter()
	cb := newLMCacheBackend(nil)
	if cb.Spec.Integration == nil {
		cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	}
	cb.Spec.Integration.EngineHostNetwork = true // rejected at admission; belt-and-braces here

	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: EngineContainerName}}}
	if err := a.InjectEngineConfig(pod, "cache.ns.svc.cluster.local:65432", cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	if pod.HostNetwork {
		t.Fatal("the LMCache adapter moved an engine pod onto hostNetwork; only Mooncake's mesh needs that")
	}
}

func TestVLLMMooncakeInjectEngineConfig(t *testing.T) {
	a := NewVLLMMooncakeAdapter()
	cb := newMooncakeBackend(nil)
	pod := &corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name: EngineContainerName,
				Args: []string{"--enable-prefix-caching", "--max-model-len", "8192"},
				Env:  []corev1.EnvVar{{Name: "HF_TOKEN", Value: "secret-token"}},
			},
			{
				Name: "sidecar",
				Env:  []corev1.EnvVar{{Name: "SIDECAR_VAR", Value: "untouched"}},
			},
		},
	}

	if err := a.InjectEngineConfig(pod, "cache.ns1.svc.cluster.local:50051", cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}

	engine := pod.Containers[0]
	// The defining difference from LMCache: the remote URL carries the
	// mooncakestore:// scheme, pointed at the master's RPC endpoint.
	if url, ok := lookupEnv(engine.Env, EnvLMCacheRemoteURL); !ok || url != wantMooncakeRemoteURL {
		t.Fatalf("%s = (%q, %v), want %q", EnvLMCacheRemoteURL, url, ok, wantMooncakeRemoteURL)
	}
	// The connector + invariants are the shared LMCache wire.
	if v, _ := lookupEnv(engine.Env, EnvLMCacheRemoteSerde); v != "naive" {
		t.Fatalf("%s = %q, want naive", EnvLMCacheRemoteSerde, v)
	}
	if v, _ := lookupEnv(engine.Env, EnvLMCacheChunkSize); v != "256" {
		t.Fatalf("%s = %q, want 256", EnvLMCacheChunkSize, v)
	}
	if v, _ := lookupEnv(engine.Env, EnvVLLMUseV1); v != "1" {
		t.Fatalf("%s = %q, want 1", EnvVLLMUseV1, v)
	}
	if v, ok := lookupEnv(engine.Env, EnvPythonHashSeed); !ok || v != "0" {
		t.Fatalf("%s = (%q, %v), want 0", EnvPythonHashSeed, v, ok)
	}
	// Existing engine env/args are preserved (merge, not clobber).
	if v, _ := lookupEnv(engine.Env, "HF_TOKEN"); v != "secret-token" {
		t.Fatalf("HF_TOKEN was clobbered: got %q", v)
	}
	if !containsArg(engine.Args, "--enable-prefix-caching") {
		t.Fatalf("--enable-prefix-caching was dropped: %v", engine.Args)
	}
	wantTransfer := kvTransferConfig(cachev1alpha1.CacheBackendIntegrationRoleReadWrite)
	if !containsArgPair(engine.Args, defaultEngineKVTransferConfigArg, wantTransfer) {
		t.Fatalf("connector args missing %s %s: %v", defaultEngineKVTransferConfigArg, wantTransfer, engine.Args)
	}

	// The non-engine container is untouched.
	sidecar := pod.Containers[1]
	if _, ok := lookupEnv(sidecar.Env, EnvLMCacheRemoteURL); ok {
		t.Fatalf("sidecar got cache env injected; adapter must target only the engine container")
	}
	if v, _ := lookupEnv(sidecar.Env, "SIDECAR_VAR"); v != "untouched" {
		t.Fatalf("SIDECAR_VAR was clobbered: got %q", v)
	}
}

func TestVLLMMooncakeInjectEngineConfigIdempotent(t *testing.T) {
	a := NewVLLMMooncakeAdapter()
	cb := newMooncakeBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: EngineContainerName}}}

	if err := a.InjectEngineConfig(pod, "first.svc:50051", cb); err != nil {
		t.Fatalf("first InjectEngineConfig: %v", err)
	}
	if err := a.InjectEngineConfig(pod, "second.svc:50051", cb); err != nil {
		t.Fatalf("second InjectEngineConfig: %v", err)
	}
	engine := pod.Containers[0]

	// Exactly one remote-URL env, holding the latest endpoint.
	count := 0
	for _, e := range engine.Env {
		if e.Name == EnvLMCacheRemoteURL {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("%s appears %d times, want 1 (idempotent upsert)", EnvLMCacheRemoteURL, count)
	}
	if url, _ := lookupEnv(engine.Env, EnvLMCacheRemoteURL); url != "mooncakestore://second.svc:50051" {
		t.Fatalf("%s = %q, want second endpoint", EnvLMCacheRemoteURL, url)
	}
	// Exactly one --kv-transfer-config flag.
	flagCount := 0
	for _, arg := range engine.Args {
		if arg == defaultEngineKVTransferConfigArg {
			flagCount++
		}
	}
	if flagCount != 1 {
		t.Fatalf("%s appears %d times, want 1", defaultEngineKVTransferConfigArg, flagCount)
	}
}

func TestVLLMMooncakeInjectEngineConfigMultiContainerWithoutVLLMNameErrors(t *testing.T) {
	a := NewVLLMMooncakeAdapter()
	cb := newMooncakeBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{
		{Name: "engine", Env: []corev1.EnvVar{{Name: "EXISTING", Value: "x"}}},
		{Name: "sidecar", Env: []corev1.EnvVar{{Name: "SIDECAR_VAR", Value: "untouched"}}},
	}}

	if err := a.InjectEngineConfig(pod, "cache.ns1.svc.cluster.local:50051", cb); err == nil {
		t.Fatalf("expected an error for multi-container pod without a vllm-named container")
	}
	// No partial mutation footprint.
	for _, c := range pod.Containers {
		if _, ok := lookupEnv(c.Env, EnvLMCacheRemoteURL); ok {
			t.Fatalf("container %q got %s injected before the error", c.Name, EnvLMCacheRemoteURL)
		}
	}
}

func TestVLLMMooncakeInjectEngineConfigPassesThroughMooncakeScheme(t *testing.T) {
	// An endpoint already carrying the scheme is not doubled.
	a := NewVLLMMooncakeAdapter()
	cb := newMooncakeBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: EngineContainerName}}}

	if err := a.InjectEngineConfig(pod, "mooncakestore://cache.ns1.svc.cluster.local:50051", cb); err != nil {
		t.Fatalf("InjectEngineConfig: %v", err)
	}
	if url, _ := lookupEnv(pod.Containers[0].Env, EnvLMCacheRemoteURL); url != wantMooncakeRemoteURL {
		t.Fatalf("%s = %q, want %q (scheme must not be doubled)", EnvLMCacheRemoteURL, url, wantMooncakeRemoteURL)
	}
}

func TestVLLMMooncakeInjectEngineConfigRoleMapping(t *testing.T) {
	cases := []struct {
		role cachev1alpha1.CacheBackendIntegrationRole
		want string
	}{
		{cachev1alpha1.CacheBackendIntegrationRoleReadOnly, kvTransferConfig(cachev1alpha1.CacheBackendIntegrationRoleReadOnly)},
		{cachev1alpha1.CacheBackendIntegrationRoleWriteOnly, kvTransferConfig(cachev1alpha1.CacheBackendIntegrationRoleWriteOnly)},
		{cachev1alpha1.CacheBackendIntegrationRoleReadWrite, kvTransferConfig(cachev1alpha1.CacheBackendIntegrationRoleReadWrite)},
	}
	for _, tc := range cases {
		t.Run(string(tc.role), func(t *testing.T) {
			a := NewVLLMMooncakeAdapter()
			cb := newMooncakeBackend(nil)
			cb.Spec.Integration.Role = tc.role
			pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: EngineContainerName}}}
			if err := a.InjectEngineConfig(pod, "cache.ns1.svc:50051", cb); err != nil {
				t.Fatalf("InjectEngineConfig: %v", err)
			}
			if !containsArgPair(pod.Containers[0].Args, defaultEngineKVTransferConfigArg, tc.want) {
				t.Fatalf("role %s: connector arg = %v, want pair value %q", tc.role, pod.Containers[0].Args, tc.want)
			}
		})
	}
}

func TestVLLMMooncakeInjectEngineConfigBadInput(t *testing.T) {
	a := NewVLLMMooncakeAdapter()
	cb := newMooncakeBackend(nil)
	good := &corev1.PodSpec{Containers: []corev1.Container{{Name: EngineContainerName}}}
	cases := []struct {
		name     string
		pod      *corev1.PodSpec
		endpoint string
		cache    *cachev1alpha1.CacheBackend
	}{
		{"nil pod", nil, "x:50051", cb},
		{"nil cache", good, "x:50051", nil},
		{"empty endpoint", good, "", cb},
		{"no containers", &corev1.PodSpec{}, "x:50051", cb},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := a.InjectEngineConfig(tc.pod, tc.endpoint, tc.cache); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func TestVLLMMooncakeInjectRouterConfigIsNoop(t *testing.T) {
	a := NewVLLMMooncakeAdapter()
	cb := newMooncakeBackend(nil)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: EngineContainerName, Env: []corev1.EnvVar{{Name: "KEEP", Value: "1"}}}}}
	if err := a.InjectRouterConfig(pod, "cache.ns1.svc:50051", cb); err != nil {
		t.Fatalf("InjectRouterConfig: %v", err)
	}
	// No router component: the pod is untouched.
	if len(pod.Containers[0].Env) != 1 || pod.Containers[0].Env[0].Name != "KEEP" {
		t.Fatalf("InjectRouterConfig mutated the pod: %+v", pod.Containers[0].Env)
	}
}

func TestVLLMMooncakeReservedArgs(t *testing.T) {
	a := NewVLLMMooncakeAdapter()
	got := a.ReservedArgs()
	if len(got) != 1 || got[0] != defaultEngineKVTransferConfigArg {
		t.Fatalf("ReservedArgs = %v, want [%s]", got, defaultEngineKVTransferConfigArg)
	}
}

func TestVLLMMooncakeReservedEnv(t *testing.T) {
	a := NewVLLMMooncakeAdapter()
	want := map[string]bool{
		EnvLMCacheRemoteURL:       true,
		EnvVLLMUseV1:              true,
		EnvInferenceCacheFailOpen: true,
		EnvPythonHashSeed:         true,
	}
	got := a.ReservedEnv()
	if len(got) != len(want) {
		t.Fatalf("ReservedEnv = %v, want %d entries", got, len(want))
	}
	for _, name := range got {
		if !want[name] {
			t.Fatalf("ReservedEnv has unexpected entry %q; want %v", name, want)
		}
	}
	// Tunables must NOT be reserved.
	for _, name := range got {
		if name == EnvLMCacheChunkSize || name == EnvLMCacheRemoteSerde {
			t.Fatalf("ReservedEnv must not reserve tunable %q", name)
		}
	}
}

func TestVLLMMooncakeEngineContainerName(t *testing.T) {
	if got := NewVLLMMooncakeAdapter().EngineContainerName(); got != EngineContainerName {
		t.Fatalf("EngineContainerName = %q, want %q", got, EngineContainerName)
	}
}

func TestVLLMMooncakeObservationSidecarShape(t *testing.T) {
	a := NewVLLMMooncakeAdapter(WithSubscriberImage(DefaultSubscriberImage))
	cb := newMooncakeBackend(map[string]string{"model": "Qwen/Qwen2.5-0.5B-Instruct"})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "engine-a", Namespace: "engines"}}

	c, err := a.ObservationSidecar(cb, pod)
	if err != nil {
		t.Fatalf("ObservationSidecar: %v", err)
	}
	if c == nil {
		t.Fatalf("ObservationSidecar returned nil for vLLM+Mooncake with a model + image set")
	}
	if c.Name != SubscriberContainerName {
		t.Fatalf("container name = %q, want %q", c.Name, SubscriberContainerName)
	}
	if c.Image != DefaultSubscriberImage {
		t.Fatalf("container image = %q, want %q", c.Image, DefaultSubscriberImage)
	}
	if !envHasFieldRef(c.Env, "POD_NAME", "metadata.name") {
		t.Fatalf("env missing POD_NAME via downward API: %v", c.Env)
	}
	for _, want := range []string{
		"--engine-endpoint=tcp://127.0.0.1:5557",
		"--server=" + DefaultPolicyServerGRPCAddress,
		"--replica-id=$(POD_NAME)",
		"--tenant-id=$(POD_NAMESPACE)",
		"--model-id=Qwen/Qwen2.5-0.5B-Instruct",
		// vLLM emits the same block-hash scheme regardless of the L2 store, so
		// the subscriber tags events "vllm" for Mooncake exactly as for LMCache.
		"--hash-scheme=vllm",
		// Mooncake is an L2 remote store like LMCache, so block-removed events
		// must be ignored to avoid dropping a still-resident routing hint.
		"--ignore-block-removed=true",
	} {
		if !containsArg(c.Args, want) {
			t.Fatalf("subscriber args missing %q; args = %v", want, c.Args)
		}
	}
	if c.SecurityContext == nil || c.SecurityContext.RunAsNonRoot == nil || !*c.SecurityContext.RunAsNonRoot {
		t.Fatalf("SecurityContext must run non-root; got %+v", c.SecurityContext)
	}
}

func TestVLLMMooncakeObservationSidecarSkipsWithoutModel(t *testing.T) {
	a := NewVLLMMooncakeAdapter(WithSubscriberImage(DefaultSubscriberImage))
	cb := newMooncakeBackend(nil)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "engine-a"}}
	c, err := a.ObservationSidecar(cb, pod)
	if err != nil {
		t.Fatalf("ObservationSidecar: %v", err)
	}
	if c != nil {
		t.Fatalf("expected nil sidecar when backendConfig.model is unset, got %+v", c)
	}
}

func TestVLLMMooncakeObservationSidecarSkipsWithoutImage(t *testing.T) {
	a := NewVLLMMooncakeAdapter() // no image configured
	cb := newMooncakeBackend(map[string]string{"model": "MyOrg/MyModel"})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "engine-a"}}
	c, err := a.ObservationSidecar(cb, pod)
	if err != nil {
		t.Fatalf("ObservationSidecar: %v", err)
	}
	if c != nil {
		t.Fatalf("expected nil sidecar when subscriber image is unconfigured, got %+v", c)
	}
}

func TestVLLMMooncakeObservationSidecarBadInput(t *testing.T) {
	a := NewVLLMMooncakeAdapter(WithSubscriberImage(DefaultSubscriberImage))
	cb := newMooncakeBackend(map[string]string{"model": "m"})
	if _, err := a.ObservationSidecar(nil, &corev1.Pod{}); err == nil {
		t.Fatalf("expected error for nil cache")
	}
	if _, err := a.ObservationSidecar(cb, nil); err == nil {
		t.Fatalf("expected error for nil pod")
	}
}

func TestVLLMMooncakeObservationSidecarArgsParseAgainstSubscriberFlagSet(t *testing.T) {
	// Same guard as the LMCache adapter: a rendered arg the kvevent-subscriber
	// binary doesn't recognise crashes the sidecar at startup (Go's flag
	// package exits on unknown flags). Parse through a FlagSet mirroring the
	// binary's event-path flag surface.
	a := NewVLLMMooncakeAdapter(WithSubscriberImage(DefaultSubscriberImage))
	cb := newMooncakeBackend(map[string]string{"model": "Qwen/Qwen2.5-0.5B-Instruct"})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "engine-a", Namespace: "engines"}}
	c, err := a.ObservationSidecar(cb, pod)
	if err != nil || c == nil {
		t.Fatalf("ObservationSidecar: (%v, %v)", c, err)
	}

	fs := flag.NewFlagSet("kvevent-subscriber", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
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
}

func TestDefaultRegistryResolvesVLLMMooncake(t *testing.T) {
	r := DefaultRegistry()
	// Mooncake resolves to the Mooncake adapter.
	a, err := r.Select(RuntimeVLLM, newMooncakeBackend(nil))
	if err != nil {
		t.Fatalf("DefaultRegistry().Select(vllm, Mooncake): %v", err)
	}
	if !a.Supports(RuntimeVLLM, newMooncakeBackend(nil)) {
		t.Fatalf("resolved adapter does not Support (vllm, Mooncake)")
	}
	// Registering Mooncake must not have displaced LMCache.
	if _, err := r.Select(RuntimeVLLM, newLMCacheBackend(nil)); err != nil {
		t.Fatalf("DefaultRegistry().Select(vllm, LMCache) regressed: %v", err)
	}
	// Mooncake must surface in the supported-pairs list (admission messages).
	found := false
	for _, p := range r.SupportedPairs() {
		if p.Runtime == RuntimeVLLM && p.Backend == cachev1alpha1.CacheBackendTypeMooncake {
			found = true
		}
	}
	if !found {
		t.Fatalf("DefaultRegistry().SupportedPairs() missing vllm/Mooncake: %v", r.SupportedPairs())
	}
}

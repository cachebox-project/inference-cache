package backend

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

func TestForLMCacheRegistered(t *testing.T) {
	b, ok := For(cachev1alpha1.CacheBackendTypeLMCache)
	if !ok {
		t.Fatalf("expected an LMCache builder to be registered")
	}
	if b.Type() != cachev1alpha1.CacheBackendTypeLMCache {
		t.Fatalf("builder.Type() = %q, want LMCache", b.Type())
	}
}

func TestForUnmanagedTypeAbsent(t *testing.T) {
	if _, ok := For(cachev1alpha1.CacheBackendTypeMooncake); ok {
		t.Fatalf("did not expect a builder for Mooncake in Phase 1")
	}
	if _, ok := For(cachev1alpha1.CacheBackendTypeExternal); ok {
		t.Fatalf("did not expect a builder for External (unmanaged)")
	}
}

func TestLMCacheBuildDefaults(t *testing.T) {
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns1"},
		Spec:       cachev1alpha1.CacheBackendSpec{Type: cachev1alpha1.CacheBackendTypeLMCache},
	}
	b, _ := For(cachev1alpha1.CacheBackendTypeLMCache)
	w, err := b.Build(cb)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	if w.Deployment.Name != "cache" || w.Deployment.Namespace != "ns1" {
		t.Fatalf("deployment meta = %s/%s, want ns1/cache", w.Deployment.Namespace, w.Deployment.Name)
	}
	if r := w.Deployment.Spec.Replicas; r == nil || *r != 1 {
		t.Fatalf("default replicas = %v, want 1", r)
	}
	if w.Endpoint != "cache.ns1.svc.cluster.local:8000" {
		t.Fatalf("endpoint = %q", w.Endpoint)
	}

	c := w.Deployment.Spec.Template.Spec.Containers[0]
	if c.Image != defaultLMCacheImage {
		t.Fatalf("image = %q, want default %q", c.Image, defaultLMCacheImage)
	}
	if len(c.Command) == 0 || c.Command[len(c.Command)-1] != defaultLMCacheModel {
		t.Fatalf("command = %v, want default model last", c.Command)
	}

	// Selector must be a subset of the pod template labels (so the Deployment matches its pods).
	sel := w.Deployment.Spec.Selector.MatchLabels
	podLabels := w.Deployment.Spec.Template.Labels
	for k, v := range sel {
		if podLabels[k] != v {
			t.Fatalf("selector label %s=%s not present in pod template labels %v", k, v, podLabels)
		}
	}
	if w.Service.Spec.Selector["app.kubernetes.io/instance"] != "cache" {
		t.Fatalf("service selector = %v, want instance=cache", w.Service.Spec.Selector)
	}
}

func TestLMCacheBuildOverrides(t *testing.T) {
	tgp := int64(45)
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns1"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:     cachev1alpha1.CacheBackendTypeLMCache,
			Replicas: ptrTo(int32(4)),
			BackendConfig: map[string]string{
				cfgKeyImage:         "example.com/img:tag",
				cfgKeyModel:         "org/model",
				cfgKeyHFTokenSecret: "my-hf-secret",
			},
			Template: &cachev1alpha1.CacheBackendPodSpecOverride{
				NodeSelector:                  map[string]string{"gpu": "true"},
				ServiceAccountName:            "backend-sa",
				TerminationGracePeriodSeconds: &tgp,
			},
		},
	}
	b, _ := For(cachev1alpha1.CacheBackendTypeLMCache)
	w, err := b.Build(cb)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	if r := w.Deployment.Spec.Replicas; r == nil || *r != 4 {
		t.Fatalf("replicas = %v, want 4", r)
	}
	spec := w.Deployment.Spec.Template.Spec
	c := spec.Containers[0]
	if c.Image != "example.com/img:tag" {
		t.Fatalf("image = %q, want override", c.Image)
	}
	if c.Command[len(c.Command)-1] != "org/model" {
		t.Fatalf("model = %v, want override", c.Command)
	}
	if spec.NodeSelector["gpu"] != "true" || spec.ServiceAccountName != "backend-sa" {
		t.Fatalf("pod overrides not applied: %+v", spec)
	}
	if spec.TerminationGracePeriodSeconds == nil || *spec.TerminationGracePeriodSeconds != 45 {
		t.Fatalf("terminationGracePeriodSeconds = %v, want 45", spec.TerminationGracePeriodSeconds)
	}

	hf := findEnv(c.Env, "HF_TOKEN")
	if hf == nil || hf.ValueFrom == nil || hf.ValueFrom.SecretKeyRef == nil || hf.ValueFrom.SecretKeyRef.Name != "my-hf-secret" {
		t.Fatalf("HF_TOKEN secret override = %+v, want my-hf-secret", hf)
	}
}

func TestLMCacheBuildNil(t *testing.T) {
	b, _ := For(cachev1alpha1.CacheBackendTypeLMCache)
	if _, err := b.Build(nil); err == nil {
		t.Fatalf("expected error for nil CacheBackend")
	}
}

func TestLMCacheBuildCPUProfile(t *testing.T) {
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns1"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type: cachev1alpha1.CacheBackendTypeLMCache,
			BackendConfig: map[string]string{
				cfgKeyProfile: "cpu",
				cfgKeyImage:   "vllm/vllm-openai-cpu:latest-arm64",
			},
		},
	}
	b, _ := For(cachev1alpha1.CacheBackendTypeLMCache)
	w, err := b.Build(cb)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	c := w.Deployment.Spec.Template.Spec.Containers[0]
	if c.Image != "vllm/vllm-openai-cpu:latest-arm64" {
		t.Fatalf("image = %q, want the supplied CPU image", c.Image)
	}
	if c.Command[len(c.Command)-1] != defaultCPUModel {
		t.Fatalf("model = %v, want CPU default %q", c.Command, defaultCPUModel)
	}
	// No GPU limit on the CPU profile.
	if _, ok := c.Resources.Limits["nvidia.com/gpu"]; ok {
		t.Fatalf("CPU profile must not request a GPU: %v", c.Resources.Limits)
	}
	// LMCache connector is dropped; prefix caching + KV events stay on.
	if argsContain(c.Args, "--kv-transfer-config") {
		t.Fatalf("CPU profile must not set the LMCache connector: %v", c.Args)
	}
	if !argsContain(c.Args, "--enable-prefix-caching") || !argsContain(c.Args, "--kv-events-config") || !argsContain(c.Args, "--enforce-eager") {
		t.Fatalf("CPU profile args missing expected flags: %v", c.Args)
	}
	// CPU env, not the LMCache/GPU env.
	if findEnv(c.Env, "VLLM_CPU_KVCACHE_SPACE") == nil {
		t.Fatalf("CPU profile missing VLLM_CPU_KVCACHE_SPACE: %v", c.Env)
	}
	if findEnv(c.Env, "VLLM_USE_V1") != nil || findEnv(c.Env, "LMCACHE_CHUNK_SIZE") != nil {
		t.Fatalf("CPU profile must not carry LMCache/GPU env: %v", c.Env)
	}
	// HF_TOKEN still optional (for overridden gated models).
	if hf := findEnv(c.Env, "HF_TOKEN"); hf == nil || hf.ValueFrom == nil || hf.ValueFrom.SecretKeyRef.Optional == nil || !*hf.ValueFrom.SecretKeyRef.Optional {
		t.Fatalf("HF_TOKEN should remain an optional secret ref: %+v", hf)
	}
	// Same wiring as GPU: 3 ports + readiness probe.
	if len(c.Ports) != 3 || c.ReadinessProbe == nil {
		t.Fatalf("CPU profile lost ports/probe: ports=%d probe=%v", len(c.Ports), c.ReadinessProbe)
	}
}

func TestLMCacheBuildCPUProfileOverrides(t *testing.T) {
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns1"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type: cachev1alpha1.CacheBackendTypeLMCache,
			BackendConfig: map[string]string{
				cfgKeyProfile: "CPU", // case-insensitive
				cfgKeyImage:   "vllm/vllm-openai-cpu:latest-arm64",
				cfgKeyModel:   "org/tiny",
			},
		},
	}
	b, _ := For(cachev1alpha1.CacheBackendTypeLMCache)
	w, err := b.Build(cb)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	c := w.Deployment.Spec.Template.Spec.Containers[0]
	if c.Image != "vllm/vllm-openai-cpu:latest-arm64" {
		t.Fatalf("image = %q, want CPU override", c.Image)
	}
	if c.Command[len(c.Command)-1] != "org/tiny" {
		t.Fatalf("model = %v, want override", c.Command)
	}
	if _, ok := c.Resources.Limits["nvidia.com/gpu"]; ok {
		t.Fatalf("CPU profile must not request a GPU")
	}
}

func TestLMCacheBuildCPUProfileRequiresImage(t *testing.T) {
	cb := &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns1"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type:          cachev1alpha1.CacheBackendTypeLMCache,
			BackendConfig: map[string]string{cfgKeyProfile: "cpu"}, // no image
		},
	}
	b, _ := For(cachev1alpha1.CacheBackendTypeLMCache)
	if _, err := b.Build(cb); err == nil {
		t.Fatalf("expected an error: profile=cpu without an image has no safe default")
	}
}

func TestLMCacheBuildDefaultProfileIsGPU(t *testing.T) {
	for _, profile := range []string{"", "gpu", "unknown"} {
		cb := &cachev1alpha1.CacheBackend{
			ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "ns1"},
			Spec: cachev1alpha1.CacheBackendSpec{
				Type:          cachev1alpha1.CacheBackendTypeLMCache,
				BackendConfig: map[string]string{cfgKeyProfile: profile},
			},
		}
		b, _ := For(cachev1alpha1.CacheBackendTypeLMCache)
		w, err := b.Build(cb)
		if err != nil {
			t.Fatalf("build profile=%q: %v", profile, err)
		}
		c := w.Deployment.Spec.Template.Spec.Containers[0]
		if gpu := c.Resources.Limits["nvidia.com/gpu"]; gpu.Value() != 1 {
			t.Fatalf("profile=%q should default to GPU (gpu limit=%v)", profile, gpu.Value())
		}
		if !argsContain(c.Args, "--kv-transfer-config") {
			t.Fatalf("profile=%q should keep the LMCache connector", profile)
		}
	}
}

func argsContain(args []string, want string) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
}

func findEnv(env []corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range env {
		if env[i].Name == name {
			return &env[i]
		}
	}
	return nil
}

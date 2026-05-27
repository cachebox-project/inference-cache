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

func findEnv(env []corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range env {
		if env[i].Name == name {
			return &env[i]
		}
	}
	return nil
}

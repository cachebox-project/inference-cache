package sglang

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

func newHiCacheBackend(spec *cachev1alpha1.SGLangHiCacheSpec) *cachev1alpha1.CacheBackend {
	return &cachev1alpha1.CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "hicache", Namespace: "ns1"},
		Spec: cachev1alpha1.CacheBackendSpec{
			Type: cachev1alpha1.CacheBackendTypeSGLangHiCache,
			Integration: &cachev1alpha1.CacheBackendIntegrationSpec{
				Engine: "sglang",
				Mode:   cachev1alpha1.CacheBackendIntegrationModeOffload,
				Role:   cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &cachev1alpha1.CacheBackendEngineSelector{
				MatchLabels: map[string]string{"app": "sglang"},
			},
			HiCache: spec,
		},
	}
}

func TestValidateHiCacheBackendAcceptsCanonicalConfig(t *testing.T) {
	if err := ValidateHiCacheBackend(newHiCacheBackend(
		&cachev1alpha1.SGLangHiCacheSpec{Ratio: "2"},
	)); err != nil {
		t.Fatalf("ValidateHiCacheBackend: %v", err)
	}
}

func TestValidateHiCacheBackendRejectsInvalidConfig(t *testing.T) {
	falseValue := false
	zero := int32(0)
	cases := []struct {
		name   string
		mutate func(*cachev1alpha1.CacheBackend)
	}{
		{"missing config", func(cache *cachev1alpha1.CacheBackend) { cache.Spec.HiCache = nil }},
		{"both capacities", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.HiCache.SizeGB = &zero
		}},
		{"zero size", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.HiCache.Ratio = ""
			cache.Spec.HiCache.SizeGB = &zero
		}},
		{"invalid ratio", func(cache *cachev1alpha1.CacheBackend) { cache.Spec.HiCache.Ratio = "NaN" }},
		{"wrong engine", func(cache *cachev1alpha1.CacheBackend) { cache.Spec.Integration.Engine = "vllm" }},
		{"events only", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.Integration.Mode = cachev1alpha1.CacheBackendIntegrationModeEventsOnly
		}},
		{"read only", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.Integration.Role = cachev1alpha1.CacheBackendIntegrationRoleReadOnly
		}},
		{"fail closed", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.Integration.FailOpen = &falseValue
		}},
		{"autoscaling", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 2}
		}},
		{"endpoint", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.Endpoint = "cache.example.com:8200"
		}},
		{"missing selector", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.EngineSelector = nil
		}},
		{"unknown backendConfig", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.BackendConfig = map[string]string{"l1SizeGB": "8"}
		}},
		{"reserved arg override", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.Integration.EngineOverrides = &cachev1alpha1.EngineInjectionOverrides{
				Args: []string{SGLangHiCacheRatioArg + "=3"},
			}
		}},
		{"reserved arg suppression", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.Integration.EngineOverrides = &cachev1alpha1.EngineInjectionOverrides{
				SuppressArgs: []string{SGLangEnableHiCacheArg},
			}
		}},
		{"invalid write policy", func(cache *cachev1alpha1.CacheBackend) {
			cache.Spec.HiCache.WritePolicy = "sometimes"
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cache := newHiCacheBackend(&cachev1alpha1.SGLangHiCacheSpec{Ratio: "2"})
			tc.mutate(cache)
			if err := ValidateHiCacheBackend(cache); err == nil {
				t.Fatal("ValidateHiCacheBackend returned no error")
			}
		})
	}
}

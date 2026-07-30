package runtime

import (
	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	provideradapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend/provider"
)

// ResolveLMCacheServer delegates to the storage-provider adapter.
//
// Deprecated: provider lifecycle belongs to
// pkg/adapters/backend/provider.ResolveLMCacheServer.
func ResolveLMCacheServer(cache *cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	return provideradapter.ResolveLMCacheServer(cache)
}

// ResolveRedisL2Server delegates to the storage-provider adapter.
//
// Deprecated: provider lifecycle belongs to
// pkg/adapters/backend/provider.ResolveRedisL2Server.
func ResolveRedisL2Server(cache *cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	return provideradapter.ResolveRedisL2Server(cache)
}

// ResolveMooncakeServer delegates to the storage-provider adapter.
//
// Deprecated: provider lifecycle belongs to
// pkg/adapters/backend/provider.ResolveMooncakeServer.
func ResolveMooncakeServer(cache *cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	return provideradapter.ResolveMooncakeServer(cache)
}

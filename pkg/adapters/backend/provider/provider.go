// Package provider contains the shipping remote-storage provider adapters.
package provider

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
)

type managedProvider struct {
	provider cachev1alpha1.CacheBackendRemoteStorageProvider
	render   func(*cachev1alpha1.CacheBackend) (*backendadapter.RenderedStorage, error)
}

func (p managedProvider) Supports(storage *cachev1alpha1.CacheBackendRemoteStorageSpec) bool {
	return storage != nil &&
		storage.Provider == p.provider &&
		storage.Ownership == cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged
}

func (p managedProvider) Render(cache *cachev1alpha1.CacheBackend) (*backendadapter.RenderedStorage, error) {
	return p.render(cache)
}

type externalProvider struct {
	provider cachev1alpha1.CacheBackendRemoteStorageProvider
	protocol backendadapter.Protocol
}

func (p externalProvider) Supports(storage *cachev1alpha1.CacheBackendRemoteStorageSpec) bool {
	return storage != nil &&
		storage.Provider == p.provider &&
		storage.Ownership == cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal
}

func (p externalProvider) Render(cache *cachev1alpha1.CacheBackend) (*backendadapter.RenderedStorage, error) {
	if cache == nil {
		return nil, fmt.Errorf("render external %s storage: cache is nil", p.provider)
	}
	return &backendadapter.RenderedStorage{Protocol: p.protocol}, nil
}

// DefaultRegistry returns the shipping provider capabilities. Engine/runtime
// compatibility is intentionally not encoded here.
func DefaultRegistry() *backendadapter.Registry {
	registry := backendadapter.NewRegistry()
	registry.Register(managedProvider{
		provider: cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
		render: func(cache *cachev1alpha1.CacheBackend) (*backendadapter.RenderedStorage, error) {
			pod, service, err := ResolveRedisL2Server(cache)
			return rendered(pod, service, backendadapter.ProtocolRESP, err)
		},
	})
	registry.Register(managedProvider{
		provider: cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer,
		render: func(cache *cachev1alpha1.CacheBackend) (*backendadapter.RenderedStorage, error) {
			pod, service, err := ResolveLMCacheServer(cache)
			return rendered(pod, service, backendadapter.ProtocolLMCache, err)
		},
	})
	registry.Register(managedProvider{
		provider: cachev1alpha1.CacheBackendRemoteStorageProviderMooncake,
		render: func(cache *cachev1alpha1.CacheBackend) (*backendadapter.RenderedStorage, error) {
			pod, service, err := ResolveMooncakeServer(cache)
			return rendered(pod, service, backendadapter.ProtocolMooncakeStore, err)
		},
	})
	for _, provider := range []externalProvider{
		{provider: cachev1alpha1.CacheBackendRemoteStorageProviderRedis, protocol: backendadapter.ProtocolRESP},
		{provider: cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer, protocol: backendadapter.ProtocolLMCache},
		{provider: cachev1alpha1.CacheBackendRemoteStorageProviderMooncake, protocol: backendadapter.ProtocolMooncakeStore},
	} {
		registry.Register(provider)
	}
	return registry
}

func rendered(podSpec *corev1.PodSpec, service *corev1.Service, protocol backendadapter.Protocol, err error) (*backendadapter.RenderedStorage, error) {
	if err != nil {
		return nil, err
	}
	return &backendadapter.RenderedStorage{PodSpec: podSpec, Service: service, Protocol: protocol}, nil
}

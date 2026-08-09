// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

// Package backend defines the remote-storage provider boundary. Runtime
// adapters consume the resulting Binding; they do not select or provision a
// provider.
package backend

import (
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// Protocol identifies the connection contract exposed by a remote provider.
type Protocol string

const (
	ProtocolLMCache       Protocol = "lm"
	ProtocolRESP          Protocol = "resp"
	ProtocolMooncakeStore Protocol = "mooncakestore"
)

// Binding is the structured connection information an engine adapter accepts.
// A nil binding means the requested hierarchy is host-only.
type Binding struct {
	Protocol Protocol
	Endpoint string

	// Redis carries typed RESP adapter configuration. Secret selectors remain
	// references; credentials are never copied into the CacheBackend status or
	// engine arguments. Nil for non-Redis bindings.
	Redis *RedisBinding
}

// RedisBinding is the runtime-facing, provider-specific portion of a RESP
// binding. It deliberately excludes managed-workload fields such as image and
// resources.
type RedisBinding struct {
	Authentication *cachev1alpha1.RedisAuthenticationSpec
	TLS            *cachev1alpha1.RemoteStorageTLSSpec
	Database       *int32
}

// RenderedStorage is the provider-owned workload shape. PodSpec and Service are
// nil for External ownership; Protocol remains populated so engine wiring can
// validate the binding independently from provider lifecycle.
type RenderedStorage struct {
	PodSpec  *corev1.PodSpec
	Service  *corev1.Service
	Protocol Protocol
}

// Provider owns one remote provider/ownership capability.
type Provider interface {
	Supports(*cachev1alpha1.CacheBackendRemoteStorageSpec) bool
	Render(*cachev1alpha1.CacheBackend) (*RenderedStorage, error)
}

// ErrNoProvider is returned when no registered provider accepts a declaration.
var ErrNoProvider = errors.New("no remote-storage provider supports the declaration")

// Registry resolves remote-storage providers independently from runtime/cache
// engine adapters.
type Registry struct {
	providers []Provider
}

// NewRegistry returns an empty provider registry.
func NewRegistry() *Registry { return &Registry{} }

// Register appends a provider. Nil providers are ignored.
func (r *Registry) Register(provider Provider) {
	if provider != nil {
		r.providers = append(r.providers, provider)
	}
}

// Select resolves the provider for storage.
func (r *Registry) Select(storage *cachev1alpha1.CacheBackendRemoteStorageSpec) (Provider, error) {
	if storage == nil {
		return nil, fmt.Errorf("%w: remoteStorage=<nil>", ErrNoProvider)
	}
	for _, provider := range r.providers {
		if provider.Supports(storage) {
			return provider, nil
		}
	}
	return nil, fmt.Errorf("%w: provider=%q ownership=%q", ErrNoProvider, storage.Provider, storage.Ownership)
}

// BindingFor combines a provider protocol with the caller-resolved endpoint.
// The caller is responsible for selecting and validating the managed or
// external source; keeping that value authoritative avoids reintroducing an
// untrimmed external spec value after validation.
func BindingFor(storage *cachev1alpha1.CacheBackendRemoteStorageSpec, protocol Protocol, resolvedEndpoint string) *Binding {
	if storage == nil {
		return nil
	}
	binding := &Binding{Protocol: protocol, Endpoint: resolvedEndpoint}
	if storage.Provider == cachev1alpha1.CacheBackendRemoteStorageProviderRedis && storage.Redis != nil {
		redis := storage.Redis.DeepCopy()
		binding.Redis = &RedisBinding{
			Authentication: redis.Authentication,
			TLS:            redis.TLS,
			Database:       redis.Database,
		}
	}
	return binding
}

// ProtocolFor returns the connection protocol associated with a provider.
func ProtocolFor(storage *cachev1alpha1.CacheBackendRemoteStorageSpec) (Protocol, error) {
	if storage == nil {
		return "", nil
	}
	switch storage.Provider {
	case cachev1alpha1.CacheBackendRemoteStorageProviderRedis:
		return ProtocolRESP, nil
	case cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer:
		return ProtocolLMCache, nil
	case cachev1alpha1.CacheBackendRemoteStorageProviderMooncake:
		return ProtocolMooncakeStore, nil
	default:
		return "", fmt.Errorf("%w: unknown provider=%q", ErrNoProvider, storage.Provider)
	}
}

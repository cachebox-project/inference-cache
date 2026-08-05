// Package backend defines the remote-storage provider boundary. Runtime
// adapters consume the resulting Binding; they do not select or provision a
// provider.
package backend

import (
	"errors"
	"fmt"
	"net"
	"path"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// Protocol identifies the connection contract exposed by a remote provider.
type Protocol string

const (
	ProtocolLMCache       Protocol = "lm"
	ProtocolRESP          Protocol = "resp"
	ProtocolMooncakeStore Protocol = "mooncakestore"
	ProtocolFile          Protocol = "file"
)

// NFSBinding is the mount contract exposed by an externally owned NFS
// provider. Runtime adapters translate it into their engine-specific file
// storage wiring.
type NFSBinding struct {
	Server    string
	Path      string
	MountPath string
}

// ValidateNFSServer enforces the hostname/IP portion of the shared NFS
// binding contract. Admission and runtime adapters both call this helper so a
// stored or admission-bypassed CacheBackend cannot reach pod injection with a
// server value the validating webhook would reject.
//
// IPv6 is intentionally accepted only as a raw literal (for example
// "2001:db8::25"), not an already-bracketed URI authority. Kubernetes stores
// that raw value in corev1.NFSVolumeSource.Server; kubelet's in-tree NFS plugin
// detects it with netutil.IsIPv6String and adds brackets in
// getServerFromSource before constructing the mount source as
// "[2001:db8::25]:/export". Keeping that boundary explicit avoids rejecting a
// Kubernetes-supported input or moving kubelet-owned formatting into this
// controller.
func ValidateNFSServer(server string) error {
	switch {
	case strings.TrimSpace(server) == "":
		return errors.New("NFS server must not be empty")
	case server != strings.TrimSpace(server):
		return errors.New("NFS server must not contain surrounding whitespace")
	case net.ParseIP(server) == nil && len(validation.IsDNS1123Subdomain(server)) != 0:
		return errors.New("NFS server must be a valid IPv4 address, IPv6 address, or DNS-1123 hostname")
	default:
		return nil
	}
}

// ValidateInlineNFSBinding validates the source fields used by today's
// externally owned, inline NFS Pod volume. Managed NFS must use a distinct
// provider/binding contract (for example a controller-managed PVC) rather
// than reusing this server/path shape.
func ValidateInlineNFSBinding(storage *cachev1alpha1.CacheBackendRemoteStorageSpec) error {
	if storage == nil {
		return errors.New("remoteStorage must not be nil for an inline NFS binding")
	}
	if storage.Provider != cachev1alpha1.CacheBackendRemoteStorageProviderNFS {
		return fmt.Errorf("remoteStorage.provider must be NFS for an inline NFS binding, got %q", storage.Provider)
	}
	if storage.Ownership != cachev1alpha1.CacheBackendRemoteStorageOwnershipExternal {
		return fmt.Errorf("remoteStorage.ownership must be External for an inline NFS binding, got %q", storage.Ownership)
	}
	if strings.TrimSpace(storage.Endpoint) != "" {
		return errors.New("remoteStorage.endpoint must be empty for an inline NFS binding")
	}
	if storage.NFS == nil {
		return errors.New("remoteStorage.nfs is required for an inline NFS binding")
	}
	if err := ValidateNFSServer(storage.NFS.Server); err != nil {
		return fmt.Errorf("remoteStorage.nfs.server: %w", err)
	}
	if err := validateNFSPath(storage.NFS.Path, "remoteStorage.nfs.path", true); err != nil {
		return err
	}
	return validateNFSPath(storage.NFS.MountPath, "remoteStorage.nfs.mountPath", false)
}

func validateNFSPath(value, fieldName string, allowRoot bool) error {
	if strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) ||
		!path.IsAbs(value) || path.Clean(value) != value {
		return fmt.Errorf("%s must be a clean absolute path without surrounding whitespace", fieldName)
	}
	if !allowRoot && value == "/" {
		return fmt.Errorf("%s must not mount remote storage over the container root", fieldName)
	}
	return nil
}

// Binding is the structured connection information an engine adapter accepts.
// A nil binding means the requested hierarchy is host-only.
type Binding struct {
	Protocol Protocol
	Endpoint string
	NFS      *NFSBinding
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
	if storage.NFS != nil {
		binding.NFS = &NFSBinding{
			Server:    storage.NFS.Server,
			Path:      storage.NFS.Path,
			MountPath: storage.NFS.MountPath,
		}
	}
	return binding
}

// BindingRequiresEndpoint reports whether the engine wire dials a network
// endpoint. File-backed NFS is mounted into the Pod and therefore carries no
// status/spec endpoint.
func BindingRequiresEndpoint(binding *Binding) bool {
	return binding != nil && binding.Protocol != ProtocolFile
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
	case cachev1alpha1.CacheBackendRemoteStorageProviderNFS:
		return ProtocolFile, nil
	default:
		return "", fmt.Errorf("%w: unknown provider=%q", ErrNoProvider, storage.Provider)
	}
}

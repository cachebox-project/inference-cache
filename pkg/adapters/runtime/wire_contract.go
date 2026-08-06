package runtime

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"unicode"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// Engine-side wire names are public so admission, controllers, tests, and
// out-of-tree adapters can share the exact protocol spellings without
// importing a built-in implementation.
const (
	EnvLMCacheRemoteURL       = "LMCACHE_REMOTE_URL"
	EnvLMCacheRemoteSerde     = "LMCACHE_REMOTE_SERDE"
	EnvLMCacheChunkSize       = "LMCACHE_CHUNK_SIZE"
	EnvLMCacheLocalCPU        = "LMCACHE_LOCAL_CPU"
	EnvLMCacheMaxLocalCPU     = "LMCACHE_MAX_LOCAL_CPU_SIZE"
	EnvVLLMUseV1              = "VLLM_USE_V1"
	EnvInferenceCacheFailOpen = "INFERENCECACHE_FAIL_OPEN"
	EnvPythonHashSeed         = "PYTHONHASHSEED"
	EngineContainerName       = "vllm"
)

// EngineHostNetworkRequested reports whether the operator opted an engine pod
// using a Mooncake remote binding into host networking.
func EngineHostNetworkRequested(cache *cachev1alpha1.CacheBackend) bool {
	return cache != nil && cache.Spec.Integration != nil && cache.Spec.Integration.EngineHostNetwork
}

// ValidateLMCacheEndpoint validates a bare host:port or lm://host:port.
func ValidateLMCacheEndpoint(value string) error {
	raw := strings.TrimSpace(value)
	if raw == "" {
		return fmt.Errorf("endpoint is empty")
	}
	if strings.ContainsFunc(raw, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return fmt.Errorf("endpoint must not contain whitespace or control characters within the host or port; use host:port or lm://host:port with no embedded spaces")
	}
	rest := raw
	if i := strings.Index(raw, "://"); i >= 0 {
		scheme := strings.ToLower(raw[:i])
		rest = raw[i+3:]
		if scheme != "lm" {
			return fmt.Errorf("endpoint scheme %q is not supported; use a bare host:port (the LMCache adapter adds the lm:// scheme) or an explicit lm://host:port URL", scheme)
		}
	}
	if strings.ContainsAny(rest, "/?#") {
		return fmt.Errorf("endpoint must be host:port (optionally prefixed lm://); paths/queries/fragments are not part of the LMCache wire and would be silently dropped")
	}
	host, port, ok := splitLMCacheHostPort(rest)
	if !ok || host == "" || port == "" {
		return fmt.Errorf("endpoint must be a non-empty host AND port (e.g. cache.example.com:8200 or lm://cache.example.com:8200); a scheme alone, a host with no port, an empty port, or a port with no host is not a valid LMCache endpoint")
	}
	return nil
}

func splitLMCacheHostPort(value string) (host, port string, hasPort bool) {
	if value == "" {
		return "", "", false
	}
	if strings.HasPrefix(value, "[") {
		end := strings.Index(value, "]")
		if end <= 1 {
			return "", "", false
		}
		host = value[1:end]
		tail := value[end+1:]
		if tail == "" {
			return host, "", false
		}
		if !strings.HasPrefix(tail, ":") || strings.Contains(tail[1:], ":") {
			return "", "", false
		}
		return host, tail[1:], true
	}
	if strings.Count(value, ":") > 1 {
		return "", "", false
	}
	if i := strings.LastIndex(value, ":"); i >= 0 {
		return value[:i], value[i+1:], true
	}
	return value, "", false
}

// ValidateExternalEndpoint validates an endpoint for the selected remote
// storage provider's engine-side protocol.
func ValidateExternalEndpoint(provider cachev1alpha1.CacheBackendRemoteStorageProvider, endpoint string) error {
	trimmed := strings.TrimSpace(endpoint)
	switch provider {
	case cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer:
		return ValidateLMCacheEndpoint(trimmed)
	case cachev1alpha1.CacheBackendRemoteStorageProviderRedis:
		if scheme, _, ok := strings.Cut(trimmed, "://"); ok {
			return fmt.Errorf("scheme %q is not supported for remoteStorage.provider=%s; use bare host:port", scheme, provider)
		}
		if err := ValidateLMCacheEndpoint(trimmed); err != nil {
			return err
		}
		_, port, err := net.SplitHostPort(trimmed)
		if err != nil {
			return fmt.Errorf("Redis endpoint must be a bare host:port: %w", err)
		}
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("Redis endpoint port %q must be an integer in 1-65535", port)
		}
		return nil
	case cachev1alpha1.CacheBackendRemoteStorageProviderMooncake:
		if scheme, address, ok := strings.Cut(trimmed, "://"); ok {
			if !strings.EqualFold(scheme, "mooncakestore") {
				return fmt.Errorf("scheme %q is not supported for remoteStorage.provider=%s; use bare host:port or mooncakestore://host:port", scheme, provider)
			}
			if strings.Contains(address, "://") {
				return fmt.Errorf("nested endpoint schemes are not supported for remoteStorage.provider=%s; use mooncakestore://host:port", provider)
			}
			trimmed = address
		}
		return ValidateLMCacheEndpoint(trimmed)
	default:
		return fmt.Errorf("remote-storage provider %q has no endpoint protocol", provider)
	}
}

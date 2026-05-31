// Package enginewire holds the engine-side wire format shared by every
// runtime adapter that fronts an LMCache-compatible cache (the in-tree
// vLLM+LMCache adapter and the External passthrough adapter today; future
// adapters that also speak the LMCache connector protocol can import it
// the same way).
//
// Centralising the wire keeps the adapters from drifting: an external cache
// the operator manages themselves still presents the same lm:// endpoint
// and the engine still parses the same --kv-transfer-config / LMCACHE_*
// env, so the injection logic is identical and only the endpoint source
// differs. The package lives under internal/ so it stays import-scoped to
// adapter authors and is never confused with a public API the engine team
// can rely on.
package enginewire

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// Engine env var names. The cache plane's contract with the engine: an
// engine pod that carries these variables (plus the --kv-transfer-config
// arg below) is wired to an LMCache-compatible cache.
const (
	EnvLMCacheRemoteURL       = "LMCACHE_REMOTE_URL"
	EnvLMCacheRemoteSerde     = "LMCACHE_REMOTE_SERDE"
	EnvLMCacheChunkSize       = "LMCACHE_CHUNK_SIZE"
	EnvLMCacheLocalCPU        = "LMCACHE_LOCAL_CPU"
	EnvLMCacheMaxLocalCPU     = "LMCACHE_MAX_LOCAL_CPU_SIZE"
	EnvVLLMUseV1              = "VLLM_USE_V1"
	EnvInferenceCacheFailOpen = "INFERENCECACHE_FAIL_OPEN"
)

// EngineContainerName is the conventional name of the vLLM container in an
// engine pod. When a pod has no container with this name, a single-container
// pod is treated as the engine; a multi-container pod is rejected — silently
// mutating every container would inject vLLM-only flags onto sidecars and
// crash them.
const EngineContainerName = "vllm"

// Defaults the engine env carries when the operator doesn't override them
// via spec.backendConfig. The CPU-safe LMCACHE_REMOTE_SERDE is "naive";
// "cachegen" is faster but pulls in CUDA-only codepaths.
const (
	defaultChunkSize    = "256"
	defaultRemoteSerde  = "naive"
	defaultLocalCPU     = "False"
	defaultMaxLocalCPU  = "20"
	defaultVLLMUseV1    = "1"
	kvTransferConfigArg = "--kv-transfer-config"
	kvRoleConsumer      = "kv_consumer"
	kvRoleProducer      = "kv_producer"
	kvRoleBoth          = "kv_both"
	cfgKeyChunkSize     = "chunkSize"
	cfgKeyRemoteSerde   = "remoteSerde"
	cfgKeyLocalCPU      = "localCPU"
	cfgKeyMaxLocalCPU   = "maxLocalCPU"
)

// InjectVLLMLMCache adds the LMCache connector arg and LMCACHE_* env to the
// vLLM container in pod, given endpoint as the cache server's address.
// endpoint accepts either a bare `host:port` (canonical) or an already-
// prefixed `lm://host:port` ([LMCacheRemoteURL] passes the prefix through
// rather than doubling it); the helper renders both into the same
// LMCACHE_REMOTE_URL=`lm://host:port`. It merges: existing args/env on
// the vLLM container are preserved, repeat injections are idempotent,
// sidecars are left alone. The engine container is identified by
// [EngineContainerName]; a single-container pod is also accepted (the
// lone container is treated as the engine); a multi-container pod with
// no `vllm` container is rejected.
//
// Both the in-tree vLLM+LMCache adapter (managed backend) and the External
// passthrough adapter call this — same wire shape, the only difference is
// the source of endpoint (controller-resolved Service DNS vs operator-
// supplied address in spec.endpoint).
func InjectVLLMLMCache(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	if err := ValidateInjectInputs(pod, endpoint, cache, "engine"); err != nil {
		return err
	}
	cfg := cache.Spec.BackendConfig
	env := []corev1.EnvVar{
		{Name: EnvLMCacheRemoteURL, Value: LMCacheRemoteURL(endpoint)},
		{Name: EnvLMCacheRemoteSerde, Value: ConfigOr(cfg, cfgKeyRemoteSerde, defaultRemoteSerde)},
		{Name: EnvLMCacheChunkSize, Value: ConfigOr(cfg, cfgKeyChunkSize, defaultChunkSize)},
		{Name: EnvLMCacheLocalCPU, Value: ConfigOr(cfg, cfgKeyLocalCPU, defaultLocalCPU)},
		{Name: EnvLMCacheMaxLocalCPU, Value: ConfigOr(cfg, cfgKeyMaxLocalCPU, defaultMaxLocalCPU)},
		{Name: EnvVLLMUseV1, Value: defaultVLLMUseV1},
		{Name: EnvInferenceCacheFailOpen, Value: FailOpenString(cache)},
	}
	args := []string{kvTransferConfigArg, KVTransferConfig(IntegrationRole(cache))}

	i, err := EngineContainerIndex(pod)
	if err != nil {
		return err
	}
	for _, e := range env {
		pod.Containers[i].Env = UpsertEnv(pod.Containers[i].Env, e)
	}
	pod.Containers[i].Args = UpsertArgPair(pod.Containers[i].Args, args[0], args[1])
	return nil
}

// ValidateInjectInputs centralises the bad-input checks Inject* paths share.
// The role tag flows into the error message so callers can tell which path
// rejected the input ("engine", "router", ...).
func ValidateInjectInputs(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend, role string) error {
	if pod == nil {
		return fmt.Errorf("inject %s config: pod is nil", role)
	}
	if cache == nil {
		return fmt.Errorf("inject %s config: cache is nil", role)
	}
	if endpoint == "" {
		return fmt.Errorf("inject %s config: endpoint is empty", role)
	}
	if len(pod.Containers) == 0 {
		return fmt.Errorf("inject %s config: pod has no containers", role)
	}
	return nil
}

// EngineContainerIndex returns the index of the engine container the adapter
// should mutate. See [InjectVLLMLMCache] for the selection rules.
func EngineContainerIndex(pod *corev1.PodSpec) (int, error) {
	for i := range pod.Containers {
		if pod.Containers[i].Name == EngineContainerName {
			return i, nil
		}
	}
	if len(pod.Containers) == 1 {
		return 0, nil
	}
	names := make([]string, len(pod.Containers))
	for i := range pod.Containers {
		names[i] = pod.Containers[i].Name
	}
	return -1, fmt.Errorf("inject engine config: pod has %d containers %v but none is named %q; injecting vLLM flags into unrelated sidecars would crash them — name the engine container %q",
		len(pod.Containers), names, EngineContainerName, EngineContainerName)
}

// LMCacheRemoteURL prefixes an engine-agnostic host:port endpoint with the
// LMCache lm:// scheme. An endpoint already carrying the lm:// scheme is
// normalised to lower-case `lm://` — the admission validator lowercases
// the scheme during shape checks (so `LM://cache.example` admits), and
// without normalisation here that would inject
// `LMCACHE_REMOTE_URL=lm://LM://cache.example`, a double-prefix the engine
// connector rejects. The prefix match is case-insensitive on the scheme
// only; the host portion is preserved verbatim (DNS is case-insensitive
// but rewriting operator-typed casing is not the helper's job).
func LMCacheRemoteURL(endpoint string) string {
	const scheme = "lm://"
	if len(endpoint) >= len(scheme) && strings.EqualFold(endpoint[:len(scheme)], scheme) {
		return scheme + endpoint[len(scheme):]
	}
	return scheme + endpoint
}

// FailOpenString returns the bool form of the effective fail-open mode for
// the engine env. The CRD defaults to fail-open via the defaulting webhook;
// the helper handles nil Integration / nil failOpen too — pre-defaulting
// code paths shouldn't crash.
func FailOpenString(cache *cachev1alpha1.CacheBackend) string {
	if cachev1alpha1.IntegrationFailOpen(cache.Spec.Integration) {
		return "true"
	}
	return "false"
}

// IntegrationRole returns the engine's participation role, defaulting to
// ReadWrite (matching the CRD's documented behaviour when integration is
// unset).
func IntegrationRole(cache *cachev1alpha1.CacheBackend) cachev1alpha1.CacheBackendIntegrationRole {
	if cache.Spec.Integration == nil || cache.Spec.Integration.Role == "" {
		return cachev1alpha1.CacheBackendIntegrationRoleReadWrite
	}
	return cache.Spec.Integration.Role
}

// KVTransferConfig renders the --kv-transfer-config JSON for the given role.
// An unrecognised role falls back to kv_both so a future CRD value (added
// after this adapter ships) is not silently dropped from the kv path.
func KVTransferConfig(role cachev1alpha1.CacheBackendIntegrationRole) string {
	kvRole := kvRoleBoth
	switch role {
	case cachev1alpha1.CacheBackendIntegrationRoleReadOnly:
		kvRole = kvRoleConsumer
	case cachev1alpha1.CacheBackendIntegrationRoleWriteOnly:
		kvRole = kvRoleProducer
	case cachev1alpha1.CacheBackendIntegrationRoleReadWrite:
		kvRole = kvRoleBoth
	}
	return fmt.Sprintf(`{"kv_connector":"LMCacheConnectorV1","kv_role":%q}`, kvRole)
}

// UpsertArgPair inserts or updates the flag/value pair `flag value` in args,
// preserving every other arg. Both the two-arg form (`--flag`, `value`) and
// the equals form (`--flag=value`) are recognised: an existing entry in
// either form is updated in place (to the two-arg form), no duplicate is
// appended. A trailing two-arg `--flag` with no value is treated as missing.
// Normalising on the two-arg form keeps the rendered args stable across
// repeat injections so an idempotent reconcile doesn't churn.
func UpsertArgPair(args []string, flag, value string) []string {
	prefix := flag + "="
	for i, a := range args {
		switch {
		case a == flag:
			if i+1 < len(args) {
				args[i+1] = value
				return args
			}
			return append(args, value)
		case strings.HasPrefix(a, prefix):
			args[i] = flag
			out := make([]string, 0, len(args)+1)
			out = append(out, args[:i+1]...)
			out = append(out, value)
			out = append(out, args[i+1:]...)
			return out
		}
	}
	return append(args, flag, value)
}

// UpsertEnv returns env with want.Name set to want.Value/ValueFrom: updates
// in place if the entry exists, appends otherwise. Used so a second call to
// Inject*Config never produces duplicate env entries and never disturbs
// unrelated ones — the same property real adapters must preserve.
func UpsertEnv(env []corev1.EnvVar, want corev1.EnvVar) []corev1.EnvVar {
	for i := range env {
		if env[i].Name == want.Name {
			env[i].Value = want.Value
			env[i].ValueFrom = want.ValueFrom
			return env
		}
	}
	return append(env, want)
}

// ConfigOr reads key from cfg or returns fallback when key is absent or empty.
func ConfigOr(cfg map[string]string, key, fallback string) string {
	if v, ok := cfg[key]; ok && v != "" {
		return v
	}
	return fallback
}

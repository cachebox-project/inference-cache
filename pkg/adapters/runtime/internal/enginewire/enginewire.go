// Package enginewire holds the engine-side wire format shared by every
// runtime adapter that fronts an LMCache-compatible cache (the in-tree
// vLLM+LMCache adapter, the vLLM+Mooncake adapter, and the External
// passthrough adapter today; future adapters that also speak the LMCache
// connector protocol can import it the same way).
//
// Centralising the wire keeps the adapters from drifting: an external cache
// the operator manages themselves still presents the same lm:// endpoint
// and the engine still parses the same --kv-transfer-config / LMCACHE_*
// env, so the injection logic is identical and only the endpoint source
// differs. The Mooncake adapter reuses the same connector wire — vLLM runs
// the LMCache connector pointed at a mooncakestore:// remote store instead
// of an lm:// one — so it differs from the LMCache path in nothing but the
// remote-URL scheme (see [InjectVLLMMooncake]). The package lives under
// internal/ so it stays import-scoped to adapter authors and is never
// confused with a public API the engine team can rely on.
package enginewire

import (
	"fmt"
	"strings"
	"unicode"

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
	// EnvPythonHashSeed pins Python's hash seed so the NONE_HASH that seeds
	// vLLM's prefix-cache block-hash chain is deterministic across the
	// scheduler and the TP worker processes. Under TP>1 those are separate
	// OS processes; with PYTHONHASHSEED unset each derives a different
	// NONE_HASH, so the reload lookup's hashes never match the workers'
	// stored hashes — LMCache reload silently 0-hits and the engine fully
	// recomputes with no crash and no error. A correctness invariant, not a
	// tunable.
	EnvPythonHashSeed = "PYTHONHASHSEED"
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
	defaultChunkSize   = "256"
	defaultRemoteSerde = "naive"
	defaultLocalCPU    = "False"
	defaultMaxLocalCPU = "20"
	defaultVLLMUseV1   = "1"
	// defaultPythonHashSeed = "0" makes every engine process derive the same
	// NONE_HASH so LMCache reload matches under TP>1 (see [EnvPythonHashSeed]).
	defaultPythonHashSeed = "0"
	kvTransferConfigArg   = "--kv-transfer-config"
	kvRoleConsumer        = "kv_consumer"
	kvRoleProducer        = "kv_producer"
	kvRoleBoth            = "kv_both"
	cfgKeyChunkSize       = "chunkSize"
	cfgKeyRemoteSerde     = "remoteSerde"
	cfgKeyLocalCPU        = "localCPU"
	cfgKeyMaxLocalCPU     = "maxLocalCPU"
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
	return injectLMCacheConnector(pod, endpoint, LMCacheRemoteURL(endpoint), cache)
}

// InjectVLLMMooncake wires a vLLM engine to a Mooncake store. Mooncake
// integrates with vLLM as an LMCache *remote backend*: the engine runs the
// exact same LMCache connector (kv_connector=LMCacheConnectorV1) and reads the
// same LMCACHE_* env, the only difference being the remote-store URL scheme —
// mooncakestore://host:port instead of lm://host:port. LMCache parses that
// scheme (its MooncakestoreConnectorAdapter registers "mooncakestore://") and
// connects the engine to the Mooncake master at host:port, so the injected
// wire is byte-identical to [InjectVLLMLMCache] save for the scheme. endpoint
// accepts a bare host:port (canonical — the Mooncake master Service DNS the
// reconciler published into status.endpoint) or an already-prefixed
// mooncakestore://host:port; both render to LMCACHE_REMOTE_URL=
// mooncakestore://host:port.
//
// Static Mooncake transfer-engine tuning (metadata_server, protocol,
// device_name, segment sizes) lives in LMCache's extra_config, which is
// supplied via an engine-side config file (LMCACHE_CONFIG_FILE /
// MOONCAKE_CONFIG_PATH) the operator owns — it is not env-injectable, so this
// helper wires only the controller-resolved master address + the connector,
// and the transfer-engine defaults (P2P-handshake metadata) cover the simplest
// deployment. See docs/design/cachebackend-api.md for the operator-side config.
func InjectVLLMMooncake(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	return injectLMCacheConnector(pod, endpoint, MooncakeStoreRemoteURL(endpoint), cache)
}

// injectLMCacheConnector is the shared body behind [InjectVLLMLMCache] and
// [InjectVLLMMooncake]: it merges the LMCache connector arg and LMCACHE_* env
// onto the vLLM container in pod, with remoteURL as the already-scheme-prefixed
// LMCACHE_REMOTE_URL value (lm:// for LMCache, mooncakestore:// for Mooncake).
// endpoint is the pre-scheme address, passed only so the input validation
// reports the same "endpoint is empty" error regardless of scheme. It merges:
// existing args/env on the vLLM container are preserved, repeat injections are
// idempotent, sidecars are left alone. The engine container is identified by
// [EngineContainerName]; a single-container pod is also accepted (the lone
// container is treated as the engine); a multi-container pod with no `vllm`
// container is rejected.
func injectLMCacheConnector(pod *corev1.PodSpec, endpoint, remoteURL string, cache *cachev1alpha1.CacheBackend) error {
	if err := ValidateInjectInputs(pod, endpoint, cache, "engine"); err != nil {
		return err
	}
	cfg := cache.Spec.BackendConfig
	env := []corev1.EnvVar{
		{Name: EnvLMCacheRemoteURL, Value: remoteURL},
		{Name: EnvLMCacheRemoteSerde, Value: ConfigOr(cfg, cfgKeyRemoteSerde, defaultRemoteSerde)},
		{Name: EnvLMCacheChunkSize, Value: ConfigOr(cfg, cfgKeyChunkSize, defaultChunkSize)},
		{Name: EnvLMCacheLocalCPU, Value: ConfigOr(cfg, cfgKeyLocalCPU, defaultLocalCPU)},
		{Name: EnvLMCacheMaxLocalCPU, Value: ConfigOr(cfg, cfgKeyMaxLocalCPU, defaultMaxLocalCPU)},
		{Name: EnvVLLMUseV1, Value: defaultVLLMUseV1},
		{Name: EnvInferenceCacheFailOpen, Value: FailOpenString(cache)},
		{Name: EnvPythonHashSeed, Value: defaultPythonHashSeed},
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

// SGLang engine-side wire. SGLang fronts LMCache through its own launch
// surface, distinct from vLLM's --kv-transfer-config JSON:
//
//   - the connector is turned on by the bare boolean flag --enable-lmcache
//     (SGLang's `server_args.py`; an `action="store_true"` arg, so it carries
//     no value), and
//   - SGLang's experimental LMCache integration is gated on the
//     LMCACHE_USE_EXPERIMENTAL=True env (SGLang's
//     mem_cache/storage/lmcache README + unit_test set exactly this).
//
// The LMCache *client* it loads is the same library vLLM uses, so the
// LMCACHE_* connection/tunable env is identical (remote URL + serde + chunk
// size + local-CPU knobs) and points at the same lm:// server. Two vLLM-only
// env vars are deliberately NOT injected for SGLang:
//
//   - VLLM_USE_V1 selects a vLLM-internal codepath that has no SGLang analogue;
//   - PYTHONHASHSEED pins vLLM's builtin-hash()-seeded NONE_HASH so its
//     block-hash chain matches across TP workers — SGLang derives its prefix
//     hash with hashlib.sha256 over the token-id bytes, which is independent of
//     PYTHONHASHSEED, so pinning it would be cargo-culted, not load-bearing.
const (
	// EnvLMCacheUseExperimental gates SGLang's experimental LMCache path; it
	// MUST be "True" for --enable-lmcache to engage the connector.
	EnvLMCacheUseExperimental = "LMCACHE_USE_EXPERIMENTAL"
	// SGLangEngineContainerName is the conventional name of the SGLang engine
	// container in a pod the adapter mutates (parallel to [EngineContainerName]
	// for vLLM). A single-container pod is also treated as the engine.
	SGLangEngineContainerName = "sglang"
	// SGLangEnableLMCacheArg is SGLang's boolean flag that turns on the LMCache
	// connector. It takes no value (store_true), so it is upserted as a bare
	// flag rather than a --flag value pair. Exported so the SGLang adapter can
	// name it in ReservedArgs (the admission validator blocks an
	// engineOverrides entry that would suppress it).
	SGLangEnableLMCacheArg    = "--enable-lmcache"
	lmcacheUseExperimentalVal = "True"
)

// InjectSGLangLMCache injects the shipped SGLang LMCache launch wire onto the
// SGLang container in pod: the --enable-lmcache flag plus the LMCACHE_* env —
// LMCACHE_REMOTE_URL rendered from endpoint as lm://host:port (endpoint accepts a
// bare host:port or an already-prefixed lm://host:port; see [LMCacheRemoteURL]),
// serde/chunk-size/local-CPU tunables, and LMCACHE_USE_EXPERIMENTAL=True. It
// merges (existing args/env preserved, repeat injections idempotent, sidecars
// untouched); the engine container is [SGLangEngineContainerName], a
// single-container pod is accepted, and a multi-container pod with no `sglang`
// container is rejected.
//
// KNOWN LIMITATION (the old wire-test TODO here, now resolved by live GPU
// validation): this wire does NOT produce a working cache. Unlike vLLM, SGLang
// does not read the LMCACHE_* env — it drives LMCache in multiprocess (MP) mode,
// configured by a --lmcache-config-file (carrying mp_host/mp_port) and served by a
// node-local MP worker. So the injected LMCACHE_REMOTE_URL is inert, and a bare
// lm:// URL does not offload anywhere useful (lm:// is not even a valid MP
// --l2-adapter type); only LMCACHE_USE_EXPERIMENTAL=True still matters (it gates
// the connector). This function keeps injecting the shipped (non-functional) env
// for now; the working MP-mode wire — a config-file init container, an MP-worker
// sidecar, and a shared L2 store — is the tracked follow-up. Full design +
// evidence: docs/design/sglang-lmcache-mp-mode.md.
func InjectSGLangLMCache(pod *corev1.PodSpec, endpoint string, cache *cachev1alpha1.CacheBackend) error {
	if err := ValidateInjectInputs(pod, endpoint, cache, "engine"); err != nil {
		return err
	}
	cfg := cache.Spec.BackendConfig
	env := []corev1.EnvVar{
		{Name: EnvLMCacheRemoteURL, Value: LMCacheRemoteURL(endpoint)},
		{Name: EnvLMCacheUseExperimental, Value: lmcacheUseExperimentalVal},
		{Name: EnvLMCacheRemoteSerde, Value: ConfigOr(cfg, cfgKeyRemoteSerde, defaultRemoteSerde)},
		{Name: EnvLMCacheChunkSize, Value: ConfigOr(cfg, cfgKeyChunkSize, defaultChunkSize)},
		{Name: EnvLMCacheLocalCPU, Value: ConfigOr(cfg, cfgKeyLocalCPU, defaultLocalCPU)},
		{Name: EnvLMCacheMaxLocalCPU, Value: ConfigOr(cfg, cfgKeyMaxLocalCPU, defaultMaxLocalCPU)},
		{Name: EnvInferenceCacheFailOpen, Value: FailOpenString(cache)},
	}

	i, err := EngineContainerIndexNamed(pod, SGLangEngineContainerName)
	if err != nil {
		return err
	}
	for _, e := range env {
		pod.Containers[i].Env = UpsertEnv(pod.Containers[i].Env, e)
	}
	pod.Containers[i].Args = UpsertFlag(pod.Containers[i].Args, SGLangEnableLMCacheArg)
	return nil
}

// UpsertFlag appends the bare boolean flag (e.g. "--enable-lmcache") when
// it is absent, preserving every existing arg; a second call is a no-op. Used
// for store_true flags that carry no value — distinct from [UpsertArgPair],
// which manages a `--flag value` pair.
func UpsertFlag(args []string, flag string) []string {
	for _, a := range args {
		if a == flag {
			return args
		}
	}
	return append(args, flag)
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

// EngineContainerIndex returns the index of the vLLM engine container the
// adapter should mutate. See [InjectVLLMLMCache] for the selection rules.
func EngineContainerIndex(pod *corev1.PodSpec) (int, error) {
	return EngineContainerIndexNamed(pod, EngineContainerName)
}

// EngineContainerIndexNamed returns the index of the engine container named
// name, falling back to the lone container in a single-container pod (there is
// no sidecar to crash). A multi-container pod with no container named name is
// rejected — blindly mutating every container would inject engine-only flags
// onto unrelated sidecars and crash them. Adapters for engines whose canonical
// container name differs from vLLM's (e.g. SGLang) call this with their own
// name; [EngineContainerIndex] is the vLLM-named convenience wrapper.
func EngineContainerIndexNamed(pod *corev1.PodSpec, name string) (int, error) {
	for i := range pod.Containers {
		if pod.Containers[i].Name == name {
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
	return -1, fmt.Errorf("inject engine config: pod has %d containers %v but none is named %q; injecting engine flags into unrelated sidecars would crash them — name the engine container %q",
		len(pod.Containers), names, name, name)
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
	return prefixScheme(endpoint, "lm://")
}

// MooncakeStoreRemoteURL prefixes an engine-agnostic host:port endpoint with
// the LMCache mooncakestore:// remote-store scheme — the Mooncake analog of
// lm://. host:port is the Mooncake master's address (the controller-resolved
// Service DNS the reconciler publishes into status.endpoint); LMCache's
// MooncakestoreConnectorAdapter parses this scheme and connects the engine's
// LMCache connector to the master there. Like [LMCacheRemoteURL] it is
// idempotent — an endpoint already carrying the scheme is normalised to a
// single lower-case `mooncakestore://` prefix rather than doubled — so a
// re-injection produces the same value and the merge stays a no-op. The host
// portion is preserved verbatim (DNS is case-insensitive; rewriting
// operator-typed casing is not this helper's job).
func MooncakeStoreRemoteURL(endpoint string) string {
	return prefixScheme(endpoint, "mooncakestore://")
}

// prefixScheme returns endpoint with scheme guaranteed as a single, lower-case
// prefix: an endpoint that already starts with the scheme (case-insensitively)
// is normalised to the lower-case form rather than double-prefixed; otherwise
// scheme is prepended. Shared by [LMCacheRemoteURL] and
// [MooncakeStoreRemoteURL] so both LMCache-compatible schemes apply the exact
// same idempotent rule.
func prefixScheme(endpoint, scheme string) string {
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

// ValidateLMCacheEndpoint reports whether s is a usable input to
// [LMCacheRemoteURL] — i.e. whether the resulting LMCACHE_REMOTE_URL the
// engine wire would inject is well-formed. Returns nil on success, or an
// error whose message names the specific shape problem.
//
// The contract surface (must match the validating admission webhook and
// the API/design docs):
//
//   - leading/trailing whitespace is trimmed (operator friendliness);
//   - allowed shapes: bare `host:port` or explicit `lm://host:port`;
//   - host AND port are both required and both non-empty;
//   - other URI schemes (`http://`, `https://`, …) are rejected (the
//     adapter would otherwise concatenate `lm://` onto the leading scheme
//     and produce an unparseable URL);
//   - path/query/fragment components are rejected (the LMCache connector
//     speaks TCP and would silently drop them);
//   - unbracketed IPv6 literals are rejected (`[::1]:8200` is required —
//     without brackets the host/port boundary is ambiguous);
//   - embedded whitespace or control characters inside the trimmed value
//     are rejected (they would inject a malformed LMCACHE_REMOTE_URL the
//     engine connector refuses at startup; also defence-in-depth against
//     control-char injection into anything that might later template the
//     value).
//
// Shared between admission (which wraps the error in a field.Invalid),
// the C2 reconciler (which degrades Ready=False on invalid stored
// values), and the pod-mutating webhook (which fails open on invalid
// stored values so the engine pod admits unwired rather than crashing).
// Centralising the rule here means a future tightening only needs to
// touch one place to ripple to all three layers.
func ValidateLMCacheEndpoint(s string) error {
	raw := strings.TrimSpace(s)
	if raw == "" {
		return fmt.Errorf("endpoint is empty")
	}
	if strings.ContainsFunc(raw, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}) {
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

// splitLMCacheHostPort parses a host:port string into its host and port
// halves with bracket-aware IPv6 handling. Returns (host, port, hasPort)
// so callers can tell apart `cache` (no port → hasPort=false) from
// `cache:` (empty port → hasPort=true, port=""). IPv6 literals MUST be
// bracketed (`[::1]:8200`); an unbracketed multi-colon string is
// rejected as malformed. See [ValidateLMCacheEndpoint] for the contract.
func splitLMCacheHostPort(s string) (host, port string, hasPort bool) {
	if s == "" {
		return "", "", false
	}
	if strings.HasPrefix(s, "[") {
		end := strings.Index(s, "]")
		if end <= 1 {
			return "", "", false
		}
		host = s[1:end]
		tail := s[end+1:]
		if tail == "" {
			return host, "", false
		}
		if !strings.HasPrefix(tail, ":") {
			// Unexpected suffix after the bracketed host (e.g. `[::1]junk`).
			return "", "", false
		}
		port = tail[1:]
		// The port half cannot itself contain a colon — `[::1]:8200:bad`
		// would otherwise pass with port="8200:bad" and inject an
		// invalid LMCACHE_REMOTE_URL=lm://[::1]:8200:bad. The bracketed
		// form is the canonical shape precisely because it makes the
		// host/port boundary unambiguous; reject anything that tries to
		// smuggle an extra colon past it.
		if strings.Contains(port, ":") {
			return "", "", false
		}
		return host, port, true
	}
	// Unbracketed multi-colon string is almost certainly an unbracketed
	// IPv6 literal — refuse rather than guess at the host/port split.
	if strings.Count(s, ":") > 1 {
		return "", "", false
	}
	if i := strings.LastIndex(s, ":"); i >= 0 {
		return s[:i], s[i+1:], true
	}
	return s, "", false
}

// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	runtimeadapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

const (
	vllmLMCacheMPConnectorProfile = "vllm-lmcache-mp-v1"
	vllmLMCacheMPClientVersion    = "0.5.3"

	vllmLMCacheMPConnectorName       = "LMCacheMPConnector"
	vllmLMCacheMPConnectorModulePath = "lmcache.integration.vllm.lmcache_mp_connector"
	vllmDisableHybridKVCacheArg      = "--disable-hybrid-kv-cache-manager"
)

// vllmLMCacheMPAdapter is the typed PodLocal vLLM adapter. It embeds the
// legacy adapter only to reuse engine-neutral observation and kernel-check
// providers; selection and engine injection are implemented independently so
// the legacy LMCacheConnectorV1/IP wire cannot leak into the MP path.
type vllmLMCacheMPAdapter struct {
	vllmLMCacheAdapter
}

// NewVLLMLMCacheMPAdapter returns the explicit typed PodLocal adapter. Register
// it before NewVLLMLMCacheAdapter because both advertise the canonical
// vllm/LMCache pair and the registry selects the first matching adapter.
func NewVLLMLMCacheMPAdapter(subscriber SubscriberConfig) runtimeadapter.KVCacheRuntimeAdapter {
	return vllmLMCacheMPAdapter{vllmLMCacheAdapter: vllmLMCacheAdapter{subscriber: subscriber}}
}

func (vllmLMCacheMPAdapter) Supports(runtime runtimeadapter.RuntimeID, cache *cachev1alpha1.CacheBackend) bool {
	return cache != nil &&
		runtime == runtimeadapter.RuntimeVLLM &&
		cache.Spec.EffectiveCacheType() == cachev1alpha1.CacheBackendTypeLMCache &&
		cache.Spec.LMCache != nil &&
		cache.Spec.LMCache.Topology == cachev1alpha1.LMCacheTopologyPodLocal
}

func (vllmLMCacheMPAdapter) SupportsBinding(binding *backendadapter.Binding) bool {
	return binding == nil || binding.Protocol == backendadapter.ProtocolRESP
}

func (vllmLMCacheMPAdapter) ConnectorRequirement(*cachev1alpha1.CacheBackend) runtimeadapter.LMCacheConnectorRequirement {
	return runtimeadapter.LMCacheConnectorRequirement{
		Profile:       vllmLMCacheMPConnectorProfile,
		ClientVersion: vllmLMCacheMPClientVersion,
	}
}

// ValidateMPEnginePod rejects only constraints that can be classified from the
// concrete Pod. The pinned LMCache connector fixes one MP server per vLLM
// instance, and the initial production profile does not claim pipeline or
// multi-process data parallelism. Tensor parallelism remains valid and is GPU
// exercised at TP=1/2 before this phase exits.
func (vllmLMCacheMPAdapter) ValidateMPEnginePod(pod *corev1.Pod, cache *cachev1alpha1.CacheBackend) error {
	if pod == nil {
		return fmt.Errorf("vLLM LMCache MP engine pod is nil")
	}
	if cache == nil || cache.Spec.LMCache == nil {
		return fmt.Errorf("vLLM LMCache MP CacheBackend configuration is missing")
	}
	lm := cache.Spec.LMCache
	if lm.Topology != cachev1alpha1.LMCacheTopologyPodLocal {
		return fmt.Errorf("vLLM LMCache MP topology %q is not implemented; want %q",
			lm.Topology, cachev1alpha1.LMCacheTopologyPodLocal)
	}
	if lm.PodLocal == nil || lm.PodLocal.Server == nil {
		return fmt.Errorf("vLLM LMCache PodLocal server configuration is missing")
	}
	engineIndex, err := EngineContainerIndexNamed(&pod.Spec, EngineContainerName)
	if err != nil {
		return err
	}
	args := pod.Spec.Containers[engineIndex].Args
	if _, err := vllmPositiveParallelSize(args, []string{"--tensor-parallel-size", "-tp"}, 1); err != nil {
		return fmt.Errorf("vLLM LMCache MP tensor parallelism: %w", err)
	}
	pp, err := vllmPositiveParallelSize(args, []string{"--pipeline-parallel-size", "-pp"}, 1)
	if err != nil {
		return fmt.Errorf("vLLM LMCache MP pipeline parallelism: %w", err)
	}
	if pp != 1 {
		return fmt.Errorf("vLLM LMCache MP pipeline parallel size %d is not supported by the initial PodLocal profile; use 1", pp)
	}
	dp, err := vllmPositiveParallelSize(args, []string{"--data-parallel-size", "-dp"}, 1)
	if err != nil {
		return fmt.Errorf("vLLM LMCache MP data parallelism: %w", err)
	}
	if dp != 1 {
		return fmt.Errorf("vLLM LMCache MP data parallel size %d is not supported by the initial PodLocal profile; use 1", dp)
	}
	for _, flag := range []string{
		"--data-parallel-rank",
		"--data-parallel-start-rank",
		"--data-parallel-size-local",
		"--data-parallel-address",
		"--data-parallel-rpc-port",
	} {
		if hasArg(args, flag) {
			return fmt.Errorf("vLLM LMCache MP multi-process data parallel flag %s is not supported by the initial PodLocal profile", flag)
		}
	}
	if err := validateVLLMBooleanArg(args, vllmDisableHybridKVCacheArg); err != nil {
		return err
	}
	values, malformed := argValues(args, defaultEngineKVTransferConfigArg)
	if malformed || len(values) > 1 {
		return fmt.Errorf("vLLM LMCache MP %s must appear at most once with one JSON value", defaultEngineKVTransferConfigArg)
	}
	return nil
}

func vllmPositiveParallelSize(args, flags []string, fallback int64) (int64, error) {
	var values []string
	var seenFlags []string
	for index := 0; index < len(args); index++ {
		arg := args[index]
		for _, flag := range flags {
			switch {
			case arg == flag:
				if index+1 >= len(args) || strings.HasPrefix(args[index+1], "-") {
					return 0, fmt.Errorf("%s is malformed; declare one positive integer value", flag)
				}
				values = append(values, args[index+1])
				seenFlags = append(seenFlags, flag)
				index++
			case strings.HasPrefix(arg, flag+"="):
				value := strings.TrimPrefix(arg, flag+"=")
				if value == "" {
					return 0, fmt.Errorf("%s is malformed; declare one positive integer value", flag)
				}
				values = append(values, value)
				seenFlags = append(seenFlags, flag)
			}
		}
	}
	if len(values) == 0 {
		return fallback, nil
	}
	if len(values) > 1 {
		return 0, fmt.Errorf("parallel-size aliases are duplicated: %s", strings.Join(seenFlags, ", "))
	}
	value, err := strconv.ParseInt(values[0], 10, 32)
	if err != nil || value < 1 {
		return 0, fmt.Errorf("%s=%q must be a positive integer", seenFlags[0], values[0])
	}
	return value, nil
}

func validateVLLMBooleanArg(args []string, flag string) error {
	count := 0
	for index, arg := range args {
		switch {
		case arg == flag:
			count++
			if index+1 < len(args) {
				next := strings.ToLower(args[index+1])
				if next == "true" || next == "false" || next == "0" || next == "1" {
					return fmt.Errorf("vLLM LMCache MP %s is a boolean flag and must not carry value %q", flag, args[index+1])
				}
			}
		case strings.HasPrefix(arg, flag+"="):
			return fmt.Errorf("vLLM LMCache MP %s is a boolean flag and must not carry a value", flag)
		}
	}
	if count > 1 {
		return fmt.Errorf("vLLM LMCache MP %s is duplicated", flag)
	}
	return nil
}

type vllmMPKVTransferConfig struct {
	Connector       string                     `json:"kv_connector"`
	ConnectorModule string                     `json:"kv_connector_module_path"`
	Role            string                     `json:"kv_role"`
	ExtraConfig     vllmMPConnectorExtraConfig `json:"kv_connector_extra_config"`
}

type vllmMPConnectorExtraConfig struct {
	Host string `json:"lmcache.mp.host"`
	Port string `json:"lmcache.mp.port"`
}

func vllmMPKVTransferConfigJSON(role cachev1alpha1.CacheBackendIntegrationRole, port int32) (string, error) {
	kvRole := ""
	switch role {
	case cachev1alpha1.CacheBackendIntegrationRoleReadOnly:
		kvRole = kvRoleConsumer
	case cachev1alpha1.CacheBackendIntegrationRoleWriteOnly:
		kvRole = kvRoleProducer
	case "", cachev1alpha1.CacheBackendIntegrationRoleReadWrite:
		kvRole = kvRoleBoth
	default:
		return "", fmt.Errorf("vLLM LMCache MP integration role %q is unsupported", role)
	}
	raw, err := json.Marshal(vllmMPKVTransferConfig{
		Connector:       vllmLMCacheMPConnectorName,
		ConnectorModule: vllmLMCacheMPConnectorModulePath,
		Role:            kvRole,
		ExtraConfig: vllmMPConnectorExtraConfig{
			Host: "tcp://127.0.0.1",
			Port: strconv.FormatInt(int64(port), 10),
		},
	})
	if err != nil {
		return "", fmt.Errorf("marshal vLLM LMCache MP connector config: %w", err)
	}
	return string(raw), nil
}

func (vllmLMCacheMPAdapter) InjectEngineConfig(pod *corev1.PodSpec, binding *backendadapter.Binding, cache *cachev1alpha1.CacheBackend) error {
	if err := validateInjectPodCacheInputs(pod, cache, "engine"); err != nil {
		return err
	}
	lm := cache.Spec.LMCache
	if lm == nil || lm.Topology != cachev1alpha1.LMCacheTopologyPodLocal || lm.PodLocal == nil || lm.PodLocal.Server == nil {
		return fmt.Errorf("inject vLLM LMCache MP: typed PodLocal server configuration is required")
	}
	if !(vllmLMCacheMPAdapter{}).SupportsBinding(binding) {
		return fmt.Errorf("vLLM LMCache MP adapter does not support remote binding protocol %q", binding.Protocol)
	}
	server := lm.PodLocal.Server
	configJSON, err := vllmMPKVTransferConfigJSON(IntegrationRole(cache), server.Port)
	if err != nil {
		return err
	}

	work := pod.DeepCopy()
	if _, err := renderLMCachePodLocalServer(work, EngineContainerName, lmCacheMPServerConfig{
		Image:             server.Image,
		Port:              server.Port,
		ChunkSizeTokens:   effectiveLMCacheChunkSize(lm),
		L1Capacity:        server.L1Capacity,
		MaxWorkers:        server.MaxWorkers,
		Resources:         server.Resources,
		Binding:           binding,
		WriteClientConfig: false,
	}); err != nil {
		return err
	}
	engineIndex, err := EngineContainerIndexNamed(work, EngineContainerName)
	if err != nil {
		return err
	}
	engine := &work.Containers[engineIndex]
	engine.Args = UpsertArgPair(engine.Args, defaultEngineKVTransferConfigArg, configJSON)
	engine.Args = UpsertFlag(engine.Args, vllmDisableHybridKVCacheArg)
	for _, name := range []string{
		EnvLMCacheRemoteURL,
		EnvLMCacheRemoteSerde,
		EnvLMCacheChunkSize,
		EnvLMCacheLocalCPU,
		EnvLMCacheMaxLocalCPU,
	} {
		engine.Env = removeEnv(engine.Env, name)
	}
	engine.Env = removeEnv(engine.Env, EnvPythonHashSeed)
	engine.Env = append(engine.Env, corev1.EnvVar{Name: EnvPythonHashSeed, Value: defaultPythonHashSeed})
	engine.Env = removeEnv(engine.Env, EnvInferenceCacheFailOpen)
	engine.Env = append(engine.Env, corev1.EnvVar{Name: EnvInferenceCacheFailOpen, Value: FailOpenString(cache)})

	*pod = *work
	return nil
}

func (vllmLMCacheMPAdapter) ReservedArgs() []string {
	return []string{defaultEngineKVTransferConfigArg, vllmDisableHybridKVCacheArg}
}

func (vllmLMCacheMPAdapter) ReservedEnv() []string {
	return []string{EnvPythonHashSeed, EnvInferenceCacheFailOpen}
}

var _ runtimeadapter.KVCacheRuntimeAdapter = vllmLMCacheMPAdapter{}
var _ runtimeadapter.LMCacheMPRuntimeAdapter = vllmLMCacheMPAdapter{}

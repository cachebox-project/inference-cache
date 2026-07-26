package sglang

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	runtimeadapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
)

const (
	SGLangEnableHiCacheArg       = "--enable-hierarchical-cache"
	SGLangHiCacheSizeArg         = "--hicache-size"
	SGLangHiCacheRatioArg        = "--hicache-ratio"
	SGLangHiCacheWritePolicyArg  = "--hicache-write-policy"
	SGLangHiCacheIOBackendArg    = "--hicache-io-backend"
	SGLangHiCacheMemoryLayoutArg = "--hicache-mem-layout"
)

func hiCacheReservedArgs() []string {
	return []string{
		SGLangEnableHiCacheArg,
		SGLangHiCacheSizeArg,
		SGLangHiCacheRatioArg,
		SGLangHiCacheWritePolicyArg,
		SGLangHiCacheIOBackendArg,
		SGLangHiCacheMemoryLayoutArg,
	}
}

type resolvedHiCacheConfig struct {
	sizeGB       *int32
	ratio        string
	writePolicy  cachev1alpha1.SGLangHiCacheWritePolicy
	ioBackend    cachev1alpha1.SGLangHiCacheIOBackend
	memoryLayout cachev1alpha1.SGLangHiCacheMemoryLayout
}

// ValidateHiCacheBackend validates the contract again at the adapter boundary.
// CacheBackend admission normally catches these errors first; this guard keeps
// an admission-bypassed object from producing a partially configured Pod.
func ValidateHiCacheBackend(cache *cachev1alpha1.CacheBackend) error {
	_, err := resolveHiCacheConfig(cache)
	return err
}

func resolveHiCacheConfig(cache *cachev1alpha1.CacheBackend) (resolvedHiCacheConfig, error) {
	if cache == nil {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: cache is nil")
	}
	if cache.Spec.Type != cachev1alpha1.CacheBackendTypeSGLangHiCache {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: backend type must be %q",
			cachev1alpha1.CacheBackendTypeSGLangHiCache)
	}
	if runtimeadapter.ResolveRuntimeID(cache) != runtimeadapter.RuntimeSGLang {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: integration.engine must be sglang")
	}
	if cachev1alpha1.IntegrationMode(cache.Spec.Integration) != cachev1alpha1.CacheBackendIntegrationModeOffload {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: integration.mode must be Offload")
	}
	if cache.Spec.Integration != nil {
		role := cache.Spec.Integration.Role
		if role != "" && role != cachev1alpha1.CacheBackendIntegrationRoleReadWrite {
			return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: integration.role must be ReadWrite")
		}
		if !cachev1alpha1.IntegrationFailOpen(cache.Spec.Integration) {
			return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: integration.failOpen must be true")
		}
	}
	if cache.Spec.Autoscaling != nil {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: autoscaling is unsupported for an engine-local backend")
	}
	if strings.TrimSpace(cache.Spec.Endpoint) != "" {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: spec.endpoint is unsupported for an engine-local backend")
	}
	if cache.Spec.EngineSelector == nil || len(cache.Spec.EngineSelector.MatchLabels) == 0 {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: spec.engineSelector.matchLabels is required")
	}
	for key := range cache.Spec.BackendConfig {
		if key != "model" {
			return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: backendConfig key %q is unsupported; only model is allowed", key)
		}
	}
	if cache.Spec.Integration != nil && cache.Spec.Integration.EngineOverrides != nil {
		overrides := cache.Spec.Integration.EngineOverrides
		for _, arg := range overrides.Args {
			if flag := leadingFlagToken(arg); isHiCacheReservedArg(flag) {
				return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: engine override for reserved argument %q is unsupported", flag)
			}
		}
		for _, flag := range overrides.SuppressArgs {
			if isHiCacheReservedArg(flag) {
				return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: suppression of reserved argument %q is unsupported", flag)
			}
		}
	}
	if cache.Spec.HiCache == nil {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: spec.hiCache is required")
	}
	spec := cache.Spec.HiCache
	if (spec.SizeGB == nil) == (spec.Ratio == "") {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: exactly one of spec.hiCache.sizeGB and ratio must be set")
	}
	if spec.SizeGB != nil && *spec.SizeGB < 1 {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: sizeGB must be at least 1")
	}
	if spec.Ratio != "" {
		ratio, err := strconv.ParseFloat(spec.Ratio, 64)
		if err != nil || ratio <= 0 || math.IsNaN(ratio) || math.IsInf(ratio, 0) {
			return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: ratio must be a finite number greater than zero")
		}
	}
	if !validWritePolicy(spec.WritePolicy) {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: unsupported writePolicy %q", spec.WritePolicy)
	}
	if !validIOBackend(spec.IOBackend) {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: unsupported ioBackend %q", spec.IOBackend)
	}
	if !validMemoryLayout(spec.MemoryLayout) {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: unsupported memoryLayout %q", spec.MemoryLayout)
	}
	return resolvedHiCacheConfig{
		sizeGB:       spec.SizeGB,
		ratio:        spec.Ratio,
		writePolicy:  spec.WritePolicy,
		ioBackend:    spec.IOBackend,
		memoryLayout: spec.MemoryLayout,
	}, nil
}

func validWritePolicy(value cachev1alpha1.SGLangHiCacheWritePolicy) bool {
	switch value {
	case "",
		cachev1alpha1.SGLangHiCacheWriteBack,
		cachev1alpha1.SGLangHiCacheWriteThrough,
		cachev1alpha1.SGLangHiCacheWriteThroughSelective:
		return true
	default:
		return false
	}
}

func validIOBackend(value cachev1alpha1.SGLangHiCacheIOBackend) bool {
	switch value {
	case "",
		cachev1alpha1.SGLangHiCacheIODirect,
		cachev1alpha1.SGLangHiCacheIOKernel,
		cachev1alpha1.SGLangHiCacheIOKernelAscend:
		return true
	default:
		return false
	}
}

func validMemoryLayout(value cachev1alpha1.SGLangHiCacheMemoryLayout) bool {
	switch value {
	case "",
		cachev1alpha1.SGLangHiCacheMemoryLayerFirst,
		cachev1alpha1.SGLangHiCacheMemoryPageFirst,
		cachev1alpha1.SGLangHiCacheMemoryPageFirstDirect,
		cachev1alpha1.SGLangHiCacheMemoryPageFirstKVSplit,
		cachev1alpha1.SGLangHiCacheMemoryPageHead:
		return true
	default:
		return false
	}
}

func leadingFlagToken(arg string) string {
	if !strings.HasPrefix(arg, "-") {
		return ""
	}
	if index := strings.IndexByte(arg, '='); index >= 0 {
		return arg[:index]
	}
	return arg
}

func isHiCacheReservedArg(flag string) bool {
	for _, reserved := range hiCacheReservedArgs() {
		if flag == reserved {
			return true
		}
	}
	return false
}

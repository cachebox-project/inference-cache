// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
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

type sglangHiCacheAdapter struct {
	subscriber SubscriberConfig
}

// NewSGLangHiCacheAdapter returns the endpoint-free adapter for SGLang's native
// host-memory hierarchical cache.
func NewSGLangHiCacheAdapter(subscriber SubscriberConfig) runtimeadapter.KVCacheRuntimeAdapter {
	return sglangHiCacheAdapter{subscriber: subscriber}
}

func (sglangHiCacheAdapter) Supports(runtime runtimeadapter.RuntimeID, cache *cachev1alpha1.CacheBackend) bool {
	return cache != nil &&
		runtime == runtimeadapter.RuntimeSGLang &&
		cache.Spec.Type == cachev1alpha1.CacheBackendTypeSGLangHiCache
}

func (sglangHiCacheAdapter) SupportedPairs() []runtimeadapter.SupportedPair {
	return []runtimeadapter.SupportedPair{{
		Runtime: runtimeadapter.RuntimeSGLang,
		Backend: cachev1alpha1.CacheBackendTypeSGLangHiCache,
	}}
}

func (sglangHiCacheAdapter) SupportsBinding(binding *backendadapter.Binding) bool {
	return binding == nil
}

func (sglangHiCacheAdapter) InjectEngineConfig(pod *corev1.PodSpec, binding *backendadapter.Binding, cache *cachev1alpha1.CacheBackend) error {
	if binding != nil {
		return fmt.Errorf("SGLang HiCache adapter does not support remote binding protocol %q", binding.Protocol)
	}
	cfg, err := resolveHiCacheConfig(cache)
	if err != nil {
		return err
	}
	if pod == nil {
		return fmt.Errorf("inject SGLang HiCache config: pod is nil")
	}
	if len(pod.Containers) == 0 {
		return fmt.Errorf("inject SGLang HiCache config: pod has no containers")
	}
	engineIndex, err := EngineContainerIndexNamed(pod, SGLangEngineContainerName)
	if err != nil {
		return err
	}

	// Validate every collision against the original args before changing a
	// copy. The pod webhook fail-opens on an error, so injection must be
	// all-or-nothing.
	args := pod.Containers[engineIndex].Args
	if hasArg(args, SGLangEnableLMCacheArg) || hasArg(args, SGLangConfigFileArg) {
		return fmt.Errorf("inject SGLang HiCache config: native HiCache conflicts with SGLang LMCache arguments")
	}
	if err := validateEnableArg(args); err != nil {
		return err
	}
	for _, flag := range []string{
		SGLangHiCacheWritePolicyArg,
		SGLangHiCacheIOBackendArg,
		SGLangHiCacheMemoryLayoutArg,
	} {
		values, malformed := argValues(args, flag)
		if malformed || len(values) > 1 {
			return fmt.Errorf("inject SGLang HiCache config: %s is duplicated or malformed", flag)
		}
	}

	type desiredArg struct {
		flag       string
		value      string
		equivalent func(string, string) bool
	}
	desired := make([]desiredArg, 0, 4)
	if cfg.sizeGB != nil {
		desired = append(desired, desiredArg{
			flag:       SGLangHiCacheSizeArg,
			value:      strconv.FormatInt(int64(*cfg.sizeGB), 10),
			equivalent: equivalentInteger,
		})
		if err := rejectPresentArg(args, SGLangHiCacheRatioArg, "conflicts with spec.hiCache.sizeGB"); err != nil {
			return err
		}
	} else {
		desired = append(desired, desiredArg{
			flag:       SGLangHiCacheRatioArg,
			value:      cfg.ratio,
			equivalent: equivalentNumber,
		})
		if err := rejectPresentArg(args, SGLangHiCacheSizeArg, "conflicts with spec.hiCache.ratio"); err != nil {
			return err
		}
	}
	if cfg.writePolicy != "" {
		desired = append(desired, desiredArg{
			flag:       SGLangHiCacheWritePolicyArg,
			value:      string(cfg.writePolicy),
			equivalent: equivalentExact,
		})
	}
	if cfg.ioBackend != "" {
		desired = append(desired, desiredArg{
			flag:       SGLangHiCacheIOBackendArg,
			value:      string(cfg.ioBackend),
			equivalent: equivalentExact,
		})
	}
	if cfg.memoryLayout != "" {
		desired = append(desired, desiredArg{
			flag:       SGLangHiCacheMemoryLayoutArg,
			value:      string(cfg.memoryLayout),
			equivalent: equivalentExact,
		})
	}

	present := make(map[string]bool, len(desired))
	for _, want := range desired {
		values, malformed := argValues(args, want.flag)
		if malformed || len(values) > 1 {
			return fmt.Errorf("inject SGLang HiCache config: %s is duplicated or malformed", want.flag)
		}
		if len(values) == 0 {
			continue
		}
		if !want.equivalent(values[0], want.value) {
			return fmt.Errorf("inject SGLang HiCache config: existing %s=%q conflicts with desired value %q",
				want.flag, values[0], want.value)
		}
		present[want.flag] = true
	}

	work := pod.DeepCopy()
	updated := append([]string(nil), work.Containers[engineIndex].Args...)
	if !hasExactArg(updated, SGLangEnableHiCacheArg) {
		updated = append(updated, SGLangEnableHiCacheArg)
	}
	for _, want := range desired {
		if !present[want.flag] {
			updated = append(updated, want.flag, want.value)
		}
	}
	// SGLang defaults /metrics OFF; enable it so the observation sidecar's stats
	// scraper has an endpoint to read.
	updated = UpsertFlag(updated, SGLangEnableMetricsArg)
	work.Containers[engineIndex].Args = updated
	*pod = *work
	return nil
}

func (sglangHiCacheAdapter) InjectRouterConfig(*corev1.PodSpec, *backendadapter.Binding, *cachev1alpha1.CacheBackend) error {
	return nil
}

func (a sglangHiCacheAdapter) ObservationSidecar(cache *cachev1alpha1.CacheBackend, pod *corev1.Pod) (*corev1.Container, error) {
	return renderSubscriberSidecar(subscriberSidecarParams{
		Config:               a.subscriber,
		Cache:                cache,
		Pod:                  pod,
		HashScheme:           sglangSubscriberHashScheme,
		EngineZMQPortStr:     sglangDefaultEngineZMQPortStr,
		EngineMetricsPortStr: sglangDefaultMetricsPortStr,
		EngineContainerName:  a.EngineContainerName(),
	})
}

func (sglangHiCacheAdapter) ReservedArgs() []string {
	return hiCacheReservedArgs()
}

func hiCacheReservedArgs() []string {
	return []string{
		SGLangEnableHiCacheArg,
		SGLangHiCacheSizeArg,
		SGLangHiCacheRatioArg,
		SGLangHiCacheWritePolicyArg,
		SGLangHiCacheIOBackendArg,
		SGLangHiCacheMemoryLayoutArg,
		// Injected by this adapter (SGLang defaults /metrics off); reserving it
		// stops engineOverrides from suppressing the load-aware stats path.
		SGLangEnableMetricsArg,
	}
}

func (sglangHiCacheAdapter) ReservedEnv() []string { return nil }

func (sglangHiCacheAdapter) EngineContainerName() string {
	return SGLangEngineContainerName
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
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: spec.runtime must be SGLang")
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
	if cache.Spec.EngineSelector == nil || len(cache.Spec.EngineSelector.MatchLabels) == 0 {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: spec.engineSelector.matchLabels is required")
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

func validateEnableArg(args []string) error {
	count := 0
	for _, arg := range args {
		switch {
		case arg == SGLangEnableHiCacheArg:
			count++
		case strings.HasPrefix(arg, SGLangEnableHiCacheArg+"="):
			return fmt.Errorf("inject SGLang HiCache config: %s is a boolean flag and must not carry a value", SGLangEnableHiCacheArg)
		}
	}
	if count > 1 {
		return fmt.Errorf("inject SGLang HiCache config: %s is duplicated", SGLangEnableHiCacheArg)
	}
	return nil
}

func rejectPresentArg(args []string, flag, reason string) error {
	values, malformed := argValues(args, flag)
	if malformed {
		return fmt.Errorf("inject SGLang HiCache config: %s is malformed", flag)
	}
	if len(values) > 0 {
		return fmt.Errorf("inject SGLang HiCache config: existing %s %s", flag, reason)
	}
	return nil
}

func argValues(args []string, flag string) (values []string, malformed bool) {
	prefix := flag + "="
	for index := 0; index < len(args); index++ {
		switch {
		case args[index] == flag:
			if index+1 >= len(args) || strings.HasPrefix(args[index+1], "--") {
				malformed = true
				continue
			}
			values = append(values, args[index+1])
			index++
		case strings.HasPrefix(args[index], prefix):
			value := strings.TrimPrefix(args[index], prefix)
			if value == "" {
				malformed = true
				continue
			}
			values = append(values, value)
		}
	}
	return values, malformed
}

func hasArg(args []string, flag string) bool {
	for _, arg := range args {
		if arg == flag || strings.HasPrefix(arg, flag+"=") {
			return true
		}
	}
	return false
}

func hasExactArg(args []string, flag string) bool {
	for _, arg := range args {
		if arg == flag {
			return true
		}
	}
	return false
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

func equivalentExact(actual, desired string) bool { return actual == desired }

func equivalentInteger(actual, desired string) bool {
	actualValue, actualErr := strconv.ParseInt(actual, 10, 64)
	desiredValue, desiredErr := strconv.ParseInt(desired, 10, 64)
	return actualErr == nil && desiredErr == nil && actualValue == desiredValue
}

func equivalentNumber(actual, desired string) bool {
	actualValue, actualErr := strconv.ParseFloat(actual, 64)
	desiredValue, desiredErr := strconv.ParseFloat(desired, 64)
	return actualErr == nil && desiredErr == nil &&
		!math.IsNaN(actualValue) && !math.IsNaN(desiredValue) &&
		!math.IsInf(actualValue, 0) && !math.IsInf(desiredValue, 0) &&
		actualValue == desiredValue
}

var _ runtimeadapter.KVCacheRuntimeAdapter = sglangHiCacheAdapter{}

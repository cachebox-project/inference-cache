package sglang

import (
	"fmt"
	"math"
	"path"
	"reflect"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
	runtimeadapter "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	"github.com/cachebox-project/inference-cache/pkg/adapters/runtime/internal/enginewire"
)

const (
	SGLangEnableHiCacheArg                = "--enable-hierarchical-cache"
	SGLangHiCacheSizeArg                  = "--hicache-size"
	SGLangHiCacheRatioArg                 = "--hicache-ratio"
	SGLangHiCacheWritePolicyArg           = "--hicache-write-policy"
	SGLangHiCacheIOBackendArg             = "--hicache-io-backend"
	SGLangHiCacheMemoryLayoutArg          = "--hicache-mem-layout"
	SGLangHiCacheStorageBackendArg        = "--hicache-storage-backend"
	SGLangHiCacheStoragePrefetchPolicyArg = "--hicache-storage-prefetch-policy"

	SGLangHiCacheFileStorageDirectoryEnv = "SGLANG_HICACHE_FILE_BACKEND_STORAGE_DIR"
	SGLangHiCacheStorageVolumeName       = "inferencecache-hicache-l3"
)

type hiCacheAdapter struct {
	subscriberImage         string
	policyServerGRPCAddress string
}

// NewHiCacheAdapter returns the endpoint-free adapter for SGLang's native
// hierarchical cache, including its optional file-backed NFS storage tier.
func NewHiCacheAdapter(opts ...runtimeadapter.Option) runtimeadapter.KVCacheRuntimeAdapter {
	var cfg runtimeadapter.Options
	for _, option := range opts {
		option(&cfg)
	}
	return hiCacheAdapter{
		subscriberImage:         cfg.SubscriberImage,
		policyServerGRPCAddress: cfg.PolicyServerGRPCAddress,
	}
}

func (hiCacheAdapter) Supports(runtime runtimeadapter.RuntimeID, cache *cachev1alpha1.CacheBackend) bool {
	return cache != nil &&
		runtime == runtimeadapter.RuntimeSGLang &&
		cache.Spec.Type == cachev1alpha1.CacheBackendTypeSGLangHiCache
}

func (hiCacheAdapter) SupportedPairs() []runtimeadapter.SupportedPair {
	return []runtimeadapter.SupportedPair{{
		Runtime: runtimeadapter.RuntimeSGLang,
		Backend: cachev1alpha1.CacheBackendTypeSGLangHiCache,
	}}
}

func (hiCacheAdapter) RequiresEndpoint() bool { return false }

func (hiCacheAdapter) SupportsRemoteBinding(binding *backendadapter.Binding) bool {
	return binding == nil ||
		(binding.Protocol == backendadapter.ProtocolFile && binding.NFS != nil)
}

func (a hiCacheAdapter) InjectEngineConfigWithBinding(pod *corev1.PodSpec, binding *backendadapter.Binding, cache *cachev1alpha1.CacheBackend) error {
	if !a.SupportsRemoteBinding(binding) {
		return fmt.Errorf("SGLang HiCache adapter does not support remote binding protocol %q", binding.Protocol)
	}
	return injectHiCacheEngineConfig(pod, binding, cache)
}

func (hiCacheAdapter) ResolveCacheServer(cache *cachev1alpha1.CacheBackend) (*corev1.PodSpec, *corev1.Service, error) {
	if err := ValidateHiCacheBackend(cache); err != nil {
		return nil, nil, err
	}
	return nil, nil, nil
}

func (hiCacheAdapter) InjectEngineConfig(pod *corev1.PodSpec, _ string, cache *cachev1alpha1.CacheBackend) error {
	return injectHiCacheEngineConfig(pod, nil, cache)
}

func injectHiCacheEngineConfig(pod *corev1.PodSpec, binding *backendadapter.Binding, cache *cachev1alpha1.CacheBackend) error {
	cfg, err := resolveHiCacheConfig(cache)
	if err != nil {
		return err
	}
	nfsConfigured := cache.Spec.RemoteStorage != nil &&
		cache.Spec.RemoteStorage.Provider == cachev1alpha1.CacheBackendRemoteStorageProviderNFS
	if nfsConfigured != (binding != nil) {
		return fmt.Errorf("inject SGLang HiCache config: remoteStorage.provider=NFS and the file binding must be configured together")
	}
	if pod == nil {
		return fmt.Errorf("inject SGLang HiCache config: pod is nil")
	}
	if len(pod.Containers) == 0 {
		return fmt.Errorf("inject SGLang HiCache config: pod has no containers")
	}
	engineIndex, err := enginewire.EngineContainerIndexNamed(pod, enginewire.SGLangEngineContainerName)
	if err != nil {
		return err
	}

	// Validate every collision against the original args before changing a
	// copy. The pod webhook fail-opens on an error, so injection must be
	// all-or-nothing.
	args := pod.Containers[engineIndex].Args
	if hasArg(args, enginewire.SGLangEnableLMCacheArg) || hasArg(args, enginewire.SGLangConfigFileArg) {
		return fmt.Errorf("inject SGLang HiCache config: native HiCache conflicts with SGLang LMCache arguments")
	}
	if err := validateEnableArg(args); err != nil {
		return err
	}
	for _, flag := range []string{
		SGLangHiCacheWritePolicyArg,
		SGLangHiCacheIOBackendArg,
		SGLangHiCacheMemoryLayoutArg,
		SGLangHiCacheStorageBackendArg,
		SGLangHiCacheStoragePrefetchPolicyArg,
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
	desired := make([]desiredArg, 0, 6)
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
	if binding == nil {
		for _, flag := range []string{SGLangHiCacheStorageBackendArg, SGLangHiCacheStoragePrefetchPolicyArg} {
			if err := rejectPresentArg(args, flag, "requires remoteStorage.provider=NFS"); err != nil {
				return err
			}
		}
	} else {
		desired = append(desired,
			desiredArg{flag: SGLangHiCacheStorageBackendArg, value: "file", equivalent: equivalentExact},
			desiredArg{
				flag:       SGLangHiCacheStoragePrefetchPolicyArg,
				value:      string(cfg.storagePrefetchPolicy),
				equivalent: equivalentExact,
			},
		)
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

	storagePlan, err := planHiCacheNFSWiring(pod, engineIndex, binding)
	if err != nil {
		return err
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
	work.Containers[engineIndex].Args = updated
	if storagePlan.addEnv {
		work.Containers[engineIndex].Env = append(work.Containers[engineIndex].Env, storagePlan.env)
	}
	if storagePlan.addVolumeMount {
		work.Containers[engineIndex].VolumeMounts = append(work.Containers[engineIndex].VolumeMounts, storagePlan.volumeMount)
	}
	if storagePlan.addVolume {
		work.Volumes = append(work.Volumes, storagePlan.volume)
	}
	*pod = *work
	return nil
}

func (hiCacheAdapter) InjectRouterConfig(*corev1.PodSpec, string, *cachev1alpha1.CacheBackend) error {
	return nil
}

func (a hiCacheAdapter) ObservationSidecar(cache *cachev1alpha1.CacheBackend, pod *corev1.Pod) (*corev1.Container, error) {
	return runtimeadapter.RenderSubscriberSidecar(runtimeadapter.SubscriberSidecarParams{
		Image:            a.subscriberImage,
		ServerAddr:       a.policyServerGRPCAddress,
		Cache:            cache,
		Pod:              pod,
		HashScheme:       subscriberHashScheme,
		EngineZMQPortStr: defaultEngineZMQPortStr,
	})
}

func (hiCacheAdapter) ReservedArgs() []string {
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
		SGLangHiCacheStorageBackendArg,
		SGLangHiCacheStoragePrefetchPolicyArg,
	}
}

func (hiCacheAdapter) ReservedEnv() []string {
	return []string{SGLangHiCacheFileStorageDirectoryEnv}
}

func (hiCacheAdapter) EngineContainerName() string {
	return enginewire.SGLangEngineContainerName
}

type resolvedHiCacheConfig struct {
	sizeGB                *int32
	ratio                 string
	writePolicy           cachev1alpha1.SGLangHiCacheWritePolicy
	ioBackend             cachev1alpha1.SGLangHiCacheIOBackend
	memoryLayout          cachev1alpha1.SGLangHiCacheMemoryLayout
	storagePrefetchPolicy cachev1alpha1.SGLangHiCacheStoragePrefetchPolicy
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
	nfsStorage := cache.Spec.RemoteStorage != nil &&
		cache.Spec.RemoteStorage.Provider == cachev1alpha1.CacheBackendRemoteStorageProviderNFS
	if cache.Spec.Integration != nil {
		role := cache.Spec.Integration.Role
		if role != "" && role != cachev1alpha1.CacheBackendIntegrationRoleReadWrite {
			return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: integration.role must be ReadWrite")
		}
	}
	failOpen := cachev1alpha1.IntegrationFailOpen(cache.Spec.Integration)
	if nfsStorage && failOpen {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: integration.failOpen must be false with remoteStorage.provider=NFS because the inline NFS volume is a Pod startup dependency")
	}
	if !nfsStorage && !failOpen {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: integration.failOpen must be true")
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
		for _, env := range overrides.Env {
			if env.Name == SGLangHiCacheFileStorageDirectoryEnv {
				return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: engine override for reserved environment variable %q is unsupported", env.Name)
			}
		}
		for _, name := range overrides.SuppressEnv {
			if name == SGLangHiCacheFileStorageDirectoryEnv {
				return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: suppression of reserved environment variable %q is unsupported", name)
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
	if !validStoragePrefetchPolicy(spec.StoragePrefetchPolicy) {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: unsupported storagePrefetchPolicy %q", spec.StoragePrefetchPolicy)
	}
	if nfsStorage && spec.StoragePrefetchPolicy == "" {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: storagePrefetchPolicy is required with remoteStorage.provider=NFS")
	}
	if !nfsStorage && spec.StoragePrefetchPolicy != "" {
		return resolvedHiCacheConfig{}, fmt.Errorf("resolve SGLang HiCache config: storagePrefetchPolicy requires remoteStorage.provider=NFS")
	}
	return resolvedHiCacheConfig{
		sizeGB:                spec.SizeGB,
		ratio:                 spec.Ratio,
		writePolicy:           spec.WritePolicy,
		ioBackend:             spec.IOBackend,
		memoryLayout:          spec.MemoryLayout,
		storagePrefetchPolicy: spec.StoragePrefetchPolicy,
	}, nil
}

func validStoragePrefetchPolicy(value cachev1alpha1.SGLangHiCacheStoragePrefetchPolicy) bool {
	switch value {
	case "",
		cachev1alpha1.SGLangHiCacheStoragePrefetchBestEffort,
		cachev1alpha1.SGLangHiCacheStoragePrefetchWaitComplete,
		cachev1alpha1.SGLangHiCacheStoragePrefetchTimeout:
		return true
	default:
		return false
	}
}

type hiCacheNFSWiringPlan struct {
	addEnv         bool
	env            corev1.EnvVar
	addVolume      bool
	volume         corev1.Volume
	addVolumeMount bool
	volumeMount    corev1.VolumeMount
}

func planHiCacheNFSWiring(
	pod *corev1.PodSpec,
	engineIndex int,
	binding *backendadapter.Binding,
) (hiCacheNFSWiringPlan, error) {
	if binding == nil {
		return hiCacheNFSWiringPlan{}, nil
	}
	if binding.Protocol != backendadapter.ProtocolFile || binding.NFS == nil {
		return hiCacheNFSWiringPlan{}, fmt.Errorf("inject SGLang HiCache config: file binding requires NFS mount configuration")
	}
	nfs := binding.NFS
	if err := backendadapter.ValidateNFSServer(nfs.Server); err != nil {
		return hiCacheNFSWiringPlan{}, fmt.Errorf("inject SGLang HiCache config: %w", err)
	}
	if err := validateHiCachePath(nfs.Path, "remoteStorage.nfs.path", true); err != nil {
		return hiCacheNFSWiringPlan{}, err
	}
	if err := validateHiCachePath(nfs.MountPath, "remoteStorage.nfs.mountPath", false); err != nil {
		return hiCacheNFSWiringPlan{}, err
	}

	plan := hiCacheNFSWiringPlan{
		env: corev1.EnvVar{
			Name:  SGLangHiCacheFileStorageDirectoryEnv,
			Value: nfs.MountPath,
		},
		volume: corev1.Volume{
			Name: SGLangHiCacheStorageVolumeName,
			VolumeSource: corev1.VolumeSource{NFS: &corev1.NFSVolumeSource{
				Server: nfs.Server,
				Path:   nfs.Path,
			}},
		},
		volumeMount: corev1.VolumeMount{
			Name:      SGLangHiCacheStorageVolumeName,
			MountPath: nfs.MountPath,
		},
	}

	envCount := 0
	for _, env := range pod.Containers[engineIndex].Env {
		if env.Name != plan.env.Name {
			continue
		}
		envCount++
		if !reflect.DeepEqual(env, plan.env) {
			return hiCacheNFSWiringPlan{}, fmt.Errorf(
				"inject SGLang HiCache config: existing env %s conflicts with remoteStorage.nfs.mountPath",
				plan.env.Name,
			)
		}
	}
	if envCount > 1 {
		return hiCacheNFSWiringPlan{}, fmt.Errorf("inject SGLang HiCache config: env %s is duplicated", plan.env.Name)
	}
	plan.addEnv = envCount == 0

	volumeCount := 0
	for _, volume := range pod.Volumes {
		if volume.Name != plan.volume.Name {
			continue
		}
		volumeCount++
		if !reflect.DeepEqual(volume, plan.volume) {
			return hiCacheNFSWiringPlan{}, fmt.Errorf(
				"inject SGLang HiCache config: volume %q conflicts with remoteStorage.nfs",
				plan.volume.Name,
			)
		}
	}
	if volumeCount > 1 {
		return hiCacheNFSWiringPlan{}, fmt.Errorf("inject SGLang HiCache config: volume %q is duplicated", plan.volume.Name)
	}
	plan.addVolume = volumeCount == 0

	mountCount := 0
	for _, mount := range pod.Containers[engineIndex].VolumeMounts {
		if mount.Name != plan.volumeMount.Name && mount.MountPath != plan.volumeMount.MountPath {
			continue
		}
		mountCount++
		if !reflect.DeepEqual(mount, plan.volumeMount) {
			return hiCacheNFSWiringPlan{}, fmt.Errorf(
				"inject SGLang HiCache config: volume mount name %q or path %q conflicts with remoteStorage.nfs",
				plan.volumeMount.Name,
				plan.volumeMount.MountPath,
			)
		}
	}
	if mountCount > 1 {
		return hiCacheNFSWiringPlan{}, fmt.Errorf("inject SGLang HiCache config: volume mount %q is duplicated", plan.volumeMount.Name)
	}
	plan.addVolumeMount = mountCount == 0
	return plan, nil
}

func validateHiCachePath(value, fieldName string, allowRoot bool) error {
	if strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) ||
		!path.IsAbs(value) || path.Clean(value) != value {
		return fmt.Errorf("inject SGLang HiCache config: %s must be a clean absolute path", fieldName)
	}
	if !allowRoot && value == "/" {
		return fmt.Errorf("inject SGLang HiCache config: %s must not be the container root", fieldName)
	}
	return nil
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

var (
	_ runtimeadapter.KVCacheRuntimeAdapter = hiCacheAdapter{}
	_ runtimeadapter.EndpointRequirement   = hiCacheAdapter{}
)

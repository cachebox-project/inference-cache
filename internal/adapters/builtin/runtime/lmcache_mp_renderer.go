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
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
)

// The PodLocal MP server wire is engine-neutral. Runtime adapters own only the
// engine launch surface that points at the returned client config path.
const (
	lmCacheMPServerContainerName = "lmcache-mp-server"
	lmCacheMPServerManagedEnv    = "INFERENCECACHE_MP_SERVER"
	lmCacheMPServerManagedValue  = "true"

	lmCacheMPConfigVolumeName = "lmcache-mp-config"
	lmCacheMPConfigMountPath  = "/var/run/inference-cache/lmcache"
	lmCacheMPConfigFileName   = "client.yaml"
	lmCacheMPConfigFilePath   = lmCacheMPConfigMountPath + "/" + lmCacheMPConfigFileName

	lmCacheMPShmVolumeName = "lmcache-mp-shm"
	lmCacheMPShmMountPath  = "/dev/shm"

	lmCacheMPServerPortName = "lmcache-mp"
	lmCacheMPHTTPPortName   = "lmcache-http"
	lmCacheMPHTTPPort       = int32(8080)

	lmCacheMPHealthPath = "/healthcheck"

	lmCacheRESPUsernameEnv = "LMCACHE_RESP_USERNAME"
	lmCacheRESPPasswordEnv = "LMCACHE_RESP_PASSWORD"
)

// lmCacheMPServerConfig is the internal, engine-neutral input to the PodLocal
// renderer. It is deliberately narrower than CacheBackend: the SGLang and vLLM
// adapters translate the public API into this model and keep their connector
// launch formats outside this file.
type lmCacheMPServerConfig struct {
	Image           string
	Port            int32
	ChunkSizeTokens int32
	L1Capacity      resource.Quantity
	MaxWorkers      int32
	Resources       corev1.ResourceRequirements
	Binding         *backendadapter.Binding

	// WriteClientConfig asks the native sidecar to create the generic LMCache
	// MP client YAML shared with the engine. SGLang consumes this file. vLLM's
	// connector consumes JSON and will set this false in Phase 4.
	WriteClientConfig bool
}

// renderLMCachePodLocalServer atomically injects the common PodLocal MP server
// wire and returns the generic client-config path when requested. A re-render
// converges resources, image, arguments, probes, and volumes in place.
func renderLMCachePodLocalServer(pod *corev1.PodSpec, engineContainerName string, cfg lmCacheMPServerConfig) (string, error) {
	if pod == nil {
		return "", fmt.Errorf("render LMCache MP server: pod spec is nil")
	}
	engineIndex, err := EngineContainerIndexNamed(pod, engineContainerName)
	if err != nil {
		return "", err
	}
	if err := validateLMCacheMPServerConfig(cfg); err != nil {
		return "", err
	}

	l2Adapter, bindingEnv, err := renderLMCacheMPL2Binding(cfg.Binding)
	if err != nil {
		return "", err
	}

	// Mutate a copy and commit only after every collision/writability guard
	// succeeds. Admission therefore receives either the complete wire or the
	// original PodSpec, never a partially injected serving Pod.
	work := pod.DeepCopy()
	engine := &work.Containers[engineIndex]
	owned := lmCacheMPWireIsOurs(pod)

	if legacy := findContainerByName(work.InitContainers, sglangMPWorkerContainerName); legacy != nil {
		return "", fmt.Errorf("render LMCache MP server: pod already has legacy native sidecar %q; remove the legacy topology-less injection before enabling typed PodLocal", legacy.Name)
	}

	if cfg.WriteClientConfig {
		if existing := mountAtPath(engine.VolumeMounts, lmCacheMPConfigMountPath); existing != nil &&
			!(owned && existing.Name == lmCacheMPConfigVolumeName) {
			return "", fmt.Errorf("render LMCache MP server: engine container already mounts %q from volume %q; that path is reserved for the LMCache MP client config", lmCacheMPConfigMountPath, existing.Name)
		}
		work.Volumes, err = adoptVolume(work.Volumes, corev1.Volume{
			Name: lmCacheMPConfigVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		}, owned)
		if err != nil {
			return "", err
		}
		engine.VolumeMounts = upsertMountByName(engine.VolumeMounts, corev1.VolumeMount{
			Name:      lmCacheMPConfigVolumeName,
			MountPath: lmCacheMPConfigMountPath,
		})
	}

	shmMount := corev1.VolumeMount{Name: lmCacheMPShmVolumeName, MountPath: lmCacheMPShmMountPath}
	if existing := mountAtPath(engine.VolumeMounts, lmCacheMPShmMountPath); existing != nil &&
		!(owned && existing.Name == lmCacheMPShmVolumeName) {
		if err := checkLMCacheMPShmReusable(work.Volumes, *existing); err != nil {
			return "", err
		}
		shmMount = corev1.VolumeMount{
			Name:      existing.Name,
			MountPath: lmCacheMPShmMountPath,
			SubPath:   existing.SubPath,
		}
	} else {
		l1 := cfg.L1Capacity.DeepCopy()
		work.Volumes, err = adoptVolume(work.Volumes, corev1.Volume{
			Name: lmCacheMPShmVolumeName,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
				Medium:    corev1.StorageMediumMemory,
				SizeLimit: &l1,
			}},
		}, owned)
		if err != nil {
			return "", err
		}
		engine.VolumeMounts = upsertMountByName(engine.VolumeMounts, shmMount)
	}

	server, err := lmCacheMPServerContainer(cfg, l2Adapter, bindingEnv, shmMount)
	if err != nil {
		return "", err
	}
	work.InitContainers, err = adoptContainer(work.InitContainers, server, owned)
	if err != nil {
		return "", err
	}

	*pod = *work
	if cfg.WriteClientConfig {
		return lmCacheMPConfigFilePath, nil
	}
	return "", nil
}

func validateLMCacheMPServerConfig(cfg lmCacheMPServerConfig) error {
	switch {
	case strings.TrimSpace(cfg.Image) == "":
		return fmt.Errorf("render LMCache MP server: image is empty")
	case cfg.Port < 1 || cfg.Port > 65535:
		return fmt.Errorf("render LMCache MP server: port %d is outside 1-65535", cfg.Port)
	case cfg.Port == lmCacheMPHTTPPort:
		return fmt.Errorf("render LMCache MP server: MP port %d collides with HTTP health/control port", cfg.Port)
	case cfg.ChunkSizeTokens < 1:
		return fmt.Errorf("render LMCache MP server: chunkSizeTokens must be at least 1")
	case cfg.L1Capacity.Sign() <= 0:
		return fmt.Errorf("render LMCache MP server: l1Capacity must be greater than zero")
	case cfg.MaxWorkers < 1:
		return fmt.Errorf("render LMCache MP server: maxWorkers must be at least 1")
	}
	return nil
}

func lmCacheMPServerContainer(cfg lmCacheMPServerConfig, l2Adapter string, bindingEnv []corev1.EnvVar, shmMount corev1.VolumeMount) (corev1.Container, error) {
	l1GiB, err := quantityAsGiB(cfg.L1Capacity)
	if err != nil {
		return corev1.Container{}, err
	}

	serverArgs := []string{
		"server",
		"--host", "127.0.0.1",
		"--port", strconv.FormatInt(int64(cfg.Port), 10),
		"--http-host", "0.0.0.0",
		"--http-port", strconv.FormatInt(int64(lmCacheMPHTTPPort), 10),
		"--chunk-size", strconv.FormatInt(int64(cfg.ChunkSizeTokens), 10),
		"--l1-size-gb", l1GiB,
		"--eviction-policy", "LRU",
		"--max-workers", strconv.FormatInt(int64(cfg.MaxWorkers), 10),
	}
	if l2Adapter != "" {
		serverArgs = append(serverArgs, "--l2-adapter", l2Adapter)
	}

	command := []string{"lmcache"}
	args := serverArgs
	mounts := []corev1.VolumeMount{shmMount}
	if cfg.WriteClientConfig {
		// Values are passed as positional arguments rather than interpolated into
		// shell source. The shell only writes the SGLang-readable config and then
		// execs the validated CLI argument vector unchanged.
		const script = `set -eu
config_path="$1"
chunk_size="$2"
mp_port="$3"
shift 3
printf 'chunk_size: %s\nmp_host: "127.0.0.1"\nmp_port: %s\n' "$chunk_size" "$mp_port" > "$config_path"
exec "$@"`
		command = []string{"/bin/sh", "-c"}
		args = []string{
			script,
			"inference-cache-lmcache-config",
			lmCacheMPConfigFilePath,
			strconv.FormatInt(int64(cfg.ChunkSizeTokens), 10),
			strconv.FormatInt(int64(cfg.Port), 10),
			"lmcache",
		}
		args = append(args, serverArgs...)
		mounts = append(mounts, corev1.VolumeMount{Name: lmCacheMPConfigVolumeName, MountPath: lmCacheMPConfigMountPath})
	}

	env := []corev1.EnvVar{
		{Name: "NVIDIA_VISIBLE_DEVICES", Value: "all"},
		{Name: lmCacheMPServerManagedEnv, Value: lmCacheMPServerManagedValue},
	}
	for i := range bindingEnv {
		env = UpsertEnv(env, bindingEnv[i])
	}
	always := corev1.ContainerRestartPolicyAlways
	return corev1.Container{
		Name:            lmCacheMPServerContainerName,
		Image:           strings.TrimSpace(cfg.Image),
		ImagePullPolicy: corev1.PullIfNotPresent,
		RestartPolicy:   &always,
		Command:         command,
		Args:            args,
		Env:             env,
		Resources:       *cfg.Resources.DeepCopy(),
		VolumeMounts:    mounts,
		Ports: []corev1.ContainerPort{
			{Name: lmCacheMPServerPortName, ContainerPort: cfg.Port, Protocol: corev1.ProtocolTCP},
			{Name: lmCacheMPHTTPPortName, ContainerPort: lmCacheMPHTTPPort, Protocol: corev1.ProtocolTCP},
		},
		StartupProbe:    lmCacheMPHTTPProbe(3, 40),
		ReadinessProbe:  lmCacheMPHTTPProbe(5, 3),
		LivenessProbe:   lmCacheMPHTTPProbe(10, 3),
		SecurityContext: lmCacheMPServerSecurityContext(nil),
	}, nil
}

func lmCacheMPHTTPProbe(period, failures int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
			Path:   lmCacheMPHealthPath,
			Port:   intstr.FromString(lmCacheMPHTTPPortName),
			Scheme: corev1.URISchemeHTTP,
		}},
		PeriodSeconds:    period,
		FailureThreshold: failures,
		TimeoutSeconds:   2,
	}
}

func renderLMCacheMPL2Binding(binding *backendadapter.Binding) (string, []corev1.EnvVar, error) {
	if binding == nil {
		return "", nil, nil
	}
	if binding.Protocol != backendadapter.ProtocolRESP {
		return "", nil, fmt.Errorf("render LMCache MP server: unsupported remote protocol %q", binding.Protocol)
	}
	if binding.Redis != nil {
		if binding.Redis.TLS != nil {
			return "", nil, fmt.Errorf("render LMCache MP server: LMCache 0.5.3 resp adapter does not support TLS")
		}
		if binding.Redis.Database != nil {
			return "", nil, fmt.Errorf("render LMCache MP server: LMCache 0.5.3 resp adapter does not support database selection")
		}
	}

	host, portText, ok := splitLMCacheHostPort(strings.TrimSpace(binding.Endpoint))
	if !ok || host == "" || portText == "" {
		return "", nil, fmt.Errorf("render LMCache MP server: Redis endpoint %q is not host:port", binding.Endpoint)
	}
	port, err := strconv.ParseInt(portText, 10, 32)
	if err != nil || port < 1 || port > 65535 {
		return "", nil, fmt.Errorf("render LMCache MP server: Redis endpoint %q has invalid port %q", binding.Endpoint, portText)
	}
	payload := map[string]any{"type": "resp", "host": host, "port": port}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", nil, fmt.Errorf("render LMCache MP server: marshal RESP adapter: %w", err)
	}

	var env []corev1.EnvVar
	if binding.Redis != nil && binding.Redis.Authentication != nil {
		auth := binding.Redis.Authentication
		if auth.Username != nil {
			env = append(env, corev1.EnvVar{Name: lmCacheRESPUsernameEnv, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: auth.Username.DeepCopy()}})
		}
		env = append(env, corev1.EnvVar{Name: lmCacheRESPPasswordEnv, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: auth.Password.DeepCopy()}})
	}
	return string(raw), env, nil
}

func quantityAsGiB(q resource.Quantity) (string, error) {
	bytes := q.Value()
	if bytes <= 0 {
		return "", fmt.Errorf("render LMCache MP server: l1Capacity must be greater than zero")
	}
	const gib = int64(1 << 30)
	whole := bytes / gib
	remainder := bytes % gib
	if remainder == 0 {
		return strconv.FormatInt(whole, 10), nil
	}
	// LMCache parses this argument as float GB and multiplies by 2^30. Keep
	// enough decimal precision to preserve ordinary Kubernetes quantities.
	value := float64(bytes) / float64(gib)
	return strconv.FormatFloat(value, 'f', -1, 64), nil
}

func lmCacheMPWireIsOurs(pod *corev1.PodSpec) bool {
	if pod == nil {
		return false
	}
	c := findContainerByName(pod.InitContainers, lmCacheMPServerContainerName)
	if c == nil {
		return false
	}
	for i := range c.Env {
		if c.Env[i].Name == lmCacheMPServerManagedEnv && c.Env[i].Value == lmCacheMPServerManagedValue {
			return true
		}
	}
	return false
}

func findContainerByName(containers []corev1.Container, name string) *corev1.Container {
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i]
		}
	}
	return nil
}

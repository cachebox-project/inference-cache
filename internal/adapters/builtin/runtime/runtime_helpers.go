// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

const (
	EngineContainerName              = "vllm"
	EnvInferenceCacheFailOpen        = "INFERENCECACHE_FAIL_OPEN"
	EnvPythonHashSeed                = "PYTHONHASHSEED"
	defaultPythonHashSeed            = "0"
	defaultEngineKVTransferConfigArg = "--kv-transfer-config"
)

func UpsertFlag(args []string, flag string) []string {
	for _, arg := range args {
		if arg == flag {
			return args
		}
	}
	return append(args, flag)
}

func validateInjectPodCacheInputs(pod *corev1.PodSpec, cache *cachev1alpha1.CacheBackend, role string) error {
	if pod == nil {
		return fmt.Errorf("inject %s config: pod is nil", role)
	}
	if cache == nil {
		return fmt.Errorf("inject %s config: cache is nil", role)
	}
	if len(pod.Containers) == 0 {
		return fmt.Errorf("inject %s config: pod has no containers", role)
	}
	return nil
}

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

func FailOpenString(cache *cachev1alpha1.CacheBackend) string {
	if cachev1alpha1.IntegrationFailOpen(cache.Spec.Integration) {
		return "true"
	}
	return "false"
}

func IntegrationRole(cache *cachev1alpha1.CacheBackend) cachev1alpha1.CacheBackendIntegrationRole {
	if cache.Spec.Integration == nil || cache.Spec.Integration.Role == "" {
		return cachev1alpha1.CacheBackendIntegrationRoleReadWrite
	}
	return cache.Spec.Integration.Role
}

func UpsertArgPair(args []string, flag, value string) []string {
	prefix := flag + "="
	for i, arg := range args {
		switch {
		case arg == flag:
			if i+1 < len(args) {
				args[i+1] = value
				return args
			}
			return append(args, value)
		case strings.HasPrefix(arg, prefix):
			args = append(args, "")
			copy(args[i+2:], args[i+1:])
			args[i] = flag
			args[i+1] = value
			return args
		}
	}
	return append(args, flag, value)
}

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

func removeEnv(env []corev1.EnvVar, name string) []corev1.EnvVar {
	out := env[:0]
	for _, entry := range env {
		if entry.Name != name {
			out = append(out, entry)
		}
	}
	return out
}

func mountAtPath(mounts []corev1.VolumeMount, path string) *corev1.VolumeMount {
	for i := range mounts {
		if mounts[i].MountPath == path {
			return &mounts[i]
		}
	}
	return nil
}

func lmCacheMPServerSecurityContext(engine *corev1.SecurityContext) *corev1.SecurityContext {
	no := false
	securityContext := &corev1.SecurityContext{
		AllowPrivilegeEscalation: &no,
		Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		SeccompProfile:           &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
	}
	if engine != nil {
		securityContext.RunAsNonRoot = engine.RunAsNonRoot
		securityContext.RunAsUser = engine.RunAsUser
		securityContext.RunAsGroup = engine.RunAsGroup
	}
	return securityContext
}

func adoptContainer(containers []corev1.Container, want corev1.Container, owned bool) ([]corev1.Container, error) {
	for i := range containers {
		if containers[i].Name != want.Name {
			continue
		}
		if !owned {
			return nil, fmt.Errorf("inject engine config: pod already has a container named %q that this adapter did not render; that name is reserved for the LMCache MP native sidecar — rename your container", want.Name)
		}
		containers[i] = want
		return containers, nil
	}
	return append(containers, want), nil
}

func adoptVolume(volumes []corev1.Volume, want corev1.Volume, owned bool) ([]corev1.Volume, error) {
	for i := range volumes {
		if volumes[i].Name != want.Name {
			continue
		}
		if !owned {
			return nil, fmt.Errorf("inject engine config: pod already has a volume named %q that this adapter did not render; that name is reserved for the LMCache MP wire — rename your volume", want.Name)
		}
		volumes[i] = want
		return volumes, nil
	}
	return append(volumes, want), nil
}

func checkLMCacheMPShmReusable(volumes []corev1.Volume, mount corev1.VolumeMount) error {
	if mount.ReadOnly {
		return fmt.Errorf("inject engine config: engine container mounts %q read-only (volume %q), but the LMCache MP data path writes there — drop readOnly or mount it elsewhere", lmCacheMPShmMountPath, mount.Name)
	}
	if mount.SubPathExpr != "" {
		return fmt.Errorf("inject engine config: engine container mounts %q with subPathExpr %q (volume %q); the LMCache MP server cannot reproduce that expansion in its own env — use a literal subPath, or mount %[1]q without it", lmCacheMPShmMountPath, mount.SubPathExpr, mount.Name)
	}
	for i := range volumes {
		if volumes[i].Name != mount.Name {
			continue
		}
		source := volumes[i].VolumeSource
		readOnly := source.ConfigMap != nil || source.Secret != nil || source.DownwardAPI != nil || source.Projected != nil ||
			(source.PersistentVolumeClaim != nil && source.PersistentVolumeClaim.ReadOnly) ||
			(source.CSI != nil && source.CSI.ReadOnly != nil && *source.CSI.ReadOnly) ||
			(source.NFS != nil && source.NFS.ReadOnly)
		if readOnly {
			return fmt.Errorf("inject engine config: engine container mounts %q from read-only volume %q, but the LMCache MP data path writes there — use an emptyDir (medium: Memory) instead", lmCacheMPShmMountPath, mount.Name)
		}
		return nil
	}
	return nil
}

func upsertMountByName(mounts []corev1.VolumeMount, want corev1.VolumeMount) []corev1.VolumeMount {
	for i := range mounts {
		if mounts[i].Name == want.Name {
			mounts[i] = want
			return mounts
		}
	}
	return append(mounts, want)
}

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
		if !strings.HasPrefix(tail, ":") || strings.Contains(tail[1:], ":") {
			return "", "", false
		}
		return host, tail[1:], true
	}
	if strings.Count(s, ":") > 1 {
		return "", "", false
	}
	if i := strings.LastIndex(s, ":"); i >= 0 {
		return s[:i], s[i+1:], true
	}
	return s, "", false
}

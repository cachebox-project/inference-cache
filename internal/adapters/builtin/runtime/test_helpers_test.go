// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import corev1 "k8s.io/api/core/v1"

import backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"

const testLMCacheServerImage = "registry.example/lmcache@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const testNodeLocalCleanupImage = "registry.example/inference-cache-shm-cleanup@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func findVolume(volumes []corev1.Volume, name string) *corev1.Volume {
	for i := range volumes {
		if volumes[i].Name == name {
			return &volumes[i]
		}
	}
	return nil
}

func containsArg(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}

func respBinding(endpoint string) *backendadapter.Binding {
	return &backendadapter.Binding{Protocol: backendadapter.ProtocolRESP, Endpoint: endpoint}
}

func lookupEnv(env []corev1.EnvVar, name string) (string, bool) {
	for i := range env {
		if env[i].Name == name {
			return env[i].Value, true
		}
	}
	return "", false
}

func findInitContainer(containers []corev1.Container, name string) *corev1.Container {
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i]
		}
	}
	return nil
}

func hasMount(mounts []corev1.VolumeMount, name string) bool {
	for i := range mounts {
		if mounts[i].Name == name {
			return true
		}
	}
	return false
}

func envHasFieldRef(env []corev1.EnvVar, name, path string) bool {
	for i := range env {
		ref := env[i].ValueFrom
		if env[i].Name == name && ref != nil && ref.FieldRef != nil && ref.FieldRef.FieldPath == path {
			return true
		}
	}
	return false
}

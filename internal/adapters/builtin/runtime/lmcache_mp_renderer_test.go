// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
)

const testMPServerImage = "registry.example.com/lmcache@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func testLMCacheMPConfig() lmCacheMPServerConfig {
	return lmCacheMPServerConfig{
		Image:             testMPServerImage,
		Port:              6500,
		ChunkSizeTokens:   256,
		L1Capacity:        resource.MustParse("4Gi"),
		MaxWorkers:        3,
		WriteClientConfig: true,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("5Gi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse("2"),
				corev1.ResourceMemory:           resource.MustParse("6Gi"),
				corev1.ResourceEphemeralStorage: resource.MustParse("1Gi"),
			},
		},
	}
}

func TestRenderLMCachePodLocalServerGolden(t *testing.T) {
	pod := &corev1.PodSpec{Containers: []corev1.Container{{
		Name:         "engine",
		Image:        "engine:not-connector-owned",
		Args:         []string{"--model", "gemma"},
		Env:          []corev1.EnvVar{{Name: "KEEP", Value: "yes"}},
		VolumeMounts: []corev1.VolumeMount{{Name: "models", MountPath: "/models"}},
	}}}
	cfg := testLMCacheMPConfig()
	cfg.Binding = &backendadapter.Binding{Protocol: backendadapter.ProtocolRESP, Endpoint: "redis.cache.svc:6379"}

	path, err := renderLMCachePodLocalServer(pod, "engine", cfg)
	if err != nil {
		t.Fatalf("renderLMCachePodLocalServer: %v", err)
	}
	if path != lmCacheMPConfigFilePath {
		t.Fatalf("config path = %q, want %q", path, lmCacheMPConfigFilePath)
	}
	if got := pod.Containers[0].Image; got != "engine:not-connector-owned" {
		t.Fatalf("engine image changed to %q", got)
	}
	if got := pod.Containers[0].Args; !reflect.DeepEqual(got, []string{"--model", "gemma"}) {
		t.Fatalf("common renderer changed engine args: %v", got)
	}

	server := findContainerByName(pod.InitContainers, lmCacheMPServerContainerName)
	if server == nil {
		t.Fatalf("MP server missing: %+v", pod.InitContainers)
	}
	if server.Image != testMPServerImage || server.Image == pod.Containers[0].Image {
		t.Fatalf("server image = %q, engine image = %q", server.Image, pod.Containers[0].Image)
	}
	if server.RestartPolicy == nil || *server.RestartPolicy != corev1.ContainerRestartPolicyAlways {
		t.Fatalf("server is not a native sidecar: %v", server.RestartPolicy)
	}
	joined := strings.Join(append(server.Command, server.Args...), " ")
	for _, want := range []string{
		"lmcache server", "--host 127.0.0.1", "--port 6500",
		"--http-port 8080", "--chunk-size 256", "--l1-size-gb 4",
		"--max-workers 3", `{"host":"redis.cache.svc","port":6379,"type":"resp"}`,
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("server command missing %q: %s", want, joined)
		}
	}
	if strings.Contains(joined, "python3 -m") {
		t.Fatalf("server uses unsupported module entrypoint: %s", joined)
	}
	if server.StartupProbe == nil || server.ReadinessProbe == nil || server.LivenessProbe == nil {
		t.Fatalf("HTTP probes incomplete: startup=%+v readiness=%+v liveness=%+v", server.StartupProbe, server.ReadinessProbe, server.LivenessProbe)
	}
	for _, probe := range []*corev1.Probe{server.StartupProbe, server.ReadinessProbe, server.LivenessProbe} {
		if probe.HTTPGet == nil || probe.HTTPGet.Path != lmCacheMPHealthPath || probe.HTTPGet.Port.StrVal != lmCacheMPHTTPPortName {
			t.Fatalf("probe is not %s on named HTTP port: %+v", lmCacheMPHealthPath, probe)
		}
	}
	if got := server.Resources.Limits[corev1.ResourceEphemeralStorage]; got.Cmp(resource.MustParse("1Gi")) != 0 {
		t.Fatalf("ephemeral-storage limit = %s, want 1Gi", got.String())
	}
	if server.SecurityContext == nil || server.SecurityContext.AllowPrivilegeEscalation == nil || *server.SecurityContext.AllowPrivilegeEscalation {
		t.Fatalf("server security context allows privilege escalation: %+v", server.SecurityContext)
	}
	if len(server.SecurityContext.Capabilities.Drop) != 1 || server.SecurityContext.Capabilities.Drop[0] != "ALL" {
		t.Fatalf("server capabilities = %+v, want drop ALL", server.SecurityContext.Capabilities)
	}
	if !hasNamedContainerPort(server.Ports, lmCacheMPServerPortName, 6500) || !hasNamedContainerPort(server.Ports, lmCacheMPHTTPPortName, 8080) {
		t.Fatalf("server ports = %+v", server.Ports)
	}
	shm := findVolume(pod.Volumes, lmCacheMPShmVolumeName)
	if shm == nil || shm.EmptyDir == nil || shm.EmptyDir.Medium != corev1.StorageMediumMemory || shm.EmptyDir.SizeLimit == nil || shm.EmptyDir.SizeLimit.Cmp(resource.MustParse("5Gi")) != 0 {
		t.Fatalf("shared-memory volume = %+v, want bounded 5Gi tmpfs (4Gi L1 + 1Gi headroom)", shm)
	}
	if findVolume(pod.Volumes, lmCacheMPConfigVolumeName) == nil {
		t.Fatalf("client config volume missing: %+v", pod.Volumes)
	}
}

func TestRenderLMCachePodLocalServerSecretAuthUsesEnvReferences(t *testing.T) {
	cfg := testLMCacheMPConfig()
	cfg.Binding = &backendadapter.Binding{
		Protocol: backendadapter.ProtocolRESP,
		Endpoint: "redis.example:6379",
		Redis: &backendadapter.RedisBinding{Authentication: &cachev1alpha1.RedisAuthenticationSpec{
			Username: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "redis-auth"}, Key: "username"},
			Password: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "redis-auth"}, Key: "password"},
		}},
	}
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "engine", Image: "engine:test"}}}
	if _, err := renderLMCachePodLocalServer(pod, "engine", cfg); err != nil {
		t.Fatalf("renderLMCachePodLocalServer: %v", err)
	}
	server := findContainerByName(pod.InitContainers, lmCacheMPServerContainerName)
	if server == nil {
		t.Fatal("MP server missing")
	}
	joined := strings.Join(append(server.Command, server.Args...), " ")
	if strings.Contains(joined, "redis-auth") || strings.Contains(joined, "password") || strings.Contains(joined, "username") {
		t.Fatalf("secret selector leaked into process arguments: %s", joined)
	}
	assertSecretEnv := func(name, secret, key string) {
		t.Helper()
		for i := range server.Env {
			if server.Env[i].Name == name {
				ref := server.Env[i].ValueFrom
				if ref == nil || ref.SecretKeyRef == nil || ref.SecretKeyRef.Name != secret || ref.SecretKeyRef.Key != key {
					t.Fatalf("%s = %+v, want %s/%s SecretKeyRef", name, server.Env[i], secret, key)
				}
				return
			}
		}
		t.Fatalf("%s missing", name)
	}
	assertSecretEnv(lmCacheRESPUsernameEnv, "redis-auth", "username")
	assertSecretEnv(lmCacheRESPPasswordEnv, "redis-auth", "password")
}

func TestRenderLMCachePodLocalServerIdempotent(t *testing.T) {
	cfg := testLMCacheMPConfig()
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "engine", Image: "engine:test"}}}
	if _, err := renderLMCachePodLocalServer(pod, "engine", cfg); err != nil {
		t.Fatalf("first render: %v", err)
	}
	cfg.MaxWorkers = 7
	if _, err := renderLMCachePodLocalServer(pod, "engine", cfg); err != nil {
		t.Fatalf("second render: %v", err)
	}
	if len(pod.InitContainers) != 1 || len(pod.Volumes) != 2 {
		t.Fatalf("re-render duplicated wire: init=%d volumes=%d", len(pod.InitContainers), len(pod.Volumes))
	}
	joined := strings.Join(pod.InitContainers[0].Args, " ")
	if !strings.Contains(joined, "--max-workers 7") || strings.Contains(joined, "--max-workers 3") {
		t.Fatalf("re-render did not converge maxWorkers: %s", joined)
	}
}

func TestRenderLMCachePodLocalServerCollisionIsAtomic(t *testing.T) {
	tests := []struct {
		name string
		pod  corev1.PodSpec
	}{
		{
			name: "foreign reserved server",
			pod: corev1.PodSpec{
				Containers:     []corev1.Container{{Name: "engine"}},
				InitContainers: []corev1.Container{{Name: lmCacheMPServerContainerName, Image: "operator:test"}},
			},
		},
		{
			name: "foreign config volume",
			pod: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "engine"}},
				Volumes:    []corev1.Volume{{Name: lmCacheMPConfigVolumeName, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "foreign"}}}},
			},
		},
		{
			name: "foreign config mount",
			pod: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "engine", VolumeMounts: []corev1.VolumeMount{{Name: "foreign-config", MountPath: lmCacheMPConfigMountPath}}}},
				Volumes:    []corev1.Volume{{Name: "foreign-config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{}}}},
			},
		},
		{
			name: "read-only shm mount",
			pod: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "engine", VolumeMounts: []corev1.VolumeMount{{Name: "engine-shm", MountPath: lmCacheMPShmMountPath, ReadOnly: true}}}},
				Volumes: []corev1.Volume{{Name: "engine-shm", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
					Medium: corev1.StorageMediumMemory, SizeLimit: func() *resource.Quantity { q := resource.MustParse("6Gi"); return &q }(),
				}}}},
			},
		},
		{
			name: "foreign reserved shm volume",
			pod: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "engine"}},
				Volumes:    []corev1.Volume{{Name: lmCacheMPShmVolumeName, VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "foreign"}}}},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			before := tc.pod.DeepCopy()
			if _, err := renderLMCachePodLocalServer(&tc.pod, "engine", testLMCacheMPConfig()); err == nil {
				t.Fatal("expected collision error")
			}
			if !reflect.DeepEqual(&tc.pod, before) {
				t.Fatalf("failed render mutated pod\nbefore=%+v\nafter=%+v", before, &tc.pod)
			}
		})
	}
}

func TestRenderLMCachePodLocalServerRejectsInvalidInputs(t *testing.T) {
	validPod := func() *corev1.PodSpec {
		return &corev1.PodSpec{Containers: []corev1.Container{{Name: "engine"}}}
	}
	tests := []struct {
		name    string
		pod     *corev1.PodSpec
		cfg     lmCacheMPServerConfig
		wantErr string
	}{
		{name: "nil pod", cfg: testLMCacheMPConfig(), wantErr: "pod spec is nil"},
		{name: "ambiguous engine", pod: &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}, {Name: "sidecar"}}}, cfg: testLMCacheMPConfig(), wantErr: "none is named \"engine\""},
		{name: "invalid config", pod: validPod(), cfg: lmCacheMPServerConfig{}, wantErr: "image is empty"},
		{name: "unsupported binding", pod: validPod(), cfg: func() lmCacheMPServerConfig {
			cfg := testLMCacheMPConfig()
			cfg.Binding = &backendadapter.Binding{Protocol: backendadapter.Protocol("grpc")}
			return cfg
		}(), wantErr: "unsupported remote protocol"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := renderLMCachePodLocalServer(tc.pod, "engine", tc.cfg)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("renderLMCachePodLocalServer error = %v, want substring %q", err, tc.wantErr)
			}
		})
	}

	budget := resource.MustParse("5Gi")
	if err := checkLMCacheMPShmBudget(nil, corev1.VolumeMount{Name: "missing"}, budget); err == nil || !strings.Contains(err.Error(), "missing volume") {
		t.Fatalf("checkLMCacheMPShmBudget error = %v, want missing volume", err)
	}
	if _, err := lmCacheMPServerContainer(lmCacheMPServerConfig{}, "", nil, corev1.VolumeMount{}); err == nil || !strings.Contains(err.Error(), "greater than zero") {
		t.Fatalf("lmCacheMPServerContainer error = %v, want invalid capacity", err)
	}
}

func TestRenderLMCachePodLocalServerReusesWritableShm(t *testing.T) {
	pod := &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name: "engine",
			VolumeMounts: []corev1.VolumeMount{{
				Name: "engine-shm", MountPath: "/dev/shm", SubPath: "shared",
			}},
		}},
		Volumes: []corev1.Volume{{Name: "engine-shm", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
			Medium:    corev1.StorageMediumMemory,
			SizeLimit: func() *resource.Quantity { q := resource.MustParse("6Gi"); return &q }(),
		}}}},
	}
	if _, err := renderLMCachePodLocalServer(pod, "engine", testLMCacheMPConfig()); err != nil {
		t.Fatalf("renderLMCachePodLocalServer: %v", err)
	}
	if findVolume(pod.Volumes, lmCacheMPShmVolumeName) != nil {
		t.Fatalf("renderer added a duplicate shm volume: %+v", pod.Volumes)
	}
	server := findContainerByName(pod.InitContainers, lmCacheMPServerContainerName)
	if server == nil || len(server.VolumeMounts) == 0 || server.VolumeMounts[0].Name != "engine-shm" || server.VolumeMounts[0].SubPath != "shared" {
		t.Fatalf("server did not mirror engine shm mount: %+v", server)
	}
}

func TestRenderLMCachePodLocalServerRejectsUnsafeExistingShmBudget(t *testing.T) {
	tests := []struct {
		name   string
		source corev1.VolumeSource
		want   string
	}{
		{
			name:   "unbounded memory emptyDir",
			source: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{Medium: corev1.StorageMediumMemory}},
			want:   "sizeLimit unbounded",
		},
		{
			name: "undersized memory emptyDir",
			source: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
				Medium:    corev1.StorageMediumMemory,
				SizeLimit: func() *resource.Quantity { q := resource.MustParse("4Gi"); return &q }(),
			}},
			want: "need at least 5Gi",
		},
		{
			name: "disk-backed emptyDir",
			source: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{
				SizeLimit: func() *resource.Quantity { q := resource.MustParse("6Gi"); return &q }(),
			}},
			want: "memory-backed emptyDir",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.PodSpec{
				Containers: []corev1.Container{{Name: "engine", VolumeMounts: []corev1.VolumeMount{{Name: "engine-shm", MountPath: "/dev/shm"}}}},
				Volumes:    []corev1.Volume{{Name: "engine-shm", VolumeSource: tc.source}},
			}
			before := pod.DeepCopy()
			_, err := renderLMCachePodLocalServer(pod, "engine", testLMCacheMPConfig())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			if !reflect.DeepEqual(pod, before) {
				t.Fatalf("failed render mutated pod\nbefore=%+v\nafter=%+v", before, pod)
			}
		})
	}
}

func TestRenderLMCacheMPL2BindingRejectsUnsupportedV053Features(t *testing.T) {
	db := int32(1)
	for _, tc := range []struct {
		name  string
		redis *backendadapter.RedisBinding
		want  string
	}{
		{name: "TLS", redis: &backendadapter.RedisBinding{TLS: &cachev1alpha1.RemoteStorageTLSSpec{}}, want: "does not support TLS"},
		{name: "database", redis: &backendadapter.RedisBinding{Database: &db}, want: "does not support database"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := renderLMCacheMPL2Binding(&backendadapter.Binding{Protocol: backendadapter.ProtocolRESP, Endpoint: "redis:6379", Redis: tc.redis})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

func hasNamedContainerPort(ports []corev1.ContainerPort, name string, port int32) bool {
	for i := range ports {
		if ports[i].Name == name && ports[i].ContainerPort == port {
			return true
		}
	}
	return false
}

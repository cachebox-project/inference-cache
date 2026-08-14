// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	backendadapter "github.com/cachebox-project/inference-cache/pkg/adapters/backend"
)

func TestRuntimeHelpersCurrentBranches(t *testing.T) {
	cache := &cachev1alpha1.CacheBackend{}
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "engine"}}}
	for name, err := range map[string]error{
		"nil pod":       validateInjectPodCacheInputs(nil, cache, "engine"),
		"nil cache":     validateInjectPodCacheInputs(pod, nil, "engine"),
		"no containers": validateInjectPodCacheInputs(&corev1.PodSpec{}, cache, "engine"),
	} {
		if err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}

	no := false
	cache.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
		Role:     cachev1alpha1.CacheBackendIntegrationRoleReadWrite,
		FailOpen: &no,
	}
	if got := FailOpenString(cache); got != "false" {
		t.Fatalf("FailOpenString() = %q", got)
	}
	if got := IntegrationRole(cache); got != cachev1alpha1.CacheBackendIntegrationRoleReadWrite {
		t.Fatalf("IntegrationRole() = %q", got)
	}

	if got := UpsertArgPair([]string{"--port"}, "--port", "2"); len(got) != 2 || got[1] != "2" {
		t.Fatalf("append missing pair value = %v", got)
	}
	if got := UpsertArgPair([]string{"before", "--port=1", "after"}, "--port", "2"); len(got) != 4 || got[1] != "--port" || got[2] != "2" {
		t.Fatalf("replace equals pair = %v", got)
	}
	ref := &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}
	if got := UpsertEnv([]corev1.EnvVar{{Name: "A", Value: "old"}}, corev1.EnvVar{Name: "A", ValueFrom: ref}); got[0].Value != "" || got[0].ValueFrom == nil {
		t.Fatalf("replace env = %+v", got)
	}

	uid, gid := int64(1000), int64(2000)
	nonRoot := true
	security := lmCacheMPServerSecurityContext(&corev1.SecurityContext{RunAsUser: &uid, RunAsGroup: &gid, RunAsNonRoot: &nonRoot})
	if security.RunAsUser == nil || *security.RunAsUser != uid || security.RunAsGroup == nil || *security.RunAsGroup != gid || security.RunAsNonRoot == nil || !*security.RunAsNonRoot {
		t.Fatalf("security context did not inherit engine identity: %+v", security)
	}
}

func TestCheckLMCacheMPShmReusableBranches(t *testing.T) {
	readOnly := true
	tests := []struct {
		name    string
		mount   corev1.VolumeMount
		source  corev1.VolumeSource
		wantErr string
	}{
		{name: "read-only mount", mount: corev1.VolumeMount{Name: "shm", ReadOnly: true}, wantErr: "read-only"},
		{name: "subPathExpr", mount: corev1.VolumeMount{Name: "shm", SubPathExpr: "$(POD_NAME)"}, wantErr: "subPathExpr"},
		{name: "config map", mount: corev1.VolumeMount{Name: "shm"}, source: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{}}, wantErr: "read-only volume"},
		{name: "read-only CSI", mount: corev1.VolumeMount{Name: "shm"}, source: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{ReadOnly: &readOnly}}, wantErr: "read-only volume"},
		{name: "writable", mount: corev1.VolumeMount{Name: "shm"}, source: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{name: "missing volume", mount: corev1.VolumeMount{Name: "missing"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var volumes []corev1.Volume
			if tc.mount.Name == "shm" {
				volumes = []corev1.Volume{{Name: "shm", VolumeSource: tc.source}}
			}
			err := checkLMCacheMPShmReusable(volumes, tc.mount)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestSplitLMCacheHostPort(t *testing.T) {
	for _, tc := range []struct {
		input, host, port string
		hasPort           bool
	}{
		{input: ""},
		{input: "redis", host: "redis"},
		{input: "redis:6379", host: "redis", port: "6379", hasPort: true},
		{input: "[2001:db8::1]", host: "2001:db8::1"},
		{input: "[2001:db8::1]:6379", host: "2001:db8::1", port: "6379", hasPort: true},
		{input: "[x"},
		{input: "[::1]bad"},
		{input: "[::1]:1:2"},
		{input: "2001:db8::1"},
	} {
		host, port, hasPort := splitLMCacheHostPort(tc.input)
		if host != tc.host || port != tc.port || hasPort != tc.hasPort {
			t.Errorf("splitLMCacheHostPort(%q) = (%q, %q, %t), want (%q, %q, %t)", tc.input, host, port, hasPort, tc.host, tc.port, tc.hasPort)
		}
	}
}

func TestLMCacheMPRendererValidationBranches(t *testing.T) {
	valid := lmCacheMPServerConfig{Image: "lmcache:test", Port: 5555, ChunkSizeTokens: 256, L1Capacity: resource.MustParse("1Gi"), MaxWorkers: 1}
	for _, mutate := range []func(*lmCacheMPServerConfig){
		func(c *lmCacheMPServerConfig) { c.Image = " " },
		func(c *lmCacheMPServerConfig) { c.Port = 0 },
		func(c *lmCacheMPServerConfig) { c.Port = lmCacheMPHTTPPort },
		func(c *lmCacheMPServerConfig) { c.ChunkSizeTokens = 0 },
		func(c *lmCacheMPServerConfig) { c.L1Capacity = resource.Quantity{} },
		func(c *lmCacheMPServerConfig) { c.MaxWorkers = 0 },
	} {
		cfg := valid
		mutate(&cfg)
		if err := validateLMCacheMPServerConfig(cfg); err == nil {
			t.Fatalf("expected invalid config error: %+v", cfg)
		}
	}

	if _, _, err := renderLMCacheMPL2Binding(nil); err != nil {
		t.Fatalf("nil binding: %v", err)
	}
	for _, binding := range []*backendadapter.Binding{
		{Protocol: backendadapter.Protocol("other"), Endpoint: "redis:6379"},
		{Protocol: backendadapter.ProtocolRESP, Endpoint: "redis"},
		{Protocol: backendadapter.ProtocolRESP, Endpoint: "redis:not-a-port"},
		{Protocol: backendadapter.ProtocolRESP, Endpoint: "redis:70000"},
	} {
		if _, _, err := renderLMCacheMPL2Binding(binding); err == nil {
			t.Fatalf("expected invalid binding error: %+v", binding)
		}
	}

	if _, err := quantityAsGiB(resource.Quantity{}); err == nil {
		t.Fatal("expected zero quantity error")
	}
	if got, err := quantityAsGiB(resource.MustParse("1536Mi")); err != nil || got != "1.5" {
		t.Fatalf("quantityAsGiB() = %q, %v", got, err)
	}
	if lmCacheMPWireIsOurs(nil) {
		t.Fatal("nil pod cannot contain an owned MP wire")
	}
}

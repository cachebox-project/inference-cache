package pod

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// TestApplyEngineInjectionOverrides_NilSafe pins the nil-safety contract: a
// nil overrides argument leaves both args and env strictly unchanged (same
// values AND same backing slices), so the webhook can call the helper
// unconditionally without branching.
func TestApplyEngineInjectionOverrides_NilSafe(t *testing.T) {
	canonicalArgs := []string{"--model", "Qwen/Qwen2.5-0.5B-Instruct", "--kv-transfer-config", "{json}"}
	canonicalEnv := []corev1.EnvVar{{Name: "VLLM_USE_V1", Value: "1"}}

	args, env := applyEngineInjectionOverrides(canonicalArgs, canonicalEnv, nil)
	if !reflect.DeepEqual(args, canonicalArgs) {
		t.Errorf("nil overrides should leave args untouched, got %v", args)
	}
	if !reflect.DeepEqual(env, canonicalEnv) {
		t.Errorf("nil overrides should leave env untouched, got %v", env)
	}
}

// TestApplyEngineInjectionOverrides_Args drives every documented arg-merge
// branch through one table so a regression on any single behaviour shows up
// as a precise diff against the table's expected slice.
func TestApplyEngineInjectionOverrides_Args(t *testing.T) {
	tests := []struct {
		name      string
		canonical []string
		overrides *cachev1alpha1.EngineInjectionOverrides
		want      []string
	}{
		{
			name:      "append new flag",
			canonical: []string{"--model", "Q/M"},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Args: []string{"--max-model-len", "8192"},
			},
			want: []string{"--model", "Q/M", "--max-model-len", "8192"},
		},
		{
			name:      "append toggle flag",
			canonical: []string{"--model", "Q/M"},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Args: []string{"--enforce-eager"},
			},
			want: []string{"--model", "Q/M", "--enforce-eager"},
		},
		{
			name:      "override by flag, two-arg over two-arg, preserves order",
			canonical: []string{"--model", "Q/M", "--max-model-len", "1024"},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Args: []string{"--max-model-len", "8192"},
			},
			want: []string{"--model", "Q/M", "--max-model-len", "8192"},
		},
		{
			name:      "override by flag, equals form replaces two-arg form",
			canonical: []string{"--max-model-len", "1024"},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Args: []string{"--max-model-len=8192"},
			},
			want: []string{"--max-model-len=8192"},
		},
		{
			name:      "override by flag, two-arg form replaces equals form",
			canonical: []string{"--max-model-len=1024"},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Args: []string{"--max-model-len", "8192"},
			},
			want: []string{"--max-model-len", "8192"},
		},
		{
			name:      "suppress two-arg flag drops both slots",
			canonical: []string{"--model", "Q/M", "--max-model-len", "1024"},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				SuppressArgs: []string{"--max-model-len"},
			},
			want: []string{"--model", "Q/M"},
		},
		{
			name:      "suppress equals-form flag drops single slot",
			canonical: []string{"--model", "Q/M", "--max-model-len=1024"},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				SuppressArgs: []string{"--max-model-len"},
			},
			want: []string{"--model", "Q/M"},
		},
		{
			name:      "suppress then re-add via Args",
			canonical: []string{"--max-model-len", "1024", "--model", "Q/M"},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				SuppressArgs: []string{"--max-model-len"},
				Args:         []string{"--max-model-len", "8192"},
			},
			// Suppression strips the canonical entry, Args then appends.
			want: []string{"--model", "Q/M", "--max-model-len", "8192"},
		},
		{
			name:      "suppress unknown flag is a no-op",
			canonical: []string{"--model", "Q/M"},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				SuppressArgs: []string{"--bogus"},
			},
			want: []string{"--model", "Q/M"},
		},
		{
			name:      "nil canonical, override appends",
			canonical: nil,
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Args: []string{"--enforce-eager"},
			},
			want: []string{"--enforce-eager"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := applyEngineInjectionOverrides(tc.canonical, nil, tc.overrides)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("args mismatch\n got: %v\nwant: %v", got, tc.want)
			}
		})
	}
}

// TestApplyEngineInjectionOverrides_Env mirrors the args table for the env
// side: upsert-by-name semantics, ordering of canonical entries preserved,
// suppress removes by name.
func TestApplyEngineInjectionOverrides_Env(t *testing.T) {
	tests := []struct {
		name      string
		canonical []corev1.EnvVar
		overrides *cachev1alpha1.EngineInjectionOverrides
		want      []corev1.EnvVar
	}{
		{
			name:      "append new env",
			canonical: []corev1.EnvVar{{Name: "VLLM_USE_V1", Value: "1"}},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Env: []corev1.EnvVar{{Name: "FOO", Value: "bar"}},
			},
			want: []corev1.EnvVar{
				{Name: "VLLM_USE_V1", Value: "1"},
				{Name: "FOO", Value: "bar"},
			},
		},
		{
			name: "override by name preserves order",
			canonical: []corev1.EnvVar{
				{Name: "LMCACHE_CHUNK_SIZE", Value: "256"},
				{Name: "VLLM_USE_V1", Value: "1"},
			},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Env: []corev1.EnvVar{{Name: "LMCACHE_CHUNK_SIZE", Value: "512"}},
			},
			want: []corev1.EnvVar{
				{Name: "LMCACHE_CHUNK_SIZE", Value: "512"},
				{Name: "VLLM_USE_V1", Value: "1"},
			},
		},
		{
			name: "suppress removes by name",
			canonical: []corev1.EnvVar{
				{Name: "LMCACHE_CHUNK_SIZE", Value: "256"},
				{Name: "VLLM_USE_V1", Value: "1"},
			},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				SuppressEnv: []string{"LMCACHE_CHUNK_SIZE"},
			},
			want: []corev1.EnvVar{{Name: "VLLM_USE_V1", Value: "1"}},
		},
		{
			name: "suppress then re-add via Env",
			canonical: []corev1.EnvVar{
				{Name: "LMCACHE_CHUNK_SIZE", Value: "256"},
			},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				SuppressEnv: []string{"LMCACHE_CHUNK_SIZE"},
				Env:         []corev1.EnvVar{{Name: "LMCACHE_CHUNK_SIZE", Value: "512"}},
			},
			// Suppression strips the canonical entry; Env then appends.
			want: []corev1.EnvVar{{Name: "LMCACHE_CHUNK_SIZE", Value: "512"}},
		},
		{
			name:      "nil canonical, override appends",
			canonical: nil,
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Env: []corev1.EnvVar{{Name: "FOO", Value: "bar"}},
			},
			want: []corev1.EnvVar{{Name: "FOO", Value: "bar"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, got := applyEngineInjectionOverrides(nil, tc.canonical, tc.overrides)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("env mismatch\n got: %v\nwant: %v", got, tc.want)
			}
		})
	}
}

// TestOverrideTargetIndex pins the resolver's three documented behaviours:
// name match, single-container fallback when the name isn't present, and
// the empty-engineContainerName signal that overrides apply to no container.
func TestOverrideTargetIndex(t *testing.T) {
	tests := []struct {
		name       string
		containers []corev1.Container
		want       string
		wantOK     bool
	}{
		{
			name: "name match in multi-container pod",
			containers: []corev1.Container{
				{Name: "sidecar"}, {Name: "vllm"}, {Name: "subscriber"},
			},
			want:   "vllm",
			wantOK: true,
		},
		{
			name:       "single-container fallback",
			containers: []corev1.Container{{Name: "lone"}},
			want:       "lone",
			wantOK:     true,
		},
		{
			name: "no name match in multi-container pod, no fallback",
			containers: []corev1.Container{
				{Name: "sidecar"}, {Name: "subscriber"},
			},
			wantOK: false,
		},
		{
			name: "empty engineContainerName is always (-1, false)",
			containers: []corev1.Container{
				{Name: "lone"},
			},
			wantOK: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			engineName := "vllm"
			if tc.name == "empty engineContainerName is always (-1, false)" {
				engineName = ""
			}
			idx, ok := overrideTargetIndex(tc.containers, engineName)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if tc.containers[idx].Name != tc.want {
				t.Errorf("resolved %q, want %q", tc.containers[idx].Name, tc.want)
			}
		})
	}
}

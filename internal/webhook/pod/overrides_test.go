package pod

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// TestApplyEngineInjectionOverrides_NilSafe pins the nil-safety contract: a
// nil overrides argument returns the post inputs unchanged, so the webhook
// can call the helper unconditionally without branching on the field.
func TestApplyEngineInjectionOverrides_NilSafe(t *testing.T) {
	preArgs := []string{"--model", "Q/M"}
	postArgs := []string{"--model", "Q/M", "--kv-transfer-config", "{json}"}
	preEnv := []corev1.EnvVar{{Name: "USER_FLAG", Value: "u"}}
	postEnv := []corev1.EnvVar{{Name: "USER_FLAG", Value: "u"}, {Name: "VLLM_USE_V1", Value: "1"}}

	gotArgs, gotEnv := applyEngineInjectionOverrides(preArgs, postArgs, preEnv, postEnv, nil)
	if !reflect.DeepEqual(gotArgs, postArgs) {
		t.Errorf("nil overrides should leave args untouched, got %v", gotArgs)
	}
	if !reflect.DeepEqual(gotEnv, postEnv) {
		t.Errorf("nil overrides should leave env untouched, got %v", gotEnv)
	}
}

// TestApplyEngineInjectionOverrides_Args exercises the documented arg-merge
// branches AND the adapter-owned scoping: only entries the diff identifies
// as adapter-contributed are touched, user pod-template args are protected.
func TestApplyEngineInjectionOverrides_Args(t *testing.T) {
	tests := []struct {
		name      string
		pre, post []string
		overrides *cachev1alpha1.EngineInjectionOverrides
		want      []string
	}{
		{
			// Append a new flag the adapter doesn't inject — token is in
			// neither pre nor adapter set, so it's appended freely.
			name: "append new flag",
			pre:  []string{"--model", "Q/M"},
			post: []string{"--model", "Q/M", "--kv-transfer-config", "{json}"},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Args: []string{"--max-model-len", "8192"},
			},
			want: []string{"--model", "Q/M", "--kv-transfer-config", "{json}", "--max-model-len", "8192"},
		},
		{
			// User pre-set --enforce-eager; adapter doesn't touch it.
			// Override.Args targets that flag — user-owned, so silent no-op
			// (would shadow the user's choice otherwise).
			name: "override targeting user-owned flag is a no-op",
			pre:  []string{"--enforce-eager"},
			post: []string{"--enforce-eager", "--kv-transfer-config", "{json}"},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Args: []string{"--enforce-eager=false"},
			},
			want: []string{"--enforce-eager", "--kv-transfer-config", "{json}"},
		},
		{
			// SuppressArgs targeting a user-owned flag is also a no-op;
			// the CR has no authority over the engine pod template.
			name: "suppress targeting user-owned flag is a no-op",
			pre:  []string{"--enforce-eager"},
			post: []string{"--enforce-eager", "--kv-transfer-config", "{json}"},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				SuppressArgs: []string{"--enforce-eager"},
			},
			want: []string{"--enforce-eager", "--kv-transfer-config", "{json}"},
		},
		{
			// SuppressArgs DOES strip an adapter-owned flag (two-arg form
			// drops both slots). Admission would block this for the
			// reserved --kv-transfer-config, but the merge function trusts
			// admission and just executes.
			name: "suppress adapter-owned two-arg flag drops both slots",
			pre:  []string{"--model", "Q/M"},
			post: []string{"--model", "Q/M", "--tune-knob", "9001"},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				SuppressArgs: []string{"--tune-knob"},
			},
			want: []string{"--model", "Q/M"},
		},
		{
			// Override an adapter-owned flag: two-arg-over-two-arg replaces
			// both slots in place, preserving the position.
			name: "override adapter-owned two-arg replaces value",
			pre:  []string{"--model", "Q/M"},
			post: []string{"--model", "Q/M", "--tune-knob", "1024"},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Args: []string{"--tune-knob", "8192"},
			},
			want: []string{"--model", "Q/M", "--tune-knob", "8192"},
		},
		{
			// Equals form override replaces a two-arg adapter entry.
			name: "override equals-form replaces two-arg adapter entry",
			pre:  []string{},
			post: []string{"--tune-knob", "1024"},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Args: []string{"--tune-knob=8192"},
			},
			want: []string{"--tune-knob=8192"},
		},
		{
			// Suppress on a flag the adapter didn't inject at all — and
			// the user didn't either — is a no-op.
			name: "suppress unknown flag is a no-op",
			pre:  []string{"--model", "Q/M"},
			post: []string{"--model", "Q/M", "--kv-transfer-config", "{json}"},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				SuppressArgs: []string{"--bogus"},
			},
			want: []string{"--model", "Q/M", "--kv-transfer-config", "{json}"},
		},
		{
			// Empty canonical injection (degenerate case): everything goes
			// through the "append-new" branch.
			name: "no canonical injection appends override",
			pre:  []string{"--model", "Q/M"},
			post: []string{"--model", "Q/M"},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Args: []string{"--enforce-eager"},
			},
			want: []string{"--model", "Q/M", "--enforce-eager"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := applyEngineInjectionOverrides(tc.pre, tc.post, nil, nil, tc.overrides)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("args mismatch\n got: %v\nwant: %v", got, tc.want)
			}
		})
	}
}

// TestApplyEngineInjectionOverrides_Env mirrors the args table for the env
// side: upsert/suppress are restricted to adapter-owned Names; user-owned,
// adapter-untouched env is protected from a CR-driven change.
func TestApplyEngineInjectionOverrides_Env(t *testing.T) {
	tests := []struct {
		name      string
		pre, post []corev1.EnvVar
		overrides *cachev1alpha1.EngineInjectionOverrides
		want      []corev1.EnvVar
	}{
		{
			name: "append new env name",
			pre:  []corev1.EnvVar{{Name: "USER_FLAG", Value: "u"}},
			post: []corev1.EnvVar{{Name: "USER_FLAG", Value: "u"}, {Name: "VLLM_USE_V1", Value: "1"}},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Env: []corev1.EnvVar{{Name: "FOO", Value: "bar"}},
			},
			want: []corev1.EnvVar{
				{Name: "USER_FLAG", Value: "u"},
				{Name: "VLLM_USE_V1", Value: "1"},
				{Name: "FOO", Value: "bar"},
			},
		},
		{
			// Override targeting a user-owned, adapter-untouched env is a
			// silent no-op — the CR cannot rewrite the user template.
			name: "override targeting user-owned env is a no-op",
			pre:  []corev1.EnvVar{{Name: "USER_FLAG", Value: "u"}},
			post: []corev1.EnvVar{{Name: "USER_FLAG", Value: "u"}, {Name: "VLLM_USE_V1", Value: "1"}},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Env: []corev1.EnvVar{{Name: "USER_FLAG", Value: "override-wins?"}},
			},
			want: []corev1.EnvVar{
				{Name: "USER_FLAG", Value: "u"},
				{Name: "VLLM_USE_V1", Value: "1"},
			},
		},
		{
			// SuppressEnv targeting a user-owned name is also a no-op.
			name: "suppress targeting user-owned env is a no-op",
			pre:  []corev1.EnvVar{{Name: "USER_FLAG", Value: "u"}},
			post: []corev1.EnvVar{{Name: "USER_FLAG", Value: "u"}, {Name: "VLLM_USE_V1", Value: "1"}},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				SuppressEnv: []string{"USER_FLAG"},
			},
			want: []corev1.EnvVar{
				{Name: "USER_FLAG", Value: "u"},
				{Name: "VLLM_USE_V1", Value: "1"},
			},
		},
		{
			// Override an adapter-owned tunable in place; order is
			// preserved relative to other canonical entries.
			name: "override adapter-owned tunable replaces value in place",
			pre:  []corev1.EnvVar{{Name: "USER_FLAG", Value: "u"}},
			post: []corev1.EnvVar{
				{Name: "USER_FLAG", Value: "u"},
				{Name: "LMCACHE_CHUNK_SIZE", Value: "256"},
				{Name: "VLLM_USE_V1", Value: "1"},
			},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Env: []corev1.EnvVar{{Name: "LMCACHE_CHUNK_SIZE", Value: "512"}},
			},
			want: []corev1.EnvVar{
				{Name: "USER_FLAG", Value: "u"},
				{Name: "LMCACHE_CHUNK_SIZE", Value: "512"},
				{Name: "VLLM_USE_V1", Value: "1"},
			},
		},
		{
			// SuppressEnv strips an adapter-owned canonical entry.
			name: "suppress adapter-owned env strips it",
			pre:  []corev1.EnvVar{{Name: "USER_FLAG", Value: "u"}},
			post: []corev1.EnvVar{
				{Name: "USER_FLAG", Value: "u"},
				{Name: "LMCACHE_CHUNK_SIZE", Value: "256"},
			},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				SuppressEnv: []string{"LMCACHE_CHUNK_SIZE"},
			},
			want: []corev1.EnvVar{{Name: "USER_FLAG", Value: "u"}},
		},
		{
			// Adapter-modified env: the adapter overwrote a user-template
			// value (USER_FLAG: u → adapter). The CR can now override
			// because the adapter has taken ownership.
			name: "override an env the adapter modified is permitted",
			pre:  []corev1.EnvVar{{Name: "X", Value: "user"}},
			post: []corev1.EnvVar{{Name: "X", Value: "adapter"}},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Env: []corev1.EnvVar{{Name: "X", Value: "override"}},
			},
			want: []corev1.EnvVar{{Name: "X", Value: "override"}},
		},
		{
			// Regression: SuppressEnv followed by Env for the same
			// adapter-owned name must re-add the override. Earlier the
			// replace-in-place loop dropped the override silently when
			// suppress had already removed the canonical entry. Mirrors
			// the args side's suppress-then-re-add behaviour.
			name: "suppress then re-add adapter-owned env",
			pre:  []corev1.EnvVar{},
			post: []corev1.EnvVar{{Name: "LMCACHE_CHUNK_SIZE", Value: "256"}},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				SuppressEnv: []string{"LMCACHE_CHUNK_SIZE"},
				Env:         []corev1.EnvVar{{Name: "LMCACHE_CHUNK_SIZE", Value: "512"}},
			},
			want: []corev1.EnvVar{{Name: "LMCACHE_CHUNK_SIZE", Value: "512"}},
		},
		{
			// Env-side ownership detection: an adapter that swaps one
			// ValueFrom source for another (without touching Value)
			// must still be recognised as adapter-owned, so the override
			// surface can amend it. reflect.DeepEqual over the whole
			// EnvVar catches this; a Value-only comparison would have
			// misclassified the swap as user-untouched.
			name: "adapter ValueFrom swap is recognised as adapter-owned",
			pre: []corev1.EnvVar{{
				Name: "POD_INFO",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
				},
			}},
			post: []corev1.EnvVar{{
				Name: "POD_INFO",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
				},
			}},
			overrides: &cachev1alpha1.EngineInjectionOverrides{
				Env: []corev1.EnvVar{{Name: "POD_INFO", Value: "literal"}},
			},
			want: []corev1.EnvVar{{Name: "POD_INFO", Value: "literal"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, got := applyEngineInjectionOverrides(nil, nil, tc.pre, tc.post, tc.overrides)
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
		engineName string
		want       string
		wantOK     bool
	}{
		{
			name: "name match in multi-container pod",
			containers: []corev1.Container{
				{Name: "sidecar"}, {Name: "vllm"}, {Name: "subscriber"},
			},
			engineName: "vllm",
			want:       "vllm",
			wantOK:     true,
		},
		{
			name:       "single-container fallback",
			containers: []corev1.Container{{Name: "lone"}},
			engineName: "vllm",
			want:       "lone",
			wantOK:     true,
		},
		{
			name: "no name match in multi-container pod, no fallback",
			containers: []corev1.Container{
				{Name: "sidecar"}, {Name: "subscriber"},
			},
			engineName: "vllm",
			wantOK:     false,
		},
		{
			name:       "empty engineContainerName is always (-1, false)",
			containers: []corev1.Container{{Name: "lone"}},
			engineName: "",
			wantOK:     false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx, ok := overrideTargetIndex(tc.containers, tc.engineName)
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

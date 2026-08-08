// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"strings"
	"testing"
)

func TestValidator_EngineOverrides_NoOverrideAdmitted(t *testing.T) {
	// Sanity baseline: a CacheBackend whose integration is set but carries
	// no engineOverrides block must admit unchanged. Locked decision #7
	// (byte-identical default) hinges on this.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend()
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("no-override CR rejected: %v", err)
	}
}

func TestValidator_EngineOverrides_SuppressReservedArgRejected(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		SuppressArgs: []string{"--kv-transfer-config"},
	})
	requireInvalidWithCause(t, v,
		cb,
		"spec.integration.engineOverrides.suppressArgs[0]",
		"--kv-transfer-config")
	// And the rejection MUST name the offending adapter so the operator
	// can trace the contract back.
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.suppressArgs[0]", "\"vllm\"")
}

func TestValidator_EngineOverrides_OverrideReservedArgRejected(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistry()}
	// Two forms: bare flag and equals form. Both must trip the rule, since
	// both express the same leading flag token.
	for _, form := range []string{"--kv-transfer-config", "--kv-transfer-config=alt"} {
		cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
			Args: []string{form},
		})
		requireInvalidWithCause(t, v, cb,
			"spec.integration.engineOverrides.args[0]",
			"--kv-transfer-config")
	}
}

func TestValidator_EngineOverrides_SuppressReservedEnvRejected(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		SuppressEnv: []string{"VLLM_USE_V1"},
	})
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.suppressEnv[0]",
		"VLLM_USE_V1")
}

func TestValidator_EngineOverrides_OverrideReservedEnvRejected(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Env: []corev1.EnvVar{{Name: "INFERENCECACHE_FAIL_OPEN", Value: "false"}},
	})
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.env[0].name",
		"INFERENCECACHE_FAIL_OPEN")
}

func TestValidator_EngineOverrides_OverridePythonHashSeedRejected(t *testing.T) {
	// PYTHONHASHSEED is reserved (the deterministic-NONE_HASH correctness
	// invariant). An operator override must be hard-rejected, not silently
	// applied — re-randomizing the seed 0-hits LMCache reload under TP>1.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Env: []corev1.EnvVar{{Name: "PYTHONHASHSEED", Value: "1"}},
	})
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.env[0].name",
		"PYTHONHASHSEED")
}

func TestValidator_EngineOverrides_SuppressPythonHashSeedRejected(t *testing.T) {
	// ...and suppression is equally rejected: the operator must not be able to
	// drop the invariant either.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		SuppressEnv: []string{"PYTHONHASHSEED"},
	})
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.suppressEnv[0]",
		"PYTHONHASHSEED")
}

func TestValidator_EngineOverrides_SGLangReservedRejected(t *testing.T) {
	// The SGLang adapter reserves a DIFFERENT set than vLLM: --enable-lmcache and
	// --lmcache-config-file (args), LMCACHE_USE_EXPERIMENTAL /
	// INFERENCECACHE_FAIL_OPEN (env). In MP mode the old lm:// LMCACHE_REMOTE_URL is
	// neither injected nor reserved. Admission must reject an engineOverrides entry
	// that overrides OR suppresses any reserved item, and name the sglang adapter.
	// Exercises the real shipping registry so the sglang reserved lists are enforced.
	v := &CacheBackendValidator{Registry: defaultShippingRegistry()}
	cases := []struct {
		name     string
		o        cachev1alpha1.EngineInjectionOverrides
		field    string
		contains string
	}{
		{"suppress --enable-lmcache", cachev1alpha1.EngineInjectionOverrides{SuppressArgs: []string{"--enable-lmcache"}}, "spec.integration.engineOverrides.suppressArgs[0]", "--enable-lmcache"},
		{"override --enable-lmcache", cachev1alpha1.EngineInjectionOverrides{Args: []string{"--enable-lmcache"}}, "spec.integration.engineOverrides.args[0]", "--enable-lmcache"},
		{"suppress --lmcache-config-file", cachev1alpha1.EngineInjectionOverrides{SuppressArgs: []string{"--lmcache-config-file"}}, "spec.integration.engineOverrides.suppressArgs[0]", "--lmcache-config-file"},
		{"suppress LMCACHE_USE_EXPERIMENTAL", cachev1alpha1.EngineInjectionOverrides{SuppressEnv: []string{"LMCACHE_USE_EXPERIMENTAL"}}, "spec.integration.engineOverrides.suppressEnv[0]", "LMCACHE_USE_EXPERIMENTAL"},
		{"override INFERENCECACHE_FAIL_OPEN", cachev1alpha1.EngineInjectionOverrides{Env: []corev1.EnvVar{{Name: "INFERENCECACHE_FAIL_OPEN", Value: "false"}}}, "spec.integration.engineOverrides.env[0].name", "INFERENCECACHE_FAIL_OPEN"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cb := withSGLangOverrides(tc.o)
			requireInvalidWithCause(t, v, cb, tc.field, tc.contains)
			// The rejection names the offending (sglang) adapter so the
			// operator can trace the contract back.
			requireInvalidWithCause(t, v, cb, tc.field, "sglang")
		})
	}
}

func TestValidator_EngineOverrides_SGLangAdmitsVLLMOnlyNames(t *testing.T) {
	// VLLM_USE_V1 and PYTHONHASHSEED are reserved for vLLM but NOT for SGLang
	// (the SGLang adapter never injects them), and the LMCACHE_* tunables are
	// not reserved for either. Overriding them on a sglang backend must be
	// ADMITTED — proving the reserved set is per-selected-adapter, so the
	// vLLM-only reservations don't bleed onto SGLang.
	v := &CacheBackendValidator{Registry: defaultShippingRegistry()}
	cb := withSGLangOverrides(cachev1alpha1.EngineInjectionOverrides{
		Env: []corev1.EnvVar{
			{Name: "VLLM_USE_V1", Value: "0"},
			{Name: "PYTHONHASHSEED", Value: "1"},
			{Name: "LMCACHE_CHUNK_SIZE", Value: "512"},
		},
	})
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("vLLM-only env names (not reserved for sglang) rejected: %v", err)
	}
}

func TestValidator_EngineOverrides_NonReservedAdmitted(t *testing.T) {
	// Positive case: a non-reserved arg + env + suppression combination
	// must pass admission. This is the CPU-vLLM use case — the operator
	// suppresses a flag the adapter wouldn't inject anyway (no-op) and
	// adds a perf knob. We pin the happy path here so a future tightening
	// of the rule doesn't accidentally reject legitimate overrides.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Args:         []string{"--max-model-len", "8192"},
		SuppressArgs: []string{"--enforce-eager"},
		Env: []corev1.EnvVar{
			{Name: "LMCACHE_CHUNK_SIZE", Value: "512"},
		},
		SuppressEnv: []string{"VLLM_LOG_LEVEL"},
	})
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("non-reserved overrides rejected: %v", err)
	}
}

func TestValidator_EngineOverrides_PositionalArgIgnored(t *testing.T) {
	// Positionals (no leading "-") cannot overlap a reserved flag name
	// because the merge classifies them differently. Admission must treat
	// them the same way and not surface a spurious rejection — the engine
	// would happily accept the positional, so admission must too.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Args: []string{"some-positional"},
	})
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("positional arg rejected: %v", err)
	}
}

func TestValidator_EngineOverrides_RejectsEmptyEnvName(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Env: []corev1.EnvVar{{Name: "", Value: "x"}},
	})
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.env[0].name",
		"must declare a Name")
}

func TestValidator_EngineOverrides_RejectsInvalidEnvName(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		// "=" is forbidden in K8s env var names.
		Env: []corev1.EnvVar{{Name: "FOO=BAR", Value: "x"}},
	})
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.env[0].name",
		"invalid env var name")
}

func TestValidator_EngineOverrides_RejectsValueAndValueFrom(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Env: []corev1.EnvVar{{
			Name:  "BOTH",
			Value: "literal",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
		}},
	})
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.env[0]",
		"value OR valueFrom, not both")
}

func TestValidator_EngineOverrides_RejectsEmptyValueFrom(t *testing.T) {
	// valueFrom with zero sources fails K8s Pod validation; admission
	// must catch it before it reaches engine pods.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Env: []corev1.EnvVar{{
			Name:      "BAD",
			ValueFrom: &corev1.EnvVarSource{},
		}},
	})
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.env[0].valueFrom",
		"exactly one source")
}

func TestValidator_EngineOverrides_RejectsMultipleValueFromSources(t *testing.T) {
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Env: []corev1.EnvVar{{
			Name: "BAD",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "s"},
					Key:                  "k",
				},
			},
		}},
	})
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.env[0].valueFrom",
		"multiple set")
}

func TestEnvVarSourceCount_CountsAllNonNilPointerFields(t *testing.T) {
	// Pin the reflection-based count's contract: it walks pointer fields
	// on EnvVarSource and counts non-nil ones. This is the future-proof
	// path for new source kinds upstream adds (e.g. fileKeyRef) — the
	// generated CRD already embeds them from the upstream OpenAPI, and the
	// validator's one-of check needs to stay aligned without a code
	// change for each new field.
	cases := []struct {
		name string
		src  *corev1.EnvVarSource
		want int
	}{
		{"nil", nil, 0},
		{"empty", &corev1.EnvVarSource{}, 0},
		{
			"one source",
			&corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
			1,
		},
		{
			"two sources",
			&corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{Name: "s"},
					Key:                  "k",
				},
			},
			2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := envVarSourceCount(tc.src)
			if got != tc.want {
				t.Fatalf("envVarSourceCount = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestValidator_EngineOverrides_ValueFromAloneAdmitted(t *testing.T) {
	// Positive case: a ValueFrom-only entry (no Value) is a valid K8s env
	// shape and must pass.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Env: []corev1.EnvVar{{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
		}},
	})
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("ValueFrom-only env rejected: %v", err)
	}
}

func TestValidator_EngineOverrides_ExternalBackendChecksReservedSet(t *testing.T) {
	// engineOverrides on an externally owned binding is structurally meaningful:
	// the same canonical
	// LMCache wire reaches the engine pod whether the cache is managed
	// or operator-supplied, so suppressing `--kv-transfer-config` would
	// silently un-wire the integration in both cases. The
	// reserved-args/env check must therefore fire regardless of ownership.
	v := &CacheBackendValidator{Registry: stubRegistryWithExternal()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		SuppressArgs: []string{"--kv-transfer-config"},
	})
	setCanonicalExternalStorage(cb, "shared.team-a.svc.cluster.local:9000")
	_, err := v.ValidateCreate(context.Background(), cb)
	if err == nil {
		t.Fatalf("External CR suppressing --kv-transfer-config admitted; reserved-arg check must fire on External too")
	}
	if !strings.Contains(err.Error(), "--kv-transfer-config") {
		t.Fatalf("reserved-arg rejection should name the offending flag; got %v", err)
	}
}

func TestValidator_EngineOverrides_MooncakeBackendChecksReservedSet(t *testing.T) {
	// A Mooncake binding reuses the LMCache connector wire (pointed at a
	// mooncakestore:// remote), so the same runtime adapter declares the same
	// reserved args/env. An operator must not be able to
	// un-wire it via engineOverrides any more than on LMCache/External. Use the
	// explicitly injected built-in shipping registry so the shipping
	// adapter's ReservedArgs/ReservedEnv drive the admission check.
	v := shippingValidator()

	// Arg side: suppressing the connector arg must hard-reject, naming the flag.
	cbArg := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		SuppressArgs: []string{"--kv-transfer-config"},
	})
	setCanonicalMooncakeStorage(cbArg)
	if _, err := v.ValidateCreate(context.Background(), cbArg); err == nil ||
		!strings.Contains(err.Error(), "--kv-transfer-config") {
		t.Fatalf("Mooncake CR suppressing --kv-transfer-config must reject naming the flag; got %v", err)
	}

	// Env side: overriding the reserved remote-URL env must hard-reject too.
	cbEnv := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Env: []corev1.EnvVar{{Name: "LMCACHE_REMOTE_URL", Value: "mooncakestore://evil:50051"}},
	})
	setCanonicalMooncakeStorage(cbEnv)
	if _, err := v.ValidateCreate(context.Background(), cbEnv); err == nil ||
		!strings.Contains(err.Error(), "LMCACHE_REMOTE_URL") {
		t.Fatalf("Mooncake CR overriding reserved LMCACHE_REMOTE_URL must reject naming the env; got %v", err)
	}
}

func TestValidator_EngineOverrides_NilRegistry_FallsBackToShippingSet(t *testing.T) {
	// A zero-value validator (Registry: nil) must consult the SAME
	// shipping adapter set in BOTH checkRuntimeAdapter and
	// checkEngineOverrides — otherwise an external binding could admit and then
	// silently bypass reserved-arg enforcement, letting an
	// operator un-wire the cache at the engine pod. Pin both halves of
	// the contract: nil-registry rejects External + suppressed
	// --kv-transfer-config with a field-scoped error.
	v := shippingValidator()
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		SuppressArgs: []string{"--kv-transfer-config"},
	})
	setCanonicalExternalStorage(cb, "shared.team-a.svc.cluster.local:9000")
	_, err := v.ValidateCreate(context.Background(), cb)
	if err == nil {
		t.Fatalf("nil-registry validator admitted External + suppressed --kv-transfer-config; reserved-arg check must fire via the shipping-set fallback")
	}
	if !strings.Contains(err.Error(), "--kv-transfer-config") {
		t.Fatalf("expected rejection naming the offending flag; got %v", err)
	}
}

func TestValidator_EngineOverrides_ExternalBackendAdmittedWhenSafe(t *testing.T) {
	// An externally owned CR carrying engineOverrides that DON'T touch the
	// adapter's reserved set must still admit. The LMCache wire is shared across
	// ownership modes. LMCACHE_CHUNK_SIZE
	// is a perf knob, not reserved; suppressing or amending it is fine.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Env: []corev1.EnvVar{{Name: "LMCACHE_CHUNK_SIZE", Value: "512"}},
	})
	setCanonicalExternalStorage(cb, "shared.team-a.svc.cluster.local:9000")
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("External CR with non-reserved override rejected: %v", err)
	}
}

func TestValidator_EngineOverrides_ExternalRejectsPythonHashSeedOverride(t *testing.T) {
	// The shared LMCache runtime adapter reserves the same env across ownership
	// modes, so a PYTHONHASHSEED override on an externally owned CR
	// is hard-rejected for the same reason — proving the correctness
	// invariant holds across both ownership modes, not just managed.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := withVLLMOverrides(cachev1alpha1.EngineInjectionOverrides{
		Env: []corev1.EnvVar{{Name: "PYTHONHASHSEED", Value: "1"}},
	})
	setCanonicalExternalStorage(cb, "shared.team-a.svc.cluster.local:9000")
	requireInvalidWithCause(t, v, cb,
		"spec.integration.engineOverrides.env[0].name",
		"PYTHONHASHSEED")
}

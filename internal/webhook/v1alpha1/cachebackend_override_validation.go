// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"fmt"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"reflect"
	"strings"
)

// checkEngineOverrides rejects spec.integration.engineOverrides entries
// that would touch the adapter's reserved args/env — the args/env strictly
// required for the integration to function. Catching the overlap here
// gives the operator a field-scoped error naming the offending flag/env
// and the adapter, instead of a crashed engine later.
//
// The adapter is resolved through the same registry the reconciler and
// pod webhook consult; an empty spec.type or a missing adapter is left
// alone (the structural rules above already cover those). The validator
// strips the value half of two-arg args from the operator's input so a
// CR carrying `args: ["--kv-transfer-config", "{json}"]` rejects against
// the reserved leading-flag token rather than treating the JSON value as
// an unrelated entry.
func (v *CacheBackendValidator) checkEngineOverrides(cb *cachev1alpha1.CacheBackend) field.ErrorList {
	if cb.Spec.Integration == nil || cb.Spec.Integration.EngineOverrides == nil {
		return nil
	}
	overrides := cb.Spec.Integration.EngineOverrides
	basePath := field.NewPath("spec", "integration", "engineOverrides")
	var errs field.ErrorList

	// Structural env-shape checks run regardless of whether an adapter
	// matches. The pod webhook copies these EnvVar entries onto engine
	// pods at admission; if any one is shaped in a way the apiserver
	// rejects (empty name, invalid name, both value and valueFrom set),
	// the mutated pod fails Kubernetes Pod validation, blocking workload
	// admission. That would turn a cache misconfiguration into a serving
	// outage — the exact failure mode the fail-open posture exists to
	// avoid — so we reject the CR up front.
	errs = append(errs, checkEngineOverrideEnvShape(overrides.Env, basePath.Child("env"))...)

	// Bypassed only for an empty spec.type — the structural rules
	// already reject that, and piling an "adapter for backend=\"\""
	// cause on top would not help the user.
	if cb.Spec.Type == "" {
		return errs
	}
	registry := v.Registry
	if registry == nil {
		return append(errs, field.InternalError(
			basePath,
			fmt.Errorf("runtime adapter registry is not configured"),
		))
	}
	runtimeID := adapterruntime.ResolveRuntimeID(cb)
	adapter, err := registry.Select(runtimeID, cb)
	if err != nil {
		// checkRuntimeAdapter already reports the unsupported pair; piling
		// a derived "engineOverrides vs (unknown adapter)" complaint on
		// top would not help the user.
		return errs
	}

	reservedArgs := stringSet(adapter.ReservedArgs())
	reservedEnv := stringSet(adapter.ReservedEnv())

	if len(reservedArgs) > 0 {
		argsPath := basePath.Child("args")
		for i, entry := range overrides.Args {
			token := overrideArgFlagToken(entry)
			if token == "" {
				continue
			}
			if reservedArgs[token] {
				errs = append(errs, field.Forbidden(
					argsPath.Index(i),
					reservedArgMessage(token, runtimeID),
				))
			}
		}
		suppressPath := basePath.Child("suppressArgs")
		for i, entry := range overrides.SuppressArgs {
			if reservedArgs[entry] {
				errs = append(errs, field.Forbidden(
					suppressPath.Index(i),
					reservedArgMessage(entry, runtimeID),
				))
			}
		}
	}

	if len(reservedEnv) > 0 {
		envPath := basePath.Child("env")
		for i, entry := range overrides.Env {
			if reservedEnv[entry.Name] {
				errs = append(errs, field.Forbidden(
					envPath.Index(i).Child("name"),
					reservedEnvMessage(entry.Name, runtimeID),
				))
			}
		}
		suppressPath := basePath.Child("suppressEnv")
		for i, entry := range overrides.SuppressEnv {
			if reservedEnv[entry] {
				errs = append(errs, field.Forbidden(
					suppressPath.Index(i),
					reservedEnvMessage(entry, runtimeID),
				))
			}
		}
	}
	return errs
}

// checkEngineOverrideEnvShape rejects EnvVar entries the apiserver itself
// would reject on the engine pod after the webhook copies them in. Mirrors
// the upstream validation rules in k8s.io/kubernetes core validation:
//
//   - Name must be set and conform to [validation.IsEnvVarName].
//   - Value and ValueFrom are mutually exclusive at the K8s API level.
//   - When ValueFrom is set, it must select EXACTLY ONE source (FieldRef,
//     ResourceFieldRef, ConfigMapKeyRef, or SecretKeyRef). Zero sources or
//     multiple sources both fail K8s Pod validation.
//
// Without these checks an operator could admit a CacheBackend that then
// makes every selected engine pod fail K8s pod validation after the
// webhook mutation — a cache misconfiguration cascading into a workload
// outage, which violates the fail-open posture.
func checkEngineOverrideEnvShape(env []corev1.EnvVar, basePath *field.Path) field.ErrorList {
	var errs field.ErrorList
	for i := range env {
		entry := &env[i]
		entryPath := basePath.Index(i)
		if entry.Name == "" {
			errs = append(errs, field.Required(entryPath.Child("name"),
				"engineOverrides env entries must declare a Name"))
		} else {
			for _, msg := range validation.IsEnvVarName(entry.Name) {
				errs = append(errs, field.Invalid(entryPath.Child("name"),
					entry.Name, "invalid env var name: "+msg))
			}
		}
		if entry.Value != "" && entry.ValueFrom != nil {
			errs = append(errs, field.Invalid(entryPath, entry.Name,
				"env entry may set value OR valueFrom, not both — engine pods carrying this entry would fail Kubernetes Pod validation"))
		}
		if entry.ValueFrom != nil {
			errs = append(errs, checkEnvVarSource(entry.ValueFrom, entryPath.Child("valueFrom"))...)
		}
	}
	return errs
}

// checkEnvVarSource enforces the K8s one-of constraint on EnvVarSource:
// exactly one source selector must be set. Both an empty valueFrom and a
// valueFrom with multiple selectors fail K8s Pod validation; admitting
// either would cascade a cache misconfiguration into engine-pod admission
// failure.
//
// Source detection uses reflection over the pointer fields of
// [corev1.EnvVarSource] so the count is future-proof: when a newer
// k8s.io/api adds a new selector (e.g. fileKeyRef, which controller-gen
// already embeds in the generated CRD schema from the upstream OpenAPI),
// the validator picks it up without code changes. Counting a hard-coded
// list of fields would silently undercount and let a multi-source shape
// slip past admission only to be rejected at engine-pod CREATE.
func checkEnvVarSource(src *corev1.EnvVarSource, path *field.Path) field.ErrorList {
	n := envVarSourceCount(src)
	switch {
	case n == 0:
		return field.ErrorList{field.Required(path,
			"valueFrom must select exactly one source (e.g. fieldRef, resourceFieldRef, configMapKeyRef, or secretKeyRef)")}
	case n > 1:
		return field.ErrorList{field.Invalid(path, n,
			"valueFrom must select exactly one source — multiple set")}
	}
	return nil
}

// envVarSourceCount returns the number of non-nil pointer fields on src.
// Each pointer field on [corev1.EnvVarSource] models one selectable
// source; the K8s API rule is exactly one of them. Reflection keeps the
// count aligned with k8s.io/api as it evolves.
func envVarSourceCount(src *corev1.EnvVarSource) int {
	if src == nil {
		return 0
	}
	v := reflect.ValueOf(*src)
	n := 0
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		if f.Kind() == reflect.Pointer && !f.IsNil() {
			n++
		}
	}
	return n
}

// reservedArgMessage formats the rejection a user sees when their
// engineOverrides try to override or suppress an arg the adapter declares
// as reserved. Naming both the flag and the adapter lets the operator
// trace the contract back to its source without digging into adapter code.
func reservedArgMessage(flag string, runtimeID adapterruntime.RuntimeID) string {
	return fmt.Sprintf(
		"arg %q is reserved by the %q runtime adapter and cannot be overridden or suppressed via spec.integration.engineOverrides; "+
			"the adapter strictly requires this flag for the integration to function",
		flag, runtimeID,
	)
}

// reservedEnvMessage formats the rejection a user sees when their
// engineOverrides try to override or suppress an env var the adapter
// declares as reserved.
func reservedEnvMessage(name string, runtimeID adapterruntime.RuntimeID) string {
	return fmt.Sprintf(
		"env %q is reserved by the %q runtime adapter and cannot be overridden or suppressed via spec.integration.engineOverrides; "+
			"the adapter strictly requires this env for the integration to function",
		name, runtimeID,
	)
}

// overrideArgFlagToken returns the leading flag token of an arg entry,
// matching the parser the pod-webhook merge uses so admission rejects
// what the merge would otherwise treat as a flag. "--flag=value" returns
// "--flag"; "--flag" returns "--flag"; a positional (no leading "-")
// returns "" so admission ignores it (positionals never overlap a
// reserved flag name).
func overrideArgFlagToken(arg string) string {
	if !strings.HasPrefix(arg, "-") {
		return ""
	}
	if i := strings.IndexByte(arg, '='); i >= 0 {
		return arg[:i]
	}
	return arg
}

// stringSet returns a set keyed by xs. nil/empty input returns nil so
// callers can use len()/lookup-from-nil semantics without branching.
func stringSet(xs []string) map[string]bool {
	if len(xs) == 0 {
		return nil
	}
	out := make(map[string]bool, len(xs))
	for _, s := range xs {
		out[s] = true
	}
	return out
}

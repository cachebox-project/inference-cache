package pod

import (
	"strings"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// applyEngineInjectionOverrides amends canonical args / env produced by the
// runtime adapter with the operator-supplied overrides from
// spec.integration.engineOverrides. It is the heart of the engine-injection
// override surface: callers (the pod webhook) feed in the post-canonical
// engine container's args and env, apply this, and assign the result back.
//
// Merge contract (locked, see docs/design/cachebackend-api.md):
//
//   - SuppressArgs: each leading flag token (e.g. "--max-model-len") removes
//     the matching canonical entry. Two-arg form ("--flag", "value") drops
//     both; equals form ("--flag=value") drops the single entry.
//   - Args: each entry is parsed for its leading flag token; if the token
//     matches a remaining canonical entry, the canonical entry is REPLACED
//     in place (preserving order). Otherwise the entry is APPENDED.
//   - SuppressEnv: removes any canonical entry whose Name matches.
//   - Env: upserts by Name — override wins for duplicates with a canonical
//     name; canonical entries for other names are preserved.
//
// Admission rejects entries that would touch the adapter's reserved
// args/env, so this helper does not need to enforce reserved semantics
// itself; it operates assuming the override struct is already valid.
//
// Nil-safe: a nil overrides value returns the inputs unchanged.
func applyEngineInjectionOverrides(args []string, env []corev1.EnvVar, overrides *cachev1alpha1.EngineInjectionOverrides) ([]string, []corev1.EnvVar) {
	if overrides == nil {
		return args, env
	}
	args = suppressArgs(args, overrides.SuppressArgs)
	args = overrideArgs(args, overrides.Args)
	env = suppressEnv(env, overrides.SuppressEnv)
	env = overrideEnv(env, overrides.Env)
	return args, env
}

// argFlagToken returns the leading flag token from an arg entry. For
// "--flag=value" it returns "--flag"; for "--flag" it returns "--flag";
// for "value" (a positional) it returns "". The caller uses an empty
// return as "not a flag — never match for suppress / override".
func argFlagToken(arg string) string {
	if !strings.HasPrefix(arg, "-") {
		return ""
	}
	if i := strings.IndexByte(arg, '='); i >= 0 {
		return arg[:i]
	}
	return arg
}

// suppressArgs returns args with every entry whose leading flag token
// matches any name in suppress removed. Two-arg flags drop both their
// own slot and the following value slot (when that slot is not itself a
// flag); equals-form flags drop their single slot. Order is preserved.
func suppressArgs(args []string, suppress []string) []string {
	if len(suppress) == 0 || len(args) == 0 {
		return args
	}
	drop := stringSet(suppress)
	out := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		token := argFlagToken(args[i])
		if token != "" && drop[token] {
			// Two-arg form: also drop the following value slot if it
			// doesn't itself start with `-`. Equals-form (the original
			// entry contained "=") has no trailing value to drop.
			if !strings.ContainsRune(args[i], '=') && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				i++
			}
			continue
		}
		out = append(out, args[i])
	}
	return out
}

// overrideArgs applies the operator's Args overrides on top of args.
// For each override entry whose leading flag token matches a remaining
// canonical entry, the canonical entry is replaced in place (the override's
// form — two-arg vs equals — is preserved verbatim). Entries with no
// matching canonical flag are appended in order. Positionals (no leading
// `-`) are always appended.
func overrideArgs(args []string, overrides []string) []string {
	if len(overrides) == 0 {
		return args
	}
	// Walk the override list, classifying each entry into a logical
	// override request: flag token + the slice of source slots it
	// occupies (1 for equals form or toggle, 2 for two-arg form).
	for i := 0; i < len(overrides); i++ {
		entry := overrides[i]
		token := argFlagToken(entry)
		// Detect two-arg form on the override side: a bare "--flag"
		// followed by a non-flag value.
		twoArg := token != "" && !strings.ContainsRune(entry, '=') && i+1 < len(overrides) && !strings.HasPrefix(overrides[i+1], "-")
		var replacement []string
		if twoArg {
			replacement = []string{entry, overrides[i+1]}
			i++
		} else {
			replacement = []string{entry}
		}

		if token == "" {
			// Positional: always append; positionals can't override a flag.
			args = append(args, replacement...)
			continue
		}
		args = replaceArgFlag(args, token, replacement)
	}
	return args
}

// replaceArgFlag locates the leftmost entry in args whose leading flag
// token matches flag and replaces it (and its trailing value slot, when
// the entry is in two-arg form) with replacement. If no canonical entry
// matches, replacement is appended to args. Order is preserved.
func replaceArgFlag(args []string, flag string, replacement []string) []string {
	for i := 0; i < len(args); i++ {
		token := argFlagToken(args[i])
		if token != flag {
			continue
		}
		// Determine how many slots the canonical entry occupies.
		canonSlots := 1
		if !strings.ContainsRune(args[i], '=') && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			canonSlots = 2
		}
		// Splice replacement into args at position i, dropping canonSlots.
		out := make([]string, 0, len(args)-canonSlots+len(replacement))
		out = append(out, args[:i]...)
		out = append(out, replacement...)
		out = append(out, args[i+canonSlots:]...)
		return out
	}
	return append(args, replacement...)
}

// suppressEnv returns env with every entry whose Name appears in suppress
// removed. Order is preserved.
func suppressEnv(env []corev1.EnvVar, suppress []string) []corev1.EnvVar {
	if len(suppress) == 0 || len(env) == 0 {
		return env
	}
	drop := stringSet(suppress)
	out := make([]corev1.EnvVar, 0, len(env))
	for i := range env {
		if drop[env[i].Name] {
			continue
		}
		out = append(out, env[i])
	}
	return out
}

// overrideEnv upserts each override entry into env by Name: an existing
// entry with the same Name is replaced in place; a new Name is appended.
// Order of canonical entries is preserved.
func overrideEnv(env []corev1.EnvVar, overrides []corev1.EnvVar) []corev1.EnvVar {
	if len(overrides) == 0 {
		return env
	}
	for _, ovr := range overrides {
		replaced := false
		for i := range env {
			if env[i].Name == ovr.Name {
				env[i] = ovr
				replaced = true
				break
			}
		}
		if !replaced {
			env = append(env, ovr)
		}
	}
	return env
}

// engineOverridesFor returns spec.integration.engineOverrides if set on
// cache, else nil. Centralised so the handler does not nil-check the
// integration sub-struct itself at the call site.
func engineOverridesFor(cache *cachev1alpha1.CacheBackend) *cachev1alpha1.EngineInjectionOverrides {
	if cache == nil || cache.Spec.Integration == nil {
		return nil
	}
	return cache.Spec.Integration.EngineOverrides
}

// overrideTargetIndex finds the index of the container the overrides apply
// to. It looks for a container with Name == engineContainerName, falling
// back to the lone container when the pod has exactly one. Returns
// (idx, true) on a hit and (-1, false) when no target can be resolved
// (multi-container pod with no name match, or an empty engineContainerName
// — which is how an adapter signals "skip override application").
//
// Mirrors the adapter's own container-resolution rule so the override
// merge lands on the same container the canonical injection modified.
func overrideTargetIndex(containers []corev1.Container, engineContainerName string) (int, bool) {
	if engineContainerName == "" {
		return -1, false
	}
	for i := range containers {
		if containers[i].Name == engineContainerName {
			return i, true
		}
	}
	if len(containers) == 1 {
		return 0, true
	}
	return -1, false
}

// stringSet returns a set built from xs. A nil or empty input returns nil,
// which the callers above treat as "no items to filter against".
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

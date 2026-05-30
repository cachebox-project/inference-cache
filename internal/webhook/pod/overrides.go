package pod

import (
	"strings"

	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// applyEngineInjectionOverrides amends the engine container's args/env
// produced by the runtime adapter, scoped to the entries the adapter
// itself injected. The webhook snapshots the container BEFORE
// [adapter.InjectEngineConfig] runs and passes (preArgs, preEnv) here; the
// post-injection slices are read from the container. The diff between pre
// and post defines the "adapter-owned" set — what the override surface
// can touch. User pod-template args/env that the adapter did not modify
// are PROTECTED: a CR-driven Suppress or Override that names them is a
// no-op rather than silently mutating the engine pod's own template.
//
// Merge contract (see docs/design/cachebackend-api.md):
//
//   - SuppressArgs strips an entry only when its leading flag token is
//     in BOTH the suppress list AND the adapter-owned set.
//   - Args overrides: for each entry whose leading flag token matches an
//     adapter-owned entry, the canonical entry is REPLACED in place;
//     entries whose token is in neither the adapter-owned nor the user
//     pod's pre-injection set are APPENDED; tokens that collide with a
//     user-owned, adapter-untouched entry are a silent no-op.
//   - SuppressEnv strips by Name, restricted to adapter-owned env.
//   - Env upserts: a Name matching an adapter-owned canonical entry is
//     replaced; a Name not seen in pre-injection is appended; a Name
//     that matches a user-owned, adapter-untouched entry is a silent
//     no-op.
//
// Admission rejects entries that overlap the adapter's reserved set,
// so this helper does not enforce reserved semantics — it operates
// assuming the override struct already passed admission.
//
// Nil-safe: a nil overrides argument returns post inputs unchanged.
func applyEngineInjectionOverrides(
	preArgs, postArgs []string,
	preEnv, postEnv []corev1.EnvVar,
	overrides *cachev1alpha1.EngineInjectionOverrides,
) ([]string, []corev1.EnvVar) {
	if overrides == nil {
		return postArgs, postEnv
	}
	adapterArgs, userArgs := classifyArgFlags(preArgs, postArgs)
	adapterEnv, userEnv := classifyEnvNames(preEnv, postEnv)

	postArgs = suppressArgs(postArgs, overrides.SuppressArgs, adapterArgs)
	postArgs = overrideArgs(postArgs, overrides.Args, adapterArgs, userArgs)
	postEnv = suppressEnv(postEnv, overrides.SuppressEnv, adapterEnv)
	postEnv = overrideEnv(postEnv, overrides.Env, adapterEnv, userEnv)
	return postArgs, postEnv
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

// argsByFlag walks args and returns a map from leading flag token to the
// effective value carried by that flag — the value half of a two-arg pair,
// the right-hand side of an "=" form, or "" for a toggle. Positionals are
// not included (they have no flag identity to key on). Used to diff pre
// vs post snapshots and identify what the adapter owns.
func argsByFlag(args []string) map[string]string {
	out := make(map[string]string, len(args))
	for i := 0; i < len(args); i++ {
		token := argFlagToken(args[i])
		if token == "" {
			continue
		}
		switch {
		case strings.ContainsRune(args[i], '='):
			out[token] = args[i][len(token)+1:]
		case i+1 < len(args) && !strings.HasPrefix(args[i+1], "-"):
			out[token] = args[i+1]
			i++
		default:
			out[token] = ""
		}
	}
	return out
}

// classifyArgFlags returns the set of flag tokens the adapter contributed
// (added or value-changed between pre and post) and the set of flag tokens
// the user pod-template owned and the adapter did NOT touch. Used to scope
// override application so the CR cannot silently mutate user-template args.
func classifyArgFlags(pre, post []string) (adapter, user map[string]bool) {
	preMap := argsByFlag(pre)
	postMap := argsByFlag(post)
	adapter = make(map[string]bool)
	for flag, postVal := range postMap {
		preVal, inPre := preMap[flag]
		if !inPre || preVal != postVal {
			adapter[flag] = true
		}
	}
	user = make(map[string]bool)
	for flag := range preMap {
		if !adapter[flag] {
			user[flag] = true
		}
	}
	return adapter, user
}

// suppressArgs returns args with every entry whose leading flag token
// matches a name in suppress AND that name is in the adapter-owned set
// removed. Two-arg flags drop both their own slot and the following value
// slot (when not itself a flag); equals-form flags drop their single slot.
// Order is preserved.
func suppressArgs(args, suppress []string, adapter map[string]bool) []string {
	if len(suppress) == 0 || len(args) == 0 || len(adapter) == 0 {
		return args
	}
	drop := make(map[string]bool, len(suppress))
	for _, s := range suppress {
		if adapter[s] {
			drop[s] = true
		}
	}
	if len(drop) == 0 {
		return args
	}
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

// overrideArgs applies the operator's Args overrides on top of args, scoped
// to adapter-owned flags. For each override entry whose leading flag token:
//   - matches an adapter-owned canonical entry: the canonical entry is
//     replaced in place (the override's form — two-arg vs equals — wins).
//   - is absent from BOTH the adapter-owned set and the user pre-injection
//     set: appended.
//   - matches a user-owned, adapter-untouched entry: silently skipped (the
//     CR has no authority over the engine pod's own template entries).
//
// Positionals (no leading `-`) are always appended. Order is preserved.
func overrideArgs(args, overrides []string, adapter, user map[string]bool) []string {
	if len(overrides) == 0 {
		return args
	}
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
			// Positional: append; positionals can't override a flag.
			args = append(args, replacement...)
			continue
		}
		switch {
		case adapter[token]:
			args = replaceArgFlag(args, token, replacement)
		case !user[token]:
			args = append(args, replacement...)
		}
		// else: user-owned, adapter-untouched — silent no-op.
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

// envByName returns a map from env var Name to (Value, ValueFrom-present).
// Used to diff pre vs post snapshots and identify adapter-owned env.
// The bool half catches the case where the adapter swapped a Value-bearing
// entry for a ValueFrom-bearing one (or vice versa) without changing Value,
// so the change is still recognised as adapter-owned.
func envByName(env []corev1.EnvVar) map[string]envSignature {
	out := make(map[string]envSignature, len(env))
	for i := range env {
		out[env[i].Name] = envSignature{
			value:     env[i].Value,
			hasValueF: env[i].ValueFrom != nil,
		}
	}
	return out
}

type envSignature struct {
	value     string
	hasValueF bool
}

// classifyEnvNames returns the set of env Names the adapter contributed
// (added or value-changed) and the set of user-owned, adapter-untouched
// Names. Used the same way as classifyArgFlags but on env.
func classifyEnvNames(pre, post []corev1.EnvVar) (adapter, user map[string]bool) {
	preMap := envByName(pre)
	postMap := envByName(post)
	adapter = make(map[string]bool)
	for name, postSig := range postMap {
		preSig, inPre := preMap[name]
		if !inPre || preSig != postSig {
			adapter[name] = true
		}
	}
	user = make(map[string]bool)
	for name := range preMap {
		if !adapter[name] {
			user[name] = true
		}
	}
	return adapter, user
}

// suppressEnv returns env with every entry whose Name is in BOTH suppress
// AND adapter removed. User-owned, adapter-untouched env is protected.
// Order is preserved.
func suppressEnv(env []corev1.EnvVar, suppress []string, adapter map[string]bool) []corev1.EnvVar {
	if len(suppress) == 0 || len(env) == 0 || len(adapter) == 0 {
		return env
	}
	drop := make(map[string]bool, len(suppress))
	for _, s := range suppress {
		if adapter[s] {
			drop[s] = true
		}
	}
	if len(drop) == 0 {
		return env
	}
	out := make([]corev1.EnvVar, 0, len(env))
	for i := range env {
		if drop[env[i].Name] {
			continue
		}
		out = append(out, env[i])
	}
	return out
}

// overrideEnv upserts each override entry into env, scoped to adapter-owned
// Names. A Name that matches an adapter-owned canonical entry is replaced;
// a Name not seen in pre-injection is appended; a Name that matches a
// user-owned, adapter-untouched entry is silently skipped. Order of
// canonical entries is preserved.
func overrideEnv(env []corev1.EnvVar, overrides []corev1.EnvVar, adapter, user map[string]bool) []corev1.EnvVar {
	if len(overrides) == 0 {
		return env
	}
	for _, ovr := range overrides {
		switch {
		case adapter[ovr.Name]:
			for i := range env {
				if env[i].Name == ovr.Name {
					env[i] = ovr
					break
				}
			}
		case !user[ovr.Name]:
			env = append(env, ovr)
		}
		// else: user-owned, adapter-untouched — silent no-op.
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

package runtime

import (
	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// KernelCheckInitContainer renders the lmcache-kernel-check init container for
// a vLLM+LMCache engine pod, or (nil, nil) when the gate does not apply.
//
// Decision table (mode from AnnotationLMCacheKernelCheck on the CacheBackend):
//
//	off                         → nil (never inject)
//	auto (default) + no GPU     → nil (c_ops/CUDA is GPU-only; a CPU build has
//	                              no c_ops and must not false-positive)
//	auto + GPU                  → inject report-only
//	report-only                 → inject report-only (even on CPU; operator forced)
//	strict                      → inject strict (exit 1 on failure → pod stuck in Init)
//
// The init container reuses the resolved engine container's IMAGE,
// ImagePullPolicy, and SecurityContext so the check runs in the exact runtime
// that would load c_ops — no extra image pull (safe to default-on, unlike the
// subscriber sidecar), no skew between a cached check image and a freshly
// pulled serving image (mutable tags + Always), and the same security posture
// (so a pod valid under a restricted Pod Security Standard stays valid — the
// init container can't make an otherwise-admissible engine pod fail admission,
// preserving the fail-open contract). It requests no GPU (the missing-libcudart
// failure is caught at dlopen without a device).
//
// Returns (nil, nil) when the engine container can't be resolved (multi-
// container pod with no container named EngineContainerName) — emitting a
// container with no image to copy is worse than skipping; the webhook logs it.
func (vllmLMCacheAdapter) KernelCheckInitContainer(cache *cachev1alpha1.CacheBackend, pod *corev1.Pod) (*corev1.Container, error) {
	if cache == nil || pod == nil {
		return nil, nil
	}
	mode := resolveKernelCheckMode(cache)
	if mode == KernelCheckModeOff {
		return nil, nil
	}
	engine := engineContainerForKernelCheck(pod)
	if engine == nil || engine.Image == "" {
		return nil, nil
	}
	if mode == KernelCheckModeAuto && !requestsGPU(engine) {
		return nil, nil
	}

	strict := mode == KernelCheckModeStrict

	// Copy the engine's env so the check loads c_ops in the engine's actual
	// environment (an operator-set PYTHONPATH / LD_LIBRARY_PATH that c_ops
	// needs to dlopen would otherwise be absent and false-fail the check).
	env := append([]corev1.EnvVar(nil), engine.Env...)
	if strict {
		env = append(env, corev1.EnvVar{Name: EnvKernelCheckStrict, Value: "1"})
	}

	// Command form is mode-dependent so report-only is TRULY fail-open at the
	// POD level, not just within the script. A bare `python3 -c` exits non-zero
	// on interpreter-not-found (127) or an OOM/SIGKILL during `import torch`
	// (137) — OUTSIDE the script's report-only exit-0 path — which Kubernetes
	// retries and which would block the engine pod, breaking the "cache is
	// never a serving dependency" contract. report-only therefore wraps the
	// interpreter in a shell that always exits 0 (the script still writes its
	// OK/FAIL termination message first, so the reconciler still sees the
	// result). Strict runs python3 directly so a real failure — including the
	// check being unable to run at all — propagates and holds the pod, which
	// is the point of opting into strict.
	command := []string{"python3", "-c", kernelCheckScript}
	if !strict {
		command = []string{"/bin/sh", "-c", `python3 -c "$0"; exit 0`, kernelCheckScript}
	}

	return &corev1.Container{
		Name:  LMCacheKernelCheckContainerName,
		Image: engine.Image,
		// Copy (don't hard-code) the engine's pull policy, security context,
		// env, env-from, mounts, and working dir so the check runs in the
		// engine's exact image, security posture, and load environment: a
		// mutable tag with ImagePullPolicy=Always must not let the check run a
		// stale cached image; a restricted-PSA-compliant engine pod must stay
		// admissible after the init container is appended; and an engine that
		// depends on operator-supplied env/mounts to load c_ops must not be
		// false-failed by a stripped-down checker.
		ImagePullPolicy:          engine.ImagePullPolicy,
		SecurityContext:          engine.SecurityContext.DeepCopy(),
		WorkingDir:               engine.WorkingDir,
		Command:                  command,
		Env:                      env,
		EnvFrom:                  append([]corev1.EnvFromSource(nil), engine.EnvFrom...),
		VolumeMounts:             append([]corev1.VolumeMount(nil), engine.VolumeMounts...),
		VolumeDevices:            append([]corev1.VolumeDevice(nil), engine.VolumeDevices...),
		Resources:                kernelCheckResources(),
		TerminationMessagePath:   "/dev/termination-log",
		TerminationMessagePolicy: corev1.TerminationMessageReadFile,
	}, nil
}

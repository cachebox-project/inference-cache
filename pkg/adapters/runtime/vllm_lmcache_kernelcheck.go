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
	// needs to dlopen would otherwise be absent and false-fail the check) — but
	// strip any pre-existing KERNEL_CHECK_STRICT and set it explicitly per mode.
	// Inheriting a stray KERNEL_CHECK_STRICT=1 from the engine container would
	// otherwise flip report-only into a fail-CLOSED check and make the
	// controller mis-classify the pod as strict. Setting it explicitly in
	// container.Env also overrides any value sourced from the engine's
	// EnvFrom (container.Env wins over envFrom for the same key).
	strictVal := "0"
	if strict {
		strictVal = "1"
	}
	env := make([]corev1.EnvVar, 0, len(engine.Env)+1)
	for _, e := range engine.Env {
		if e.Name == EnvKernelCheckStrict {
			continue
		}
		env = append(env, e)
	}
	env = append(env, corev1.EnvVar{Name: EnvKernelCheckStrict, Value: strictVal})

	// Both modes invoke the engine image's own python3 directly; the script's
	// KERNEL_CHECK_STRICT-keyed exit code is what makes report-only fail-open
	// (always exit 0, even on a c_ops failure) and strict fail-closed (exit 1).
	// python3 is the right entrypoint: it is the engine's OWN interpreter, so
	// it is guaranteed present on any functioning vLLM/LMCache image — if it
	// can't run, the Python engine itself is already broken, which is not a
	// false serving outage caused by this check. (We deliberately do NOT wrap
	// in /bin/sh to "guarantee" exit 0: a minimal/distroless image may lack a
	// shell, which would reintroduce the very pod-block the wrapper was meant
	// to avoid — and such an image lacks python3/lmcache too, so the check is
	// moot there.) The residual block window — python3 truly cannot start, or
	// an OOM during `import torch` — is mitigated by the generous memory limit
	// in kernelCheckResources and is documented in cachebackend-api.md.
	command := []string{"python3", "-c", kernelCheckScript}

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

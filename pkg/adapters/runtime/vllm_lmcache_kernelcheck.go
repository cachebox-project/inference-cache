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

	var env []corev1.EnvVar
	if mode == KernelCheckModeStrict {
		env = []corev1.EnvVar{{Name: EnvKernelCheckStrict, Value: "1"}}
	}
	return &corev1.Container{
		Name:  LMCacheKernelCheckContainerName,
		Image: engine.Image,
		// Copy (don't hard-code) the engine's pull policy + security context so
		// the check runs in the engine's exact image and security posture: a
		// mutable tag with ImagePullPolicy=Always must not let the check run a
		// stale cached image, and a restricted-PSA-compliant engine pod must
		// stay admissible after the init container is appended.
		ImagePullPolicy:          engine.ImagePullPolicy,
		SecurityContext:          engine.SecurityContext.DeepCopy(),
		Command:                  []string{"python3", "-c", kernelCheckScript},
		Env:                      env,
		Resources:                kernelCheckResources(),
		TerminationMessagePath:   "/dev/termination-log",
		TerminationMessagePolicy: corev1.TerminationMessageReadFile,
	}, nil
}

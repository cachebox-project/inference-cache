// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package runtime

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
)

// gpuResourceName is the extended resource an engine container requests when
// it wants a GPU. Auto mode skips CPU-only engines.
const gpuResourceName = corev1.ResourceName("nvidia.com/gpu")

// kernelCheckScript is the Python the init container runs against the engine
// image. It locates the package dir WITHOUT executing lmcache.__init__ (which
// swallows the c_ops failure into a WARNING and overrides
// sys.modules["lmcache.c_ops"] with a fallback shim, so a naive
// `import lmcache.c_ops` ALWAYS succeeds — a silent no-op). Instead it
// dlopens the native c_ops*.so from disk via ctypes.CDLL, which re-does the
// real dynamic load and raises on a missing/mismatched libcudart (empirically:
// "OSError: libcudart.so.13: cannot open shared object file"). torch MUST be
// imported first — the extension DT_NEEDs libtorch's libc10.so.
const kernelCheckScript = `
import sys, os, glob, importlib.util, ctypes
STRICT = os.environ.get("KERNEL_CHECK_STRICT") == "1"
MSG = "/dev/termination-log"
def emit(s):
    try:
        with open(MSG, "w") as f: f.write(s[:3500])
    except Exception:
        pass
def fail(s):
    emit("FAIL: " + s)
    sys.exit(1 if STRICT else 0)
try:
    spec = importlib.util.find_spec("lmcache")
    locs = list(spec.submodule_search_locations) if spec else []
    if not locs:
        fail("lmcache not importable")
    sos = sorted(glob.glob(os.path.join(locs[0], "c_ops*.so")))
    if not sos:
        fail("no native c_ops extension present (pure-python/CPU build)")
    import torch  # required: c_ops.so DT_NEEDED libtorch (libc10.so)
    # dlopen the native extension to force the dynamic loader to resolve every
    # DT_NEEDED lib (libtorch, libcudart, ...). This is where a CUDA-kernel
    # mismatch surfaces (e.g. a cu13 wheel on a cu12 image → "libcudart.so.13:
    # cannot open shared object file"). ctypes.CDLL is used rather than
    # importlib.exec_module on purpose: exec_module derives the C init symbol
    # (PyInit_<module>) from the spec name and would FAIL to find it for any
    # name other than the extension's own, false-failing a HEALTHY engine.
    # CDLL needs no init symbol — it tests exactly the dlopen/DT_NEEDED
    # resolution where the kernel/CUDA mismatch lives.
    ctypes.CDLL(sos[0])
    emit("OK")
except SystemExit:
    raise
except BaseException as e:
    fail("%s: %r" % (type(e).__name__, e))
`

// resolveKernelCheckMode returns the effective mode for a CacheBackend.
// Unrecognized values fall back to auto; admission rejects them before they
// reach here (IsValidKernelCheckMode), so in practice only the known values
// arrive — the fallback is a defense-in-depth default, not the typo guard.
func resolveKernelCheckMode(cache *cachev1alpha1.CacheBackend) string {
	if cache == nil {
		return enginebinding.KernelCheckModeAuto
	}
	switch cache.Annotations[enginebinding.AnnotationLMCacheKernelCheck] {
	case enginebinding.KernelCheckModeReportOnly:
		return enginebinding.KernelCheckModeReportOnly
	case enginebinding.KernelCheckModeStrict:
		return enginebinding.KernelCheckModeStrict
	case enginebinding.KernelCheckModeOff:
		return enginebinding.KernelCheckModeOff
	default:
		return enginebinding.KernelCheckModeAuto
	}
}

// engineContainerForKernelCheck resolves the engine container in pod whose
// image the init container reuses. Mirrors the adapter's documented
// convention: prefer the container named EngineContainerName; else, a
// single-container pod IS the engine; else (multi-container, no match) return
// nil so the caller skips. MUST be resolved before the webhook appends the
// observation sidecar (which would defeat the single-container fallback).
func engineContainerForKernelCheck(pod *corev1.Pod) *corev1.Container {
	if pod == nil {
		return nil
	}
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == EngineContainerName {
			return &pod.Spec.Containers[i]
		}
	}
	if len(pod.Spec.Containers) == 1 {
		return &pod.Spec.Containers[0]
	}
	return nil
}

// requestsGPU reports whether c requests an nvidia.com/gpu (limit or request
// with a positive quantity).
func requestsGPU(c *corev1.Container) bool {
	if c == nil {
		return false
	}
	for _, rl := range []corev1.ResourceList{c.Resources.Limits, c.Resources.Requests} {
		if q, ok := rl[gpuResourceName]; ok && q.Sign() > 0 {
			return true
		}
	}
	return false
}

// kernelCheckResources is the resource envelope for the init container: small
// CPU/memory requests, no limits, no nvidia.com/gpu. There is no resource shape
// that is fail-open under EVERY namespace policy (a ResourceQuota/LimitRange may
// REQUIRE per-container requests, while a LimitRange max may REJECT large
// ones); this is the most broadly-compatible compromise:
//   - No nvidia.com/gpu: the missing-libcudart dlopen failure is caught at load
//     time without a device.
//   - Small requests (not none): a namespace with a `requests.*` ResourceQuota
//     or a min-only LimitRange rejects a container that specifies no request,
//     which would block the engine pod — so the check declares modest ones. The
//     engine container (a GPU vLLM image needing GiB of RAM) requests far more,
//     so these are below any per-container max it already satisfies AND are
//     subsumed by it in the pod's effective request (init requests are max'd
//     with, not summed onto, app requests) — no scheduling/quota footprint
//     increase.
//   - No limits: an explicit limit could exceed a LimitRange per-container max
//     the engine still satisfies; omitting it lets any LimitRange default apply
//     within bounds and leaves `import torch` bounded only by the pod/node.
func kernelCheckResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("50m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
}

// KernelCheckInitContainer renders the LMCache kernel-check init container for
// a vLLM+LMCache engine pod, or nil when the configured gate does not apply.
// Auto mode checks GPU pods in report-only mode; report-only and strict force
// injection; off disables it.
func (vllmLMCacheAdapter) KernelCheckInitContainer(cache *cachev1alpha1.CacheBackend, pod *corev1.Pod) (*corev1.Container, error) {
	if cache == nil || pod == nil {
		return nil, nil
	}
	mode := resolveKernelCheckMode(cache)
	if mode == enginebinding.KernelCheckModeOff {
		return nil, nil
	}
	engine := engineContainerForKernelCheck(pod)
	if engine == nil || engine.Image == "" {
		return nil, nil
	}
	if mode == enginebinding.KernelCheckModeAuto && !requestsGPU(engine) {
		return nil, nil
	}

	strictValue := "0"
	if mode == enginebinding.KernelCheckModeStrict {
		strictValue = "1"
	}
	env := make([]corev1.EnvVar, 0, len(engine.Env)+1)
	for _, entry := range engine.Env {
		if entry.Name != enginebinding.EnvKernelCheckStrict {
			env = append(env, entry)
		}
	}
	env = append(env, corev1.EnvVar{Name: enginebinding.EnvKernelCheckStrict, Value: strictValue})

	return &corev1.Container{
		Name:                     enginebinding.LMCacheKernelCheckContainerName,
		Image:                    engine.Image,
		ImagePullPolicy:          engine.ImagePullPolicy,
		SecurityContext:          engine.SecurityContext.DeepCopy(),
		WorkingDir:               engine.WorkingDir,
		Command:                  []string{"python3", "-c", kernelCheckScript},
		Env:                      env,
		EnvFrom:                  append([]corev1.EnvFromSource(nil), engine.EnvFrom...),
		VolumeMounts:             append([]corev1.VolumeMount(nil), engine.VolumeMounts...),
		VolumeDevices:            append([]corev1.VolumeDevice(nil), engine.VolumeDevices...),
		Resources:                kernelCheckResources(),
		TerminationMessagePath:   "/dev/termination-log",
		TerminationMessagePolicy: corev1.TerminationMessageReadFile,
	}, nil
}

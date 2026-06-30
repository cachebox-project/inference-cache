package runtime

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// Kernel-check wire contract. These constants are the single source of truth
// shared between the injecting adapter (writes the init container + script)
// and the C2 reconciler (reads the annotation + parses the termination
// message). The reconciler imports them so the two sides cannot drift.
const (
	// LMCacheKernelCheckContainerName is the init container the adapter
	// injects into LMCache GPU engine pods. It force-loads the native
	// lmcache c_ops extension to detect a CUDA-kernel/runtime mismatch
	// (e.g. a cu13-built wheel on a cu12.9 image) that otherwise degrades
	// T2 reload to a slow single-stream torch fallback, silently.
	LMCacheKernelCheckContainerName = "lmcache-kernel-check"

	// AnnotationLMCacheKernelCheck selects the kernel-check mode, read off
	// the bound CacheBackend. Values: KernelCheckMode*. Unset == auto.
	AnnotationLMCacheKernelCheck = "inferencecache.io/lmcache-kernel-check"

	// Kernel-check modes (values of AnnotationLMCacheKernelCheck).
	//   auto        — inject report-only iff the engine container requests a GPU.
	//   report-only — always inject, exit 0 even on failure (fail-open).
	//   strict      — always inject, exit 1 on failure (engine pod stuck in Init).
	//   off         — never inject.
	KernelCheckModeAuto       = "auto"
	KernelCheckModeReportOnly = "report-only"
	KernelCheckModeStrict     = "strict"
	KernelCheckModeOff        = "off"

	// Kernel-check termination-message contract. The init container writes
	// exactly one of these prefixes to /dev/termination-log. The reconciler
	// asserts a kernel mismatch ONLY on KernelCheckMsgFailPrefix; any other
	// terminated message (or a non-zero exit with no message) is an
	// indeterminate error, never a mismatch (avoids false alarms) and never
	// healthy (avoids false greens).
	KernelCheckMsgOK         = "OK"
	KernelCheckMsgFailPrefix = "FAIL:"

	// EnvKernelCheckStrict is the env var the adapter sets on the init
	// container: "1" in strict mode, "0" otherwise (rendered explicitly in both
	// modes so it overrides any value inherited from the engine's env/envFrom).
	// The detector script and the controller both treat only "1" as strict.
	EnvKernelCheckStrict = "KERNEL_CHECK_STRICT"

	// gpuResourceName is the extended resource an engine container requests
	// when it wants a GPU. The kernel-check is GPU-only (c_ops/CUDA does not
	// exist on a CPU build), so auto mode injects only when this is requested.
	gpuResourceName = corev1.ResourceName("nvidia.com/gpu")
)

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

// InitContainerProvider is the OPTIONAL interface an adapter implements when
// it injects a deploy-time init container into the engine pod. The pod
// webhook type-asserts the selected adapter to this interface (mirroring the
// PairLister optional-interface pattern), so adapters that have no init
// container (External passthrough, reference) need no change. Returning
// (nil, nil) means "no init container for this (cache, pod)".
type InitContainerProvider interface {
	KernelCheckInitContainer(cache *cachev1alpha1.CacheBackend, pod *corev1.Pod) (*corev1.Container, error)
}

// IsValidKernelCheckMode reports whether s is an accepted value for the
// AnnotationLMCacheKernelCheck annotation. The empty string is accepted (the
// annotation is unset / treated as auto); any other unrecognized value is
// rejected by admission (see the CacheBackend validating webhook) so a typo
// like "strcit" can't silently relax strict enforcement back to report-only.
func IsValidKernelCheckMode(s string) bool {
	switch s {
	case "", KernelCheckModeAuto, KernelCheckModeReportOnly, KernelCheckModeStrict, KernelCheckModeOff:
		return true
	default:
		return false
	}
}

// resolveKernelCheckMode returns the effective mode for a CacheBackend.
// Unrecognized values fall back to auto; admission rejects them before they
// reach here (IsValidKernelCheckMode), so in practice only the known values
// arrive — the fallback is a defense-in-depth default, not the typo guard.
func resolveKernelCheckMode(cache *cachev1alpha1.CacheBackend) string {
	if cache == nil {
		return KernelCheckModeAuto
	}
	switch cache.Annotations[AnnotationLMCacheKernelCheck] {
	case KernelCheckModeReportOnly:
		return KernelCheckModeReportOnly
	case KernelCheckModeStrict:
		return KernelCheckModeStrict
	case KernelCheckModeOff:
		return KernelCheckModeOff
	default:
		return KernelCheckModeAuto
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

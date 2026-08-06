package runtime

import (
	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// Kernel-check wire contract shared by the injecting built-in adapter and the
// controller that reads its annotation and termination message.
const (
	LMCacheKernelCheckContainerName = "lmcache-kernel-check"
	AnnotationLMCacheKernelCheck    = "inferencecache.io/lmcache-kernel-check"

	KernelCheckModeAuto       = "auto"
	KernelCheckModeReportOnly = "report-only"
	KernelCheckModeStrict     = "strict"
	KernelCheckModeOff        = "off"

	KernelCheckMsgOK         = "OK"
	KernelCheckMsgFailPrefix = "FAIL:"
	EnvKernelCheckStrict     = "KERNEL_CHECK_STRICT"
)

// InitContainerProvider is the optional capability implemented by an adapter
// that renders an engine-pod init container. Returning nil means no check is
// required for the given cache and pod.
type InitContainerProvider interface {
	KernelCheckInitContainer(cache *cachev1alpha1.CacheBackend, pod *corev1.Pod) (*corev1.Container, error)
}

// IsValidKernelCheckMode reports whether s is an accepted annotation value.
// Empty means the default auto mode.
func IsValidKernelCheckMode(s string) bool {
	switch s {
	case "", KernelCheckModeAuto, KernelCheckModeReportOnly, KernelCheckModeStrict, KernelCheckModeOff:
		return true
	default:
		return false
	}
}

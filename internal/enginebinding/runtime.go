// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package enginebinding

import (
	corev1 "k8s.io/api/core/v1"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// Private wire contracts shared by built-in adapters, admission, and
// controllers. They are implementation details of the controller binary, not
// part of the public build-time adapter interface.
const (
	SubscriberContainerName = "kvevent-subscriber"

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

// InitContainerProvider is the private capability implemented by a built-in
// adapter that renders an engine-pod init container. Returning nil means no
// check is required for the given cache and pod.
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

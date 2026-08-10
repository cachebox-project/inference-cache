// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package enginebinding

import (
	"strconv"
	"strings"
)

const (
	// LabelLMCacheMPMetrics marks an engine Pod whose successfully injected
	// PodLocal LMCache native sidecar exposes Prometheus metrics. The optional
	// observability overlay selects this label and scrapes the sidecar directly;
	// it is deliberately not applied to legacy in-process connectors.
	LabelLMCacheMPMetrics = "inferencecache.io/lmcache-mp-metrics"

	// LabelLMCacheMPMetricsEnabled is the selector value used by the shipped
	// LMCache PodMonitor.
	LabelLMCacheMPMetricsEnabled = "true"

	// AnnotationSkip lets an operator explicitly opt a pod out of injection.
	AnnotationSkip = "inferencecache.io/skip-inject"

	// AnnotationInjectedBy records the namespace/name of the CacheBackend that
	// wired an engine pod.
	AnnotationInjectedBy = "inferencecache.io/injected-by"

	// AnnotationInjectedByUID records the UID of the CacheBackend observed at
	// admission time and prevents stale name-only binding claims.
	AnnotationInjectedByUID = "inferencecache.io/injected-by-uid"

	// AnnotationInjectedGeneration records the CacheBackend generation whose
	// connector/server configuration was rendered into the immutable Pod. It
	// lets status distinguish current wiring from Pods that predate a spec
	// update and therefore require recreation.
	AnnotationInjectedGeneration = "inferencecache.io/injected-generation"

	// AnnotationInjectSkipped marks an intentional operator opt-out.
	AnnotationInjectSkipped = "inferencecache.io/inject-skipped"

	// InjectSkippedReasonSkipAnnotation is the stable opt-out reason value.
	InjectSkippedReasonSkipAnnotation = "skip-inject-annotation"
)

// SkipAnnotationOptsOut reports whether an annotation value disables
// injection. ParseBool truthy values and unrecognized non-empty values opt out;
// explicit false values and common false synonyms leave injection enabled.
func SkipAnnotationOptsOut(value string) bool {
	if value == "" {
		return false
	}
	if parsed, err := strconv.ParseBool(value); err == nil {
		return parsed
	}
	switch strings.ToLower(value) {
	case "no", "off", "disable", "disabled":
		return false
	default:
		return true
	}
}

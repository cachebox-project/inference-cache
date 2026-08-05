package enginebinding

import (
	"strconv"
	"strings"
)

const (
	// AnnotationSkip lets an operator explicitly opt a pod out of injection.
	AnnotationSkip = "inferencecache.io/skip-inject"

	// AnnotationInjectedBy records the namespace/name of the CacheBackend that
	// wired an engine pod.
	AnnotationInjectedBy = "inferencecache.io/injected-by"

	// AnnotationInjectedByUID records the UID of the CacheBackend observed at
	// admission time and prevents stale name-only binding claims.
	AnnotationInjectedByUID = "inferencecache.io/injected-by-uid"

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

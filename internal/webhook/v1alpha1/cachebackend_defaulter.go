// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"time"
)

// Phase-1 defaults applied by the mutating webhook. Centralised here so the
// tests pin the same constants the handler uses.
//
// Literal-value defaults (spec.type=LMCache, spec.integration.mode=Offload,
// spec.integration.role=ReadWrite, spec.integration.failOpen=true) are
// expressed via `+kubebuilder:default=` markers on the API types and stamped
// by the apiserver before this webhook runs. The webhook handles
// context-sensitive defaults and defaults the schema cannot express:
//
//   - spec.observation.firstEventTimeout: the CRD-schema default only fires
//     when spec.observation is present in the submitted object; when the
//     operator omits observation entirely the webhook materialises it here so
//     the persisted CR carries the readiness-gate deadline rather than relying
//     on the controller's runtime fallback.
//
// Per-field rationale lives in the godoc on each spec field; this comment
// is the index for the webhook-stamped defaults specifically.
const (
	// defaultFirstEventTimeout mirrors the +kubebuilder:default on
	// spec.observation.firstEventTimeout. The CRD-schema default only applies
	// when spec.observation is present in the submitted object.
	defaultFirstEventTimeout = 5 * time.Minute
)

// CacheBackendDefaulter applies the Phase-1 defaults that CRD-schema
// `+kubebuilder:default=` markers cannot express at admission time. Literal
// defaults (spec.type, integration.mode, integration.role,
// integration.failOpen) ride on schema
// markers and are stamped by the apiserver before this handler runs;
// the webhook handles context-sensitive and schema-inexpressible defaults:
//
//   - Materialises spec.integration so its nested schema defaults are applied.
//   - Materialises spec.observation to persist
//     spec.observation.firstEventTimeout when the operator omits the parent
//     block entirely.
//
// It does NOT stamp spec.integration.failOpen explicitly — once the
// defaulter materialises spec.integration above, the apiserver applies
// the `+kubebuilder:default=true` marker on the now-present failOpen
// field (alongside mode and role) before persisting,
// so an admitted CR with no integration block ends up with failOpen
// populated in etcd. The read-time fallback in [IntegrationFailOpen]
// covers callers that bypass the apiserver (raw-struct test invocation,
// partial deserialization). It implements [admission.Defaulter] over
// CacheBackend.
type CacheBackendDefaulter struct{}

// +kubebuilder:webhook:path=/mutate-inferencecache-io-v1alpha1-cachebackend,mutating=true,failurePolicy=fail,sideEffects=None,groups=inferencecache.io,resources=cachebackends,verbs=create;update,versions=v1alpha1,name=mcachebackend.inferencecache.io,admissionReviewVersions=v1

// Default implements [admission.Defaulter]. It applies the defaults the
// CRD-schema markers cannot express:
//
//   - Materialises spec.integration when omitted so its nested schema defaults
//     are applied.
//   - Materialises spec.observation when omitted so
//     spec.observation.firstEventTimeout carries the readiness-gate deadline.
//
// Every other default (spec.type=LMCache, integration.mode=Offload,
//
//	integration.role=ReadWrite,
//
// integration.failOpen=true) rides on a `+kubebuilder:default=` marker and is
// stamped by the apiserver before this handler runs. Note that the nested
// integration.* markers only fire when spec.integration is already present
// in the submitted object — when the operator omits the integration block
// entirely the apiserver has nothing to apply nested defaults to, which is
// why the webhook materialises the parent below. Callers that bypass
// admission use the API's read-time mode and fail-open helpers and the
// built-in adapters' equivalent ReadWrite role fallback.
//
// A non-nil pointer or non-empty value is treated as an explicit operator
// choice and left alone, preserving the established "defaulter never
// clobbers" contract.
func (d *CacheBackendDefaulter) Default(ctx context.Context, cb *cachev1alpha1.CacheBackend) error {
	logf.FromContext(ctx).V(1).Info("defaulting CacheBackend",
		"namespace", cb.Namespace, "name", cb.Name)

	if cb.Spec.Integration == nil {
		cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	}
	if cb.Spec.Observation == nil {
		cb.Spec.Observation = &cachev1alpha1.CacheBackendObservationSpec{}
	}
	if cb.Spec.Observation.FirstEventTimeout == nil {
		cb.Spec.Observation.FirstEventTimeout = &metav1.Duration{Duration: defaultFirstEventTimeout}
	}

	return nil
}

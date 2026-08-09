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
// Literal-value defaults (spec.type=LMCache, spec.deploymentKind=Deployment,
// spec.replicas=1, spec.integration.mode=Offload,
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
//   - spec.autoscaling.minReplicas: cluster-context default computed from
//     spec.replicas at admission so the HPA's floor matches the operator's
//     baseline declaration rather than a hard-coded constant.
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
// defaults (spec.type, deploymentKind, replicas,
// integration.mode, integration.role, integration.failOpen) ride on schema
// markers and are stamped by the apiserver before this handler runs;
// the webhook handles context-sensitive and schema-inexpressible defaults:
//
//   - Materialises spec.integration so its nested schema defaults are applied.
//   - Materialises spec.observation to persist
//     spec.observation.firstEventTimeout when the operator omits the parent
//     block entirely.
//   - Computes spec.autoscaling.minReplicas from spec.replicas when
//     autoscaling is opted into and minReplicas is left unset — the HPA
//     floor needs to follow the workload's baseline declaration, which is
//     cluster-context the schema cannot encode.
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
//   - Computes spec.autoscaling.minReplicas from spec.replicas when
//     autoscaling is opted in and minReplicas is left unset.
//
// Every other Phase-1 default (spec.type=LMCache, deploymentKind=Deployment,
// replicas=1, integration.mode=Offload,
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

	// autoscaling.minReplicas defaults to spec.replicas when autoscaling is
	// opted into and the operator left the floor unset. The literal
	// spec.replicas default (=1) is applied by the apiserver from the
	// `+kubebuilder:default` marker before this handler runs, so reading
	// cb.Spec.Replicas here sees either the operator's explicit value or
	// the schema default — never nil for a CR that came through admission.
	// The nil guard is defence-in-depth for tests that construct a
	// CacheBackend directly and call Default without the apiserver in the
	// loop; we leave minReplicas alone in that case rather than dereference
	// a nil pointer.
	//
	// The `>= 1` guard mirrors the CRD schema's `minimum: 1` on
	// autoscaling.minReplicas: spec.replicas allows 0 (scale-to-zero), so a
	// CR with `replicas: 0` + opted-in autoscaling would otherwise have the
	// defaulter stamp `minReplicas: 0`, which the apiserver then rejects
	// against the schema's minimum. Refusing to default in that case leaves
	// the field unset so the operator's misconfiguration surfaces as a
	// missing-required-field validation error against autoscaling rather
	// than a webhook-introduced schema violation.
	if cb.Spec.Autoscaling != nil && cb.Spec.Autoscaling.MinReplicas == nil &&
		cb.Spec.Replicas != nil && *cb.Spec.Replicas >= 1 {
		v := *cb.Spec.Replicas
		cb.Spec.Autoscaling.MinReplicas = &v
	}

	return nil
}

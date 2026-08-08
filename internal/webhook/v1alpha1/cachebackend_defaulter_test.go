// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
	"time"
)

func TestDefaulter_MaterialisesIntegrationAndObservation(t *testing.T) {
	d := &CacheBackendDefaulter{}
	cb := newBackend()

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if cb.Spec.Integration == nil {
		t.Fatal("integration block not materialised")
	}
	if cb.Spec.Observation == nil || cb.Spec.Observation.FirstEventTimeout == nil || cb.Spec.Observation.FirstEventTimeout.Duration != defaultFirstEventTimeout {
		t.Fatalf("observation.firstEventTimeout = %v, want %s", cb.Spec.Observation, defaultFirstEventTimeout)
	}
}

func TestDefaulter_PreservesExplicitObservationTimeout(t *testing.T) {
	cb := newBackend()
	cb.Spec.Observation = &cachev1alpha1.CacheBackendObservationSpec{
		FirstEventTimeout: &metav1.Duration{Duration: 90 * time.Second},
	}

	if err := (&CacheBackendDefaulter{}).Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}
	if cb.Spec.Observation == nil || cb.Spec.Observation.FirstEventTimeout == nil {
		t.Fatalf("observation timeout not materialised: %+v", cb.Spec.Observation)
	}
	if got := cb.Spec.Observation.FirstEventTimeout.Duration; got != 90*time.Second {
		t.Fatalf("observation.firstEventTimeout = %s, want 90s", got)
	}
}

func TestDefaulter_DoesNotClobberOperatorValues(t *testing.T) {
	d := &CacheBackendDefaulter{}
	cb := newBackend()
	cb.Spec.Replicas = i32p(7)
	cb.Spec.Observation = &cachev1alpha1.CacheBackendObservationSpec{
		FirstEventTimeout: &metav1.Duration{Duration: 90 * time.Second},
	}

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	// Replicas now defaults via a CRD-schema marker (not the webhook), but the
	// non-clobber contract still holds: an operator-set value must survive the
	// webhook regardless of which layer applied the default.
	if *cb.Spec.Replicas != 7 {
		t.Errorf("replicas clobbered: got %d, want 7", *cb.Spec.Replicas)
	}
	if cb.Spec.Observation.FirstEventTimeout == nil || cb.Spec.Observation.FirstEventTimeout.Duration != 90*time.Second {
		t.Errorf("firstEventTimeout clobbered: got %v, want 90s", cb.Spec.Observation.FirstEventTimeout)
	}
}

func TestDefaulter_AutoscalingMinReplicasComputedFromReplicas(t *testing.T) {
	// When the operator opts into autoscaling without pinning the floor, the
	// defaulter computes minReplicas from spec.replicas so the HPA's lower
	// bound follows the baseline declaration rather than a hard-coded
	// constant. (spec.replicas itself is stamped by the apiserver from its
	// `+kubebuilder:default=1` marker; the unit test seeds it explicitly so
	// the assertion does not depend on the marker firing.)
	d := &CacheBackendDefaulter{}
	cb := newBackend()
	cb.Spec.Replicas = i32p(3)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 10}

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if cb.Spec.Autoscaling.MinReplicas == nil || *cb.Spec.Autoscaling.MinReplicas != 3 {
		t.Errorf("autoscaling.minReplicas = %v, want 3 (= spec.replicas)", cb.Spec.Autoscaling.MinReplicas)
	}
}

func TestDefaulter_AutoscalingMinReplicasNotClobbered(t *testing.T) {
	// An operator-set minReplicas survives the defaulter. The non-clobber
	// contract extends to every default this handler stamps.
	d := &CacheBackendDefaulter{}
	cb := newBackend()
	cb.Spec.Replicas = i32p(3)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{
		MinReplicas: i32p(2),
		MaxReplicas: 10,
	}

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if *cb.Spec.Autoscaling.MinReplicas != 2 {
		t.Errorf("autoscaling.minReplicas clobbered: got %d, want 2", *cb.Spec.Autoscaling.MinReplicas)
	}
}

func TestDefaulter_AutoscalingMinReplicasSkippedWhenAutoscalingOff(t *testing.T) {
	// No autoscaling = no defaulting. The reconciler's autoscalingFloor
	// helper handles the nil-Autoscaling case at runtime; the defaulter
	// must not synthesise an autoscaling object operators did not request.
	d := &CacheBackendDefaulter{}
	cb := newBackend()
	cb.Spec.Replicas = i32p(3)

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if cb.Spec.Autoscaling != nil {
		t.Errorf("autoscaling materialised unexpectedly: %+v", cb.Spec.Autoscaling)
	}
}

func TestDefaulter_AutoscalingMinReplicasSkippedWhenReplicasNil(t *testing.T) {
	// Defence-in-depth: when a test calls Default() on a raw struct without
	// the apiserver in the loop, spec.replicas may still be nil (the schema
	// default did not get a chance to fire). The defaulter must leave
	// minReplicas alone in that case rather than dereference a nil pointer.
	d := &CacheBackendDefaulter{}
	cb := newBackend()
	cb.Spec.Replicas = nil
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 10}

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if cb.Spec.Autoscaling.MinReplicas != nil {
		t.Errorf("autoscaling.minReplicas should stay nil when spec.replicas is nil; got %v", *cb.Spec.Autoscaling.MinReplicas)
	}
}

func TestDefaulter_AutoscalingMinReplicasSkippedWhenReplicasZero(t *testing.T) {
	// spec.replicas permits 0 (scale-to-zero is a valid operator choice),
	// but autoscaling.minReplicas carries `+kubebuilder:validation:Minimum=1`
	// in the CRD schema. If the defaulter copied a 0 spec.replicas into
	// minReplicas the apiserver would then reject the persisted object
	// against the schema — a webhook-introduced validation failure on a CR
	// the operator did NOT explicitly misconfigure. Refusing to default in
	// that case leaves the field unset so the operator's combination of
	// `replicas: 0` + opted-in autoscaling surfaces as a missing-required
	// field violation against autoscaling itself, which is the actual
	// problem.
	d := &CacheBackendDefaulter{}
	cb := newBackend()
	cb.Spec.Replicas = i32p(0)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 10}

	if err := d.Default(context.Background(), cb); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}

	if cb.Spec.Autoscaling.MinReplicas != nil {
		t.Errorf("autoscaling.minReplicas should stay nil when spec.replicas is 0 (would violate schema Minimum=1); got %v", *cb.Spec.Autoscaling.MinReplicas)
	}
}

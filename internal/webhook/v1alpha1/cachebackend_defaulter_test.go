// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
	"time"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

func TestDefaulterMaterializesIntegrationAndObservation(t *testing.T) {
	cb := &cachev1alpha1.CacheBackend{}
	if err := (&CacheBackendDefaulter{}).Default(context.Background(), cb); err != nil {
		t.Fatalf("Default: %v", err)
	}
	if cb.Spec.Integration == nil {
		t.Fatal("spec.integration was not materialized")
	}
	if cb.Spec.Observation == nil || cb.Spec.Observation.FirstEventTimeout == nil ||
		cb.Spec.Observation.FirstEventTimeout.Duration != 5*time.Minute {
		t.Fatalf("observation default = %+v, want 5m", cb.Spec.Observation)
	}
}

func TestDefaulterPreservesObservationTimeout(t *testing.T) {
	cb := &cachev1alpha1.CacheBackend{Spec: cachev1alpha1.CacheBackendSpec{
		Observation: &cachev1alpha1.CacheBackendObservationSpec{},
	}}
	custom := 30 * time.Second
	cb.Spec.Observation.FirstEventTimeout = &metav1.Duration{Duration: custom}
	if err := (&CacheBackendDefaulter{}).Default(context.Background(), cb); err != nil {
		t.Fatalf("Default: %v", err)
	}
	if got := cb.Spec.Observation.FirstEventTimeout.Duration; got != custom {
		t.Fatalf("timeout = %s, want %s", got, custom)
	}
}

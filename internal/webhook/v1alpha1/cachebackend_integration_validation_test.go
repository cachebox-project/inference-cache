// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"context"
	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/internal/enginebinding"
	adapterruntime "github.com/cachebox-project/inference-cache/pkg/adapters/runtime"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
	"testing"
)

func TestValidator_SGLangHiCacheAccepted(t *testing.T) {
	if _, err := (shippingValidator()).ValidateCreate(context.Background(), newHiCacheBackend()); err != nil {
		t.Fatalf("valid SGLangHiCache rejected: %v", err)
	}
}

func TestValidator_CanonicalSGLangHiCacheRejectsRemoteStorage(t *testing.T) {
	cb := newHiCacheBackend()
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.Observation = &cachev1alpha1.CacheBackendObservationSpec{ModelID: "model-a"}
	cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderRedis,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
		Redis:     &cachev1alpha1.RedisRemoteStorageSpec{},
	}
	requireInvalidWithCause(t, shippingValidator(), cb, "spec.remoteStorage.provider",
		"does not accept remote-storage protocol")
}

func TestValidator_SGLangHiCacheContract(t *testing.T) {
	falseValue := false
	size := int32(64)
	zero := int32(0)
	cases := []struct {
		name   string
		mutate func(*cachev1alpha1.CacheBackend)
		want   string
	}{
		{"missing hiCache", func(cb *cachev1alpha1.CacheBackend) { cb.Spec.HiCache = nil }, "spec.hiCache"},
		{"missing capacity", func(cb *cachev1alpha1.CacheBackend) { cb.Spec.HiCache.Ratio = "" }, "exactly one"},
		{"both capacities", func(cb *cachev1alpha1.CacheBackend) { cb.Spec.HiCache.SizeGB = &size }, "mutually exclusive"},
		{"zero size", func(cb *cachev1alpha1.CacheBackend) {
			cb.Spec.HiCache.Ratio = ""
			cb.Spec.HiCache.SizeGB = &zero
		}, "sizeGB"},
		{"invalid ratio", func(cb *cachev1alpha1.CacheBackend) { cb.Spec.HiCache.Ratio = "Inf" }, "ratio"},
		{"wrong runtime", func(cb *cachev1alpha1.CacheBackend) { cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM }, "spec.runtime"},
		{"missing selector", func(cb *cachev1alpha1.CacheBackend) { cb.Spec.EngineSelector = nil }, "engineSelector.matchLabels"},
		{"events only", func(cb *cachev1alpha1.CacheBackend) {
			cb.Spec.Integration.Mode = cachev1alpha1.CacheBackendIntegrationModeEventsOnly
		}, "integration.mode"},
		{"read only", func(cb *cachev1alpha1.CacheBackend) {
			cb.Spec.Integration.Role = cachev1alpha1.CacheBackendIntegrationRoleReadOnly
		}, "integration.role"},
		{"fail closed", func(cb *cachev1alpha1.CacheBackend) {
			cb.Spec.Integration.FailOpen = &falseValue
		}, "integration.failOpen"},
		{"autoscaling", func(cb *cachev1alpha1.CacheBackend) {
			cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 2}
		}, "spec.autoscaling"},
		{"invalid write policy", func(cb *cachev1alpha1.CacheBackend) {
			cb.Spec.HiCache.WritePolicy = "sometimes"
		}, "writePolicy"},
		{"invalid io backend", func(cb *cachev1alpha1.CacheBackend) {
			cb.Spec.HiCache.IOBackend = "userspace"
		}, "ioBackend"},
		{"invalid memory layout", func(cb *cachev1alpha1.CacheBackend) {
			cb.Spec.HiCache.MemoryLayout = "tensor_first"
		}, "memoryLayout"},
	}
	validator := shippingValidator()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cb := newHiCacheBackend()
			tc.mutate(cb)
			_, err := validator.ValidateCreate(context.Background(), cb)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ValidateCreate error = %v, want text %q", err, tc.want)
			}
		})
	}
}

func TestValidator_HiCacheBlockRejectedOnOtherTypes(t *testing.T) {
	cb := newBackend()
	cb.Spec.HiCache = &cachev1alpha1.SGLangHiCacheSpec{Ratio: "2"}
	_, err := (shippingValidator()).ValidateCreate(context.Background(), cb)
	if err == nil || !strings.Contains(err.Error(), "spec.hiCache") {
		t.Fatalf("ValidateCreate error = %v, want hiCache type-scope error", err)
	}
}

func TestValidator_SGLangHiCacheArgsAreReserved(t *testing.T) {
	for _, flag := range []string{
		"--enable-hierarchical-cache",
		"--hicache-size",
		"--hicache-ratio",
		"--hicache-write-policy",
		"--hicache-io-backend",
		"--hicache-mem-layout",
	} {
		t.Run(flag, func(t *testing.T) {
			cb := newHiCacheBackend()
			cb.Spec.Integration.EngineOverrides = &cachev1alpha1.EngineInjectionOverrides{
				SuppressArgs: []string{flag},
			}
			_, err := (shippingValidator()).ValidateCreate(context.Background(), cb)
			if err == nil || !strings.Contains(err.Error(), flag) || !strings.Contains(err.Error(), "reserved") {
				t.Fatalf("ValidateCreate error = %v, want reserved %s", err, flag)
			}
		})
	}
}

func TestValidator_MooncakeWarnsUntilEngineHostNetworkOptIn(t *testing.T) {
	// Mooncake's transfer engine is a peer-to-peer mesh: engine pods must run with
	// hostNetwork or the backend reports Ready and moves zero KV. That move rewrites
	// a pod the operator owns, so it is opt-in rather than injected. Until they opt
	// in, say so at apply time and name the exact field — otherwise the failure is
	// discoverable only from a flat cache-hit graph.
	cb := mooncakeBackendWithEngineHostNetwork(false)
	t.Run("without opt-in", func(t *testing.T) {
		v := shippingValidator()
		warnings, err := v.ValidateCreate(context.Background(), cb)
		if err != nil {
			t.Fatalf("a Mooncake backend must still be admitted (warning, not rejection): %v", err)
		}
		if len(warnings) != 1 || !strings.Contains(warnings[0], "spec.integration.engineHostNetwork=true") {
			t.Fatalf("create warnings = %v, want one warning naming the opt-in field", warnings)
		}

		// It must persist across updates, not only on first apply — an operator who
		// edits the CR later should still be told.
		warnings, err = v.ValidateUpdate(context.Background(), cb, cb)
		if err != nil {
			t.Fatalf("a Mooncake update must still be admitted: %v", err)
		}
		if len(warnings) != 1 {
			t.Fatalf("update warnings = %v, want the engine-hostNetwork warning", warnings)
		}
	})
}

func TestValidator_MooncakeOptInSilencesTheWarning(t *testing.T) {
	// Once the operator opts in, the pod webhook completes the data plane. A warning
	// that keeps firing after the gap is closed trains operators to ignore warnings.
	cb := mooncakeBackendWithEngineHostNetwork(true)
	t.Run("with opt-in", func(t *testing.T) {
		v := shippingValidator()
		warnings, err := v.ValidateCreate(context.Background(), cb)
		if err != nil {
			t.Fatalf("an opted-in Mooncake backend must be admitted: %v", err)
		}
		if len(warnings) != 0 {
			t.Fatalf("warnings = %v, want none once engineHostNetwork is set", warnings)
		}
	})
}

func TestValidator_EngineHostNetworkRejectedOnBackendThatDoesNotNeedIt(t *testing.T) {
	// The flag would silently do nothing on a pod-network backend while leaving the
	// operator convinced they had granted their engine host networking. hostNetwork
	// is a privilege — a no-op that looks like it granted one is worse than an error.
	v := shippingValidator()
	cb := mooncakeBackendWithEngineHostNetwork(true)
	cb.Spec.RemoteStorage = nil
	requireInvalidWithCause(t, v, cb, "spec.integration.engineHostNetwork",
		"only meaningful when the effective remote storage provider is Mooncake")
}

func TestValidator_EngineHostNetworkGoesInertWhenTypeFlipsAwayFromMooncake(t *testing.T) {
	// The realistic way the flag rots: a Mooncake backend legitimately carrying
	// engineHostNetwork=true is retyped to LMCache, and the flag rides along as a
	// no-op that reads like a granted privilege.
	//
	// Worth pinning explicitly because ValidateUpdate only rejects errors the new
	// object *introduces* — errors already present on the old object are filtered
	// out. The old object here (Mooncake + flag) is valid, so the error IS newly
	// introduced and must be caught. Nothing about that is obvious from the rule.
	v := shippingValidator()
	old := mooncakeBackendWithEngineHostNetwork(true)
	newCB := mooncakeBackendWithEngineHostNetwork(true)
	newCB.Spec.RemoteStorage = nil
	requireUpdateInvalidWithCause(t, v, old, newCB, "spec.integration.engineHostNetwork",
		"only meaningful when the effective remote storage provider is Mooncake")
}

func TestValidator_DroppingEngineHostNetworkWithTheTypeFlipIsAccepted(t *testing.T) {
	// The escape hatch the rejection above implies: retyping away from Mooncake is
	// fine as long as the flag goes with it. If this failed, the rule would have
	// wedged the object — rejecting both keeping and dropping the flag.
	v := shippingValidator()
	old := mooncakeBackendWithEngineHostNetwork(true)
	newCB := mooncakeBackendWithEngineHostNetwork(false)
	newCB.Spec.RemoteStorage = nil
	if _, err := v.ValidateUpdate(context.Background(), old, newCB); err != nil {
		t.Fatalf("retyping away from Mooncake while dropping engineHostNetwork must be accepted, got: %v", err)
	}
}

func TestValidator_EngineHostNetworkCannotGoInertViaEventsOnly(t *testing.T) {
	// The other way engineHostNetwork could end up inert: an events-only backend
	// wires no KV connector, so the Pod webhook never calls InjectEngineConfig and
	// the flag would do nothing. Today that combination is already unreachable —
	// rejectEventsOnlyMisconfiguration forbids EventsOnly on any managed type but
	// LMCache, and engineHostNetwork is rejected on any type but Mooncake, so the
	// two rules cross.
	//
	// Pinned because that safety is emergent, not stated: it comes from two
	// independent rules meeting. Loosening either one — allowing events-only
	// Mooncake, say — would silently open the inert-flag hole this asserts shut.
	v := shippingValidator()
	cb := mooncakeBackendWithEngineHostNetwork(true)
	cb.Spec.Integration.Mode = cachev1alpha1.CacheBackendIntegrationModeEventsOnly
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage",
		"provision no remote-storage provider")
}

func TestValidator_NonMooncakeEmitsNoHostNetworkWarning(t *testing.T) {
	// Blast radius: the DEFAULT (vLLM) LMCache pairing — engine unset defaults to
	// vLLM — must stay warning-free (the Mooncake mesh warning does not apply to it).
	v := shippingValidator()
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	warnings, err := v.ValidateCreate(context.Background(), cb)
	if err != nil {
		t.Fatalf("LMCache backend must be admitted: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("LMCache warnings = %v, want none", warnings)
	}
}

func TestValidator_SGLangLMCacheEmitsNoWarning(t *testing.T) {
	// The (sglang, LMCache) adapter now renders the working LMCache MP-mode data
	// plane (node-local MP-worker sidecar + config-file wire → managed Redis L2), so
	// the old "misconfigured lm:// wiring" advisory is gone — the pair must be
	// warning-free on both create and update. Assert the FULL list is empty so a
	// resurrected or accidental new warning is caught.
	v := &CacheBackendValidator{Registry: defaultShippingRegistry()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}

	warnings, err := v.ValidateCreate(context.Background(), cb)
	if err != nil {
		t.Fatalf("(sglang, LMCache) must be admitted: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("(sglang, LMCache) must be warning-free, got: %v", warnings)
	}

	warnings, err = v.ValidateUpdate(context.Background(), cb, cb)
	if err != nil {
		t.Fatalf("(sglang, LMCache) update must be admitted: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("(sglang, LMCache) update must be warning-free, got: %v", warnings)
	}
}

func TestValidator_VLLMLMCacheEmitsNoSGLangWarning(t *testing.T) {
	// Blast radius: the sglang MP-mode warning must not fire on (vllm, LMCache),
	// which drives the lm:// server correctly.
	v := &CacheBackendValidator{Registry: defaultShippingRegistry()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	warnings, err := v.ValidateCreate(context.Background(), cb)
	if err != nil {
		t.Fatalf("(vllm, LMCache) must be admitted: %v", err)
	}
	// The contract is warning-FREE, not merely "no MP-mode warning" — assert the
	// full list is empty so a reworded warning or an accidental new one is caught.
	if len(warnings) != 0 {
		t.Fatalf("(vllm, LMCache) must be warning-free, got: %v", warnings)
	}
}

func TestValidator_SGLangEventsOnlyEmitsNoDataPlaneWarning(t *testing.T) {
	// EventsOnly (tier-1 routing) provisions no server and injects no LMCache
	// connector — only the observation sidecar — so the lm://-vs-MP mismatch is
	// absent and the warning must not fire.
	v := &CacheBackendValidator{Registry: defaultShippingRegistry()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
		Mode: cachev1alpha1.CacheBackendIntegrationModeEventsOnly,
	}
	warnings, err := v.ValidateCreate(context.Background(), cb)
	if err != nil {
		t.Fatalf("(sglang, LMCache, EventsOnly) must be admitted: %v", err)
	}
	for _, w := range warnings {
		if strings.Contains(w, "MP mode") {
			t.Fatalf("events-only backend got the sglang MP-mode warning: %q", w)
		}
	}
}

func TestValidator_MooncakeMultiReplicaRejected(t *testing.T) {
	// The Mooncake master is a singleton on the host network: a second replica cannot
	// bind the node ports the first already holds, and on a different node it comes up
	// as an independent master and silently splits the store. Both failures surface
	// long after the object looks healthy, so admission rejects them at write time.
	v := shippingValidator()
	cb := newBackend()
	setCanonicalMooncakeStorage(cb)
	two := int32(2)
	cb.Spec.Replicas = &two
	requireInvalidWithCause(t, v, cb, "spec.replicas", "singleton on the host network")
}

func TestValidator_MooncakeAutoscalingRejected(t *testing.T) {
	v := shippingValidator()
	cb := newBackend()
	setCanonicalMooncakeStorage(cb)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{}
	requireInvalidWithCause(t, v, cb, "spec.autoscaling", "not supported for remoteStorage.provider=Mooncake")
}

func TestValidator_MooncakeSingletonAndDisabledReplicasAccepted(t *testing.T) {
	// 1 is the singleton; 0 is the "disabled" case. Neither can split the store.
	v := shippingValidator()
	for _, replicas := range []int32{0, 1} {
		cb := newBackend()
		setCanonicalMooncakeStorage(cb)
		r := replicas
		cb.Spec.Replicas = &r
		if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
			t.Fatalf("Mooncake with spec.replicas=%d must be admitted: %v", replicas, err)
		}
	}
}

func TestValidator_LMCacheScaleOutUnaffectedByMooncakeRule(t *testing.T) {
	// Blast radius: the lm:// server is an ordinary pod-network workload and must
	// keep scaling out (and autoscaling) normally.
	v := shippingValidator()
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendTypeLMCache
	three := int32(3)
	cb.Spec.Replicas = &three
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{}
	managedLMCacheServer(cb)
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("multi-replica autoscaled LMCache must be admitted: %v", err)
	}
}

func TestValidator_SGLangRedisL2MultiReplicaRejected(t *testing.T) {
	// The (sglang, LMCache) backend's cache-server is a single non-clustered Redis L2
	// (the MP worker's --l2-adapter target). A second pod behind the one Service
	// shards the keyspace across independent instances, so a key stored via one is a
	// miss via the other — the L2 silently partitions. Reject at write time.
	v := shippingValidator()
	cb := sglangLMCacheBackend()
	two := int32(2)
	cb.Spec.Replicas = &two
	requireInvalidWithCause(t, v, cb, "spec.replicas", "single non-clustered instance")
}

func TestValidator_SGLangRedisL2AutoscalingRejected(t *testing.T) {
	v := shippingValidator()
	cb := sglangLMCacheBackend()
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{}
	requireInvalidWithCause(t, v, cb, "spec.autoscaling", "not supported for the (sglang, LMCache) backend")
}

func TestValidator_SGLangRedisL2SingletonAndDisabledAccepted(t *testing.T) {
	// 1 is the singleton; 0 is "disabled". Neither partitions the keyspace.
	v := shippingValidator()
	for _, replicas := range []int32{0, 1} {
		cb := sglangLMCacheBackend()
		r := replicas
		cb.Spec.Replicas = &r
		if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
			t.Fatalf("(sglang, LMCache) with spec.replicas=%d must be admitted: %v", replicas, err)
		}
	}
}

func TestValidator_SGLangEventsOnlyScaleOutAccepted(t *testing.T) {
	// EventsOnly provisions NO cache server (the reconciler sheds any owned
	// workload), so there is no Redis L2 to partition — the singleton rule must not
	// fire. Rejecting here would be factually wrong (the message explains a Redis
	// split that cannot happen) and would make SGLang gratuitously stricter than an
	// otherwise-identical (vllm, LMCache) events-only backend.
	v := shippingValidator()

	t.Run("multi-replica is admitted", func(t *testing.T) {
		cb := sglangLMCacheBackend()
		cb.Spec.Integration.Mode = cachev1alpha1.CacheBackendIntegrationModeEventsOnly
		cb.Spec.RemoteStorage = nil
		cb.Spec.Replicas = i32p(3)
		if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
			t.Fatalf("(sglang, LMCache) EventsOnly with replicas=3 must be admitted (no Redis is provisioned): %v", err)
		}
	})

	t.Run("autoscaling is rejected by the ENGINE-AGNOSTIC events-only rule, not the Redis one", func(t *testing.T) {
		// Autoscaling on EventsOnly is rejected either way — but it must be for the
		// generic "no server workload to autoscale" reason that applies to every
		// engine, NOT the SGLang Redis-partitioning reason (which would be wrong here
		// and would make SGLang stricter than vLLM). Pinning the reason is the point.
		cb := sglangLMCacheBackend()
		cb.Spec.Integration.Mode = cachev1alpha1.CacheBackendIntegrationModeEventsOnly
		cb.Spec.RemoteStorage = nil
		cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 3}
		_, err := v.ValidateCreate(context.Background(), cb)
		if err == nil {
			t.Fatalf("EventsOnly + autoscaling should be rejected (nothing to autoscale)")
		}
		if !strings.Contains(err.Error(), "provision no server workload") {
			t.Fatalf("want the engine-agnostic events-only reason, got: %v", err)
		}
		if strings.Contains(err.Error(), "non-clustered") {
			t.Fatalf("the SGLang Redis-partitioning rule fired on an events-only backend, which provisions no Redis: %v", err)
		}
	})
}

func TestValidator_VLLMLMCacheScaleOutUnaffectedBySGLangRule(t *testing.T) {
	// Blast radius: vLLM's lm:// server is an ordinary pod-network workload and must
	// keep scaling out (and autoscaling) — the singleton rule is (sglang, LMCache)-only.
	v := shippingValidator()
	cb := newBackend() // Type=LMCache, engine defaults to vllm
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	three := int32(3)
	cb.Spec.Replicas = &three
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{}
	managedLMCacheServer(cb)
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("multi-replica autoscaled (vllm, LMCache) must be admitted: %v", err)
	}
}

func TestValidator_InvalidKernelCheckAnnotationRejected(t *testing.T) {
	v := shippingValidator()

	// A typo for "strict" would silently fall back to "auto" (report-only) and
	// disable the fail-closed gate — reject it at admission instead.
	bad := newBackend()
	bad.Annotations = map[string]string{enginebinding.AnnotationLMCacheKernelCheck: "strcit"}
	requireInvalidWithCause(t, v, bad, "metadata.annotations[inferencecache.io/lmcache-kernel-check]", "must be one of")

	// Every known value — and an unset annotation — is accepted.
	for _, val := range []string{
		enginebinding.KernelCheckModeAuto,
		enginebinding.KernelCheckModeReportOnly,
		enginebinding.KernelCheckModeStrict,
		enginebinding.KernelCheckModeOff,
		"", // explicit empty == unset
	} {
		ok := newBackend()
		ok.Annotations = map[string]string{enginebinding.AnnotationLMCacheKernelCheck: val}
		if _, err := v.ValidateCreate(context.Background(), ok); err != nil {
			t.Fatalf("valid kernel-check annotation %q rejected: %v", val, err)
		}
	}
	if _, err := v.ValidateCreate(context.Background(), newBackend()); err != nil {
		t.Fatalf("unset kernel-check annotation rejected: %v", err)
	}
}

func TestValidator_ReplicasZeroWithAutoscalingAndNilMinReplicasRejected(t *testing.T) {
	// spec.replicas=0 + spec.autoscaling enabled + nil minReplicas is the
	// silent-HPA-fallback-to-1 trap: the defaulter declines to default
	// minReplicas (a 0 value would violate the schema's Minimum=1), the
	// apiserver accepts the CR with minReplicas unset, and the reconciler's
	// HPA fallback picks defaultHPAMinReplicas=1 — overriding the operator's
	// "scale to zero" intent without notification. Admission must reject
	// the combination so the operator either sets the floor explicitly or
	// removes the autoscaling block to truly scale to zero.
	v := shippingValidator()
	cb := newBackend()
	cb.Spec.Replicas = i32p(0)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{MaxReplicas: 10}
	requireInvalidWithCause(t, v, cb, "spec.autoscaling.minReplicas",
		"spec.replicas=0 with spec.autoscaling enabled requires spec.autoscaling.minReplicas")
}

func TestValidator_ReplicasZeroWithAutoscalingAndExplicitMinReplicasAdmitted(t *testing.T) {
	// Operator who pairs replicas=0 with autoscaling sets minReplicas
	// explicitly to declare the intended HPA floor. With minReplicas=1 the
	// HPA scales the workload back up to 1 immediately (minReplicas=1 means
	// "never below one"); the test pins that the admission rule fires only
	// on the nil-minReplicas trap, not on the explicit-floor case. (CRD
	// schema enforces Minimum=1 on minReplicas, so the smallest legal
	// explicit value here is 1; true scale-to-zero requires removing the
	// autoscaling block entirely, which the next test covers.)
	v := shippingValidator()
	cb := newBackend()
	cb.Spec.Replicas = i32p(0)
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{
		MinReplicas: i32p(1),
		MaxReplicas: 10,
	}
	managedLMCacheServer(cb)
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("replicas=0 + autoscaling + explicit minReplicas rejected: %v", err)
	}
}

func TestValidator_ReplicasZeroWithoutAutoscalingAdmitted(t *testing.T) {
	// Pure scale-to-zero (no autoscaling block) is allowed. The HPA-fallback
	// trap only applies when autoscaling is opted into.
	v := shippingValidator()
	cb := newBackend()
	cb.Spec.Replicas = i32p(0)
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("replicas=0 without autoscaling rejected: %v", err)
	}
}

func TestValidator_RuntimeAdapter_VLLMPlusLMCacheAdmitted(t *testing.T) {
	// Happy path: an explicit (vLLM, LMCache) pair the stub registry
	// supports must be admitted. Pins the C7 check's positive side so a
	// regression doesn't silently start rejecting it.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend() // type=LMCache
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("vLLM+LMCache rejected: %v", err)
	}
}

func TestDefaultShippingRegistryResolvesSGLangLMCache(t *testing.T) {
	// Registration check: the real shipping registry (the set the running
	// controller installs) must resolve the (sglang, LMCache) pair to an
	// adapter, and surface sglang/LMCache in its SupportedPairs so admission
	// error messages list it as a candidate. Exercises the real
	// defaultShippingRegistry rather than a stub so a regression that drops the
	// SGLang registration from any of the three wiring sites is caught here.
	r := defaultShippingRegistry()
	cb := newBackend() // type=LMCache
	if _, err := r.Select(adapterruntime.RuntimeSGLang, cb); err != nil {
		t.Fatalf("shipping registry does not resolve (sglang, LMCache): %v", err)
	}
	found := false
	for _, p := range r.SupportedPairs() {
		if p.Runtime == adapterruntime.RuntimeSGLang && p.Backend == cachev1alpha1.CacheBackendTypeLMCache {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("SupportedPairs missing sglang/LMCache; got %v", r.SupportedPairs())
	}
}

func TestValidator_RuntimeAdapter_SGLangPlusLMCacheAdmitted(t *testing.T) {
	// DoD: admission accepts (sglang, LMCache) once the adapter is registered.
	// Runs through the real shipping registry so the test fails if the SGLang
	// adapter is not actually wired into the validator's registry.
	v := &CacheBackendValidator{Registry: defaultShippingRegistry()}
	cb := newBackend() // type=LMCache
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("sglang+LMCache rejected: %v", err)
	}
}

func TestValidator_RuntimeAdapter_SGLangPlusExternalRejected(t *testing.T) {
	// The SGLang adapter supports only (sglang, LMCache) — a (sglang, External)
	// pair must still be rejected (no adapter claims it), and the message must
	// list sglang/LMCache among the supported candidates so the operator sees
	// the actionable alternative.
	v := &CacheBackendValidator{Registry: defaultShippingRegistry()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendType("unsupported")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}

	_, err := v.ValidateCreate(context.Background(), cb)
	if err == nil {
		t.Fatalf("expected (sglang, External) to be rejected")
	}
	statusErr, ok := err.(*apierrors.StatusError)
	if !ok {
		t.Fatalf("expected *apierrors.StatusError, got %T: %v", err, err)
	}
	var match *metav1.StatusCause
	for i := range statusErr.Status().Details.Causes {
		if statusErr.Status().Details.Causes[i].Field == "spec.runtime" {
			match = &statusErr.Status().Details.Causes[i]
			break
		}
	}
	if match == nil {
		t.Fatalf("no cause on spec.runtime; got: %+v", statusErr.Status().Details.Causes)
	}
	for _, want := range []string{"sglang", "unsupported", "sglang/LMCache"} {
		if !strings.Contains(match.Message, want) {
			t.Errorf("rejection message missing %q; got %q", want, match.Message)
		}
	}
}

func TestValidator_RuntimeAdapter_VLLMPlusUnsupportedTypeRejected(t *testing.T) {
	// Rejection path: a (vLLM, <unsupported type>) pair no installed adapter
	// supports must be rejected with a message that names BOTH sides of
	// the offending pair and lists the supported pairs so the user has
	// an actionable next step. The arbitrary value below is unsupported by any
	// shipping adapter.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendType("unsupported")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}

	_, err := v.ValidateCreate(context.Background(), cb)
	if err == nil {
		t.Fatalf("expected vLLM+unsupported to be rejected")
	}
	statusErr, ok := err.(*apierrors.StatusError)
	if !ok {
		t.Fatalf("expected *apierrors.StatusError, got %T: %v", err, err)
	}
	if statusErr.Status().Details == nil || len(statusErr.Status().Details.Causes) == 0 {
		t.Fatalf("Invalid status carried no causes: %v", statusErr.Status())
	}
	var match *metav1.StatusCause
	causes := statusErr.Status().Details.Causes
	for i := range causes {
		if causes[i].Field == "spec.runtime" {
			match = &causes[i]
			break
		}
	}
	if match == nil {
		t.Fatalf("no cause on spec.runtime; got: %+v", causes)
	}
	for _, want := range []string{"vllm", "unsupported", "vllm/LMCache"} {
		if !strings.Contains(match.Message, want) {
			t.Errorf("rejection message missing %q; got %q", want, match.Message)
		}
	}
}

func TestValidator_RuntimeAdapter_UnknownEngineRejected(t *testing.T) {
	// An engine name no adapter handles must also be rejected — guards
	// against a typo (`engin: vllmm`) silently riding through admission
	// and only failing at reconcile.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend() // type=LMCache
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntime("vllmm")
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	requireInvalidWithCause(t, v, cb, "spec.runtime",
		"engine=\"vllmm\"")
}

func TestValidator_RuntimeAdapter_EngineNormalisedToLowerCase(t *testing.T) {
	// The reconciler downcases the engine string before looking up an
	// adapter; admission must do the same so a CR that spells "VLLM" is
	// not admitted by one layer and rejected by the other.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend() // type=LMCache
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("VLLM (uppercase) + LMCache rejected: %v", err)
	}
}

func TestValidator_RuntimeAdapter_EmptyEngineDefaultsToVLLM(t *testing.T) {
	// Engine is optional on the CRD; the reconciler and pod webhook
	// default it to vLLM via adapterruntime.ResolveRuntimeID, so
	// admission must use the same defaulting or pairs like
	// an unsupported type with no engine slip past the webhook and only
	// fail at reconcile (the exact gap C7 closes). The value has no adapter
	// in any registry, so it stays a genuinely-unsupported example.
	//
	// With LMCache the default vLLM pair is supported → admit.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend() // type=LMCache, no Integration block
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("LMCache + defaulted vLLM engine rejected: %v", err)
	}
}

func TestValidator_RuntimeAdapter_EmptyEngineWithUnsupportedTypeRejected(t *testing.T) {
	// Counterpart to the previous test: the default vLLM resolution
	// must also fire C7 — an unsupported type with no engine must be
	// rejected at admission, since the reconciler would otherwise try
	// vllm/unsupported and fall back to unmanaged.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendType("unsupported")
	requireInvalidWithCause(t, v, cb, "spec.runtime",
		"backend=\"unsupported\"")
}

func TestValidator_RuntimeAdapter_EmptyTypeSkipsCheck(t *testing.T) {
	// Mirror edge case: an empty type must not trigger C7 either, for
	// the same "defer to required-field validation" reason.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend()
	cb.Spec.Type = ""
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("empty type must not trigger C7; got %v", err)
	}
}

func TestValidator_RuntimeAdapter_ExternalWithSupportedEngineAdmitted(t *testing.T) {
	// External ownership does not change runtime-adapter selection: this remains
	// a supported vLLM/LMCache pair with an external remote binding.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "team-a-cache.team-a.svc.cluster.local:9000")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("External with engine=vllm rejected by C7: %v", err)
	}
}

func TestValidator_RuntimeAdapter_ExternalWithUnsupportedEngineRejected(t *testing.T) {
	// External + sglang is admittable on shape (endpoint present, type set)
	// but no adapter in the registry handles that pair, so the pod webhook
	// would fail-open and never inject — the engine boots un-wired to the
	// external cache. Reject at admission with a useful error instead of
	// letting the silent miss happen.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "team-a-cache.team-a.svc.cluster.local:9000")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	requireInvalidWithCause(t, v, cb, "spec.runtime",
		"backend=\"LMCache\"")
}

func TestValidator_RuntimeAdapter_UpdateAlsoChecks(t *testing.T) {
	// ValidateUpdate runs the same check as ValidateCreate — a kubectl
	// edit that flips engine to something the registry doesn't support
	// must be rejected just as it would on create.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	old := newBackend()
	old.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	newCB := old.DeepCopy()
	newCB.Spec.Type = cachev1alpha1.CacheBackendType("unsupported")

	_, err := v.ValidateUpdate(context.Background(), old, newCB)
	if err == nil || !apierrors.IsInvalid(err) {
		t.Fatalf("expected Invalid on update with unsupported pair, got %v", err)
	}
}

func TestValidator_RuntimeAdapter_DeleteSkipsCheck(t *testing.T) {
	// Deletion of a CR that would now be rejected (e.g. registry shrank
	// since admission) must still be allowed so operators can clean up.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend()
	cb.Spec.Type = cachev1alpha1.CacheBackendType("unsupported")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	if _, err := v.ValidateDelete(context.Background(), cb); err != nil {
		t.Fatalf("ValidateDelete rejected unsupported pair: %v", err)
	}
}

func TestValidator_RuntimeAdapter_ShippingRegistryAdmitsExternal(t *testing.T) {
	// The explicitly injected shipping registry must admit the same pair the
	// running controller can reconcile and inject.
	v := shippingValidator()
	cb := newBackend()
	setCanonicalExternalStorage(cb, "ext.example.com:8200")
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("shipping registry rejected vLLM+External: %v", err)
	}
}

func TestValidator_RuntimeAdapter_NilRegistryRejectsMisconfiguration(t *testing.T) {
	v := &CacheBackendValidator{}
	_, err := v.ValidateCreate(context.Background(), newBackend())
	if err == nil || !apierrors.IsInvalid(err) || !strings.Contains(err.Error(), "registry is not configured") {
		t.Fatalf("ValidateCreate error = %v, want invalid missing-registry error", err)
	}
}

func TestValidator_RuntimeAdapter_NilRegistryFallsBackToDefault(t *testing.T) {
	// A zero-value validator (Registry nil) must still run the C7 check
	// against the complete built-in registry — the production safety net for
	// cmd/controller wiring drift. The built-in registry ships the vLLM+LMCache
	// adapter, so the happy pair admits.
	v := shippingValidator()
	cb := newBackend() // type=LMCache
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("nil-registry fallback rejected vLLM+LMCache: %v", err)
	}
}

func TestValidator_RuntimeAdapter_VLLMPlusMooncakeAdmittedViaShippingRegistry(t *testing.T) {
	// Mooncake is a remote binding for the vLLM/LMCache runtime pair, so the
	// shipping registry must admit it without a provider-specific runtime adapter.
	v := shippingValidator()
	cb := newBackend()
	setCanonicalMooncakeStorage(cb)
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("shipping registry rejected vLLM/LMCache with Mooncake binding: %v", err)
	}
}

func TestValidator_SGLangRoleRejected(t *testing.T) {
	// SGLang's LMCache integration has no producer/consumer split, so a
	// non-ReadWrite role can't be honored — admission rejects it loudly rather
	// than silently treating it as ReadWrite. Uses the real shipping registry
	// (the role rule is registry-independent, but this keeps the adapter-pair
	// check happy for the (sglang, LMCache) CR).
	v := &CacheBackendValidator{Registry: defaultShippingRegistry()}
	for _, role := range []cachev1alpha1.CacheBackendIntegrationRole{
		cachev1alpha1.CacheBackendIntegrationRoleReadOnly,
		cachev1alpha1.CacheBackendIntegrationRoleWriteOnly,
	} {
		t.Run(string(role), func(t *testing.T) {
			cb := newBackend() // type=LMCache
			cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
			cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{Role: role}
			requireInvalidWithCause(t, v, cb, "spec.integration.role", "sglang")
		})
	}
}

func TestValidator_SGLangRoleReadWriteAndUnsetAdmitted(t *testing.T) {
	// ReadWrite (and unset, which defaults to ReadWrite) are the honored
	// SGLang role; both must admit.
	v := &CacheBackendValidator{Registry: defaultShippingRegistry()}
	cases := []*cachev1alpha1.CacheBackendIntegrationSpec{
		{}, // role unset
		{Role: cachev1alpha1.CacheBackendIntegrationRoleReadWrite},
	}
	for _, integ := range cases {
		cb := newBackend()
		cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeSGLang
		cb.Spec.Integration = integ
		if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
			t.Fatalf("sglang role=%q rejected: %v", integ.Role, err)
		}
	}
}

func TestValidator_EventsOnly_ExternalTypeRejected(t *testing.T) {
	// events-only wires no KV connector, while External provisions an
	// operator-run offload server a connector would dial — the two are
	// contradictory. The rejection must point at spec.integration.mode (the
	// knob the operator flipped), not at spec.type. Use the registry that
	// uses the shipping LMCache adapter; the events-only remote-storage rule is
	// the one that must fire.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend()
	setCanonicalExternalStorage(cb, "team-a-cache.team-a.svc.cluster.local:9000")
	cb.Spec.Integration = eventsOnlyIntegration()
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage",
		"provision no remote-storage provider")
}

func TestValidator_EventsOnly_AutoscalingRejected(t *testing.T) {
	// An events-only backend provisions no server workload, so an autoscaling
	// spec has nothing to scale; admission rejects it against spec.autoscaling.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend() // type=LMCache
	cb.Spec.Integration = eventsOnlyIntegration()
	cb.Spec.Autoscaling = &cachev1alpha1.CacheBackendAutoscalingSpec{
		MaxReplicas: 3,
	}
	requireInvalidWithCause(t, v, cb, "spec.autoscaling",
		"nothing to autoscale")
}

func TestValidator_EventsOnly_RemoteStorageRejected(t *testing.T) {
	v := shippingValidator()
	cb := newBackend()
	cb.Spec.Runtime = cachev1alpha1.CacheBackendRuntimeVLLM
	cb.Spec.Integration = eventsOnlyIntegration()
	cb.Spec.RemoteStorage = &cachev1alpha1.CacheBackendRemoteStorageSpec{
		Provider:  cachev1alpha1.CacheBackendRemoteStorageProviderLMCacheServer,
		Ownership: cachev1alpha1.CacheBackendRemoteStorageOwnershipManaged,
		LMCacheServer: &cachev1alpha1.LMCacheServerRemoteStorageSpec{
			Image: "cache-server:test",
		},
	}
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage",
		"provision no remote-storage provider")
}

func TestValidator_EventsOnly_LMCacheAdmitted(t *testing.T) {
	// The supported events-only shape: type=LMCache (whose adapter supplies
	// the kvevent-subscriber the routing tier needs), no autoscaling, no
	// endpoint. The events-only rule must accept it cleanly — events-only is
	// the lighter routing-only deployment, not a misconfiguration.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend() // type=LMCache
	cb.Spec.Integration = eventsOnlyIntegration()
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("events-only LMCache (no autoscaling, no endpoint) rejected: %v", err)
	}
}

func TestValidator_EventsOnly_MooncakeRejected(t *testing.T) {
	// EventsOnly is LMCache-only. A managed Mooncake backend in events-only mode
	// would stand up a mooncake_master store but wire no KV connector to use it
	// — a contradiction. This must be rejected EXPLICITLY now that the
	// (vLLM, Mooncake) adapter is registered: the runtime-adapter check ADMITS
	// the pair (Mooncake is supported), so without the events-only rule's
	// type check the CR would slip through and reconcile as active events-only.
	// Use the explicitly injected built-in shipping registry so the
	// runtime-adapter check passes and
	// the events-only rule is the one that fires, on spec.integration.mode.
	v := shippingValidator()
	cb := newBackend()
	setCanonicalMooncakeStorage(cb)
	cb.Spec.Integration = eventsOnlyIntegration()
	requireInvalidWithCause(t, v, cb, "spec.remoteStorage",
		"provision no remote-storage provider")
}

func TestValidator_EventsOnly_OffloadDefaultLMCacheAdmitted(t *testing.T) {
	// Regression: the default integration mode (Offload) on an LMCache backend
	// — set explicitly here, but it is also the +kubebuilder default — must be
	// untouched by the events-only rule. Guards against the rule firing on the
	// Offload path.
	v := &CacheBackendValidator{Registry: stubRegistry()}
	cb := newBackend() // type=LMCache
	cb.Spec.Integration = &cachev1alpha1.CacheBackendIntegrationSpec{
		Mode: cachev1alpha1.CacheBackendIntegrationModeOffload,
	}
	if _, err := v.ValidateCreate(context.Background(), cb); err != nil {
		t.Fatalf("Offload (default) LMCache rejected: %v", err)
	}
}

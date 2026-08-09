// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package v1alpha1

import (
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

func TestCacheBackendCRDSchemaFieldsAndEnums(t *testing.T) {
	schema := loadCacheBackendOpenAPISchema(t)
	specSchema := mustPath[map[string]any](t, schema, "properties", "spec")
	statusSchema := mustPath[map[string]any](t, schema, "properties", "status")

	requireNotRequired(t, schema, "spec")

	for _, field := range []string{
		"runtime",
		"type",
		"lmCache",
		"remoteStorage",
		"observation",
		"deploymentKind",
		"replicas",
		"autoscaling",
		"integration",
		"engineSelector",
		"hiCache",
		"template",
		"allowCrossNamespace",
	} {
		if !hasProperty(specSchema, field) {
			t.Fatalf("spec.%s is missing from CRD schema", field)
		}
	}
	requireNoProperty(t, specSchema, "endpoint")
	requireNoProperty(t, specSchema, "backendConfig")
	requireNoProperty(t, specSchema, "resources")
	requireRequired(t, specSchema, "runtime")

	// indexEntries was removed in #57 (it duplicated status.indexParticipation.prefixCount);
	// health was removed in an earlier change; capacity is removed in this PR.
	// All three are guarded by requireNoProperty checks below.
	for _, field := range []string{"connector", "remoteStorage", "endpoint", "matchedEnginePods", "engineSelectorMessage", "failOpen", "conditions", "firstKVEventObservedAt", "firstAvailableAt"} {
		if !hasProperty(statusSchema, field) {
			t.Fatalf("status.%s is missing from CRD schema", field)
		}
	}
	// status.health was removed in favour of the standard
	// status.conditions[Ready] surface; guard against accidental
	// re-introduction.
	if hasProperty(statusSchema, "health") {
		t.Fatalf("status.health is present in CRD schema; it must be removed in favour of status.conditions[Ready]")
	}
	// spec.storage.pvc + status.capacity were retired: the lm:// LMCache
	// server we provision is in-memory, so a local PVC cannot back it —
	// durability is a backend choice (remote store / Mooncake), not a
	// generic volume knob (see docs/design/lmcache-server-persistence.md).
	// Guard against accidental re-introduction.
	if hasProperty(specSchema, "storage") {
		t.Fatalf("spec.storage is present in CRD schema; it was retired (durability is a backend choice — see docs/design/lmcache-server-persistence.md)")
	}
	if hasProperty(statusSchema, "capacity") {
		t.Fatalf("status.capacity is present in CRD schema; it was retired alongside spec.storage")
	}

	requireEnum(t, mustProperty(t, specSchema, "type"), []string{"LMCache", "SGLangHiCache"})
	requireEnum(t, mustProperty(t, specSchema, "runtime"), []string{"VLLM", "SGLang"})
	requireEnum(t, mustProperty(t, specSchema, "deploymentKind"), []string{
		"Deployment",
		"StatefulSet",
	})
	lmCacheSchema := mustProperty(t, specSchema, "lmCache")
	requireNoProperty(t, lmCacheSchema, "multiprocess")
	requireEnum(t, mustProperty(t, lmCacheSchema, "topology"), []string{"PodLocal", "NodeLocal"})
	podLocalSchema := mustProperty(t, lmCacheSchema, "podLocal")
	requireRequired(t, podLocalSchema, "server")
	podLocalServerSchema := mustProperty(t, podLocalSchema, "server")
	for _, field := range []string{"image", "port", "l1Capacity", "maxWorkers", "resources"} {
		requireRequired(t, podLocalServerSchema, field)
	}
	requireMinimum(t, mustProperty(t, podLocalServerSchema, "port"), 1)
	requireMaximum(t, mustProperty(t, podLocalServerSchema, "port"), 65535)
	requireMinimum(t, mustProperty(t, podLocalServerSchema, "maxWorkers"), 1)
	nodeLocalSchema := mustProperty(t, lmCacheSchema, "nodeLocal")
	requireRequired(t, nodeLocalSchema, "server")
	nodeLocalServerSchema := mustProperty(t, nodeLocalSchema, "server")
	requireMinimum(t, mustProperty(t, nodeLocalServerSchema, "maxGPUWorkers"), 1)
	requireMinimum(t, mustProperty(t, nodeLocalServerSchema, "maxCPUWorkers"), 1)
	requireMinimum(t, mustProperty(t, lmCacheSchema, "chunkSizeTokens"), 1)
	requireMinimum(t, mustProperty(t, lmCacheSchema, "workerPort"), 1)
	requireMaximum(t, mustProperty(t, lmCacheSchema, "workerPort"), 65535)
	remoteStorageSchema := mustProperty(t, specSchema, "remoteStorage")
	requireRequired(t, remoteStorageSchema, "provider")
	requireRequired(t, remoteStorageSchema, "ownership")
	requireEnum(t, mustProperty(t, remoteStorageSchema, "provider"), []string{"Redis", "LMCacheServer", "Mooncake"})
	requireEnum(t, mustProperty(t, remoteStorageSchema, "ownership"), []string{"Managed", "External"})
	for _, field := range []string{"endpoint", "redis", "lmCacheServer", "mooncake"} {
		if !hasProperty(remoteStorageSchema, field) {
			t.Fatalf("spec.remoteStorage.%s is missing from CRD schema", field)
		}
	}
	redisSchema := mustProperty(t, remoteStorageSchema, "redis")
	for _, field := range []string{"authentication", "tls", "database"} {
		if !hasProperty(redisSchema, field) {
			t.Fatalf("spec.remoteStorage.redis.%s is missing from CRD schema", field)
		}
	}
	requireMinimum(t, mustProperty(t, redisSchema, "database"), 0)

	connectorStatusSchema := mustProperty(t, statusSchema, "connector")
	requireEnum(t, mustProperty(t, connectorStatusSchema, "mode"), []string{"Multiprocess"})
	requireEnum(t, mustProperty(t, connectorStatusSchema, "topology"), []string{"PodLocal", "NodeLocal"})
	for _, field := range []string{"matchedEnginePods", "readyEnginePods", "desiredServers", "readyServers", "coveredEnginePods", "uncoveredEnginePods"} {
		requireMinimum(t, mustProperty(t, connectorStatusSchema, field), 0)
	}
	remoteStatusSchema := mustProperty(t, statusSchema, "remoteStorage")
	requireEnum(t, mustProperty(t, remoteStatusSchema, "provider"), []string{"Redis", "LMCacheServer", "Mooncake"})
	observationSchema := mustProperty(t, specSchema, "observation")
	for _, field := range []string{"modelID", "firstEventTimeout"} {
		if !hasProperty(observationSchema, field) {
			t.Fatalf("spec.observation.%s is missing from CRD schema", field)
		}
	}
	integrationSchema := mustProperty(t, specSchema, "integration")
	requireEnum(t, mustPath[map[string]any](t, integrationSchema, "properties", "role"), []string{
		"ReadOnly",
		"WriteOnly",
		"ReadWrite",
	})
	failOpenSchema := mustProperty(t, integrationSchema, "failOpen")
	if got, ok := failOpenSchema["type"].(string); !ok || got != "boolean" {
		t.Fatalf("integration.failOpen type = %v, want boolean", failOpenSchema["type"])
	}
	if got, ok := failOpenSchema["default"].(bool); !ok || !got {
		t.Fatalf("integration.failOpen default = %v, want true", failOpenSchema["default"])
	}
	templateSchema := mustProperty(t, specSchema, "template")
	requireNoPreserveUnknownFields(t, templateSchema)
	for _, field := range []string{"nodeSelector", "tolerations", "affinity"} {
		if !hasProperty(templateSchema, field) {
			t.Fatalf("spec.template.%s is missing from CRD schema", field)
		}
	}
	requireNoProperty(t, templateSchema, "containers")

	requireNotRequired(t, specSchema, "type")
	requireMinimum(t, mustProperty(t, specSchema, "replicas"), 0)
	hiCacheSchema := mustProperty(t, specSchema, "hiCache")
	requireMinimum(t, mustProperty(t, hiCacheSchema, "sizeGB"), 1)
	if got := mustProperty(t, hiCacheSchema, "ratio")["type"]; got != "string" {
		t.Fatalf("spec.hiCache.ratio type = %v, want string", got)
	}
	requireEnum(t, mustProperty(t, hiCacheSchema, "writePolicy"), []string{
		"write_back",
		"write_through",
		"write_through_selective",
	})
	requireEnum(t, mustProperty(t, hiCacheSchema, "ioBackend"), []string{
		"direct",
		"kernel",
		"kernel_ascend",
	})
	requireEnum(t, mustProperty(t, hiCacheSchema, "memoryLayout"), []string{
		"layer_first",
		"page_first",
		"page_first_direct",
		"page_first_kv_split",
		"page_head",
	})
	firstEventTimeoutSchema := mustPath[map[string]any](t, observationSchema, "properties", "firstEventTimeout")
	if got, ok := firstEventTimeoutSchema["default"].(string); !ok || got != "5m" {
		t.Fatalf("observation.firstEventTimeout default = %v, want \"5m\"", firstEventTimeoutSchema["default"])
	}
	requireMinimum(t, mustProperty(t, templateSchema, "terminationGracePeriodSeconds"), 0)

	// Operator-UX defaults. Each marker below shrinks the minimum-viable
	// CacheBackend YAML by one field; pinning the served-schema default
	// here means a future regeneration that drops one is caught at test
	// time rather than via a confused operator's failed apply.
	if got, ok := mustProperty(t, specSchema, "type")["default"].(string); !ok || got != "LMCache" {
		t.Fatalf("spec.type default = %v, want \"LMCache\"", mustProperty(t, specSchema, "type")["default"])
	}
	if got, ok := mustProperty(t, specSchema, "deploymentKind")["default"].(string); !ok || got != "Deployment" {
		t.Fatalf("spec.deploymentKind default = %v, want \"Deployment\"", mustProperty(t, specSchema, "deploymentKind")["default"])
	}
	if got, ok := mustProperty(t, specSchema, "replicas")["default"]; !ok || !reflect.DeepEqual(got, float64(1)) {
		t.Fatalf("spec.replicas default = %v (type %T), want 1", mustProperty(t, specSchema, "replicas")["default"], mustProperty(t, specSchema, "replicas")["default"])
	}
	requireNoProperty(t, integrationSchema, "engine")
	requireNoProperty(t, integrationSchema, "firstEventTimeout")
	if got, ok := mustProperty(t, integrationSchema, "role")["default"].(string); !ok || got != "ReadWrite" {
		t.Fatalf("spec.integration.role default = %v, want \"ReadWrite\"", mustProperty(t, integrationSchema, "role")["default"])
	}

	// Retired inert fields must stay absent from the served schema so a
	// regeneration can't silently reintroduce them. lookupTimeoutMs and
	// minimumPrefixTokens moved to CachePolicy; indexEntries is superseded by
	// status.indexParticipation.prefixCount.
	requireNoProperty(t, integrationSchema, "lookupTimeoutMs")
	requireNoProperty(t, integrationSchema, "minimumPrefixTokens")
	requireNoProperty(t, statusSchema, "indexEntries")

	// status.indexParticipation.prefixCount is the authoritative live count
	// surface that replaced status.indexEntries.
	requireMinimum(t, mustPath[map[string]any](t, statusSchema, "properties", "indexParticipation", "properties", "prefixCount"), 0)
	// status.indexParticipation.t2HitRate (tier-2 health surface) must be served
	// as a string — pins the marker so a regen can't silently drop the field.
	if got := mustPath[map[string]any](t, statusSchema, "properties", "indexParticipation", "properties", "t2HitRate")["type"]; got != "string" {
		t.Fatalf("status.indexParticipation.t2HitRate type = %v, want string", got)
	}
	requireMinimum(t, mustProperty(t, statusSchema, "matchedEnginePods"), 0)

	// Autoscaling validation surface.
	autoscalingSchema := mustProperty(t, specSchema, "autoscaling")
	requireRequired(t, autoscalingSchema, "maxReplicas")
	requireMinimum(t, mustProperty(t, autoscalingSchema, "minReplicas"), 1)
	requireMinimum(t, mustProperty(t, autoscalingSchema, "maxReplicas"), 1)
	requireMinimum(t, mustProperty(t, autoscalingSchema, "targetCPUUtilizationPercent"), 1)
	requireMaximum(t, mustProperty(t, autoscalingSchema, "targetCPUUtilizationPercent"), 100)
}

func TestCacheBackendMPRoundTripAndDeepCopy(t *testing.T) {
	database := int32(2)
	l1 := resource.MustParse("32Gi")
	backend := &CacheBackend{
		Spec: CacheBackendSpec{
			Runtime: CacheBackendRuntimeSGLang,
			Type:    CacheBackendTypeLMCache,
			LMCache: &LMCacheEngineSpec{
				Topology: LMCacheTopologyPodLocal,
				PodLocal: &LMCachePodLocalSpec{Server: &LMCachePodLocalServerSpec{
					Image:      "registry.example/lmcache@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					Port:       6555,
					L1Capacity: l1,
					MaxWorkers: 2,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("33Gi")},
					},
				}},
				NodeLocal: &LMCacheNodeLocalSpec{Scheduling: &LMCacheNodeLocalSchedulingSpec{
					NodeSelector: map[string]string{"pool": "cache"},
					Tolerations:  []corev1.Toleration{{Key: "cache"}},
				}},
			},
			RemoteStorage: &CacheBackendRemoteStorageSpec{
				Provider:  CacheBackendRemoteStorageProviderRedis,
				Ownership: CacheBackendRemoteStorageOwnershipExternal,
				Endpoint:  "redis.example:6379",
				Redis: &RedisRemoteStorageSpec{
					Database: &database,
					Authentication: &RedisAuthenticationSpec{
						Password: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "redis-auth"}, Key: "password"},
					},
					TLS: &RemoteStorageTLSSpec{
						CACertificate: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "redis-ca"}, Key: "ca.crt"},
					},
				},
			},
		},
		Status: CacheBackendStatus{
			Connector: &CacheBackendConnectorStatus{
				Mode:              LMCacheConnectorModeMultiprocess,
				Topology:          LMCacheTopologyPodLocal,
				MatchedEnginePods: 2,
				ReadyEnginePods:   1,
				DesiredServers:    2,
				ReadyServers:      1,
			},
			RemoteStorage: &CacheBackendRemoteStorageStatus{
				Provider: CacheBackendRemoteStorageProviderRedis,
				Endpoint: "redis.example:6379",
				Ready:    metav1.ConditionTrue,
			},
		},
	}

	data, err := json.Marshal(backend)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var roundTripped CacheBackend
	if err := json.Unmarshal(data, &roundTripped); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(backend, &roundTripped) {
		t.Fatalf("JSON round trip changed object\nwant: %#v\n got: %#v", backend, &roundTripped)
	}

	copied := backend.DeepCopy()
	backend.Spec.LMCache.PodLocal.Server.Resources.Requests[corev1.ResourceMemory] = resource.MustParse("64Gi")
	backend.Spec.LMCache.NodeLocal.Scheduling.NodeSelector["pool"] = "general"
	backend.Spec.LMCache.NodeLocal.Scheduling.Tolerations[0].Key = "general"
	*backend.Spec.RemoteStorage.Redis.Database = 9
	backend.Spec.RemoteStorage.Redis.Authentication.Password.Name = "changed"
	backend.Status.Connector.ReadyServers = 2
	backend.Status.RemoteStorage.Endpoint = "changed:6379"

	if got := copied.Spec.LMCache.PodLocal.Server.Resources.Requests[corev1.ResourceMemory]; got.Cmp(resource.MustParse("33Gi")) != 0 {
		t.Fatalf("podLocal server resources alias original: %s", got.String())
	}
	if copied.Spec.LMCache.NodeLocal.Scheduling.NodeSelector["pool"] != "cache" || copied.Spec.LMCache.NodeLocal.Scheduling.Tolerations[0].Key != "cache" {
		t.Fatalf("nodeLocal scheduling was not deep-copied")
	}
	if *copied.Spec.RemoteStorage.Redis.Database != 2 || copied.Spec.RemoteStorage.Redis.Authentication.Password.Name != "redis-auth" {
		t.Fatalf("Redis binding was not deep-copied")
	}
	if copied.Status.Connector.ReadyServers != 1 || copied.Status.RemoteStorage.Endpoint != "redis.example:6379" {
		t.Fatalf("MP status was not deep-copied")
	}
}

func TestCacheBackendCRDPrintColumns(t *testing.T) {
	version := loadCacheBackendCRDVersion(t, "v1alpha1")
	columns := mustPath[[]any](t, version, "additionalPrinterColumns")

	want := map[string]string{
		"Ready":    `.status.conditions[?(@.type=="Ready")].status`,
		"Endpoint": ".status.endpoint",
		"Matched":  ".status.matchedEnginePods",
	}
	seen := map[string]string{}
	for _, column := range columns {
		columnSchema, ok := column.(map[string]any)
		if !ok {
			t.Fatalf("print column has type %T, want object", column)
		}
		name, _ := columnSchema["name"].(string)
		jsonPath, _ := columnSchema["jsonPath"].(string)
		seen[name] = jsonPath
	}
	for name, jsonPath := range want {
		if got := seen[name]; got != jsonPath {
			t.Fatalf("print column %q jsonPath = %q, want %q", name, got, jsonPath)
		}
	}
	// Guard against the removed Health column reappearing on a future regen.
	if got, present := seen["Health"]; present {
		t.Fatalf("print column %q is present (jsonPath=%q); must be removed in favour of Ready", "Health", got)
	}
}

func TestCacheBackendDeepCopyCopiesNestedFields(t *testing.T) {
	replicas := int32(2)
	hitRate := "0.50"
	t2HitRate := "0.66"
	matchedEnginePods := int32(7)
	firstKVEventAt := metav1.NewTime(time.Unix(1_700_000_000, 0).UTC())
	firstAvailableAt := metav1.NewTime(time.Unix(1_700_000_500, 0).UTC())
	runAsNonRoot := true
	runtimeClassName := "runc"
	terminationGracePeriodSeconds := int64(30)
	autoscalingMin := int32(2)
	autoscalingTargetCPU := int32(70)
	hiCacheSize := int32(64)
	chunkSize := int32(128)
	workerPort := int32(5555)
	hostCapacity := resource.MustParse("6Gi")
	providerMemory := resource.MustParse("2Gi")
	observationTimeout := metav1.Duration{Duration: 3 * time.Minute}
	backend := &CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: CacheBackendSpec{
			Runtime:        CacheBackendRuntimeSGLang,
			Type:           CacheBackendTypeLMCache,
			DeploymentKind: CacheBackendDeploymentKindStatefulSet,
			LMCache: &LMCacheEngineSpec{
				ChunkSizeTokens: &chunkSize,
				HostMemory:      &CacheBackendHostMemorySpec{Capacity: &hostCapacity},
				WorkerPort:      &workerPort,
			},
			RemoteStorage: &CacheBackendRemoteStorageSpec{
				Provider:  CacheBackendRemoteStorageProviderLMCacheServer,
				Ownership: CacheBackendRemoteStorageOwnershipManaged,
				LMCacheServer: &LMCacheServerRemoteStorageSpec{
					Command: []string{"lmcache_server", "--flag"},
					Resources: &corev1.ResourceRequirements{
						Limits: corev1.ResourceList{corev1.ResourceMemory: providerMemory},
					},
				},
			},
			Observation: &CacheBackendObservationSpec{
				ModelID:           "model-a",
				FirstEventTimeout: &observationTimeout,
			},
			Replicas: &replicas,
			Autoscaling: &CacheBackendAutoscalingSpec{
				MinReplicas:                 &autoscalingMin,
				MaxReplicas:                 5,
				TargetCPUUtilizationPercent: &autoscalingTargetCPU,
			},
			Integration: &CacheBackendIntegrationSpec{
				Role: CacheBackendIntegrationRoleReadWrite,
			},
			EngineSelector: &CacheBackendEngineSelector{
				MatchLabels: map[string]string{"inferencecache.io/cache-enabled": "true"},
			},
			HiCache: &SGLangHiCacheSpec{
				SizeGB:      &hiCacheSize,
				WritePolicy: SGLangHiCacheWriteThrough,
			},
			Template: &CacheBackendPodSpecOverride{
				NodeSelector: map[string]string{"pool": "cache"},
				Tolerations: []corev1.Toleration{{
					Key:      "cache",
					Operator: corev1.TolerationOpExists,
				}},
				SecurityContext: &corev1.PodSecurityContext{
					RunAsNonRoot: &runAsNonRoot,
				},
				RuntimeClassName:              &runtimeClassName,
				TerminationGracePeriodSeconds: &terminationGracePeriodSeconds,
			},
		},
		Status: CacheBackendStatus{
			Endpoint: "cache.default.svc:8080",
			IndexParticipation: &CacheBackendIndexParticipation{
				PrefixCount: 7,
				HitRate:     &hitRate,
				T2HitRate:   &t2HitRate,
			},
			MatchedEnginePods:      &matchedEnginePods,
			EngineSelectorMessage:  "spec.engineSelector.matchLabels={app:engine}; no Pods in namespace match",
			FirstKVEventObservedAt: &firstKVEventAt,
			FirstAvailableAt:       &firstAvailableAt,
			Conditions: []metav1.Condition{{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				Reason:             "Available",
				Message:            "backend is ready",
				LastTransitionTime: metav1.Now(),
			}},
		},
	}

	copied := backend.DeepCopy()
	*backend.Spec.Replicas = 3
	*backend.Spec.Autoscaling.MinReplicas = 4
	backend.Spec.Autoscaling.MaxReplicas = 9
	*backend.Spec.Autoscaling.TargetCPUUtilizationPercent = 90
	*backend.Spec.LMCache.ChunkSizeTokens = 256
	changedHostCapacity := resource.MustParse("12Gi")
	*backend.Spec.LMCache.HostMemory.Capacity = changedHostCapacity
	*backend.Spec.LMCache.WorkerPort = 6666
	backend.Spec.RemoteStorage.LMCacheServer.Command[0] = "changed"
	backend.Spec.RemoteStorage.LMCacheServer.Resources.Limits[corev1.ResourceMemory] = resource.MustParse("4Gi")
	backend.Spec.Observation.ModelID = "changed"
	backend.Spec.Observation.FirstEventTimeout.Duration = time.Hour
	*backend.Spec.HiCache.SizeGB = 128
	backend.Spec.EngineSelector.MatchLabels["inferencecache.io/cache-enabled"] = "false"
	backend.Spec.Template.NodeSelector["pool"] = "general"
	backend.Spec.Template.Tolerations[0].Key = "general"
	backend.Status.IndexParticipation.PrefixCount = 99
	*backend.Status.IndexParticipation.HitRate = "0.99"
	*backend.Spec.Template.SecurityContext.RunAsNonRoot = false
	*backend.Spec.Template.RuntimeClassName = "kata"
	*backend.Spec.Template.TerminationGracePeriodSeconds = 60
	*backend.Status.MatchedEnginePods = 11
	backend.Status.EngineSelectorMessage = "changed"
	*backend.Status.FirstKVEventObservedAt = metav1.NewTime(time.Unix(0, 0).UTC())
	*backend.Status.FirstAvailableAt = metav1.NewTime(time.Unix(0, 0).UTC())
	backend.Status.Conditions[0].Message = "changed"

	if copied.Spec.Replicas == nil || *copied.Spec.Replicas != 2 {
		t.Fatalf("replicas was not deep-copied")
	}
	if copied.Spec.Autoscaling == nil {
		t.Fatalf("autoscaling was not deep-copied")
	}
	if copied.Spec.Autoscaling.MinReplicas == nil || *copied.Spec.Autoscaling.MinReplicas != 2 {
		t.Fatalf("autoscaling.minReplicas was not deep-copied")
	}
	if copied.Spec.Autoscaling.MaxReplicas != 5 {
		t.Fatalf("autoscaling.maxReplicas was not deep-copied")
	}
	if copied.Spec.Autoscaling.TargetCPUUtilizationPercent == nil || *copied.Spec.Autoscaling.TargetCPUUtilizationPercent != 70 {
		t.Fatalf("autoscaling.targetCPUUtilizationPercent was not deep-copied")
	}
	if copied.Spec.LMCache == nil ||
		copied.Spec.LMCache.ChunkSizeTokens == nil ||
		*copied.Spec.LMCache.ChunkSizeTokens != 128 ||
		copied.Spec.LMCache.HostMemory == nil ||
		copied.Spec.LMCache.HostMemory.Capacity == nil ||
		copied.Spec.LMCache.HostMemory.Capacity.Cmp(resource.MustParse("6Gi")) != 0 ||
		copied.Spec.LMCache.WorkerPort == nil ||
		*copied.Spec.LMCache.WorkerPort != 5555 {
		t.Fatalf("lmCache nested fields were not deep-copied")
	}
	if copied.Spec.RemoteStorage == nil ||
		copied.Spec.RemoteStorage.LMCacheServer == nil ||
		copied.Spec.RemoteStorage.LMCacheServer.Command[0] != "lmcache_server" ||
		copied.Spec.RemoteStorage.LMCacheServer.Resources == nil {
		t.Fatalf("remoteStorage.lmCacheServer nested fields were not deep-copied")
	}
	copiedProviderMemory := copied.Spec.RemoteStorage.LMCacheServer.Resources.Limits[corev1.ResourceMemory]
	if copiedProviderMemory.Cmp(resource.MustParse("2Gi")) != 0 {
		t.Fatalf("remoteStorage.lmCacheServer resources were not deep-copied")
	}
	if copied.Spec.Observation == nil ||
		copied.Spec.Observation.ModelID != "model-a" ||
		copied.Spec.Observation.FirstEventTimeout == nil ||
		copied.Spec.Observation.FirstEventTimeout.Duration != 3*time.Minute {
		t.Fatalf("observation nested fields were not deep-copied")
	}
	if copied.Spec.Integration == nil {
		t.Fatalf("integration was not deep-copied")
	}
	if copied.Spec.HiCache == nil || copied.Spec.HiCache.SizeGB == nil || *copied.Spec.HiCache.SizeGB != 64 {
		t.Fatalf("hiCache.sizeGB was not deep-copied")
	}
	if copied.Spec.EngineSelector == nil {
		t.Fatalf("engineSelector was not deep-copied")
	}
	if copied.Spec.EngineSelector.MatchLabels["inferencecache.io/cache-enabled"] != "true" {
		t.Fatalf("engineSelector.matchLabels was not deep-copied")
	}
	if copied.Spec.Template == nil {
		t.Fatalf("template was not deep-copied")
	}
	if copied.Spec.Template.NodeSelector["pool"] != "cache" {
		t.Fatalf("template.nodeSelector was not deep-copied")
	}
	if copied.Spec.Template.Tolerations[0].Key != "cache" {
		t.Fatalf("template.tolerations was not deep-copied")
	}
	if copied.Spec.Template.SecurityContext == nil ||
		copied.Spec.Template.SecurityContext.RunAsNonRoot == nil ||
		!*copied.Spec.Template.SecurityContext.RunAsNonRoot {
		t.Fatalf("template.securityContext was not deep-copied")
	}
	if copied.Spec.Template.RuntimeClassName == nil || *copied.Spec.Template.RuntimeClassName != "runc" {
		t.Fatalf("template.runtimeClassName was not deep-copied")
	}
	if copied.Spec.Template.TerminationGracePeriodSeconds == nil ||
		*copied.Spec.Template.TerminationGracePeriodSeconds != 30 {
		t.Fatalf("template.terminationGracePeriodSeconds was not deep-copied")
	}
	if copied.Status.IndexParticipation == nil ||
		copied.Status.IndexParticipation.PrefixCount != 7 {
		t.Fatalf("status.indexParticipation.prefixCount was not deep-copied")
	}
	if copied.Status.IndexParticipation.HitRate == nil ||
		*copied.Status.IndexParticipation.HitRate != "0.50" {
		t.Fatalf("status.indexParticipation.hitRate was not deep-copied")
	}
	if copied.Status.IndexParticipation.T2HitRate == nil ||
		*copied.Status.IndexParticipation.T2HitRate != "0.66" {
		t.Fatalf("status.indexParticipation.t2HitRate was not deep-copied")
	}
	if copied.Status.MatchedEnginePods == nil || *copied.Status.MatchedEnginePods != 7 {
		t.Fatalf("status.matchedEnginePods was not deep-copied")
	}
	if copied.Status.EngineSelectorMessage != "spec.engineSelector.matchLabels={app:engine}; no Pods in namespace match" {
		t.Fatalf("status.engineSelectorMessage was not deep-copied")
	}
	if copied.Status.FirstKVEventObservedAt == nil || !copied.Status.FirstKVEventObservedAt.Time.Equal(time.Unix(1_700_000_000, 0).UTC()) {
		t.Fatalf("status.firstKVEventObservedAt was not deep-copied")
	}
	if copied.Status.FirstAvailableAt == nil || !copied.Status.FirstAvailableAt.Time.Equal(time.Unix(1_700_000_500, 0).UTC()) {
		t.Fatalf("status.firstAvailableAt was not deep-copied")
	}
	if copied.Status.Conditions[0].Message != "backend is ready" {
		t.Fatalf("conditions were not deep-copied")
	}
}

func TestCacheBackendJSONOmitEmptySpecPointers(t *testing.T) {
	data, err := json.Marshal(CacheBackendSpec{})
	if err != nil {
		t.Fatalf("marshal empty spec: %v", err)
	}
	if string(data) != `{"runtime":""}` {
		t.Fatalf("empty spec JSON = %s, want required runtime field", data)
	}
}

func TestIntegrationFailOpenDefaultsTrue(t *testing.T) {
	falseV, trueV := false, true
	cases := []struct {
		name string
		spec *CacheBackendIntegrationSpec
		want bool
	}{
		{name: "nil spec defaults true", spec: nil, want: true},
		{name: "nil field defaults true", spec: &CacheBackendIntegrationSpec{}, want: true},
		{name: "explicit true", spec: &CacheBackendIntegrationSpec{FailOpen: &trueV}, want: true},
		{name: "explicit false honored", spec: &CacheBackendIntegrationSpec{FailOpen: &falseV}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IntegrationFailOpen(tc.spec); got != tc.want {
				t.Fatalf("IntegrationFailOpen(%+v) = %v, want %v", tc.spec, got, tc.want)
			}
		})
	}
}

func loadCacheBackendOpenAPISchema(t *testing.T) map[string]any {
	t.Helper()

	version := loadCacheBackendCRDVersion(t, "v1alpha1")
	return mustPath[map[string]any](t, version, "schema", "openAPIV3Schema")
}

func loadCacheBackendCRDVersion(t *testing.T, name string) map[string]any {
	t.Helper()

	data, err := os.ReadFile("../../config/crd/bases/inferencecache.io_cachebackends.yaml")
	if err != nil {
		t.Fatalf("read generated CRD: %v", err)
	}

	var crd map[string]any
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatalf("unmarshal generated CRD: %v", err)
	}
	versions := mustPath[[]any](t, crd, "spec", "versions")
	for _, version := range versions {
		versionSchema, ok := version.(map[string]any)
		if !ok {
			t.Fatalf("CRD version entry has type %T, want object", version)
		}

		versionName, ok := versionSchema["name"].(string)
		if !ok {
			t.Fatalf("CRD version entry has name type %T, want string", versionSchema["name"])
		}
		if versionName == name {
			return versionSchema
		}
	}

	t.Fatalf("CRD does not contain version %s", name)
	return nil
}

func hasProperty(schema map[string]any, field string) bool {
	properties, ok := schema["properties"].(map[string]any)
	if !ok {
		return false
	}
	_, ok = properties[field]
	return ok
}

func mustProperty(t *testing.T, schema map[string]any, field string) map[string]any {
	t.Helper()
	return mustPath[map[string]any](t, schema, "properties", field)
}

func requireEnum(t *testing.T, schema map[string]any, expected []string) {
	t.Helper()

	values := mustPath[[]any](t, schema, "enum")
	actual := make([]string, 0, len(values))
	for index, value := range values {
		stringValue, ok := value.(string)
		if !ok {
			t.Fatalf("enum[%d] = %v (%T), want string", index, value, value)
		}
		actual = append(actual, stringValue)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("enum = %v, want %v", actual, expected)
	}
}

func requireNoEnum(t *testing.T, schema map[string]any) {
	t.Helper()

	if _, ok := schema["enum"]; ok {
		t.Fatalf("schema unexpectedly has enum validation: %v", schema["enum"])
	}
}

func requireNoProperty(t *testing.T, schema map[string]any, field string) {
	t.Helper()

	if hasProperty(schema, field) {
		t.Fatalf("schema properties unexpectedly contain %q", field)
	}
}

func requireNoPreserveUnknownFields(t *testing.T, schema map[string]any) {
	t.Helper()

	if value, ok := schema["x-kubernetes-preserve-unknown-fields"]; ok {
		t.Fatalf("schema unexpectedly preserves unknown fields: %v", value)
	}
}

func requireRequired(t *testing.T, schema map[string]any, field string) {
	t.Helper()

	values := mustPath[[]any](t, schema, "required")
	for _, value := range values {
		if value == field {
			return
		}
	}
	t.Fatalf("required fields = %v, want %q", values, field)
}

func requireNotRequired(t *testing.T, schema map[string]any, field string) {
	t.Helper()

	if !hasProperty(schema, field) {
		t.Fatalf("schema properties do not contain %q", field)
	}

	requiredValue, ok := schema["required"]
	if !ok {
		return
	}
	values, ok := requiredValue.([]any)
	if !ok {
		t.Fatalf("required has type %T, want array", requiredValue)
	}
	for _, value := range values {
		if value == field {
			t.Fatalf("required fields = %v, did not want %q", values, field)
		}
	}
}

func requireMinimum(t *testing.T, schema map[string]any, expected int64) {
	t.Helper()

	minimum := mustPath[float64](t, schema, "minimum")
	if int64(minimum) != expected {
		t.Fatalf("minimum = %v, want %d", minimum, expected)
	}
}

func requireMaximum(t *testing.T, schema map[string]any, expected int64) {
	t.Helper()

	maximum := mustPath[float64](t, schema, "maximum")
	if int64(maximum) != expected {
		t.Fatalf("maximum = %v, want %d", maximum, expected)
	}
}

func mustPath[T any](t *testing.T, root any, path ...any) T {
	t.Helper()

	current := root
	for _, segment := range path {
		switch typedSegment := segment.(type) {
		case string:
			object, ok := current.(map[string]any)
			if !ok {
				t.Fatalf("path %v: got %T, want object before %q", path, current, typedSegment)
			}
			value, ok := object[typedSegment]
			if !ok {
				t.Fatalf("path %v: missing %q", path, typedSegment)
			}
			current = value
		case int:
			array, ok := current.([]any)
			if !ok {
				t.Fatalf("path %v: got %T, want array before %d", path, current, typedSegment)
			}
			if typedSegment < 0 || typedSegment >= len(array) {
				t.Fatalf("path %v: index %d outside array length %d", path, typedSegment, len(array))
			}
			current = array[typedSegment]
		default:
			t.Fatalf("unsupported path segment %s", strconv.Quote(reflect.TypeOf(segment).String()))
		}
	}

	typed, ok := current.(T)
	if !ok {
		t.Fatalf("path %v: got %T, want requested type", path, current)
	}
	return typed
}

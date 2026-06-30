package v1alpha1

import (
	"encoding/json"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

func TestCacheBackendCRDSchemaFieldsAndEnums(t *testing.T) {
	schema := loadCacheBackendOpenAPISchema(t)
	specSchema := mustPath[map[string]any](t, schema, "properties", "spec")
	statusSchema := mustPath[map[string]any](t, schema, "properties", "status")

	requireNotRequired(t, schema, "spec")

	for _, field := range []string{
		"type",
		"deploymentKind",
		"replicas",
		"autoscaling",
		"integration",
		"engineSelector",
		"backendConfig",
		"template",
		"endpoint",
		"allowCrossNamespace",
	} {
		if !hasProperty(specSchema, field) {
			t.Fatalf("spec.%s is missing from CRD schema", field)
		}
	}

	// indexEntries was removed in #57 (it duplicated status.indexParticipation.prefixCount);
	// health was removed in an earlier change; capacity is removed in this PR.
	// All three are guarded by requireNoProperty checks below.
	for _, field := range []string{"endpoint", "matchedEnginePods", "engineSelectorMessage", "failOpen", "conditions", "firstKVEventObservedAt", "firstAvailableAt"} {
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

	requireNoEnum(t, mustProperty(t, specSchema, "type"))
	requireEnum(t, mustProperty(t, specSchema, "deploymentKind"), []string{
		"Deployment",
		"StatefulSet",
	})
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
	firstEventTimeoutSchema := mustPath[map[string]any](t, integrationSchema, "properties", "firstEventTimeout")
	if got, ok := firstEventTimeoutSchema["default"].(string); !ok || got != "5m" {
		t.Fatalf("integration.firstEventTimeout default = %v, want \"5m\"", firstEventTimeoutSchema["default"])
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
	if got, ok := mustProperty(t, integrationSchema, "engine")["default"].(string); !ok || got != "vllm" {
		t.Fatalf("spec.integration.engine default = %v, want \"vllm\"", mustProperty(t, integrationSchema, "engine")["default"])
	}
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
	firstEventTimeout := metav1.Duration{Duration: 5 * time.Minute}
	firstKVEventAt := metav1.NewTime(time.Unix(1_700_000_000, 0).UTC())
	firstAvailableAt := metav1.NewTime(time.Unix(1_700_000_500, 0).UTC())
	runAsNonRoot := true
	runtimeClassName := "runc"
	terminationGracePeriodSeconds := int64(30)
	autoscalingMin := int32(2)
	autoscalingTargetCPU := int32(70)
	backend := &CacheBackend{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Namespace: "default"},
		Spec: CacheBackendSpec{
			Type:           CacheBackendTypeLMCache,
			DeploymentKind: CacheBackendDeploymentKindStatefulSet,
			Replicas:       &replicas,
			Autoscaling: &CacheBackendAutoscalingSpec{
				MinReplicas:                 &autoscalingMin,
				MaxReplicas:                 5,
				TargetCPUUtilizationPercent: &autoscalingTargetCPU,
			},
			Integration: &CacheBackendIntegrationSpec{
				Engine:            "SGLang",
				Role:              CacheBackendIntegrationRoleReadWrite,
				FirstEventTimeout: &firstEventTimeout,
			},
			EngineSelector: &CacheBackendEngineSelector{
				MatchLabels: map[string]string{"inferencecache.io/cache-enabled": "true"},
			},
			BackendConfig: map[string]string{"evictionPolicy": "LRU"},
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
			Endpoint: "external-cache.default.svc:8080",
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
	backend.Spec.Integration.FirstEventTimeout.Duration = time.Hour
	backend.Spec.BackendConfig["evictionPolicy"] = "FIFO"
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
	if copied.Spec.Integration == nil {
		t.Fatalf("integration was not deep-copied")
	}
	if copied.Spec.Integration.Engine != "SGLang" {
		t.Fatalf("integration.engine was not deep-copied")
	}
	if copied.Spec.BackendConfig["evictionPolicy"] != "LRU" {
		t.Fatalf("backendConfig was not deep-copied")
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
	if copied.Spec.Integration.FirstEventTimeout == nil || copied.Spec.Integration.FirstEventTimeout.Duration != 5*time.Minute {
		t.Fatalf("integration.firstEventTimeout was not deep-copied")
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
	if string(data) != "{}" {
		t.Fatalf("empty spec JSON = %s, want {}", data)
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

package v1alpha1

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	structuralpruning "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

func TestRemainingCRDSchemas(t *testing.T) {
	policySchema := loadCRDOpenAPISchema(t, "config/crd/bases/inferencecache.io_cachepolicies.yaml")
	requireRequired(t, policySchema, "spec")
	policySpec := mustPath[map[string]any](t, policySchema, "properties", "spec")
	requireDurationLike(t, mustProperty(t, policySpec, "evictionTTL"))
	requireMinimum(t, mustProperty(t, policySpec, "minimumPrefixTokens"), 0)
	requireMinimum(t, mustProperty(t, policySpec, "lookupTimeoutMs"), 0)
	// Fields trimmed at v1alpha1 because they were declarative-only — guard
	// against accidental re-introduction via a stale regen.
	for _, removed := range []string{"eviction", "failOpen", "tenantScoped"} {
		requireNoProperty(t, policySpec, removed)
	}

	tenantSchema := loadCRDOpenAPISchema(t, "config/crd/bases/inferencecache.io_cachetenants.yaml")
	requireRequired(t, tenantSchema, "spec")
	tenantSpec := mustPath[map[string]any](t, tenantSchema, "properties", "spec")
	requireRequired(t, tenantSpec, "tenantID")
	requireMinLength(t, mustProperty(t, tenantSpec, "tenantID"), 1)
	requireEnum(t, mustProperty(t, tenantSpec, "isolationMode"), []string{"Fairness"})
	requireDefault(t, mustProperty(t, tenantSpec, "isolationMode"), "Fairness")
	tenantQuota := mustProperty(t, tenantSpec, "quota")
	requireMinimum(t, mustProperty(t, tenantQuota, "maxMemoryBytes"), 0)
	requireMinimum(t, mustProperty(t, tenantQuota, "maxIndexEntries"), 0)
	requireReservedEmptyObject(t, mustProperty(t, tenantSpec, "crypto"))

	templateSchema := loadCRDOpenAPISchema(t, "config/crd/bases/inferencecache.io_prompttemplates.yaml")
	requireRequired(t, templateSchema, "spec")
	templateSpec := mustPath[map[string]any](t, templateSchema, "properties", "spec")
	requireRequired(t, templateSpec, "body")
	requireMinLength(t, mustProperty(t, templateSpec, "body"), 1)
	slotsSchema := mustProperty(t, templateSpec, "slots")
	requireListMapKey(t, slotsSchema, "name")
	slotSchema := mustPath[map[string]any](t, slotsSchema, "items")
	requireRequired(t, slotSchema, "name")
	requireMinLength(t, mustProperty(t, slotSchema, "name"), 1)
	requireRequired(t, slotSchema, "type")
	requireEnum(t, mustProperty(t, slotSchema, "type"), []string{"Stable", "Mutable"})

	topologySchema := loadCRDOpenAPISchema(t, "config/crd/bases/inferencecache.io_pdtopologies.yaml")
	topologySpec := mustPath[map[string]any](t, topologySchema, "properties", "spec")
	for _, field := range []string{"prefillPools", "decodePools", "acceleratorTypes"} {
		requireListMapKey(t, mustProperty(t, topologySpec, field), "name")
	}
	prefillPool := mustPath[map[string]any](t, topologySpec, "properties", "prefillPools", "items")
	requireRequired(t, prefillPool, "name")
	requireMinLength(t, mustProperty(t, prefillPool, "name"), 1)
	requireMinimum(t, mustProperty(t, prefillPool, "replicas"), 0)
	acceleratorType := mustPath[map[string]any](t, topologySpec, "properties", "acceleratorTypes", "items")
	requireRequired(t, acceleratorType, "name")
	requireMinLength(t, mustProperty(t, acceleratorType, "name"), 1)

	indexSchema := loadCRDOpenAPISchema(t, "config/crd/bases/inferencecache.io_cacheindices.yaml")
	requireLegacyStatusOnlySpecSchema(t, mustProperty(t, indexSchema, "spec"))
}

func TestRemainingCRDDeepCopies(t *testing.T) {
	ttl := metav1.Duration{Duration: time.Minute}
	minimumPrefixTokens := int32(32)
	lookupTimeoutMs := int32(20)
	policy := &CachePolicy{
		Spec: CachePolicySpec{
			EvictionTTL:         &ttl,
			MinimumPrefixTokens: &minimumPrefixTokens,
			LookupTimeoutMs:     &lookupTimeoutMs,
		},
		Status: CachePolicyStatus{Conditions: []metav1.Condition{{Type: "Ready", Message: "ok"}}},
	}
	policyCopy := policy.DeepCopy()
	policy.Spec.EvictionTTL.Duration = 2 * time.Minute
	policy.Status.Conditions[0].Message = "changed"
	if policyCopy.Spec.EvictionTTL.Duration != time.Minute ||
		policyCopy.Status.Conditions[0].Message != "ok" {
		t.Fatalf("CachePolicy was not deep-copied")
	}

	maxMemoryBytes := int64(1024)
	maxIndexEntries := int64(100)
	memoryUsed := int64(512)
	tenant := &CacheTenant{
		Spec: CacheTenantSpec{
			TenantID:      "tenant-a",
			IsolationMode: CacheTenantIsolationModeFairness,
			Quota: &CacheTenantQuotaSpec{
				MaxMemoryBytes:  &maxMemoryBytes,
				MaxIndexEntries: &maxIndexEntries,
			},
			Crypto: &CacheTenantCryptoSpec{},
		},
		Status: CacheTenantStatus{
			MemoryUsed: &memoryUsed,
			Conditions: []metav1.Condition{{Type: "Ready", Message: "ok"}},
		},
	}
	tenantCopy := tenant.DeepCopy()
	*tenant.Spec.Quota.MaxMemoryBytes = 2048
	*tenant.Status.MemoryUsed = 256
	tenant.Status.Conditions[0].Message = "changed"
	if *tenantCopy.Spec.Quota.MaxMemoryBytes != 1024 ||
		*tenantCopy.Status.MemoryUsed != 512 ||
		tenantCopy.Status.Conditions[0].Message != "ok" {
		t.Fatalf("CacheTenant was not deep-copied")
	}

	required := true
	template := &PromptTemplate{
		Spec: PromptTemplateSpec{
			Body: "system: {{.system}}\nuser: {{.user}}",
			Slots: []PromptTemplateSlot{{
				Name:        "system",
				Type:        PromptTemplateSlotTypeStable,
				Required:    &required,
				Description: "stable system prompt",
			}},
		},
		Status: PromptTemplateStatus{Conditions: []metav1.Condition{{Type: "Ready", Message: "ok"}}},
	}
	templateCopy := template.DeepCopy()
	*template.Spec.Slots[0].Required = false
	template.Spec.Slots[0].Description = "changed"
	template.Status.Conditions[0].Message = "changed"
	if !*templateCopy.Spec.Slots[0].Required ||
		templateCopy.Spec.Slots[0].Description != "stable system prompt" ||
		templateCopy.Status.Conditions[0].Message != "ok" {
		t.Fatalf("PromptTemplate was not deep-copied")
	}

	prefillReplicas := int32(2)
	decodeReplicas := int32(3)
	topology := &PDTopology{
		Spec: PDTopologySpec{
			PrefillPools: []PDPoolSpec{{
				Name:            "prefill-a",
				MatchLabels:     map[string]string{"role": "prefill"},
				Replicas:        &prefillReplicas,
				AcceleratorType: "a100",
			}},
			DecodePools: []PDPoolSpec{{
				Name:            "decode-a",
				MatchLabels:     map[string]string{"role": "decode"},
				Replicas:        &decodeReplicas,
				AcceleratorType: "l4",
			}},
			AcceleratorTypes: []PDAcceleratorTypeSpec{{
				Name:        "a100",
				Vendor:      "nvidia",
				MatchLabels: map[string]string{"accelerator": "a100"},
			}},
		},
		Status: PDTopologyStatus{Conditions: []metav1.Condition{{Type: "Ready", Message: "ok"}}},
	}
	topologyCopy := topology.DeepCopy()
	*topology.Spec.PrefillPools[0].Replicas = 4
	topology.Spec.PrefillPools[0].MatchLabels["role"] = "changed"
	topology.Spec.AcceleratorTypes[0].MatchLabels["accelerator"] = "changed"
	topology.Status.Conditions[0].Message = "changed"
	if *topologyCopy.Spec.PrefillPools[0].Replicas != 2 ||
		topologyCopy.Spec.PrefillPools[0].MatchLabels["role"] != "prefill" ||
		topologyCopy.Spec.AcceleratorTypes[0].MatchLabels["accelerator"] != "a100" ||
		topologyCopy.Status.Conditions[0].Message != "ok" {
		t.Fatalf("PDTopology was not deep-copied")
	}

	index := &CacheIndex{
		Status: CacheIndexStatus{
			Replicas: []ReplicaCacheStatus{{ID: "r1", CacheMemoryBytes: 100}},
			Tenants:  []TenantCacheStatus{{ID: "tenant-a", MemoryUsed: 50}},
			Prefixes: PrefixStatus{Summary: PrefixSummary{
				Total: 1,
				Hot:   0,
			}},
		},
	}
	indexCopy := index.DeepCopy()
	index.Status.Replicas[0].CacheMemoryBytes = 200
	index.Status.Tenants[0].MemoryUsed = 75
	index.Status.Prefixes.Summary.Total = 2
	if indexCopy.Status.Replicas[0].CacheMemoryBytes != 100 ||
		indexCopy.Status.Tenants[0].MemoryUsed != 50 ||
		indexCopy.Status.Prefixes.Summary.Total != 1 {
		t.Fatalf("CacheIndex was not deep-copied")
	}
}

func TestGeneratedCRDAdmissionValidationHandlesCacheIndexStatusOnlySpec(t *testing.T) {
	schema := loadCRDInternalOpenAPISchema(t, "config/crd/bases/inferencecache.io_cacheindices.yaml")

	validCases := []struct {
		name string
		obj  map[string]any
	}{
		{
			name: "omitted legacy spec",
			obj: map[string]any{
				"apiVersion": "inferencecache.io/v1alpha1",
				"kind":       "CacheIndex",
			},
		},
		{
			name: "empty legacy spec",
			obj: map[string]any{
				"apiVersion": "inferencecache.io/v1alpha1",
				"kind":       "CacheIndex",
				"spec":       map[string]any{},
			},
		},
		{
			name: "non-empty legacy spec pruned",
			obj: map[string]any{
				"apiVersion": "inferencecache.io/v1alpha1",
				"kind":       "CacheIndex",
				"spec": map[string]any{
					"foo": "bar",
				},
			},
		},
	}
	for _, tc := range validCases {
		t.Run(tc.name, func(t *testing.T) {
			if errs := validateGeneratedCustomResource(t, schema, tc.obj); len(errs) != 0 {
				t.Fatalf("valid CacheIndex produced admission validation errors: %v", errs)
			}
		})
	}

	legacyNonEmpty := map[string]any{
		"apiVersion": "inferencecache.io/v1alpha1",
		"kind":       "CacheIndex",
		"spec": map[string]any{
			"foo": "bar",
		},
	}
	pruned := pruneGeneratedCustomResource(t, schema, legacyNonEmpty)
	spec, ok := pruned["spec"].(map[string]any)
	if !ok {
		t.Fatalf("CacheIndex spec was pruned entirely, want legacy empty object: %#v", pruned["spec"])
	}
	if _, ok := spec["foo"]; ok {
		t.Fatalf("CacheIndex legacy spec field survived pruning: %#v", spec)
	}
}

func TestGeneratedCRDAdmissionValidationRejectsReservedCacheTenantCryptoConfig(t *testing.T) {
	schema := loadCRDInternalOpenAPISchema(t, "config/crd/bases/inferencecache.io_cachetenants.yaml")

	valid := map[string]any{
		"apiVersion": "inferencecache.io/v1alpha1",
		"kind":       "CacheTenant",
		"spec": map[string]any{
			"tenantID": "tenant-a",
			"crypto":   map[string]any{},
		},
	}
	if errs := validateGeneratedCustomResource(t, schema, valid); len(errs) != 0 {
		t.Fatalf("valid CacheTenant produced admission validation errors: %v", errs)
	}

	invalid := map[string]any{
		"apiVersion": "inferencecache.io/v1alpha1",
		"kind":       "CacheTenant",
		"spec": map[string]any{
			"tenantID": "tenant-a",
			"crypto": map[string]any{
				"keyRef": "secret-name",
			},
		},
	}
	pruned := pruneGeneratedCustomResource(t, schema, invalid)
	crypto := mustPath[map[string]any](t, pruned, "spec", "crypto")
	if _, ok := crypto["keyRef"]; !ok {
		t.Fatalf("reserved CacheTenant crypto config was pruned before validation: %#v", crypto)
	}
	if errs := validateGeneratedCustomResource(t, schema, invalid); len(errs) == 0 {
		t.Fatal("CacheTenant with non-empty reserved crypto config passed generated CRD admission validation")
	}
}

func TestCacheIndexJSONUsesLegacyEmptySpecByDefault(t *testing.T) {
	data, err := json.Marshal(CacheIndex{})
	if err != nil {
		t.Fatalf("marshal CacheIndex: %v", err)
	}
	if !strings.Contains(string(data), `"spec":{}`) {
		t.Fatalf("empty CacheIndex JSON = %s, want legacy empty spec", data)
	}
}

func loadCRDInternalOpenAPISchema(t *testing.T, relativePath string) *apiextensions.JSONSchemaProps {
	t.Helper()

	data, err := os.ReadFile("../../" + relativePath)
	if err != nil {
		t.Fatalf("read generated CRD %s: %v", relativePath, err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatalf("unmarshal generated CRD %s: %v", relativePath, err)
	}
	for _, version := range crd.Spec.Versions {
		if version.Name != "v1alpha1" {
			continue
		}
		if version.Schema == nil || version.Schema.OpenAPIV3Schema == nil {
			t.Fatalf("CRD %s version v1alpha1 has no OpenAPI schema", relativePath)
		}
		internalSchema := &apiextensions.JSONSchemaProps{}
		if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(
			version.Schema.OpenAPIV3Schema,
			internalSchema,
			nil,
		); err != nil {
			t.Fatalf("convert generated CRD %s schema: %v", relativePath, err)
		}
		return internalSchema
	}
	t.Fatalf("CRD %s does not contain version v1alpha1", relativePath)
	return nil
}

func validateGeneratedCustomResource(t *testing.T, schema *apiextensions.JSONSchemaProps, obj map[string]any) []string {
	t.Helper()

	candidate := pruneGeneratedCustomResource(t, schema, obj)

	validator, _, err := apiservervalidation.NewSchemaValidator(schema)
	if err != nil {
		t.Fatalf("build schema validator: %v", err)
	}
	errs := apiservervalidation.ValidateCustomResource(nil, candidate, validator)
	messages := make([]string, 0, len(errs))
	for _, err := range errs {
		messages = append(messages, err.Error())
	}
	return messages
}

func pruneGeneratedCustomResource(t *testing.T, schema *apiextensions.JSONSchemaProps, obj map[string]any) map[string]any {
	t.Helper()

	structural, err := structuralschema.NewStructural(schema)
	if err != nil {
		t.Fatalf("build structural schema: %v", err)
	}
	candidate := runtime.DeepCopyJSONValue(obj).(map[string]any)
	structuralpruning.Prune(candidate, structural, true)
	return candidate
}

func loadCRDOpenAPISchema(t *testing.T, relativePath string) map[string]any {
	t.Helper()
	version := loadCRDVersion(t, relativePath, "v1alpha1")
	return mustPath[map[string]any](t, version, "schema", "openAPIV3Schema")
}

func loadCRDVersion(t *testing.T, relativePath, name string) map[string]any {
	t.Helper()
	data, err := os.ReadFile("../../" + relativePath)
	if err != nil {
		t.Fatalf("read generated CRD %s: %v", relativePath, err)
	}
	var crd map[string]any
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatalf("unmarshal generated CRD %s: %v", relativePath, err)
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
	t.Fatalf("CRD %s does not contain version %s", relativePath, name)
	return nil
}

func requireDefault(t *testing.T, schema map[string]any, expected any) {
	t.Helper()
	value, ok := schema["default"]
	if !ok {
		t.Fatalf("schema has no default, want %v", expected)
	}
	if value != expected {
		t.Fatalf("default = %v, want %v", value, expected)
	}
}

func requireListMapKey(t *testing.T, schema map[string]any, key string) {
	t.Helper()
	if schema["x-kubernetes-list-type"] != "map" {
		t.Fatalf("x-kubernetes-list-type = %v, want map", schema["x-kubernetes-list-type"])
	}
	keys := mustPath[[]any](t, schema, "x-kubernetes-list-map-keys")
	for _, value := range keys {
		if value == key {
			return
		}
	}
	t.Fatalf("x-kubernetes-list-map-keys = %v, want %q", keys, key)
}

func requireMinLength(t *testing.T, schema map[string]any, expected int64) {
	t.Helper()

	minLength := mustPath[float64](t, schema, "minLength")
	if int64(minLength) != expected {
		t.Fatalf("minLength = %v, want %d", minLength, expected)
	}
}

func requireDurationLike(t *testing.T, schema map[string]any) {
	t.Helper()
	if got := mustPath[string](t, schema, "type"); got != "string" {
		t.Fatalf("type = %q, want string", got)
	}
	if _, ok := schema["format"]; ok {
		t.Fatalf("format = %v, want plain string duration schema", schema["format"])
	}
}

func requireLegacyStatusOnlySpecSchema(t *testing.T, schema map[string]any) {
	t.Helper()
	if got := mustPath[string](t, schema, "type"); got != "object" {
		t.Fatalf("type = %q, want object", got)
	}
	if _, ok := schema["maxProperties"]; ok {
		t.Fatalf("maxProperties = %v, want omitted for v1alpha1 compatibility", schema["maxProperties"])
	}
	if _, ok := schema["x-kubernetes-preserve-unknown-fields"]; ok {
		t.Fatalf("x-kubernetes-preserve-unknown-fields = %v, want omitted for v1alpha1 pruning compatibility", schema["x-kubernetes-preserve-unknown-fields"])
	}
}

func requireReservedEmptyObject(t *testing.T, schema map[string]any) {
	t.Helper()
	if got := mustPath[string](t, schema, "type"); got != "object" {
		t.Fatalf("type = %q, want object", got)
	}
	maxProperties := mustPath[float64](t, schema, "maxProperties")
	if maxProperties != 0 {
		t.Fatalf("maxProperties = %v, want 0", maxProperties)
	}
	if schema["x-kubernetes-preserve-unknown-fields"] != true {
		t.Fatalf("x-kubernetes-preserve-unknown-fields = %v, want true", schema["x-kubernetes-preserve-unknown-fields"])
	}
}

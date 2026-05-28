package v1alpha1

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

func TestRemainingCRDSchemas(t *testing.T) {
	policySchema := loadCRDOpenAPISchema(t, "config/crd/bases/inferencecache.io_cachepolicies.yaml")
	policySpec := mustPath[map[string]any](t, policySchema, "properties", "spec")
	requireEnum(t, mustProperty(t, policySpec, "eviction"), []string{"LRU", "LFU"})
	requireDurationLike(t, mustProperty(t, policySpec, "evictionTTL"))
	requireDefault(t, mustProperty(t, policySpec, "failOpen"), true)
	requireMinimum(t, mustProperty(t, policySpec, "minimumPrefixTokens"), 0)
	requireMinimum(t, mustProperty(t, policySpec, "lookupTimeoutMs"), 0)
	requireBooleanLike(t, mustProperty(t, policySpec, "tenantScoped"))

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
	requireStatusOnlySpecValidation(t, indexSchema)
}

func TestRemainingCRDDeepCopies(t *testing.T) {
	ttl := metav1.Duration{Duration: time.Minute}
	minimumPrefixTokens := int32(32)
	lookupTimeoutMs := int32(20)
	failOpen := true
	policy := &CachePolicy{
		Spec: CachePolicySpec{
			Eviction:            CachePolicyEvictionAlgorithmLRU,
			EvictionTTL:         &ttl,
			MinimumPrefixTokens: &minimumPrefixTokens,
			LookupTimeoutMs:     &lookupTimeoutMs,
			FailOpen:            &failOpen,
			TenantScoped:        &failOpen,
		},
		Status: CachePolicyStatus{Conditions: []metav1.Condition{{Type: "Ready", Message: "ok"}}},
	}
	policyCopy := policy.DeepCopy()
	policy.Spec.EvictionTTL.Duration = 2 * time.Minute
	*policy.Spec.FailOpen = false
	*policy.Spec.TenantScoped = false
	policy.Status.Conditions[0].Message = "changed"
	if policyCopy.Spec.EvictionTTL.Duration != time.Minute || !*policyCopy.Spec.FailOpen ||
		!*policyCopy.Spec.TenantScoped ||
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

func TestCacheIndexJSONUsesLegacyEmptySpecByDefault(t *testing.T) {
	data, err := json.Marshal(CacheIndex{})
	if err != nil {
		t.Fatalf("marshal CacheIndex: %v", err)
	}
	if !strings.Contains(string(data), `"spec":{}`) {
		t.Fatalf("empty CacheIndex JSON = %s, want legacy empty spec", data)
	}
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

func requireBooleanLike(t *testing.T, schema map[string]any) {
	t.Helper()
	if got := mustPath[string](t, schema, "type"); got != "boolean" {
		t.Fatalf("type = %q, want boolean", got)
	}
}

func requireStatusOnlySpecValidation(t *testing.T, schema map[string]any) {
	t.Helper()
	validations := mustPath[[]any](t, schema, "x-kubernetes-validations")
	for _, validation := range validations {
		validationSchema, ok := validation.(map[string]any)
		if !ok {
			t.Fatalf("x-kubernetes-validations entry has type %T, want object", validation)
		}
		if validationSchema["rule"] == "!has(self.spec) || self.spec == {}" {
			return
		}
	}
	t.Fatalf("x-kubernetes-validations = %v, want legacy-empty-spec-compatible status-only validation", validations)
}

package checks

import (
	"context"
	"fmt"
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
	"github.com/cachebox-project/inference-cache/pkg/cli/doctor"
)

const (
	checkCacheTenantHealth   = "CacheTenantHealth"
	checkCachePolicyCoverage = "CachePolicyCoverage"
)

// CacheTenantHealth checks each CacheTenant's QuotaExceeded condition. A True
// condition is a WARN that surfaces the observed entry count (from
// status.indexEntries, falling back to the condition message) so the operator
// can see how far over budget the tenant is. Tenants with no QuotaExceeded
// condition (the quota status writer has not reported on them, or they are
// within budget) are reported OK.
func CacheTenantHealth(ctx context.Context, c client.Client, ns string) []doctor.Finding {
	var tenants cachev1alpha1.CacheTenantList
	if err := c.List(ctx, &tenants, client.InNamespace(ns)); err != nil {
		return []doctor.Finding{listError(checkCacheTenantHealth, "CacheTenant", err)}
	}

	var findings []doctor.Finding
	for i := range tenants.Items {
		ct := &tenants.Items[i]
		ref := resourceRef("CacheTenant", ct.Namespace, ct.Name)
		cond := findCondition(ct.Status.Conditions, conditionQuotaExceeded)
		if cond != nil && cond.Status == metav1.ConditionTrue {
			findings = append(findings, doctor.Finding{
				Code:     doctor.CodeTenantQuotaExceeded,
				Status:   doctor.StatusWarn,
				Check:    checkCacheTenantHealth,
				Resource: ref,
				Message:  quotaExceededMessage(ct, cond),
			})
			continue
		}
		findings = append(findings, doctor.Finding{
			Code:     doctor.CodeTenantHealthy,
			Status:   doctor.StatusOK,
			Check:    checkCacheTenantHealth,
			Resource: ref,
			Message:  fmt.Sprintf("tenant %q is within quota", ct.Spec.TenantID),
		})
	}
	return findings
}

func quotaExceededMessage(ct *cachev1alpha1.CacheTenant, cond *metav1.Condition) string {
	entries := "unknown"
	if ct.Status.IndexEntries != nil {
		entries = fmt.Sprintf("%d", *ct.Status.IndexEntries)
	}
	quota := "unset"
	if ct.Spec.Quota != nil && ct.Spec.Quota.MaxIndexEntries != nil {
		quota = fmt.Sprintf("%d", *ct.Spec.Quota.MaxIndexEntries)
	}
	msg := fmt.Sprintf("QuotaExceeded=True for tenant %q: %s index entries against a maxIndexEntries quota of %s", ct.Spec.TenantID, entries, quota)
	if cond.Message != "" {
		msg += " — " + cond.Message
	}
	return msg
}

// CachePolicyCoverage reports, for each namespace that contains at least one
// CacheBackend, whether that namespace also has a CachePolicy. A namespace with
// CacheBackends but no CachePolicy is an INFO (not a WARN): the server applies
// its defaults, so this is informational — but operators tuning eviction/lookup
// behavior want to know they are relying on defaults.
func CachePolicyCoverage(ctx context.Context, c client.Client, ns string) []doctor.Finding {
	var backends cachev1alpha1.CacheBackendList
	if err := c.List(ctx, &backends, client.InNamespace(ns)); err != nil {
		return []doctor.Finding{listError(checkCachePolicyCoverage, "CacheBackend", err)}
	}
	backendNamespaces := map[string]struct{}{}
	for i := range backends.Items {
		backendNamespaces[backends.Items[i].Namespace] = struct{}{}
	}
	if len(backendNamespaces) == 0 {
		return nil
	}

	var policies cachev1alpha1.CachePolicyList
	if err := c.List(ctx, &policies, client.InNamespace(ns)); err != nil {
		return []doctor.Finding{listError(checkCachePolicyCoverage, "CachePolicy", err)}
	}
	policyNamespaces := map[string]int{}
	for i := range policies.Items {
		policyNamespaces[policies.Items[i].Namespace]++
	}

	// Deterministic output order regardless of List ordering.
	ordered := make([]string, 0, len(backendNamespaces))
	for n := range backendNamespaces {
		ordered = append(ordered, n)
	}
	sort.Strings(ordered)

	var findings []doctor.Finding
	for _, n := range ordered {
		switch count := policyNamespaces[n]; {
		case count == 0:
			findings = append(findings, doctor.Finding{
				Code:     doctor.CodePolicyCoverageMissing,
				Status:   doctor.StatusInfo,
				Check:    checkCachePolicyCoverage,
				Resource: "namespace/" + n,
				Message:  fmt.Sprintf("namespace %q has CacheBackends but no CachePolicy — the server's default eviction/lookup policy applies", n),
			})
		case count > 1:
			findings = append(findings, doctor.Finding{
				Code:     doctor.CodePolicyCoverageDuplicate,
				Status:   doctor.StatusWarn,
				Check:    checkCachePolicyCoverage,
				Resource: "namespace/" + n,
				Message:  fmt.Sprintf("namespace %q has %d CachePolicies — at most one is allowed; the controller keeps only the lexicographically-first and the rest are inert. Delete the extras to avoid confusion about which policy is in effect", n, count),
			})
		default:
			findings = append(findings, doctor.Finding{
				Code:     doctor.CodePolicyCoveragePresent,
				Status:   doctor.StatusOK,
				Check:    checkCachePolicyCoverage,
				Resource: "namespace/" + n,
				Message:  fmt.Sprintf("namespace %q has 1 CachePolicy", n),
			})
		}
	}
	return findings
}

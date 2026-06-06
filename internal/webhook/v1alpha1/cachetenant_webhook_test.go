package v1alpha1

import (
	"context"
	"errors"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

func tenant(name, ns, tenantID string) *cachev1alpha1.CacheTenant {
	return &cachev1alpha1.CacheTenant{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       cachev1alpha1.CacheTenantSpec{TenantID: tenantID},
	}
}

// --- Defaulter ---------------------------------------------------------------

func TestCacheTenantDefaulter_NoOp(t *testing.T) {
	d := &CacheTenantDefaulter{}
	ct := tenant("t1", "team-a", "team-vision")
	before := ct.DeepCopy()
	if err := d.Default(context.Background(), ct); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}
	if ct.Spec.IsolationMode != before.Spec.IsolationMode || ct.Spec.TenantID != before.Spec.TenantID {
		t.Errorf("Default mutated spec: before=%+v after=%+v", before.Spec, ct.Spec)
	}
}

// --- ValidateCreate: tenantID uniqueness -------------------------------------

func TestCacheTenantValidateCreate_TenantIDUniqueness(t *testing.T) {
	tests := []struct {
		name       string
		existing   []client.Object
		newTenant  *cachev1alpha1.CacheTenant
		wantReject bool
		wantInMsg  string
	}{
		{
			name:      "first tenant ok",
			existing:  nil,
			newTenant: tenant("t1", "team-a", "team-vision"),
		},
		{
			name:       "duplicate tenantID in same namespace rejected",
			existing:   []client.Object{tenant("existing-tenant", "team-a", "team-vision")},
			newTenant:  tenant("t2", "team-a", "team-vision"),
			wantReject: true,
			wantInMsg:  "is already claimed by CacheTenant \"existing-tenant\"",
		},
		{
			name:      "different tenantID in same namespace ok",
			existing:  []client.Object{tenant("existing-tenant", "team-a", "team-vision")},
			newTenant: tenant("t2", "team-a", "team-search"),
		},
		{
			name:      "same tenantID in different namespace ok",
			existing:  []client.Object{tenant("existing-tenant", "team-b", "team-vision")},
			newTenant: tenant("t1", "team-a", "team-vision"),
		},
		{
			name:      "re-admit same name does not collide with self",
			existing:  []client.Object{tenant("t1", "team-a", "team-vision")},
			newTenant: tenant("t1", "team-a", "team-vision"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v := &CacheTenantValidator{Reader: fakeReaderWith(t, tc.existing...)}
			_, err := v.ValidateCreate(context.Background(), tc.newTenant)
			if tc.wantReject {
				if err == nil {
					t.Fatalf("expected rejection, got nil")
				}
				if !apierrors.IsInvalid(err) {
					t.Errorf("expected Invalid status error, got %T: %v", err, err)
				}
				if tc.wantInMsg != "" && !strings.Contains(err.Error(), tc.wantInMsg) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.wantInMsg)
				}
				// The contested tenantID must appear so the operator can act.
				if !strings.Contains(err.Error(), tc.newTenant.Spec.TenantID) {
					t.Errorf("error %q does not name the contested tenantID %q", err.Error(), tc.newTenant.Spec.TenantID)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected admission, got error: %v", err)
			}
		})
	}
}

func TestCacheTenantValidateCreate_NilReaderFailsClosed(t *testing.T) {
	v := &CacheTenantValidator{}
	_, err := v.ValidateCreate(context.Background(), tenant("t1", "team-a", "team-vision"))
	if err == nil {
		t.Fatalf("expected fail-closed error with nil Reader, got nil")
	}
	if apierrors.IsInvalid(err) {
		t.Errorf("nil-Reader misconfiguration should be a plain error, not Invalid: %v", err)
	}
}

// TestCacheTenantValidateCreate_RejectsReservedProbeTenantID pins the
// reservation of the server's functional-self-test tenant id at the CRD
// admission layer. An operator-supplied CacheTenant with
// spec.tenantID == "inferencecache.io/probe" would otherwise (a) bypass
// quota enforcement at the PolicyStore layer (which exempts the probe
// tenant unconditionally) and (b) share the reserved scope with the
// probe's synthetic state. Reject at admission so the reservation lives at
// both layers (server policy + CRD admission), and the operator gets a
// clear "this id is reserved" diagnostic instead of a silent quota bypass.
func TestCacheTenantValidateCreate_RejectsReservedProbeTenantID(t *testing.T) {
	v := &CacheTenantValidator{Reader: fakeReaderWith(t)}
	_, err := v.ValidateCreate(context.Background(), tenant("conflict", "team-a", "inferencecache.io/probe"))
	if err == nil {
		t.Fatalf("expected rejection for reserved probe tenant id")
	}
	if !apierrors.IsInvalid(err) {
		t.Errorf("expected Invalid error for spec-level rejection, got: %v", err)
	}
	if !strings.Contains(err.Error(), "reserved") {
		t.Errorf("error message should explain the reservation: %v", err)
	}
}

func TestCacheTenantValidateCreate_NamesSmallestConflictDeterministically(t *testing.T) {
	// Two siblings already hold the tenantID (a pre-webhook state). The
	// rejection must name the lexicographically smallest by name, regardless of
	// list order.
	v := &CacheTenantValidator{Reader: fakeReaderWith(t,
		tenant("zeta", "team-a", "team-vision"),
		tenant("alpha", "team-a", "team-vision"),
	)}
	_, err := v.ValidateCreate(context.Background(), tenant("new", "team-a", "team-vision"))
	if err == nil {
		t.Fatalf("expected rejection")
	}
	if !strings.Contains(err.Error(), "\"alpha\"") {
		t.Errorf("error should name the smallest sibling 'alpha', got: %v", err)
	}
}

func TestCacheTenantValidateCreate_ListErrorFailsClosed(t *testing.T) {
	failing := fake.NewClientBuilder().
		WithScheme(newCacheScheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return errors.New("apiserver down")
			},
		}).Build()
	v := &CacheTenantValidator{Reader: failing}
	_, err := v.ValidateCreate(context.Background(), tenant("t1", "team-a", "team-vision"))
	if err == nil {
		t.Fatalf("expected the list error to surface (fail closed), got nil")
	}
	if apierrors.IsInvalid(err) {
		t.Errorf("a transient list error must not be reported as Invalid: %v", err)
	}
}

// --- ValidateUpdate ----------------------------------------------------------

func TestCacheTenantValidateUpdate(t *testing.T) {
	reader := fakeReaderWith(t,
		tenant("t1", "team-a", "team-vision"),
		tenant("t2", "team-a", "team-search"),
	)
	v := &CacheTenantValidator{Reader: reader}

	t.Run("unchanged tenantID is not re-checked even when a dup exists", func(t *testing.T) {
		// Simulate a pre-existing duplicate state: t-dup also holds
		// "team-vision". An unrelated edit (unchanged tenantID) must not trap
		// the CR.
		dupReader := fakeReaderWith(t,
			tenant("t1", "team-a", "team-vision"),
			tenant("t-dup", "team-a", "team-vision"),
		)
		vv := &CacheTenantValidator{Reader: dupReader}
		old := tenant("t1", "team-a", "team-vision")
		updated := tenant("t1", "team-a", "team-vision")
		updated.Labels = map[string]string{"team": "vision"}
		if _, err := vv.ValidateUpdate(context.Background(), old, updated); err != nil {
			t.Fatalf("unchanged-tenantID edit must be admitted, got %v", err)
		}
	})

	t.Run("changing tenantID onto a sibling's value is rejected", func(t *testing.T) {
		old := tenant("t1", "team-a", "team-vision")
		updated := tenant("t1", "team-a", "team-search") // collides with t2
		_, err := v.ValidateUpdate(context.Background(), old, updated)
		if err == nil || !apierrors.IsInvalid(err) {
			t.Fatalf("expected Invalid rejection, got %v", err)
		}
		if !strings.Contains(err.Error(), "team-search") {
			t.Errorf("error %q should name the contested tenantID", err.Error())
		}
	})

	t.Run("changing tenantID to the reserved probe id is rejected", func(t *testing.T) {
		// An update that flips an existing tenantID onto
		// "inferencecache.io/probe" is a newly-introduced spec-rule violation,
		// so filterIntroducedErrors surfaces it. Without this guard, the
		// admission rule applies to creates only — and the CR could drift
		// onto the reserved scope via an UPDATE that bypasses the create path.
		old := tenant("t1", "team-a", "team-vision")
		updated := tenant("t1", "team-a", "inferencecache.io/probe")
		_, err := v.ValidateUpdate(context.Background(), old, updated)
		if err == nil || !apierrors.IsInvalid(err) {
			t.Fatalf("expected Invalid rejection on UPDATE flipping tenantID to reserved probe id, got %v", err)
		}
		if !strings.Contains(err.Error(), "reserved") {
			t.Errorf("error %q should explain the reservation", err.Error())
		}
	})

	t.Run("unchanged reserved-tenantID predates the rule and is admitted", func(t *testing.T) {
		// Defensive: a CR that already holds the reserved id (pre-webhook
		// state) and edits some unrelated field must still admit — the
		// filterIntroducedErrors logic only blocks NEW violations on update.
		// Reflects the v1alpha1 tightening seam.
		old := tenant("legacy", "team-a", "inferencecache.io/probe")
		updated := tenant("legacy", "team-a", "inferencecache.io/probe")
		updated.Labels = map[string]string{"unrelated": "true"}
		if _, err := v.ValidateUpdate(context.Background(), old, updated); err != nil {
			t.Fatalf("an unchanged reserved-tenantID with unrelated edits must admit, got %v", err)
		}
	})

	t.Run("changing tenantID to a unique value is allowed", func(t *testing.T) {
		old := tenant("t1", "team-a", "team-vision")
		updated := tenant("t1", "team-a", "team-fresh")
		if _, err := v.ValidateUpdate(context.Background(), old, updated); err != nil {
			t.Fatalf("expected admission, got %v", err)
		}
	})
}

func TestCacheTenantValidateDelete_AlwaysAllows(t *testing.T) {
	v := &CacheTenantValidator{Reader: fakeReaderWith(t)}
	if _, err := v.ValidateDelete(context.Background(), tenant("t1", "team-a", "team-vision")); err != nil {
		t.Fatalf("delete must be allowed, got %v", err)
	}
}

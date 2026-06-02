package v1alpha1

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	cachev1alpha1 "github.com/cachebox-project/inference-cache/api/v1alpha1"
)

// newCacheScheme builds a scheme with the cache CRD types registered, for the
// fake clients the cross-CR uniqueness checks read through. Shared by the
// CachePolicy and CacheTenant webhook tests (same package).
func newCacheScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := cachev1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add cache scheme: %v", err)
	}
	return s
}

// fakeReaderWith returns a fake client.Reader seeded with objs.
func fakeReaderWith(t *testing.T, objs ...client.Object) client.Reader {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(newCacheScheme(t)).WithObjects(objs...).Build()
}

func durp(d time.Duration) *metav1.Duration { return &metav1.Duration{Duration: d} }

func policy(name, ns string) *cachev1alpha1.CachePolicy {
	return &cachev1alpha1.CachePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
	}
}

// --- Defaulter ---------------------------------------------------------------

func TestCachePolicyDefaulter_NoOp(t *testing.T) {
	d := &CachePolicyDefaulter{}
	cp := policy("p1", "team-a")
	before := cp.DeepCopy()
	if err := d.Default(context.Background(), cp); err != nil {
		t.Fatalf("Default returned error: %v", err)
	}
	// The defaulter must not mutate the object — kubebuilder markers own
	// every default. (Spec.Eviction stays "" here because the apiserver, not
	// the webhook, stamps the LRU default.)
	if cp.Spec != before.Spec {
		t.Errorf("Default mutated spec: before=%+v after=%+v", before.Spec, cp.Spec)
	}
}

// --- rejectNonPositiveEvictionTTL --------------------------------------------

func TestRejectNonPositiveEvictionTTL(t *testing.T) {
	tests := []struct {
		name    string
		ttl     *metav1.Duration
		wantErr bool
	}{
		{"nil ttl ok", nil, false},
		{"positive ok", durp(30 * time.Minute), false},
		{"one nanosecond ok", durp(1), false},
		{"zero rejected", durp(0), true},
		{"negative rejected", durp(-time.Second), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cp := policy("p1", "team-a")
			cp.Spec.EvictionTTL = tc.ttl
			errs := rejectNonPositiveEvictionTTL(cp)
			if tc.wantErr && len(errs) == 0 {
				t.Fatalf("expected an error, got none")
			}
			if !tc.wantErr && len(errs) != 0 {
				t.Fatalf("expected no error, got %v", errs)
			}
			if tc.wantErr && errs[0].Field != "spec.evictionTTL" {
				t.Errorf("field = %q, want spec.evictionTTL", errs[0].Field)
			}
		})
	}
}

// --- ValidateCreate: single-policy-per-namespace -----------------------------

func TestCachePolicyValidateCreate_SinglePerNamespace(t *testing.T) {
	tests := []struct {
		name       string
		existing   []client.Object
		newName    string
		newNS      string
		wantReject bool
		wantInMsg  string
	}{
		{
			name:       "first policy in namespace ok",
			existing:   nil,
			newName:    "p1",
			newNS:      "team-a",
			wantReject: false,
		},
		{
			name:       "second policy in same namespace rejected",
			existing:   []client.Object{policy("existing-pol", "team-a")},
			newName:    "p2",
			newNS:      "team-a",
			wantReject: true,
			wantInMsg:  "already has CachePolicy \"existing-pol\"",
		},
		{
			name:       "policy in different namespace ok",
			existing:   []client.Object{policy("existing-pol", "team-b")},
			newName:    "p1",
			newNS:      "team-a",
			wantReject: false,
		},
		{
			name:       "re-admit same name does not collide with self",
			existing:   []client.Object{policy("p1", "team-a")},
			newName:    "p1",
			newNS:      "team-a",
			wantReject: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v := &CachePolicyValidator{Reader: fakeReaderWith(t, tc.existing...)}
			_, err := v.ValidateCreate(context.Background(), policy(tc.newName, tc.newNS))
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
				return
			}
			if err != nil {
				t.Fatalf("expected admission, got error: %v", err)
			}
		})
	}
}

// TestCachePolicyValidateCreate_AggregatesSpecAndSibling proves a second
// policy that ALSO has a bad TTL surfaces both violations in one rejection.
func TestCachePolicyValidateCreate_AggregatesSpecAndSibling(t *testing.T) {
	v := &CachePolicyValidator{Reader: fakeReaderWith(t, policy("existing-pol", "team-a"))}
	cp := policy("p2", "team-a")
	cp.Spec.EvictionTTL = durp(-time.Second)
	_, err := v.ValidateCreate(context.Background(), cp)
	if err == nil {
		t.Fatalf("expected rejection")
	}
	statusErr := &apierrors.StatusError{}
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected StatusError, got %T", err)
	}
	causes := statusErr.ErrStatus.Details.Causes
	if len(causes) != 2 {
		t.Fatalf("expected 2 aggregated causes, got %d: %v", len(causes), causes)
	}
}

func TestCachePolicyValidateCreate_NilReaderFailsClosed(t *testing.T) {
	// A miswired validator (nil Reader) cannot enforce the one-per-namespace
	// invariant, so it must fail closed with a plain error rather than silently
	// admit — never a false-clean admission.
	v := &CachePolicyValidator{}
	_, err := v.ValidateCreate(context.Background(), policy("p1", "team-a"))
	if err == nil {
		t.Fatalf("expected fail-closed error with nil Reader, got nil")
	}
	if apierrors.IsInvalid(err) {
		t.Errorf("nil-Reader misconfiguration should be a plain error, not an Invalid admission verdict: %v", err)
	}
}

func TestCachePolicyValidateCreate_NamesSmallestConflictDeterministically(t *testing.T) {
	// Two policies already coexist (a pre-webhook state). The rejection must
	// name the lexicographically smallest by name — the one resolvePolicies
	// treats as effective — regardless of list order.
	v := &CachePolicyValidator{Reader: fakeReaderWith(t, policy("zeta", "team-a"), policy("alpha", "team-a"))}
	_, err := v.ValidateCreate(context.Background(), policy("new", "team-a"))
	if err == nil {
		t.Fatalf("expected rejection")
	}
	if !strings.Contains(err.Error(), "\"alpha\"") {
		t.Errorf("error should name the smallest existing policy 'alpha', got: %v", err)
	}
}

func TestCachePolicyValidateCreate_ListErrorFailsClosed(t *testing.T) {
	failing := fake.NewClientBuilder().
		WithScheme(newCacheScheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return errors.New("apiserver down")
			},
		}).Build()
	v := &CachePolicyValidator{Reader: failing}
	_, err := v.ValidateCreate(context.Background(), policy("p1", "team-a"))
	if err == nil {
		t.Fatalf("expected the list error to surface (fail closed), got nil")
	}
	if apierrors.IsInvalid(err) {
		t.Errorf("a transient list error must not be reported as an Invalid admission verdict: %v", err)
	}
}

// --- ValidateUpdate ----------------------------------------------------------

func TestCachePolicyValidateUpdate(t *testing.T) {
	// Two policies coexist in the namespace (a state created before the
	// webhook existed). Updating one must still succeed — the single-per-ns
	// rule is create-only.
	reader := fakeReaderWith(t, policy("p1", "team-a"), policy("p2", "team-a"))
	v := &CachePolicyValidator{Reader: reader}

	t.Run("update on coexisting policies is allowed (no uniqueness on update)", func(t *testing.T) {
		old := policy("p1", "team-a")
		updated := policy("p1", "team-a")
		updated.Spec.MinimumPrefixTokens = i32p(8)
		if _, err := v.ValidateUpdate(context.Background(), old, updated); err != nil {
			t.Fatalf("expected update admitted, got %v", err)
		}
	})

	t.Run("introducing a negative TTL is rejected", func(t *testing.T) {
		old := policy("p1", "team-a")
		old.Spec.EvictionTTL = durp(time.Minute)
		updated := policy("p1", "team-a")
		updated.Spec.EvictionTTL = durp(-time.Minute)
		_, err := v.ValidateUpdate(context.Background(), old, updated)
		if err == nil || !apierrors.IsInvalid(err) {
			t.Fatalf("expected Invalid rejection, got %v", err)
		}
	})

	t.Run("a pre-existing negative TTL left unchanged is not re-rejected", func(t *testing.T) {
		old := policy("p1", "team-a")
		old.Spec.EvictionTTL = durp(-time.Minute)
		updated := policy("p1", "team-a")
		updated.Spec.EvictionTTL = durp(-time.Minute)
		updated.Labels = map[string]string{"team": "vision"}
		if _, err := v.ValidateUpdate(context.Background(), old, updated); err != nil {
			t.Fatalf("expected unrelated edit admitted, got %v", err)
		}
	})
}

func TestCachePolicyValidateDelete_AlwaysAllows(t *testing.T) {
	v := &CachePolicyValidator{Reader: fakeReaderWith(t)}
	if _, err := v.ValidateDelete(context.Background(), policy("p1", "team-a")); err != nil {
		t.Fatalf("delete must be allowed, got %v", err)
	}
}

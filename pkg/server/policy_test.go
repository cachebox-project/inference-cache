package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPolicyStoreLookupReturnsDefaultsWhenUnset(t *testing.T) {
	s := NewPolicyStore()
	if _, ok := s.Lookup("missing"); ok {
		t.Fatalf("empty store should report no policy")
	}
	if d := s.TTL("missing"); d != 0 {
		t.Fatalf("TTL for missing tenant = %v, want 0 (fall back to global)", d)
	}
	if v := s.MinimumPrefixTokens("missing"); v != 0 {
		t.Fatalf("MinimumPrefixTokens for missing tenant = %d, want 0", v)
	}
	if d := s.LookupTimeout("missing"); d != 0 {
		t.Fatalf("LookupTimeout for missing tenant = %v, want 0", d)
	}
}

func TestPolicyStoreReplaceIsAtomicAndDropsStale(t *testing.T) {
	s := NewPolicyStore()
	s.Replace([]ResolvedPolicy{
		{Namespace: "team-a", EvictionTTL: 15 * time.Minute, MinimumPrefixTokens: 32, LookupTimeoutMs: 25},
		{Namespace: "team-b", EvictionTTL: time.Hour},
	})

	if d := s.TTL("team-a"); d != 15*time.Minute {
		t.Fatalf("team-a TTL = %v", d)
	}
	if v := s.MinimumPrefixTokens("team-a"); v != 32 {
		t.Fatalf("team-a min tokens = %d", v)
	}
	if d := s.LookupTimeout("team-a"); d != 25*time.Millisecond {
		t.Fatalf("team-a lookup timeout = %v", d)
	}

	// Replace with a snapshot that omits team-b — that namespace must revert.
	s.Replace([]ResolvedPolicy{
		{Namespace: "team-a", EvictionTTL: 5 * time.Minute},
	})
	if _, ok := s.Lookup("team-b"); ok {
		t.Fatalf("team-b should have been removed by the new snapshot")
	}
	if d := s.TTL("team-a"); d != 5*time.Minute {
		t.Fatalf("team-a TTL after replace = %v, want 5m", d)
	}
}

func TestPolicyStoreReplaceDropsEmptyNamespace(t *testing.T) {
	s := NewPolicyStore()
	s.Replace([]ResolvedPolicy{
		{Namespace: "", EvictionTTL: time.Hour}, // bogus — must be dropped
		{Namespace: "ok", EvictionTTL: time.Minute},
	})
	if _, ok := s.Lookup(""); ok {
		t.Fatalf("empty-namespace policy should not be addressable")
	}
	if d := s.TTL("ok"); d != time.Minute {
		t.Fatalf("valid policy TTL = %v", d)
	}
}

// TestPolicyStoreConcurrentReadsWithWriter hammers Lookup/TTL/Snapshot while
// a writer replaces the snapshot. The race detector + the assertion that
// reads never observe a partial state catches missing locks.
func TestPolicyStoreConcurrentReadsWithWriter(t *testing.T) {
	s := NewPolicyStore()
	s.Replace([]ResolvedPolicy{{Namespace: "t", EvictionTTL: time.Hour}})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var bad atomic.Int64

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					p, ok := s.Lookup("t")
					// Every snapshot we write below sets Namespace=="t" and
					// EvictionTTL > 0; a torn read would violate that
					// invariant.
					if ok && (p.Namespace != "t" || p.EvictionTTL <= 0) {
						bad.Add(1)
					}
					_ = s.Snapshot()
				}
			}
		}()
	}

	for i := 0; i < 500; i++ {
		s.Replace([]ResolvedPolicy{
			{Namespace: "t", EvictionTTL: time.Duration(i+1) * time.Minute},
		})
	}
	close(stop)
	wg.Wait()
	if n := bad.Load(); n > 0 {
		t.Fatalf("observed %d torn reads from PolicyStore.Lookup", n)
	}
}

func TestPolicyHandlerReplacesSnapshot(t *testing.T) {
	s := NewPolicyStore()
	srv := httptest.NewServer(policyHandler(s))
	defer srv.Close()

	body, _ := json.Marshal(PolicySnapshot{
		Version: PolicyPropagationVersion,
		Policies: []ResolvedPolicy{
			{Namespace: "team-a", EvictionTTL: 7 * time.Minute, MinimumPrefixTokens: 16},
		},
	})
	resp, err := http.Post(srv.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	_ = resp.Body.Close()

	p, ok := s.Lookup("team-a")
	if !ok || p.EvictionTTL != 7*time.Minute || p.MinimumPrefixTokens != 16 {
		t.Fatalf("policy not applied: ok=%v p=%+v", ok, p)
	}
}

func TestPolicyHandlerRejectsBadVersion(t *testing.T) {
	srv := httptest.NewServer(policyHandler(NewPolicyStore()))
	defer srv.Close()
	body, _ := json.Marshal(PolicySnapshot{Version: 99, Policies: []ResolvedPolicy{}})
	resp, err := http.Post(srv.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 on bad version", resp.StatusCode)
	}
}

func TestPolicyHandlerRejectsNonJSON(t *testing.T) {
	srv := httptest.NewServer(policyHandler(NewPolicyStore()))
	defer srv.Close()
	resp, err := http.Post(srv.URL, "text/plain", strings.NewReader("not json"))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 on garbage body", resp.StatusCode)
	}
}

func TestPolicyHandlerRejectsUnknownMethod(t *testing.T) {
	srv := httptest.NewServer(policyHandler(NewPolicyStore()))
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", resp.StatusCode)
	}
}

func TestPolicyHandlerCapsBodySize(t *testing.T) {
	srv := httptest.NewServer(policyHandler(NewPolicyStore()))
	defer srv.Close()
	// Build a snapshot that comfortably exceeds the 1 MiB cap.
	policies := make([]ResolvedPolicy, 0, 20000)
	for i := 0; i < 20000; i++ {
		policies = append(policies, ResolvedPolicy{
			Namespace: fmt.Sprintf("ns-%d-padded-with-bytes-to-exceed-cap", i),
		})
	}
	body, _ := json.Marshal(PolicySnapshot{Version: PolicyPropagationVersion, Policies: policies})
	resp, err := http.Post(srv.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNoContent {
		t.Fatalf("oversize body should not return 204")
	}
}

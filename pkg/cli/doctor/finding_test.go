package doctor

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestStatusString(t *testing.T) {
	cases := map[Status]string{
		StatusOK:   "OK",
		StatusInfo: "INFO",
		StatusWarn: "WARN",
		StatusFail: "FAIL",
		Status(99): "UNKNOWN",
		Status(-1): "UNKNOWN",
	}
	for s, want := range cases {
		if got := s.String(); got != want {
			t.Errorf("Status(%d).String() = %q, want %q", int(s), got, want)
		}
	}
}

func TestStatusMarshalJSON(t *testing.T) {
	b, err := json.Marshal(StatusWarn)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b) != `"WARN"` {
		t.Errorf("got %s, want \"WARN\"", b)
	}

	// Round-trip through a Finding to confirm the enum serializes as a string.
	f := Finding{Code: "CB002", Status: StatusWarn, Check: "x", Message: "m"}
	b, err = json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal finding: %v", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if raw["status"] != "WARN" {
		t.Errorf("status = %v, want WARN", raw["status"])
	}
	if _, ok := raw["resource"]; ok {
		t.Errorf("empty resource should be omitted, got %v", raw["resource"])
	}
}

func sampleReport() *Report {
	r := &Report{}
	r.Add(Finding{Code: "SV003", Status: StatusOK, Check: "ServerReachability", Message: "serving"})
	r.Addf("CB002", StatusWarn, "CacheBackendHealth", "cachebackend/ns/a", "matched %d pods", 0)
	r.Add(Finding{Code: "CP001", Status: StatusInfo, Check: "CachePolicyCoverage", Resource: "namespace/ns", Message: "defaults"})
	return r
}

func TestReportWorstAndExitCode(t *testing.T) {
	cases := []struct {
		name     string
		statuses []Status
		worst    Status
		exit     int
	}{
		{"empty", nil, StatusOK, 0},
		{"ok only", []Status{StatusOK, StatusOK}, StatusOK, 0},
		{"info caps at ok-exit", []Status{StatusOK, StatusInfo}, StatusInfo, 0},
		{"warn", []Status{StatusOK, StatusWarn, StatusInfo}, StatusWarn, 1},
		{"fail dominates", []Status{StatusWarn, StatusFail, StatusOK}, StatusFail, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &Report{}
			for _, s := range tc.statuses {
				r.Add(Finding{Status: s})
			}
			if got := r.Worst(); got != tc.worst {
				t.Errorf("Worst() = %v, want %v", got, tc.worst)
			}
			if got := r.ExitCode(); got != tc.exit {
				t.Errorf("ExitCode() = %d, want %d", got, tc.exit)
			}
		})
	}
}

func TestReportCounts(t *testing.T) {
	r := sampleReport()
	got := r.Counts()
	want := map[string]int{"OK": 1, "INFO": 1, "WARN": 1, "FAIL": 0}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Counts() = %v, want %v", got, want)
	}
}

func TestReportFindingsByStatus(t *testing.T) {
	r := sampleReport()
	warns := r.FindingsByStatus(StatusWarn)
	if len(warns) != 1 || warns[0].Code != "CB002" {
		t.Errorf("FindingsByStatus(WARN) = %+v", warns)
	}
	if got := r.FindingsByStatus(StatusFail); got != nil {
		t.Errorf("FindingsByStatus(FAIL) = %+v, want nil", got)
	}
}

func TestOrderedStatuses(t *testing.T) {
	got := OrderedStatuses()
	want := []Status{StatusFail, StatusWarn, StatusInfo, StatusOK}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OrderedStatuses() = %v, want %v", got, want)
	}
	// Mutating the returned slice must not affect the package-level order.
	got[0] = StatusOK
	if OrderedStatuses()[0] != StatusFail {
		t.Errorf("OrderedStatuses() returned a slice aliasing internal state")
	}
}

func TestSortedCodes(t *testing.T) {
	r := sampleReport()
	r.Add(Finding{Code: "CB002", Status: StatusWarn}) // duplicate code
	got := r.SortedCodes()
	want := []string{"CB002", "CP001", "SV003"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("SortedCodes() = %v, want %v", got, want)
	}
}

func TestAddf(t *testing.T) {
	r := &Report{}
	r.Addf("CT001", StatusWarn, "CacheTenantHealth", "cachetenant/ns/t", "%d over %d", 12, 10)
	if len(r.Findings) != 1 {
		t.Fatalf("want 1 finding, got %d", len(r.Findings))
	}
	f := r.Findings[0]
	if f.Message != "12 over 10" || f.Code != "CT001" || f.Resource != "cachetenant/ns/t" {
		t.Errorf("Addf produced %+v", f)
	}
}

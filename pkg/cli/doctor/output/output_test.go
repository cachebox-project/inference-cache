package output

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cachebox-project/inference-cache/pkg/cli/doctor"
)

var update = flag.Bool("update", false, "regenerate golden files")

// sampleReport covers every status, a finding with no resource (cluster-wide),
// and findings the human formatter groups across sections.
func sampleReport() *doctor.Report {
	r := &doctor.Report{}
	r.Add(doctor.Finding{Code: "SV001", Status: doctor.StatusFail, Check: "ServerReachability", Resource: "host:9090", Message: "gRPC health check failed: connection refused"})
	r.Add(doctor.Finding{Code: "CB002", Status: doctor.StatusWarn, Check: "CacheBackendHealth", Resource: "cachebackend/ns1/bad", Message: "status.matchedEnginePods is 0 (LikelySelectorMismatch)"})
	r.Add(doctor.Finding{Code: "CP001", Status: doctor.StatusInfo, Check: "CachePolicyCoverage", Resource: "namespace/ns1", Message: "no CachePolicy — server defaults apply"})
	r.Add(doctor.Finding{Code: "SV003", Status: doctor.StatusOK, Check: "ServerReachability", Resource: "host:9090", Message: "gRPC health check reports SERVING"})
	r.Add(doctor.Finding{Code: "EP000", Status: doctor.StatusOK, Check: "Summary", Message: "cluster-wide note with no resource"})
	return r
}

func goldenPath(name string) string { return filepath.Join("testdata", name) }

func assertGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := goldenPath(name)
	if *update {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("write golden %s: %v", path, err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v (run `go test ./pkg/cli/doctor/output -update`)", path, err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("output mismatch for %s\n--- got ---\n%s\n--- want ---\n%s", name, got, want)
	}
}

func render(t *testing.T, r *doctor.Report, format Format, color bool) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := Render(&buf, r, format, color); err != nil {
		t.Fatalf("render %s: %v", format, err)
	}
	return buf.Bytes()
}

func TestRenderGolden(t *testing.T) {
	r := sampleReport()
	assertGolden(t, "human.txt", render(t, r, FormatHuman, false))
	assertGolden(t, "human_color.txt", render(t, r, FormatHuman, true))
	assertGolden(t, "report.json", render(t, r, FormatJSON, false))
	assertGolden(t, "report.table", render(t, r, FormatTable, false))
}

func TestRenderEmptyReport(t *testing.T) {
	r := &doctor.Report{}
	human := render(t, r, FormatHuman, false)
	if !strings.Contains(string(human), "0 OK, 0 INFO, 0 WARN, 0 FAIL") {
		t.Errorf("empty human summary wrong:\n%s", human)
	}
	js := render(t, r, FormatJSON, false)
	if !strings.Contains(string(js), `"findings": []`) {
		t.Errorf("empty json findings should be [] not null:\n%s", js)
	}
	table := render(t, r, FormatTable, false)
	if !strings.HasPrefix(string(table), "STATUS") {
		t.Errorf("empty table should still print header:\n%s", table)
	}
}

func TestParseFormat(t *testing.T) {
	for _, in := range []string{"human", "json", "table"} {
		if _, err := ParseFormat(in); err != nil {
			t.Errorf("ParseFormat(%q) error: %v", in, err)
		}
	}
	if _, err := ParseFormat("yaml"); err == nil {
		t.Errorf("ParseFormat(yaml) should error")
	}
}

func TestRenderUnknownFormat(t *testing.T) {
	if err := Render(&bytes.Buffer{}, &doctor.Report{}, Format("bogus"), false); err == nil {
		t.Errorf("Render with unknown format should error")
	}
}

// failWriter fails after allowing n successful writes, to exercise the
// formatters' write-error handling.
type failWriter struct{ remaining int }

func (w *failWriter) Write(p []byte) (int, error) {
	if w.remaining <= 0 {
		return 0, os.ErrClosed
	}
	w.remaining--
	return len(p), nil
}

func TestRenderWriteErrors(t *testing.T) {
	r := sampleReport()
	for _, format := range []Format{FormatHuman, FormatJSON, FormatTable} {
		if err := Render(&failWriter{remaining: 0}, r, format, false); err == nil {
			t.Errorf("%s: expected write error", format)
		}
	}
	// Human formatter: fail partway through so the errWriter short-circuit path
	// (printf after err is set) is exercised.
	if err := Render(&failWriter{remaining: 2}, r, FormatHuman, true); err == nil {
		t.Errorf("human: expected mid-stream write error")
	}
}

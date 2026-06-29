package output

import (
	"fmt"
	"io"

	"github.com/cachebox-project/inference-cache/pkg/cli/doctor"
)

// ANSI SGR codes for the human format. Kept minimal: a color per severity plus
// bold for section headers and reset. Only emitted when color is true.
const (
	ansiReset  = "\x1b[0m"
	ansiBold   = "\x1b[1m"
	ansiRed    = "\x1b[31m"
	ansiYellow = "\x1b[33m"
	ansiGreen  = "\x1b[32m"
	ansiCyan   = "\x1b[36m"
)

func statusColor(s doctor.Status) string {
	switch s {
	case doctor.StatusFail:
		return ansiRed
	case doctor.StatusWarn:
		return ansiYellow
	case doctor.StatusInfo:
		return ansiCyan
	default:
		return ansiGreen
	}
}

// renderHuman writes the report as severity-ordered sections (FAIL, then WARN,
// INFO, OK), each finding on one line prefixed by its status and stable code,
// followed by a one-line summary. Empty sections are skipped so a clean run is
// short.
func renderHuman(w io.Writer, r *doctor.Report, color bool) error {
	paint := func(code, s string) string {
		if !color {
			return s
		}
		return code + s + ansiReset
	}

	bw := &errWriter{w: w}

	header := "inferencecache doctor"
	if color {
		header = ansiBold + header + ansiReset
	}
	bw.printf("%s\n\n", header)

	for _, status := range doctor.OrderedStatuses() {
		findings := r.FindingsByStatus(status)
		if len(findings) == 0 {
			continue
		}
		label := status.String()
		if color {
			label = ansiBold + statusColor(status) + label + ansiReset
		}
		bw.printf("%s\n", label)
		for _, f := range findings {
			tag := paint(statusColor(f.Status), fmt.Sprintf("%-4s %s", f.Status.String(), f.Code))
			if f.Resource != "" {
				bw.printf("  %s  %s\n      %s\n", tag, f.Resource, f.Message)
			} else {
				bw.printf("  %s\n      %s\n", tag, f.Message)
			}
		}
		bw.printf("\n")
	}

	counts := r.Counts()
	summary := fmt.Sprintf("Summary: %d OK, %d INFO, %d WARN, %d FAIL  (exit %d)",
		counts["OK"], counts["INFO"], counts["WARN"], counts["FAIL"], r.ExitCode())
	if color {
		summary = ansiBold + summary + ansiReset
	}
	bw.printf("%s\n", summary)

	return bw.err
}

// errWriter collapses repeated write-error checks: once a write fails, later
// printf calls are no-ops and the first error is returned by the caller.
type errWriter struct {
	w   io.Writer
	err error
}

func (e *errWriter) printf(format string, args ...any) {
	if e.err != nil {
		return
	}
	_, e.err = fmt.Fprintf(e.w, format, args...)
}

// Package output renders a doctor [doctor.Report] in the three operator-facing
// formats the `inferencecache doctor` command supports: human (default,
// color-coded when writing to a TTY), json (a documented, stable schema for
// scripting and CI pipelines), and table (one row per finding).
//
// All three formatters are pure functions of a [doctor.Report] and an
// io.Writer, with color passed in as an explicit boolean rather than sniffed
// from the writer — so the cmd layer owns TTY detection (via golang.org/x/term)
// and the formatters stay deterministic and golden-file-testable.
package output

import (
	"fmt"
	"io"

	"github.com/cachebox-project/inference-cache/pkg/cli/doctor"
)

// Format identifies an output rendering.
type Format string

const (
	// FormatHuman is the default human-readable, sectioned output.
	FormatHuman Format = "human"
	// FormatJSON is the structured JSON envelope (see [jsonReport]).
	FormatJSON Format = "json"
	// FormatTable is the one-row-per-finding tabular output.
	FormatTable Format = "table"
)

// ParseFormat validates and normalizes an --output flag value.
func ParseFormat(s string) (Format, error) {
	switch Format(s) {
	case FormatHuman:
		return FormatHuman, nil
	case FormatJSON:
		return FormatJSON, nil
	case FormatTable:
		return FormatTable, nil
	default:
		return "", fmt.Errorf("invalid output format %q (want one of: human, json, table)", s)
	}
}

// Render writes the report to w in the requested format. color is honored only
// by the human format; json and table are always plain so their output is safe
// to pipe.
func Render(w io.Writer, r *doctor.Report, format Format, color bool) error {
	switch format {
	case FormatHuman:
		return renderHuman(w, r, color)
	case FormatJSON:
		return renderJSON(w, r)
	case FormatTable:
		return renderTable(w, r)
	default:
		return fmt.Errorf("unknown output format %q", format)
	}
}

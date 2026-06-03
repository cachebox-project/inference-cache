package output

import (
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/cachebox-project/inference-cache/pkg/cli/doctor"
)

// renderTable writes one tab-aligned row per finding under a STATUS/CODE/CHECK/
// RESOURCE/MESSAGE header, in emission order. Plain text only (no color) so it
// is safe to pipe into awk/cut. An empty report still prints the header so the
// columns are discoverable.
func renderTable(w io.Writer, r *doctor.Report) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "STATUS\tCODE\tCHECK\tRESOURCE\tMESSAGE"); err != nil {
		return err
	}
	for _, f := range r.Findings {
		resource := f.Resource
		if resource == "" {
			resource = "-"
		}
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n",
			f.Status.String(), f.Code, f.Check, resource, f.Message); err != nil {
			return err
		}
	}
	return tw.Flush()
}

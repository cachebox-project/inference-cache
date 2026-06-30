package output

import (
	"encoding/json"
	"io"

	"github.com/cachebox-project/inference-cache/pkg/cli/doctor"
)

// jsonReport is the stable schema emitted by `--output=json`. It is a documented
// contract — field names and shapes MUST stay backward-compatible for any
// pipeline that parses doctor output:
//
//	{
//	  "summary": {
//	    "ok":   <int>,   // count of OK findings
//	    "info": <int>,   // count of INFO findings
//	    "warn": <int>,   // count of WARN findings
//	    "fail": <int>,   // count of FAIL findings
//	    "exitCode": <int> // 0 all-clear, 1 any WARN, 2 any FAIL
//	  },
//	  "findings": [
//	    {
//	      "code":     "<stable code, e.g. CB002>",
//	      "status":   "OK" | "INFO" | "WARN" | "FAIL",
//	      "check":    "<check group name>",
//	      "resource": "<kind/namespace/name or host:port; omitted if empty>",
//	      "message":  "<human-readable detail>"
//	    }
//	  ]
//	}
//
// findings is always present (an empty run emits []), never null.
type jsonReport struct {
	Summary  jsonSummary      `json:"summary"`
	Findings []doctor.Finding `json:"findings"`
}

type jsonSummary struct {
	OK       int `json:"ok"`
	Info     int `json:"info"`
	Warn     int `json:"warn"`
	Fail     int `json:"fail"`
	ExitCode int `json:"exitCode"`
}

func renderJSON(w io.Writer, r *doctor.Report) error {
	counts := r.Counts()
	findings := r.Findings
	if findings == nil {
		findings = []doctor.Finding{}
	}
	out := jsonReport{
		Summary: jsonSummary{
			OK:       counts["OK"],
			Info:     counts["INFO"],
			Warn:     counts["WARN"],
			Fail:     counts["FAIL"],
			ExitCode: r.ExitCode(),
		},
		Findings: findings,
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(out)
}

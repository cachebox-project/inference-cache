// Command inferencecache is the operator-facing CLI for the inference-cache
// control plane. Today it ships a single subcommand, `doctor`, an early
// carve-out of the full CLI plugin: a read-only pre-flight diagnostic that
// answers "is my deployment configured correctly?" in one command.
//
// The binary deliberately keeps its glue thin — flag parsing, Kubernetes/gRPC
// client construction, and server-endpoint discovery live here, while the
// diagnostic logic and output formatting live in the unit-tested
// github.com/cachebox-project/inference-cache/pkg/cli/doctor packages.
package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/cachebox-project/inference-cache/pkg/version"
)

func main() {
	os.Exit(run())
}

// run wires up the root command and returns the process exit code. The doctor
// subcommand writes its CI-gating exit code (0/1/2) through the shared code
// pointer; any other error (bad flags, client construction) maps to 1.
func run() int {
	var code int
	root := newRootCommand(&code)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		if code == 0 {
			code = 1
		}
	}
	return code
}

func newRootCommand(code *int) *cobra.Command {
	root := &cobra.Command{
		Use:           "inferencecache",
		Short:         "Operator CLI for the inference-cache control plane",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       fmt.Sprintf("%s (commit %s)", version.GitVersion, version.GitCommit),
	}
	root.AddCommand(newDoctorCommand(code))
	return root
}

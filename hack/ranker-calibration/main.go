// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/cachebox-project/inference-cache/internal/index/calibration"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("ranker-calibration", flag.ContinueOnError)
	flags.SetOutput(stderr)
	tracePath := flags.String("trace", "", "path to a ranker calibration trace")
	outPath := flags.String("out", "", "path to write the calibration result")
	check := flags.Bool("check", false, "verify that -out already matches the generated result")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *tracePath == "" || *outPath == "" {
		return failf(stderr, "both -trace and -out are required")
	}

	traceFile, err := os.Open(*tracePath)
	if err != nil {
		return failf(stderr, "open trace: %v", err)
	}
	trace, err := calibration.Load(traceFile)
	closeErr := traceFile.Close()
	if err != nil {
		return failf(stderr, "load trace: %v", err)
	}
	if closeErr != nil {
		return failf(stderr, "close trace: %v", closeErr)
	}

	data, err := calibration.MarshalResult(calibration.Calibrate(trace))
	if err != nil {
		return failf(stderr, "render result: %v", err)
	}
	if *check {
		current, err := os.ReadFile(*outPath)
		if err != nil {
			return failf(stderr, "read result for check: %v", err)
		}
		if !bytes.Equal(current, data) {
			return failf(stderr, "%s is stale; rerun ranker calibration", *outPath)
		}
		fmt.Fprintf(stdout, "ranker calibration is current: %s\n", *outPath)
		return 0
	}
	if err := writeAtomic(*outPath, data); err != nil {
		return failf(stderr, "write result: %v", err)
	}
	fmt.Fprintf(stdout, "wrote ranker calibration: %s\n", *outPath)
	return 0
}

func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	temp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary result: %w", err)
	}
	tempPath := temp.Name()
	defer func() { _ = os.Remove(tempPath) }()

	if err := temp.Chmod(0o644); err != nil {
		_ = temp.Close()
		return fmt.Errorf("set temporary result mode: %w", err)
	}
	if _, err := temp.Write(data); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write temporary result: %w", err)
	}
	if err := temp.Sync(); err != nil {
		_ = temp.Close()
		return fmt.Errorf("sync temporary result: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("close temporary result: %w", err)
	}
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("replace result: %w", err)
	}
	return nil
}

func failf(stderr io.Writer, format string, args ...any) int {
	fmt.Fprintf(stderr, "ranker-calibration: "+format+"\n", args...)
	return 1
}

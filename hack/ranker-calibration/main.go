// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"

	"github.com/cachebox-project/inference-cache/internal/index/calibration"
)

func main() {
	tracePath := flag.String("trace", "", "path to a ranker calibration trace")
	outPath := flag.String("out", "", "path to write the calibration result")
	check := flag.Bool("check", false, "verify that -out already matches the generated result")
	flag.Parse()
	if *tracePath == "" || *outPath == "" {
		fatalf("both -trace and -out are required")
	}

	traceFile, err := os.Open(*tracePath)
	if err != nil {
		fatalf("open trace: %v", err)
	}
	trace, err := calibration.Load(traceFile)
	closeErr := traceFile.Close()
	if err != nil {
		fatalf("load trace: %v", err)
	}
	if closeErr != nil {
		fatalf("close trace: %v", closeErr)
	}

	data, err := calibration.MarshalResult(calibration.Calibrate(trace))
	if err != nil {
		fatalf("render result: %v", err)
	}
	if *check {
		current, err := os.ReadFile(*outPath)
		if err != nil {
			fatalf("read result for check: %v", err)
		}
		if !bytes.Equal(current, data) {
			fatalf("%s is stale; rerun ranker calibration", *outPath)
		}
		fmt.Printf("ranker calibration is current: %s\n", *outPath)
		return
	}
	if err := os.WriteFile(*outPath, data, 0o644); err != nil {
		fatalf("write result: %v", err)
	}
	fmt.Printf("wrote ranker calibration: %s\n", *outPath)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "ranker-calibration: "+format+"\n", args...)
	os.Exit(1)
}

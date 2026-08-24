// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func emptyDirectory(root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(root, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: node-local-shm-cleanup DIRECTORY")
		os.Exit(2)
	}
	if err := emptyDirectory(os.Args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "empty %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
}

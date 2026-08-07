// SPDX-FileCopyrightText: 2026 The inference-cache Authors
//
// SPDX-License-Identifier: Apache-2.0

package boundarytest

import (
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const modulePath = "github.com/cachebox-project/inference-cache"

func TestPublicPackagesHaveDocumentation(t *testing.T) {
	t.Parallel()

	root := repositoryRoot(t)
	packageDirs := map[string]bool{}
	err := filepath.WalkDir(filepath.Join(root, "pkg"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.PackageClauseOnly|parser.ParseComments)
		if err != nil {
			return err
		}
		dir := filepath.Dir(path)
		if _, seen := packageDirs[dir]; !seen {
			packageDirs[dir] = false
		}
		if file.Doc != nil && strings.TrimSpace(file.Doc.Text()) != "" {
			packageDirs[dir] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan public packages: %v", err)
	}

	var undocumented []string
	for dir, documented := range packageDirs {
		if documented {
			continue
		}
		rel, err := filepath.Rel(root, dir)
		if err != nil {
			t.Fatalf("relative package path: %v", err)
		}
		undocumented = append(undocumented, rel)
	}
	sort.Strings(undocumented)
	if len(undocumented) > 0 {
		t.Fatalf("public packages without package documentation: %s", strings.Join(undocumented, ", "))
	}
}

func TestPublicProductionCodeDoesNotImportInternalPackages(t *testing.T) {
	t.Parallel()

	root := repositoryRoot(t)
	for _, topLevel := range []string{"api", "gen", "pkg"} {
		topLevel := topLevel
		t.Run(topLevel, func(t *testing.T) {
			t.Parallel()
			err := filepath.WalkDir(filepath.Join(root, topLevel), func(path string, entry fs.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
					return nil
				}
				file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
				if err != nil {
					return err
				}
				for _, imported := range file.Imports {
					importPath, err := strconv.Unquote(imported.Path.Value)
					if err != nil {
						return err
					}
					if strings.HasPrefix(importPath, modulePath+"/internal/") {
						rel, relErr := filepath.Rel(root, path)
						if relErr != nil {
							return relErr
						}
						t.Errorf("%s imports private package %q", rel, importPath)
					}
				}
				return nil
			})
			if err != nil {
				t.Fatalf("scan %s production code: %v", topLevel, err)
			}
		})
	}
}

func TestGeneratedProtobufCodeLivesUnderGen(t *testing.T) {
	t.Parallel()

	root := repositoryRoot(t)
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "bin":
				return filepath.SkipDir
			default:
				return nil
			}
		}
		if !strings.HasSuffix(entry.Name(), ".pb.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(filepath.ToSlash(rel), "gen/") {
			t.Errorf("generated protobuf Go file must live under gen/: %s", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan generated protobuf files: %v", err)
	}
}

func TestProtobufGoPackageIsPinnedToGen(t *testing.T) {
	t.Parallel()

	root := repositoryRoot(t)
	protoPath := filepath.Join(root, "proto", "inferencecache", "v1alpha1", "inferencecache.proto")
	contents, err := os.ReadFile(protoPath)
	if err != nil {
		t.Fatalf("read protobuf contract: %v", err)
	}
	want := `option go_package = "` + modulePath + `/gen/inferencecache/v1alpha1;inferencecachev1alpha1pb";`
	var got string
	for _, line := range strings.Split(string(contents), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "option go_package") {
			continue
		}
		if got != "" {
			t.Fatalf("protobuf contract contains multiple go_package options: %q and %q", got, line)
		}
		got = line
	}
	if got != want {
		t.Fatalf("protobuf go_package must remain pinned to the public gen path: got %q, want %q", got, want)
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate repository root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

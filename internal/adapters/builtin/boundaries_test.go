package builtin

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

const modulePath = "github.com/cachebox-project/inference-cache"

func TestControllerProductionImportsRespectBoundaries(t *testing.T) {
	t.Parallel()

	root := repositoryRoot(t)
	controllerRoot := filepath.Join(root, "internal", "controller")
	var files []string
	err := filepath.WalkDir(controllerRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk controller files: %v", err)
	}
	banned := map[string]struct{}{
		modulePath + "/pkg/server":       {},
		modulePath + "/internal/webhook": {},
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, imported := range file.Imports {
			pathValue, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				t.Fatalf("unquote import in %s: %v", path, err)
			}
			for prefix := range banned {
				if pathValue == prefix || strings.HasPrefix(pathValue, prefix+"/") {
					t.Errorf("%s imports implementation package %q", filepath.Base(path), pathValue)
				}
			}
		}
	}
}

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
		t.Fatalf("scan pkg packages: %v", err)
	}

	var undocumented []string
	for dir, documented := range packageDirs {
		if !documented {
			rel, err := filepath.Rel(root, dir)
			if err != nil {
				t.Fatalf("relative package path: %v", err)
			}
			undocumented = append(undocumented, rel)
		}
	}
	sort.Strings(undocumented)
	if len(undocumented) > 0 {
		t.Fatalf("pkg packages without package documentation: %s", strings.Join(undocumented, ", "))
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate repository root")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

package runtime

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// runScript runs kernelCheckScript under python3 with PYTHONPATH=pkgParent and
// the given STRICT env, returning the termination-message file contents and the
// exit code. The script's MSG path is redirected to a temp file (via an env the
// test injects) so it doesn't need to write /dev/termination-log. Skips if
// python3 is unavailable.
func runScript(t *testing.T, pkgParent string, strict bool) (string, int) {
	t.Helper()
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available; skipping hermetic detector test")
	}
	msg := filepath.Join(t.TempDir(), "termlog")
	script := strings.Replace(kernelCheckScript, `MSG = "/dev/termination-log"`,
		`MSG = os.environ["KERNEL_CHECK_MSG"]`, 1)
	cmd := exec.Command(py, "-c", script)
	cmd.Env = append(os.Environ(),
		"PYTHONPATH="+pkgParent,
		"KERNEL_CHECK_MSG="+msg,
	)
	if strict {
		cmd.Env = append(cmd.Env, "KERNEL_CHECK_STRICT=1")
	}
	err = cmd.Run()
	code := 0
	if ee, ok := err.(*exec.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("run: %v", err)
	}
	b, _ := os.ReadFile(msg)
	return string(b), code
}

// makeFakeLmcachePkg creates a temp dir with an empty `lmcache` package (no
// c_ops*.so) and returns its PARENT (for PYTHONPATH). Reproduces the
// "pure-python / no native extension" case -> FAIL.
func makeFakeLmcachePkg(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	pkg := filepath.Join(root, "lmcache")
	if err := os.MkdirAll(pkg, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pkg, "__init__.py"), []byte("# fake\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestKernelCheckScriptNoNativeSoReportOnlyExitsZero(t *testing.T) {
	msg, code := runScript(t, makeFakeLmcachePkg(t), false)
	if code != 0 {
		t.Errorf("report-only exit code = %d, want 0", code)
	}
	if !strings.HasPrefix(strings.TrimSpace(msg), KernelCheckMsgFailPrefix) {
		t.Errorf("message = %q, want FAIL: prefix", msg)
	}
	if !strings.Contains(msg, "no native c_ops") {
		t.Errorf("message = %q, want 'no native c_ops'", msg)
	}
}

func TestKernelCheckScriptNoNativeSoStrictExitsOne(t *testing.T) {
	msg, code := runScript(t, makeFakeLmcachePkg(t), true)
	if code != 1 {
		t.Errorf("strict exit code = %d, want 1", code)
	}
	if !strings.HasPrefix(strings.TrimSpace(msg), KernelCheckMsgFailPrefix) {
		t.Errorf("message = %q, want FAIL: prefix", msg)
	}
}

func TestKernelCheckScriptLmcacheAbsentReportsFail(t *testing.T) {
	// PYTHONPATH with no lmcache at all -> find_spec returns None -> FAIL.
	msg, code := runScript(t, t.TempDir(), false)
	if code != 0 {
		t.Errorf("exit = %d, want 0 (report-only)", code)
	}
	if !strings.Contains(msg, "lmcache not importable") {
		t.Errorf("message = %q, want 'lmcache not importable'", msg)
	}
}

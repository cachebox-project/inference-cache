package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	icpb "github.com/cachebox-project/inference-cache/pkg/server/proto/inferencecache/v1alpha1"
)

func TestParseLogFormat(t *testing.T) {
	cases := []struct {
		in      string
		want    LogFormat
		wantErr bool
	}{
		{"", LogFormatJSON, false},
		{"json", LogFormatJSON, false},
		{"JSON", LogFormatJSON, false},
		{"text", LogFormatText, false},
		{"  Text  ", LogFormatText, false},
		{"yaml", "", true},
	}
	for _, tc := range cases {
		got, err := ParseLogFormat(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseLogFormat(%q) err = nil, want non-nil", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseLogFormat(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("ParseLogFormat(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestParseLogLevel(t *testing.T) {
	cases := []struct {
		in      string
		want    slog.Level
		wantErr bool
	}{
		{"", slog.LevelInfo, false},
		{"info", slog.LevelInfo, false},
		{"DEBUG", slog.LevelDebug, false},
		{"warn", slog.LevelWarn, false},
		{"warning", slog.LevelWarn, false},
		{"error", slog.LevelError, false},
		{"trace", 0, true},
	}
	for _, tc := range cases {
		got, err := ParseLogLevel(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseLogLevel(%q) err = nil, want non-nil", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseLogLevel(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("ParseLogLevel(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestNewLogHandlerJSON(t *testing.T) {
	buf := &bytes.Buffer{}
	h, err := NewLogHandler(LogFormatJSON, slog.LevelInfo, buf)
	if err != nil {
		t.Fatalf("NewLogHandler: %v", err)
	}
	slog.New(h).Info("startup", "grpc_addr", ":9090")
	// JSON output: a parseable object with our message and field.
	var got map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &got); err != nil {
		t.Fatalf("output is not JSON: %v\n%s", err, buf.String())
	}
	if got["msg"] != "startup" {
		t.Fatalf("msg = %v, want startup", got["msg"])
	}
	if got["grpc_addr"] != ":9090" {
		t.Fatalf("grpc_addr = %v, want :9090", got["grpc_addr"])
	}
}

func TestNewLogHandlerText(t *testing.T) {
	buf := &bytes.Buffer{}
	h, err := NewLogHandler(LogFormatText, slog.LevelInfo, buf)
	if err != nil {
		t.Fatalf("NewLogHandler: %v", err)
	}
	slog.New(h).Info("startup", "grpc_addr", ":9090")
	out := buf.String()
	// Text output is key=value, not JSON.
	if strings.HasPrefix(strings.TrimSpace(out), "{") {
		t.Fatalf("text handler produced JSON-shaped output: %q", out)
	}
	if !strings.Contains(out, "msg=startup") {
		t.Fatalf("text output missing msg=startup: %q", out)
	}
	if !strings.Contains(out, "grpc_addr=:9090") {
		t.Fatalf("text output missing grpc_addr=:9090: %q", out)
	}
}

func TestNewLogHandlerRespectsLevel(t *testing.T) {
	buf := &bytes.Buffer{}
	h, err := NewLogHandler(LogFormatJSON, slog.LevelWarn, buf)
	if err != nil {
		t.Fatalf("NewLogHandler: %v", err)
	}
	logger := slog.New(h)
	logger.Info("below threshold")
	logger.Debug("also below threshold")
	if buf.Len() != 0 {
		t.Fatalf("info/debug logs leaked through warn threshold: %q", buf.String())
	}
	logger.Warn("at threshold")
	if buf.Len() == 0 {
		t.Fatal("warn log was dropped despite warn threshold")
	}
}

func TestNewLogHandlerRejectsUnknownFormat(t *testing.T) {
	if _, err := NewLogHandler(LogFormat("yaml"), slog.LevelInfo, &bytes.Buffer{}); err == nil {
		t.Fatal("NewLogHandler(yaml) err = nil, want non-nil")
	}
}

// installCaptureLogger swaps slog.Default() for a JSON handler writing to a
// returned buffer at debug level (so anything emitted shows up) and restores
// the previous default on test cleanup. Tests in this package run serially
// (no t.Parallel calls), so a global swap is safe.
func installCaptureLogger(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf
}

// TestLookupRouteEmitsNoLogs is the hot-path guard: the lookup contract is
// side-effect-free apart from metrics (see docs/design/grpc-contract.md), so
// the handler must not emit any log line at info or debug level — adding one
// would put per-RPC log spam on the gateway hot path.
func TestLookupRouteEmitsNoLogs(t *testing.T) {
	buf := installCaptureLogger(t)
	if _, err := newTestService().LookupRoute(context.Background(), &icpb.LookupRouteRequest{ModelId: "m"}); err != nil {
		t.Fatalf("LookupRoute: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("LookupRoute emitted log output on the hot path (forbidden):\n%s", buf.String())
	}
}

// TestServeEmitsGracefulShutdownLifecycle confirms the structured lifecycle
// events fire on a clean ctx-cancel shutdown and carry no payload fields
// (metadata-only logging).
func TestServeEmitsGracefulShutdownLifecycle(t *testing.T) {
	buf := installCaptureLogger(t)
	_, _, stop := startInProcessServer(t)
	stop()

	var events []string
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("non-JSON log line %q: %v", line, err)
		}
		if msg, ok := rec["msg"].(string); ok {
			events = append(events, msg)
		}
	}
	wantOrdered := []string{"graceful_shutdown_started", "graceful_shutdown_done"}
	got := strings.Join(events, ",")
	if !strings.Contains(got, strings.Join(wantOrdered, ",")) {
		t.Fatalf("expected lifecycle events %v in order; got %v", wantOrdered, events)
	}
	// Metadata-only: no payload fields leaked into logs.
	forbidden := []string{"prefix_hash", "rendered_prompt", "variables"}
	for _, key := range forbidden {
		if strings.Contains(buf.String(), key) {
			t.Fatalf("log output leaked %q payload field:\n%s", key, buf.String())
		}
	}
}

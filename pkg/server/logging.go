package server

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// LogFormat selects the slog.Handler the server binary installs as its
// default: structured JSON for production (parseable by log shippers) or
// human-readable text for local development.
type LogFormat string

const (
	LogFormatJSON LogFormat = "json"
	LogFormatText LogFormat = "text"
)

// ParseLogFormat resolves a --log-format flag value. Empty input defaults to
// JSON so production deployments don't need to pass the flag explicitly.
func ParseLogFormat(s string) (LogFormat, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", string(LogFormatJSON):
		return LogFormatJSON, nil
	case string(LogFormatText):
		return LogFormatText, nil
	default:
		return "", fmt.Errorf("unknown log format %q (want json|text)", s)
	}
}

// ParseLogLevel resolves a --log-level flag value. Empty input defaults to
// info. Misconfiguration fails fast at process start rather than silently
// emitting at the wrong level.
func ParseLogLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q (want debug|info|warn|error)", s)
	}
}

// NewLogHandler builds the slog.Handler the server binary installs as its
// default at the supplied level, writing to w.
func NewLogHandler(format LogFormat, level slog.Level, w io.Writer) (slog.Handler, error) {
	opts := &slog.HandlerOptions{Level: level}
	switch format {
	case LogFormatJSON:
		return slog.NewJSONHandler(w, opts), nil
	case LogFormatText:
		return slog.NewTextHandler(w, opts), nil
	default:
		return nil, fmt.Errorf("unknown log format %q", format)
	}
}

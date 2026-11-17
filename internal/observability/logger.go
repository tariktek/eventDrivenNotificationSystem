// Package observability sets up logging, metrics and tracing.
package observability

import (
	"io"
	"log/slog"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/config"
)

// NewLogger builds the process-wide structured logger. Every record carries
// the service name and version so logs from different binaries and releases
// can be told apart once aggregated.
//
// The config is assumed to be validated already (config.Load does this).
func NewLogger(w io.Writer, cfg config.LogConfig, service, version string) *slog.Logger {
	var level slog.Level
	// Validation guarantees a known level; on the impossible error path the
	// zero value (info) is a safe fallback.
	_ = level.UnmarshalText([]byte(cfg.Level))

	opts := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	if cfg.Format == "text" {
		handler = slog.NewTextHandler(w, opts)
	} else {
		handler = slog.NewJSONHandler(w, opts)
	}

	return slog.New(handler).With(
		slog.String("service", service),
		slog.String("version", version),
	)
}

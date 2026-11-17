package observability_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/config"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/observability"
)

func TestNewLogger_JSONIncludesServiceAndVersion(t *testing.T) {
	var buf bytes.Buffer
	log := observability.NewLogger(&buf, config.LogConfig{Level: "info", Format: "json"}, "api", "v1.2.3")

	log.Info("hello", "key", "value")

	var record map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &record))
	assert.Equal(t, "hello", record["msg"])
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, "api", record["service"])
	assert.Equal(t, "v1.2.3", record["version"])
	assert.Equal(t, "value", record["key"])
}

func TestNewLogger_RespectsLevel(t *testing.T) {
	var buf bytes.Buffer
	log := observability.NewLogger(&buf, config.LogConfig{Level: "warn", Format: "json"}, "worker", "dev")

	log.Info("dropped")
	log.Warn("kept")

	out := buf.String()
	assert.NotContains(t, out, "dropped")
	assert.Contains(t, out, "kept")
}

func TestNewLogger_TextFormat(t *testing.T) {
	var buf bytes.Buffer
	log := observability.NewLogger(&buf, config.LogConfig{Level: "debug", Format: "text"}, "api", "dev")

	log.Debug("plain")

	out := buf.String()
	assert.True(t, strings.HasPrefix(out, "time="), "expected logfmt-style output, got %q", out)
	assert.Contains(t, out, "msg=plain")
	assert.Contains(t, out, "service=api")
}

func TestNewLogger_AddsCorrelationIDFromContext(t *testing.T) {
	var buf bytes.Buffer
	log := observability.NewLogger(&buf, config.LogConfig{Level: "info", Format: "json"}, "api", "dev")
	ctx := observability.WithCorrelationID(t.Context(), "corr-123")

	log.With("component", "test").InfoContext(ctx, "with context")
	log.Info("without context")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 2)
	var first, second map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &first))
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &second))
	assert.Equal(t, "corr-123", first["correlation_id"], "kept through With()")
	assert.Equal(t, "test", first["component"])
	assert.NotContains(t, second, "correlation_id")
}

func TestNewLogger_CorrelationIDStaysTopLevelInGroups(t *testing.T) {
	var buf bytes.Buffer
	log := observability.NewLogger(&buf, config.LogConfig{Level: "info", Format: "json"}, "api", "dev")
	ctx := observability.WithCorrelationID(t.Context(), "corr-9")

	log.WithGroup("db").With("table", "notifications").InfoContext(ctx, "query", "rows", 3)

	var rec map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &rec))
	assert.Equal(t, "corr-9", rec["correlation_id"], "not nested under the group")
	db := rec["db"].(map[string]any)
	assert.Equal(t, "notifications", db["table"])
	assert.EqualValues(t, 3, db["rows"])
	assert.NotContains(t, db, "correlation_id")
}

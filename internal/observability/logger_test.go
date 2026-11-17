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

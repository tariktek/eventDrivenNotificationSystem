package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/config"
)

func TestMaskRecipient(t *testing.T) {
	assert.Equal(t, "+90555***4567", MaskRecipient("+905551234567"))
	assert.Equal(t, "a***@example.com", MaskRecipient("ada.lovelace@example.com"))
	assert.Equal(t, "***", MaskRecipient("fcm:device-token-abc"))
	assert.Equal(t, "***", MaskPhone("+1234"))
	assert.Equal(t, "***", MaskEmail("@nobody"))
}

func TestScrubPII(t *testing.T) {
	assert.Equal(t, "provider: invalid number +90555***4567 for a***@example.com",
		ScrubPII("provider: invalid number +905551234567 for ada@example.com"))
	for _, safe := range []string{
		"01a1ebae-5396-76a4-9e23-4d3a12eecde9", "2026-11-18T10:00:00+03:00", "http_503", "attempt 3 of 5",
	} {
		assert.Equal(t, safe, ScrubPII(safe), "ids, times and codes are left alone")
	}
}

// TestLoggerMasksPersonalData goes through the real logger.
func TestLoggerMasksPersonalData(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, config.LogConfig{Level: "info", Format: "json"}, "test", "v0")
	log.InfoContext(context.Background(), "delivery failed",
		"recipient", "+905551234567",
		"to", "ada@example.com",
		"error", errors.New("400 Bad Request: number +905551234567 is blocked"),
		"detail", "contact ada@example.com",
		"notification_id", "01a1ebae-5396-76a4-9e23-4d3a12eecde9",
		"attempt", 3,
	)
	out := buf.String()
	assert.NotContains(t, out, "905551234567")
	assert.NotContains(t, out, "ada@example.com")

	var line map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &line))
	assert.Equal(t, "+90555***4567", line["recipient"])
	assert.Equal(t, "a***@example.com", line["to"])
	assert.Equal(t, "400 Bad Request: number +90555***4567 is blocked", line["error"])
	assert.Equal(t, "contact a***@example.com", line["detail"])
	assert.Equal(t, "01a1ebae-5396-76a4-9e23-4d3a12eecde9", line["notification_id"])
	assert.InDelta(t, 3, line["attempt"], 0)
}

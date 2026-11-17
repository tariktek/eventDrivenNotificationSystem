package validation_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/validation"
)

// fieldErrors extracts the field paths from a validation error.
func fieldErrors(t *testing.T, err error) map[string]string {
	t.Helper()
	var verrs validation.Errors
	require.True(t, errors.As(err, &verrs), "want validation.Errors, got %v", err)
	out := make(map[string]string, len(verrs))
	for _, fe := range verrs {
		out[fe.Field] = fe.Reason
	}
	return out
}

func TestContent(t *testing.T) {
	tests := []struct {
		name         string
		ch           domain.Channel
		subject      string
		content      string
		wantSegments int
		wantFields   []string // empty means valid
	}{
		{"sms ok", domain.ChannelSMS, "", "Your code is 1234", 1, nil},
		{"sms six segments ok", domain.ChannelSMS, "", strings.Repeat("a", 918), 6, nil},
		{"sms seven segments", domain.ChannelSMS, "", strings.Repeat("a", 919), 7, []string{"content"}},
		{"sms turkish seven segments", domain.ChannelSMS, "", strings.Repeat("ş", 403), 7, []string{"content"}},
		{"sms subject not allowed", domain.ChannelSMS, "Hi", "body", 1, []string{"subject"}},
		{"empty content", domain.ChannelSMS, "", "", 0, []string{"content"}},

		{"email ok", domain.ChannelEmail, "Welcome", "<p>Hello</p>", 0, nil},
		{"email missing subject", domain.ChannelEmail, "", "body", 0, []string{"subject"}},
		{"email subject too long", domain.ChannelEmail, strings.Repeat("s", 256), "body", 0, []string{"subject"}},
		{"email subject header injection", domain.ChannelEmail, "Hi\r\nBcc: victim@example.com", "body", 0, []string{"subject"}},
		{"email body too large", domain.ChannelEmail, "Hi", strings.Repeat("x", 100*1024+1), 0, []string{"content"}},

		{"push ok without title", domain.ChannelPush, "", "You have a new message", 0, nil},
		{"push ok with title", domain.ChannelPush, "Sale", "50% off", 0, nil},
		{"push body too long", domain.ChannelPush, "", strings.Repeat("ü", 1025), 0, []string{"content"}},
		{"push title too long", domain.ChannelPush, strings.Repeat("t", 257), "body", 0, []string{"subject"}},

		{"nul byte rejected", domain.ChannelPush, "", "a\x00b", 0, []string{"content"}},
		{"invalid utf8 rejected", domain.ChannelEmail, "\xff", "body", 0, []string{"subject"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			segments, err := validation.Content(tt.ch, tt.subject, tt.content, "")
			if len(tt.wantFields) == 0 {
				require.NoError(t, err)
				assert.Equal(t, tt.wantSegments, segments)
				return
			}
			got := fieldErrors(t, err)
			for _, f := range tt.wantFields {
				assert.Contains(t, got, f)
			}
			assert.Len(t, got, len(tt.wantFields))
		})
	}
}

func TestContent_FieldPrefix(t *testing.T) {
	_, err := validation.Content(domain.ChannelEmail, "", "body", "items[7]")
	assert.Contains(t, fieldErrors(t, err), "items[7].subject")
}

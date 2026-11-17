package validation_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/validation"
)

var now = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

func validSMS() validation.NotificationRequest {
	return validation.NotificationRequest{
		Channel:   "sms",
		Recipient: "+905551234567",
		Content:   "Your code is 1234",
	}
}

func ptr[T any](v T) *T { return &v }

func TestNotification_ValidMinimal(t *testing.T) {
	req := validSMS()
	p, err := validation.Notification(&req, now, validation.Options{}, "")
	require.NoError(t, err)

	assert.Equal(t, domain.ChannelSMS, p.Channel)
	assert.Equal(t, domain.PriorityNormal, p.Priority)
	assert.Equal(t, domain.DefaultMaxAttempts, p.MaxAttempts)
	assert.Equal(t, 1, p.Segments)
	assert.Nil(t, p.TemplateID)
}

func TestNotification_ValidFull(t *testing.T) {
	tplID := uuid.New()
	at := now.Add(24 * time.Hour)
	req := validation.NotificationRequest{
		Channel:        "email",
		Priority:       "high",
		Recipient:      "user@example.com",
		TemplateID:     tplID.String(),
		TemplateVars:   map[string]any{"name": "Ada"},
		ScheduledAt:    &at,
		IdempotencyKey: "order-42:email",
		MaxAttempts:    ptr(3),
		CallbackURL:    "https://hooks.example.com/notify",
		Metadata:       map[string]any{"campaign": "bf"},
	}
	p, err := validation.Notification(&req, now, validation.Options{}, "")
	require.NoError(t, err)

	assert.Equal(t, domain.PriorityHigh, p.Priority)
	assert.Equal(t, &tplID, p.TemplateID)
	assert.Empty(t, p.Content, "content comes from the template later")
	assert.Equal(t, at, *p.ScheduledAt)
	assert.Equal(t, 3, p.MaxAttempts)
	assert.Equal(t, "https://hooks.example.com/notify", p.CallbackURL)
}

func TestNotification_Invalid(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(r *validation.NotificationRequest)
		field  string
	}{
		{"missing channel", func(r *validation.NotificationRequest) { r.Channel = "" }, "channel"},
		{"unknown channel", func(r *validation.NotificationRequest) { r.Channel = "fax" }, "channel"},
		{"bad recipient", func(r *validation.NotificationRequest) { r.Recipient = "0555" }, "recipient"},
		{"unknown priority", func(r *validation.NotificationRequest) { r.Priority = "urgent" }, "priority"},
		{"no content no template", func(r *validation.NotificationRequest) { r.Content = "" }, "content"},
		{"content and template", func(r *validation.NotificationRequest) { r.TemplateID = uuid.NewString() }, "template_id"},
		{"bad template id", func(r *validation.NotificationRequest) { r.Content = ""; r.TemplateID = "nope" }, "template_id"},
		{"vars without template", func(r *validation.NotificationRequest) { r.TemplateVars = map[string]any{"a": 1} }, "template_vars"},
		{"scheduled in the past", func(r *validation.NotificationRequest) { r.ScheduledAt = ptr(now.Add(-time.Hour)) }, "scheduled_at"},
		{"scheduled too far", func(r *validation.NotificationRequest) { r.ScheduledAt = ptr(now.Add(31 * 24 * time.Hour)) }, "scheduled_at"},
		{"max attempts zero", func(r *validation.NotificationRequest) { r.MaxAttempts = ptr(0) }, "max_attempts"},
		{"max attempts too high", func(r *validation.NotificationRequest) { r.MaxAttempts = ptr(11) }, "max_attempts"},
		{"idempotency key with space", func(r *validation.NotificationRequest) { r.IdempotencyKey = "a b" }, "idempotency_key"},
		{"idempotency key too long", func(r *validation.NotificationRequest) { r.IdempotencyKey = strings.Repeat("k", 256) }, "idempotency_key"},
		{"metadata too large", func(r *validation.NotificationRequest) {
			r.Metadata = map[string]any{"blob": strings.Repeat("x", 5000)}
		}, "metadata"},
		{"callback http", func(r *validation.NotificationRequest) { r.CallbackURL = "http://example.com/cb" }, "callback_url"},
		{"callback localhost", func(r *validation.NotificationRequest) { r.CallbackURL = "https://localhost/cb" }, "callback_url"},
		{"callback metadata ip", func(r *validation.NotificationRequest) { r.CallbackURL = "https://169.254.169.254/latest" }, "callback_url"},
		{"callback private ip", func(r *validation.NotificationRequest) { r.CallbackURL = "https://10.0.0.5/cb" }, "callback_url"},
		{"callback credentials", func(r *validation.NotificationRequest) { r.CallbackURL = "https://u:p@example.com/cb" }, "callback_url"},
		{"callback relative", func(r *validation.NotificationRequest) { r.CallbackURL = "/cb" }, "callback_url"},
		{"callback ftp", func(r *validation.NotificationRequest) { r.CallbackURL = "ftp://example.com" }, "callback_url"},
		{"callback fqdn localhost", func(r *validation.NotificationRequest) { r.CallbackURL = "https://localhost./cb" }, "callback_url"},
		{"callback fqdn internal", func(r *validation.NotificationRequest) { r.CallbackURL = "https://db.internal./cb" }, "callback_url"},
		{"callback nat64 metadata", func(r *validation.NotificationRequest) { r.CallbackURL = "https://[64:ff9b::a9fe:a9fe]/" }, "callback_url"},
		// PostgreSQL jsonb cannot store U+0000; this must be a 422, not a 500 at insert time.
		{"nul in metadata value", func(r *validation.NotificationRequest) {
			r.Metadata = map[string]any{"k": "a\x00b"}
		}, "metadata"},
		{"nul in nested metadata", func(r *validation.NotificationRequest) {
			r.Metadata = map[string]any{"list": []any{map[string]any{"x": "\x00"}}}
		}, "metadata"},
		{"nul in metadata key", func(r *validation.NotificationRequest) {
			r.Metadata = map[string]any{"a\x00": 1}
		}, "metadata"},
		{"nul in template vars", func(r *validation.NotificationRequest) {
			r.Content = ""
			r.TemplateID = uuid.NewString()
			r.TemplateVars = map[string]any{"name": "\x00"}
		}, "template_vars"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := validSMS()
			tt.mutate(&req)
			_, err := validation.Notification(&req, now, validation.Options{}, "")
			assert.Contains(t, fieldErrors(t, err), tt.field)
		})
	}
}

func TestNotification_SmallClockSkewAccepted(t *testing.T) {
	req := validSMS()
	req.ScheduledAt = ptr(now.Add(-30 * time.Second))
	_, err := validation.Notification(&req, now, validation.Options{}, "")
	assert.NoError(t, err, "a client clock slightly behind ours must not be rejected")
}

func TestNotification_ReportsAllErrorsAtOnce(t *testing.T) {
	req := validation.NotificationRequest{Channel: "sms", Recipient: "bad", Priority: "x", MaxAttempts: ptr(99)}
	_, err := validation.Notification(&req, now, validation.Options{}, "")

	got := fieldErrors(t, err)
	for _, f := range []string{"recipient", "priority", "content", "max_attempts"} {
		assert.Contains(t, got, f)
	}
}

func TestNotification_SubjectCheckedEvenWithoutContent(t *testing.T) {
	req := validation.NotificationRequest{
		Channel:   "email",
		Recipient: "user@example.com",
		Subject:   "x\r\nBcc: victim@example.com",
	}
	_, err := validation.Notification(&req, now, validation.Options{}, "")

	got := fieldErrors(t, err)
	assert.Contains(t, got, "content", "missing content")
	assert.Contains(t, got, "subject", "header injection must be reported in the same response")
}

func TestContent_BothFieldsReportedWhenBothInvalid(t *testing.T) {
	_, err := validation.Content(domain.ChannelEmail, "a\x00", "\xff", "")
	got := fieldErrors(t, err)
	assert.Contains(t, got, "subject")
	assert.Contains(t, got, "content")
}

func TestNotification_DevOptionsAllowLocalCallbacks(t *testing.T) {
	req := validSMS()
	req.CallbackURL = "http://localhost:9000/cb"
	opts := validation.Options{AllowInsecureCallbacks: true, AllowPrivateCallbacks: true}
	_, err := validation.Notification(&req, now, opts, "")
	assert.NoError(t, err)
}

func TestBatch(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		items := []validation.NotificationRequest{validSMS(), validSMS()}
		parsed, err := validation.Batch(items, now, validation.Options{})
		require.NoError(t, err)
		assert.Len(t, parsed, 2)
	})

	t.Run("size limits", func(t *testing.T) {
		for _, n := range []int{0, domain.MaxBatchSize + 1} {
			_, err := validation.Batch(make([]validation.NotificationRequest, n), now, validation.Options{})
			assert.Contains(t, fieldErrors(t, err), "items", "n=%d", n)
		}
	})

	t.Run("max size accepted", func(t *testing.T) {
		items := make([]validation.NotificationRequest, domain.MaxBatchSize)
		for i := range items {
			items[i] = validSMS()
		}
		_, err := validation.Batch(items, now, validation.Options{})
		assert.NoError(t, err)
	})

	t.Run("errors carry item index", func(t *testing.T) {
		items := []validation.NotificationRequest{validSMS(), validSMS(), validSMS()}
		items[2].Recipient = "bad"
		_, err := validation.Batch(items, now, validation.Options{})
		got := fieldErrors(t, err)
		assert.Contains(t, got, "items[2].recipient")
		assert.Len(t, got, 1)
	})

	t.Run("duplicate idempotency keys", func(t *testing.T) {
		items := []validation.NotificationRequest{validSMS(), validSMS(), validSMS()}
		items[0].IdempotencyKey = "k1"
		items[2].IdempotencyKey = "k1"
		_, err := validation.Batch(items, now, validation.Options{})
		got := fieldErrors(t, err)
		assert.Equal(t, "duplicates items[0].idempotency_key", got["items[2].idempotency_key"])
	})

	t.Run("error list is capped", func(t *testing.T) {
		items := make([]validation.NotificationRequest, 500) // every item invalid
		_, err := validation.Batch(items, now, validation.Options{})
		var verrs validation.Errors
		require.True(t, errors.As(err, &verrs))
		assert.Len(t, verrs, 101, "100 errors plus a truncation notice")
		assert.Contains(t, verrs[100].Reason, "too many errors")
	})
}

func FuzzNotification(f *testing.F) {
	f.Add("sms", "+905551234567", "hello", "", "")
	f.Add("email", "a@b.co", "body", "Subj", "https://x.com/cb")
	f.Add("push", "tok", "\x00", "\r\n", "http://[::1]/")
	f.Add("", "", "", "", "%%")
	f.Fuzz(func(t *testing.T, channel, recipient, content, subject, callback string) {
		req := validation.NotificationRequest{
			Channel: channel, Recipient: recipient, Content: content, Subject: subject, CallbackURL: callback,
		}
		p, err := validation.Notification(&req, now, validation.Options{}, "")
		if err != nil {
			var verrs validation.Errors
			if !errors.As(err, &verrs) || len(verrs) == 0 {
				t.Fatalf("errors must be validation.Errors, got %T %v", err, err)
			}
			return
		}
		// Anything accepted must satisfy the database constraints.
		if !p.Channel.Valid() || p.Content == "" || strings.ContainsRune(p.Content, 0) {
			t.Fatalf("accepted invalid notification: %+v", p)
		}
		if p.Channel == domain.ChannelEmail && p.Subject == "" {
			t.Fatal("accepted email without subject")
		}
		if p.Channel == domain.ChannelSMS && (p.Segments < 1 || p.Segments > validation.MaxSMSSegments) {
			t.Fatalf("accepted sms with %d segments", p.Segments)
		}
	})
}

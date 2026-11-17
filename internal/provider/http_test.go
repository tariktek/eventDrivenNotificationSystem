package provider_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/provider"
)

func client(url string, timeout time.Duration) *provider.HTTPClient {
	return provider.NewHTTPClient(provider.HTTPConfig{
		URLs:    map[domain.Channel]string{domain.ChannelSMS: url, domain.ChannelEmail: url},
		Timeout: timeout,
	})
}

func message() *provider.Message {
	return &provider.Message{
		NotificationID: uuid.MustParse("0192f5e4-7c1a-7b3e-9a41-2f0c8d6b1e77"),
		Channel:        domain.ChannelSMS,
		To:             "+905551234567",
		Content:        "Your code is 4821",
		CorrelationID:  "corr-1",
	}
}

func TestSend_RequestMatchesTheProviderContract(t *testing.T) {
	var (
		gotBody    map[string]any
		gotHeaders http.Header
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeaders = r.Header.Clone()
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"messageId":"m-1","status":"accepted","timestamp":"2026-11-17T12:00:00Z"}`)
	}))
	defer srv.Close()

	res := client(srv.URL, time.Second).Send(t.Context(), message())

	assert.Equal(t, map[string]any{"to": "+905551234567", "channel": "sms", "content": "Your code is 4821"}, gotBody,
		"exactly the documented request format; subject omitted when empty")
	assert.Equal(t, "application/json", gotHeaders.Get("Content-Type"))
	assert.Equal(t, "0192f5e4-7c1a-7b3e-9a41-2f0c8d6b1e77", gotHeaders.Get("X-Idempotency-Key"))
	assert.Equal(t, "corr-1", gotHeaders.Get("X-Correlation-ID"))

	assert.Equal(t, domain.OutcomeSuccess, res.Outcome)
	assert.Equal(t, http.StatusAccepted, res.StatusCode)
	assert.Equal(t, "m-1", res.MessageID)
	assert.Positive(t, res.Duration)
}

func TestSend_Classification(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		body       string
		header     map[string]string
		outcome    domain.AttemptOutcome
		errCode    string
		retryAfter time.Duration
	}{
		{"202 accepted", 202, `{"messageId":"m","status":"accepted","timestamp":"x"}`, nil, domain.OutcomeSuccess, "", 0},
		{"200 accepted is also fine", 200, `{"messageId":"m","status":"accepted"}`, nil, domain.OutcomeSuccess, "", 0},
		// webhook.site leaves unknown template variables unexpanded; the
		// timestamp is informational, so this must still count as sent.
		{"unparsed timestamp", 202, `{"messageId":"m","status":"accepted","timestamp":"$time$"}`, nil, domain.OutcomeSuccess, "", 0},

		{"2xx without messageId", 202, `{"status":"accepted"}`, nil, domain.OutcomeTransientError, "invalid_provider_response", 0},
		{"2xx not accepted", 202, `{"messageId":"m","status":"queued?"}`, nil, domain.OutcomeTransientError, "invalid_provider_response", 0},
		{"2xx html", 200, `<html>maintenance</html>`, nil, domain.OutcomeTransientError, "invalid_provider_response", 0},

		{"429 with Retry-After", 429, `slow down`, map[string]string{"Retry-After": "7"}, domain.OutcomeTransientError, "http_429", 7 * time.Second},
		{"503 with Retry-After", 503, ``, map[string]string{"Retry-After": "2"}, domain.OutcomeTransientError, "http_503", 2 * time.Second},
		{"500", 500, `boom`, nil, domain.OutcomeTransientError, "http_500", 0},
		{"408", 408, ``, nil, domain.OutcomeTransientError, "http_408", 0},
		{"bad Retry-After ignored", 429, ``, map[string]string{"Retry-After": "soon"}, domain.OutcomeTransientError, "http_429", 0},

		{"400 bad recipient", 400, `{"error":"invalid number"}`, nil, domain.OutcomePermanentError, "http_400", 0},
		{"401", 401, ``, nil, domain.OutcomePermanentError, "http_401", 0},
		{"404", 404, ``, nil, domain.OutcomePermanentError, "http_404", 0},
		{"422", 422, ``, nil, domain.OutcomePermanentError, "http_422", 0},
		{"redirect not followed", 302, ``, map[string]string{"Location": "https://elsewhere.example"}, domain.OutcomePermanentError, "unexpected_redirect", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				for k, v := range tt.header {
					w.Header().Set(k, v)
				}
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			}))
			defer srv.Close()

			res := client(srv.URL, time.Second).Send(t.Context(), message())
			assert.Equal(t, tt.outcome, res.Outcome)
			assert.Equal(t, tt.status, res.StatusCode)
			assert.Equal(t, tt.errCode, res.ErrorCode)
			assert.Equal(t, tt.retryAfter, res.RetryAfter)
			if tt.outcome == domain.OutcomeSuccess {
				assert.Empty(t, res.Error)
			} else {
				assert.Empty(t, res.MessageID)
			}
		})
	}
}

func TestSend_RetryAfterHTTPDate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", time.Now().Add(30*time.Second).UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	res := client(srv.URL, time.Second).Send(t.Context(), message())
	assert.InDelta(t, 30, res.RetryAfter.Seconds(), 2)
}

func TestSend_Timeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
	defer srv.Close()
	defer close(release)

	start := time.Now()
	res := client(srv.URL, 100*time.Millisecond).Send(t.Context(), message())

	assert.Less(t, time.Since(start), time.Second, "a hung provider must not hang the worker")
	assert.Equal(t, domain.OutcomeTransientError, res.Outcome)
	assert.Equal(t, "timeout", res.ErrorCode)
	assert.Zero(t, res.StatusCode)
}

func TestSend_ConnectionRefused(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing listens there any more

	res := client(url, time.Second).Send(t.Context(), message())
	assert.Equal(t, domain.OutcomeTransientError, res.Outcome)
	assert.Equal(t, "connection_error", res.ErrorCode)
}

func TestSend_ShutdownMidRequestIsTransient(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(started)
		<-release // the server notices the disconnect only once it reads; hold explicitly
	}))
	defer srv.Close()
	defer close(release)

	ctx, cancel := context.WithCancel(t.Context())
	go func() { <-started; cancel() }()
	res := client(srv.URL, 5*time.Second).Send(ctx, message())

	assert.Equal(t, domain.OutcomeTransientError, res.Outcome, "unknown whether it arrived: retry")
	assert.Equal(t, "cancelled", res.ErrorCode)
}

func TestSend_UnknownChannelIsPermanent(t *testing.T) {
	msg := message()
	msg.Channel = domain.ChannelPush // not configured in client()
	res := client("http://unused", time.Second).Send(t.Context(), msg)
	assert.Equal(t, domain.OutcomePermanentError, res.Outcome)
	assert.Equal(t, "no_provider", res.ErrorCode)
}

// TestSend_ErrorBodyIsSafeToStore: whatever the provider sends back ends up
// in a PostgreSQL text column, which rejects NUL bytes and invalid UTF-8.
func TestSend_ErrorBodyIsSafeToStore(t *testing.T) {
	body := strings.Repeat("ş", 300) + "\x00\xff" // >512 bytes: truncation lands mid-character
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()

	res := client(srv.URL, time.Second).Send(t.Context(), message())
	assert.True(t, utf8.ValidString(res.Error))
	assert.NotContains(t, res.Error, "\x00")
	assert.LessOrEqual(t, len(res.Error), 520)
}

func TestSend_HugeResponseIsCapped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"messageId":"m","status":"accepted","pad":"`+strings.Repeat("x", 3<<20)+`"}`)
	}))
	defer srv.Close()

	res := client(srv.URL, 5*time.Second).Send(t.Context(), message())
	assert.Equal(t, domain.OutcomeTransientError, res.Outcome, "a 3 MiB body is cut at 1 MiB and fails to parse")
}

// TestSend_LiveProvider checks the client against a real endpoint such as a
// webhook.site URL. It only runs when PROVIDER_LIVE_URL is set, so the
// regular suite and CI never depend on an external service:
//
//	PROVIDER_LIVE_URL=https://webhook.site/<uuid> go test -run Live ./internal/provider/
func TestSend_LiveProvider(t *testing.T) {
	url := os.Getenv("PROVIDER_LIVE_URL")
	if url == "" {
		t.Skip("PROVIDER_LIVE_URL not set")
	}
	m := message()
	m.Content = "live provider check from the notification system test suite"
	res := client(url, 10*time.Second).Send(t.Context(), m)

	require.Equal(t, domain.OutcomeSuccess, res.Outcome, "error: %s (%s)", res.Error, res.ErrorCode)
	assert.Equal(t, http.StatusAccepted, res.StatusCode)
	assert.NotEmpty(t, res.MessageID)
	t.Logf("delivered in %s, provider messageId %s", res.Duration, res.MessageID)
}

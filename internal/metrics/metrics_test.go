package metrics_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/events"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/metrics"
)

func event(typ, payload string) events.Event {
	return events.Event{ID: 1, Type: typ, AggregateType: "notification", AggregateID: uuid.New(), Payload: json.RawMessage(payload)}
}

func TestObserveEvents_CountsTransitions(t *testing.T) {
	m := metrics.New()
	m.ObserveEvents([]events.Event{
		event(events.TypeNotificationCreated, `{"channel":"sms","priority":"high","status":"pending"}`),
		event(events.TypeNotificationStatusChanged, `{"channel":"sms","status":"processing"}`),
		event(events.TypeNotificationStatusChanged, `{"channel":"sms","status":"sent"}`),
		event(events.TypeNotificationStatusChanged, `{"channel":"email","status":"failed","attempts":1,"max_attempts":5,"error_code":"http_400"}`),
		event(events.TypeNotificationStatusChanged, `{"channel":"email","status":"failed","attempts":5,"max_attempts":5,"error_code":"http_503"}`),
		event(events.TypeNotificationStatusChanged, `{"channel":"push","status":"failed","attempts":2,"max_attempts":5,"error_code":"lease_expired"}`),
		event(events.TypeNotificationStatusChanged, `{"channel":"push","status":"cancelled"}`),
		event(events.TypeBatchCompleted, `{"counts":{"sent":1}}`),
		event(events.TypeNotificationStatusChanged, `not json`),
	})
	assert.InDelta(t, 1, testutil.ToFloat64(m.Created.WithLabelValues("sms", "high")), 0)
	assert.InDelta(t, 1, testutil.ToFloat64(m.Sent.WithLabelValues("sms")), 0)
	assert.InDelta(t, 1, testutil.ToFloat64(m.Failed.WithLabelValues("email", "permanent_error")), 0)
	assert.InDelta(t, 1, testutil.ToFloat64(m.Failed.WithLabelValues("email", "attempts_exhausted")), 0)
	assert.InDelta(t, 1, testutil.ToFloat64(m.Failed.WithLabelValues("push", "lease_expired")), 0)
	assert.InDelta(t, 1, testutil.ToFloat64(m.Cancelled.WithLabelValues("push")), 0)
}

func TestHandler_ExposesPrometheusText(t *testing.T) {
	m := metrics.New()
	m.Sent.WithLabelValues("sms").Add(3)
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", http.NoBody))
	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), `notifications_sent_total{channel="sms"} 3`)
	assert.True(t, strings.Contains(string(body), "go_goroutines"), "runtime metrics included")
}

func TestStatusClass(t *testing.T) {
	for code, want := range map[int]string{0: "error", 202: "2xx", 301: "3xx", 429: "4xx", 503: "5xx"} {
		assert.Equal(t, want, metrics.StatusClass(code))
	}
}

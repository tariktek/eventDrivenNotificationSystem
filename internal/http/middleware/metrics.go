package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/metrics"
)

// Metrics counts requests and records their latency by route pattern
// ("/api/v1/notifications/{id}", never the raw path, so ids do not create
// a time series each). Requests no route matched share one label.
func Metrics(m *metrics.Metrics) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w}
			next.ServeHTTP(sw, r)

			route := "unmatched"
			if rc := chi.RouteContext(r.Context()); rc != nil && rc.RoutePattern() != "" {
				route = rc.RoutePattern()
			}
			status := strconv.Itoa(sw.Status())
			m.HTTPRequests.WithLabelValues(route, r.Method, status).Inc()
			if sw.Status() != http.StatusSwitchingProtocols { // a WebSocket's lifetime is not latency
				m.HTTPDuration.WithLabelValues(route, r.Method, status).Observe(time.Since(start).Seconds())
			}
		})
	}
}

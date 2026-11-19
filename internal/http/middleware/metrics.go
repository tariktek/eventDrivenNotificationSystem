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
// a time series each). Requests no route matched share one label, and so
// do methods outside the standard set: the method is client input, read
// before authentication, and every string a client sent would otherwise
// become a series of its own, kept forever.
//
// It must run outside Recovery: a panic unwinds past everything inside it,
// and the 500 it becomes would not be counted.
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
			method := methodLabel(r.Method)
			m.HTTPRequests.WithLabelValues(route, method, status).Inc()
			if sw.Status() != http.StatusSwitchingProtocols { // a WebSocket's lifetime is not latency
				m.HTTPDuration.WithLabelValues(route, method, status).Observe(time.Since(start).Seconds())
			}
		})
	}
}

// knownMethods get a label value of their own; anything else is "other".
var knownMethods = map[string]bool{
	http.MethodGet: true, http.MethodHead: true, http.MethodPost: true, http.MethodPut: true,
	http.MethodPatch: true, http.MethodDelete: true, http.MethodOptions: true,
}

func methodLabel(m string) string {
	if knownMethods[m] {
		return m
	}
	return "other"
}

package middleware

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Tracing starts a server span per request, continuing the caller's trace
// if it sent a traceparent header. Probes, the scrape endpoint and
// WebSocket connections (hours-long, not a request) are left out.
func Tracing(next http.Handler) http.Handler {
	traced := otelhttp.NewHandler(spanDetails(next), "http.server",
		otelhttp.WithFilter(func(r *http.Request) bool {
			switch r.URL.Path {
			case "/health", "/ready", "/metrics", "/api/v1/ws":
				return false
			}
			return true
		}))
	return traced
}

// spanDetails names the span after the route once routing has run
// ("POST /api/v1/notifications", not one name per id) and records the
// correlation ID, which links the trace to the logs.
func spanDetails(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		span := trace.SpanFromContext(r.Context())
		if !span.IsRecording() {
			return
		}
		if rc := chi.RouteContext(r.Context()); rc != nil && rc.RoutePattern() != "" {
			span.SetName(r.Method + " " + rc.RoutePattern())
			span.SetAttributes(attribute.String("http.route", rc.RoutePattern()))
		}
		// Set on the response by the CorrelationID middleware, which runs
		// inside this one.
		if id := w.Header().Get(HeaderCorrelationID); id != "" {
			span.SetAttributes(attribute.String("correlation_id", id))
		}
	})
}

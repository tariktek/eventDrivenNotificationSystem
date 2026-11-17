package middleware

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
)

// Recovery turns a panic in a handler into a logged 500 response instead of
// a dropped connection. The stack trace goes to the log, never to the client.
func Recovery(log *slog.Logger, writeErr ErrorWriter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sw := &statusWriter{ResponseWriter: w}
			// The request (and its context) reaches handlePanic through r; the
			// linter cannot see that through a deferred closure.
			defer func() { //nolint:contextcheck // see above
				if rec := recover(); rec != nil {
					handlePanic(log, writeErr, sw, r, rec)
				}
			}()
			next.ServeHTTP(sw, r)
		})
	}
}

func handlePanic(log *slog.Logger, writeErr ErrorWriter, sw *statusWriter, r *http.Request, rec any) {
	// http.ErrAbortHandler is the sanctioned way to abort a response;
	// net/http handles it quietly.
	if err, ok := rec.(error); ok && errors.Is(err, http.ErrAbortHandler) {
		panic(rec)
	}
	log.ErrorContext(r.Context(), "panic serving request",
		"panic", fmt.Sprint(rec), "method", r.Method, "path", r.URL.Path, "stack", string(debug.Stack()))
	if sw.status == 0 { // nothing sent yet, so we can still answer
		writeErr(sw, r, fmt.Errorf("panic: %v", rec))
	}
}

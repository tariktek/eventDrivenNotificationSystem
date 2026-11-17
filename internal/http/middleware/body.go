// Package middleware holds HTTP middleware shared by API routes.
package middleware

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/apierror"
)

type bodyKey struct{}

// BufferBody reads request bodies up to limit bytes into memory, stores them
// in the request context and replaces r.Body with a fresh reader.
//
// Later stages need the raw bytes: strict decoding (rejecting unknown
// fields) and idempotency (hashing the body). A body over the limit is
// rejected via onError with an *http.MaxBytesError before any handler runs.
func BufferBody(limit int64, onError func(http.ResponseWriter, *http.Request, error)) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body == nil || r.Body == http.NoBody {
				next.ServeHTTP(w, r)
				return
			}
			b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
			_ = r.Body.Close()
			if err != nil {
				var tooLarge *http.MaxBytesError
				if !errors.As(err, &tooLarge) {
					// The client hung up or sent a truncated body: its
					// failure, not ours, so 400 rather than 500.
					err = apierror.BadRequest("could not read request body: %v", err)
				}
				onError(w, r, err)
				return
			}
			r = r.WithContext(context.WithValue(r.Context(), bodyKey{}, b))
			r.Body = io.NopCloser(bytes.NewReader(b))
			next.ServeHTTP(w, r)
		})
	}
}

// Body returns the buffered request body, if BufferBody ran.
func Body(ctx context.Context) ([]byte, bool) {
	b, ok := ctx.Value(bodyKey{}).([]byte)
	return b, ok
}

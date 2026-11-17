package middleware

import (
	"net/http"
	"regexp"

	"github.com/google/uuid"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/observability"
)

// HeaderCorrelationID carries the correlation ID in requests and responses.
const HeaderCorrelationID = "X-Correlation-ID"

// A client-supplied ID is only accepted if it is short and plain: it ends up
// in every log line, so newlines or control characters would let a client
// forge log entries.
var validCorrelationID = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// CorrelationID takes the request's X-Correlation-ID (or generates one),
// stores it in the context for logs and downstream calls, and echoes it in
// the response.
func CorrelationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(HeaderCorrelationID)
		if !validCorrelationID.MatchString(id) {
			id = newID()
		}
		w.Header().Set(HeaderCorrelationID, id)
		next.ServeHTTP(w, r.WithContext(observability.WithCorrelationID(r.Context(), id)))
	})
}

func newID() string {
	if id, err := uuid.NewV7(); err == nil {
		return id.String()
	}
	return uuid.NewString()
}

// Package apierror turns errors into the API's uniform error response:
//
//	{"error": {"code": "...", "message": "...", "details": [...]}, "correlation_id": "..."}
//
// Every layer reports failures as Go errors; this is the single place that
// decides the HTTP status and the stable error code for each kind.
package apierror

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/api"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/validation"
)

// BadRequestError marks malformed input: unparseable JSON, unknown fields,
// wrong parameter types.
type BadRequestError struct{ Err error }

func (e *BadRequestError) Error() string { return e.Err.Error() }

func (e *BadRequestError) Unwrap() error { return e.Err }

// BadRequest wraps err as a BadRequestError.
func BadRequest(format string, args ...any) error {
	return &BadRequestError{Err: fmt.Errorf(format, args...)}
}

// Error is an HTTP status plus the response body. Middleware that needs a
// specific code (401, 409 request_in_progress, ...) returns one directly.
type Error struct {
	Status int
	Body   api.Error
}

func (e *Error) Error() string { return string(e.Body.Error.Code) + ": " + e.Body.Error.Message }

// New builds an Error.
func New(status int, code api.ErrorCode, message string) *Error {
	return &Error{Status: status, Body: api.Error{Error: api.ErrorBody{Code: code, Message: message}}}
}

// From classifies any error into a response. Unknown errors become a 500
// with a generic message: internal details are logged, never sent.
func From(err error) *Error {
	var (
		apiErr   *Error
		badReq   *BadRequestError
		verrs    validation.Errors
		trans    *domain.TransitionError
		tooLarge *http.MaxBytesError
	)
	switch {
	case errors.As(err, &apiErr):
		return apiErr
	case errors.As(err, &tooLarge):
		return New(http.StatusRequestEntityTooLarge, api.ErrorCodePayloadTooLarge,
			fmt.Sprintf("request body exceeds %d bytes", tooLarge.Limit))
	case errors.As(err, &badReq):
		return New(http.StatusBadRequest, api.ErrorCodeBadRequest, badReq.Error())
	case errors.As(err, &verrs):
		e := New(http.StatusUnprocessableEntity, api.ErrorCodeValidationFailed, "The request contains invalid fields.")
		details := make([]api.FieldError, len(verrs))
		for i, fe := range verrs {
			details[i] = api.FieldError{Field: fe.Field, Reason: fe.Reason}
		}
		e.Body.Error.Details = &details
		return e
	case errors.Is(err, domain.ErrInvalidCursor):
		e := New(http.StatusUnprocessableEntity, api.ErrorCodeValidationFailed, "The request contains invalid fields.")
		e.Body.Error.Details = &[]api.FieldError{{Field: "cursor", Reason: "is not a cursor returned by this API"}}
		return e
	case errors.As(err, &trans):
		return New(http.StatusConflict, api.ErrorCodeInvalidStatusTransition, transitionMessage(trans))
	case errors.Is(err, domain.ErrNotFound):
		return New(http.StatusNotFound, api.ErrorCodeNotFound, "The requested resource does not exist.")
	case errors.Is(err, domain.ErrAlreadyExists):
		return New(http.StatusConflict, api.ErrorCodeConflict, "A resource with the same unique name already exists.")
	default:
		return New(http.StatusInternalServerError, api.ErrorCodeInternalError, "An unexpected error occurred.")
	}
}

func transitionMessage(e *domain.TransitionError) string {
	switch e.To {
	case domain.StatusCancelled:
		return fmt.Sprintf("Only pending or scheduled notifications can be cancelled; this one is %s.", e.From)
	case domain.StatusPending:
		return fmt.Sprintf("Only failed notifications can be retried; this one is %s.", e.From)
	default:
		return fmt.Sprintf("A notification cannot move from %s to %s.", e.From, e.To)
	}
}

// Writer writes error responses and logs the ones that are our fault.
type Writer struct {
	Log *slog.Logger
	// CorrelationID extracts the request's correlation ID, if any.
	CorrelationID func(r *http.Request) string
}

// Write sends err as an error response.
func (wr Writer) Write(w http.ResponseWriter, r *http.Request, err error) {
	e := From(err)
	if e.Status >= http.StatusInternalServerError {
		wr.Log.ErrorContext(r.Context(), "request failed", "error", err, "method", r.Method, "path", r.URL.Path)
	}
	body := e.Body
	if wr.CorrelationID != nil {
		if id := wr.CorrelationID(r); id != "" {
			body.CorrelationId = &id
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.Status)
	_ = json.NewEncoder(w).Encode(body)
}

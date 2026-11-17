package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"unicode/utf8"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/api"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/apierror"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/middleware"
)

// requestBodies maps operations to their body type. The generated code
// decodes bodies leniently, silently dropping unknown fields; a client
// sending "recipent" would get the "recipient is required" error at best,
// or a field silently ignored at worst. Re-decoding strictly turns that into
// a clear 400.
var requestBodies = map[string]func() any{
	"CreateNotification":      func() any { return new(api.NotificationCreate) },
	"CreateNotificationBatch": func() any { return new(api.BatchCreate) },
	"CreateTemplate":          func() any { return new(api.TemplateWrite) },
	"UpdateTemplate":          func() any { return new(api.TemplateWrite) },
	"PreviewTemplate":         func() any { return new(api.TemplatePreviewRequest) },
}

// strictJSON rejects request bodies that are not valid UTF-8 or that have
// unknown fields or trailing data.
// It needs the raw body buffered by middleware.BufferBody.
func strictJSON(f api.StrictHandlerFunc, operationID string) api.StrictHandlerFunc {
	newBody, ok := requestBodies[operationID]
	if !ok {
		return f
	}
	return func(ctx context.Context, w http.ResponseWriter, r *http.Request, request any) (any, error) {
		raw, _ := middleware.Body(ctx)
		// encoding/json silently replaces invalid UTF-8 with U+FFFD, which
		// would turn a cp1254 "Şifre" into "�ifre" in the sent message.
		if !utf8.Valid(raw) {
			return nil, apierror.BadRequest("request body must be UTF-8 encoded")
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(newBody()); err != nil {
			return nil, apierror.BadRequest("invalid JSON body: %v", err)
		}
		if dec.More() {
			return nil, apierror.BadRequest("invalid JSON body: unexpected data after the JSON value")
		}
		return f(ctx, w, r, request)
	}
}

// apierrorBadRequest classifies errors from the generated parameter and body
// binding (bad UUIDs, malformed JSON, ...) as client errors.
func apierrorBadRequest(err error) error {
	return &apierror.BadRequestError{Err: err}
}

// Package handler implements the generated StrictServerInterface: it maps
// typed API requests to service calls and service results to typed API
// responses. Errors are returned as Go errors and rendered centrally by
// package apierror, so every failure has the same shape.
package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/api"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/service"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/validation"
)

// Handler serves the /api/v1 endpoints.
type Handler struct {
	svc *service.Service
	// meta extracts correlation and trace IDs from the request context.
	meta func(ctx context.Context) service.RequestMeta
}

var _ api.StrictServerInterface = (*Handler)(nil)

// New builds a Handler. meta may be nil.
func New(svc *service.Service, meta func(ctx context.Context) service.RequestMeta) *Handler {
	if meta == nil {
		meta = func(context.Context) service.RequestMeta { return service.RequestMeta{} }
	}
	return &Handler{svc: svc, meta: meta}
}

// HTTPHandler mounts the generated routes on r with strict JSON decoding
// and centralised error rendering.
func HTTPHandler(h *Handler, base api.ChiServerOptions, writeErr func(http.ResponseWriter, *http.Request, error)) http.Handler {
	strict := api.NewStrictHandlerWithOptions(h, []api.StrictMiddlewareFunc{strictJSON}, api.StrictHTTPServerOptions{
		RequestErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			writeErr(w, r, apierrorBadRequest(err))
		},
		ResponseErrorHandlerFunc: writeErr,
	})
	base.ErrorHandlerFunc = func(w http.ResponseWriter, r *http.Request, err error) {
		writeErr(w, r, apierrorBadRequest(err))
	}
	return api.HandlerWithOptions(strict, base)
}

// ---- notifications ---------------------------------------------------------

// CreateNotification implements POST /api/v1/notifications.
func (h *Handler) CreateNotification(ctx context.Context, req api.CreateNotificationRequestObject) (api.CreateNotificationResponseObject, error) {
	in := toNotificationRequest(req.Body)
	res, err := h.svc.CreateNotification(ctx, &in, h.meta(ctx))
	if err != nil {
		return nil, err
	}
	return api.CreateNotification201JSONResponse(toAPINotification(res.Notification)), nil
}

// CreateNotificationBatch implements POST /api/v1/notifications/batch.
func (h *Handler) CreateNotificationBatch(ctx context.Context, req api.CreateNotificationBatchRequestObject) (api.CreateNotificationBatchResponseObject, error) {
	items := make([]validation.NotificationRequest, len(req.Body.Items))
	for i := range req.Body.Items {
		items[i] = toNotificationRequest(&req.Body.Items[i])
	}
	var metadata map[string]any
	if req.Body.Metadata != nil {
		metadata = *req.Body.Metadata
	}
	res, err := h.svc.CreateBatch(ctx, items, metadata, h.meta(ctx))
	if err != nil {
		return nil, err
	}
	return api.CreateNotificationBatch201JSONResponse(toAPIBatchCreated(&res)), nil
}

// GetNotification implements GET /api/v1/notifications/{id}.
func (h *Handler) GetNotification(ctx context.Context, req api.GetNotificationRequestObject) (api.GetNotificationResponseObject, error) {
	n, attempts, err := h.svc.GetNotification(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	return api.GetNotification200JSONResponse(toAPINotificationDetail(n, attempts)), nil
}

// ListNotifications implements GET /api/v1/notifications.
func (h *Handler) ListNotifications(ctx context.Context, req api.ListNotificationsRequestObject) (api.ListNotificationsResponseObject, error) {
	p := req.Params
	f, err := listFilter(p.Status, p.Channel, p.Priority, p.Limit, p.Cursor)
	if err != nil {
		return nil, err
	}
	f.BatchID = p.BatchId
	f.CreatedFrom, f.CreatedTo = p.CreatedFrom, p.CreatedTo
	if f.CreatedFrom != nil && f.CreatedTo != nil && !f.CreatedFrom.Before(*f.CreatedTo) {
		return nil, validation.Field("created_to", "must be after created_from")
	}

	page, err := h.svc.ListNotifications(ctx, f)
	if err != nil {
		return nil, err
	}
	return api.ListNotifications200JSONResponse(toAPINotificationPage(page)), nil
}

// CancelNotification implements POST /api/v1/notifications/{id}/cancel.
func (h *Handler) CancelNotification(ctx context.Context, req api.CancelNotificationRequestObject) (api.CancelNotificationResponseObject, error) {
	n, err := h.svc.CancelNotification(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	return api.CancelNotification200JSONResponse(toAPINotification(n)), nil
}

// RetryNotification implements POST /api/v1/notifications/{id}/retry.
func (h *Handler) RetryNotification(ctx context.Context, req api.RetryNotificationRequestObject) (api.RetryNotificationResponseObject, error) {
	n, err := h.svc.RetryNotification(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	return api.RetryNotification200JSONResponse(toAPINotification(n)), nil
}

// Redrive limits for POST /api/v1/notifications/retry-failed.
const (
	defaultRedriveLimit = 1000
	maxRedriveLimit     = 10000
	maxErrorCodeLen     = 64
)

// RetryFailedNotifications implements POST /api/v1/notifications/retry-failed.
func (h *Handler) RetryFailedNotifications(ctx context.Context, req api.RetryFailedNotificationsRequestObject) (api.RetryFailedNotificationsResponseObject, error) {
	b := req.Body
	var (
		f    = postgres.DeadLetterFilter{BatchID: b.BatchId, ErrorCode: b.ErrorCode, FailedFrom: b.FailedFrom, FailedTo: b.FailedTo}
		errs []error
	)
	if b.Channel != nil {
		c, err := domain.ParseChannel(string(*b.Channel))
		if err != nil {
			errs = append(errs, validation.Field("channel", "must be one of sms, email, push"))
		}
		f.Channel = &c
	}
	if b.ErrorCode != nil && (*b.ErrorCode == "" || len(*b.ErrorCode) > maxErrorCodeLen || strings.ContainsRune(*b.ErrorCode, 0)) {
		errs = append(errs, validation.Field("error_code", "must be 1 to %d characters without NUL", maxErrorCodeLen))
	}
	if f.FailedFrom != nil && f.FailedTo != nil && !f.FailedFrom.Before(*f.FailedTo) {
		errs = append(errs, validation.Field("failed_to", "must be after failed_from"))
	}
	limit := defaultRedriveLimit
	if b.Limit != nil {
		limit = *b.Limit
		if limit < 1 || limit > maxRedriveLimit {
			errs = append(errs, validation.Field("limit", "must be between 1 and %d", maxRedriveLimit))
		}
	}
	if err := validation.Merge(errs...); err != nil {
		return nil, err
	}

	res, err := h.svc.RetryFailedNotifications(ctx, f, limit, deref(b.DryRun))
	if err != nil {
		return nil, err
	}
	return api.RetryFailedNotifications200JSONResponse{Requeued: int(res.Requeued), Remaining: int(res.Remaining)}, nil
}

// GetMetrics implements GET /api/v1/metrics.
func (h *Handler) GetMetrics(ctx context.Context, _ api.GetMetricsRequestObject) (api.GetMetricsResponseObject, error) {
	snap, err := h.svc.Metrics(ctx)
	if err != nil {
		return nil, err
	}
	// The snapshot's JSON shape is the schema's; convert through JSON
	// rather than copy a dozen nested maps field by field. The contract
	// tests check the result against the spec.
	b, err := json.Marshal(snap)
	if err != nil {
		return nil, err
	}
	var out api.MetricsSummary
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return api.GetMetrics200JSONResponse(out), nil
}

// ---- batches ---------------------------------------------------------------

// GetBatch implements GET /api/v1/batches/{id}.
func (h *Handler) GetBatch(ctx context.Context, req api.GetBatchRequestObject) (api.GetBatchResponseObject, error) {
	s, err := h.svc.GetBatch(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	return api.GetBatch200JSONResponse(toAPIBatch(s)), nil
}

// ListBatchNotifications implements GET /api/v1/batches/{id}/notifications.
func (h *Handler) ListBatchNotifications(ctx context.Context, req api.ListBatchNotificationsRequestObject) (api.ListBatchNotificationsResponseObject, error) {
	f, err := listFilter(req.Params.Status, nil, nil, req.Params.Limit, req.Params.Cursor)
	if err != nil {
		return nil, err
	}
	page, err := h.svc.ListBatchNotifications(ctx, req.Id, f)
	if err != nil {
		return nil, err
	}
	return api.ListBatchNotifications200JSONResponse(toAPINotificationPage(page)), nil
}

// CancelBatch implements POST /api/v1/batches/{id}/cancel.
func (h *Handler) CancelBatch(ctx context.Context, req api.CancelBatchRequestObject) (api.CancelBatchResponseObject, error) {
	n, err := h.svc.CancelBatch(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	return api.CancelBatch200JSONResponse{BatchId: req.Id, Cancelled: int(n)}, nil
}

// ---- templates -------------------------------------------------------------

// CreateTemplate implements POST /api/v1/templates.
func (h *Handler) CreateTemplate(ctx context.Context, req api.CreateTemplateRequestObject) (api.CreateTemplateResponseObject, error) {
	in := toTemplateInput(req.Body)
	t, err := h.svc.CreateTemplate(ctx, &in)
	if err != nil {
		return nil, err
	}
	return api.CreateTemplate201JSONResponse(toAPITemplate(t)), nil
}

// ListTemplates implements GET /api/v1/templates.
func (h *Handler) ListTemplates(ctx context.Context, req api.ListTemplatesRequestObject) (api.ListTemplatesResponseObject, error) {
	f, err := listFilter(nil, req.Params.Channel, nil, req.Params.Limit, req.Params.Cursor)
	if err != nil {
		return nil, err
	}
	page, err := h.svc.ListTemplates(ctx, f.Channel, f.Limit, f.Cursor)
	if err != nil {
		return nil, err
	}
	out := api.TemplatePage{Items: make([]api.Template, len(page.Items)), HasMore: page.HasMore, NextCursor: nonZero(page.NextCursor)}
	for i, t := range page.Items {
		out.Items[i] = toAPITemplate(t)
	}
	return api.ListTemplates200JSONResponse(out), nil
}

// GetTemplate implements GET /api/v1/templates/{id}.
func (h *Handler) GetTemplate(ctx context.Context, req api.GetTemplateRequestObject) (api.GetTemplateResponseObject, error) {
	t, err := h.svc.GetTemplate(ctx, req.Id)
	if err != nil {
		return nil, err
	}
	return api.GetTemplate200JSONResponse(toAPITemplate(t)), nil
}

// UpdateTemplate implements PUT /api/v1/templates/{id}.
func (h *Handler) UpdateTemplate(ctx context.Context, req api.UpdateTemplateRequestObject) (api.UpdateTemplateResponseObject, error) {
	in := toTemplateInput(req.Body)
	t, err := h.svc.UpdateTemplate(ctx, req.Id, &in)
	if err != nil {
		return nil, err
	}
	return api.UpdateTemplate200JSONResponse(toAPITemplate(t)), nil
}

// DeleteTemplate implements DELETE /api/v1/templates/{id}.
func (h *Handler) DeleteTemplate(ctx context.Context, req api.DeleteTemplateRequestObject) (api.DeleteTemplateResponseObject, error) {
	if err := h.svc.DeleteTemplate(ctx, req.Id); err != nil {
		return nil, err
	}
	return api.DeleteTemplate204Response{}, nil
}

// PreviewTemplate implements POST /api/v1/templates/{id}/preview.
func (h *Handler) PreviewTemplate(ctx context.Context, req api.PreviewTemplateRequestObject) (api.PreviewTemplateResponseObject, error) {
	var vars map[string]any
	if req.Body.TemplateVars != nil {
		vars = *req.Body.TemplateVars
	}
	p, err := h.svc.PreviewTemplate(ctx, req.Id, vars)
	if err != nil {
		return nil, err
	}
	return api.PreviewTemplate200JSONResponse{Subject: nonZero(p.Subject), Content: p.Content, Segments: nonZero(p.Segments)}, nil
}

// ---- helpers ---------------------------------------------------------------

// listFilter validates the query parameters shared by list endpoints. The
// generated binding checks types (UUID, date-time) but not enums or ranges.
func listFilter(status *api.Status, channel *api.Channel, priority *api.Priority, limit *int, cursor *string) (postgres.NotificationFilter, error) {
	var (
		f    postgres.NotificationFilter
		errs []error
	)
	if status != nil {
		s, err := domain.ParseStatus(string(*status))
		if err != nil {
			errs = append(errs, validation.Field("status", "must be one of pending, scheduled, processing, sent, failed, cancelled"))
		}
		f.Status = &s
	}
	if channel != nil {
		c, err := domain.ParseChannel(string(*channel))
		if err != nil {
			errs = append(errs, validation.Field("channel", "must be one of sms, email, push"))
		}
		f.Channel = &c
	}
	if priority != nil {
		p, err := domain.ParsePriority(string(*priority))
		if err != nil || *priority == "" {
			errs = append(errs, validation.Field("priority", "must be one of high, normal, low"))
		}
		f.Priority = &p
	}
	if limit != nil {
		if *limit < 1 || *limit > postgres.MaxPageSize {
			errs = append(errs, validation.Field("limit", "must be between 1 and %d", postgres.MaxPageSize))
		}
		f.Limit = *limit
	}
	f.Cursor = deref(cursor)
	return f, validation.Merge(errs...)
}

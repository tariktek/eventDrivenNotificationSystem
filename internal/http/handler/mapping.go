package handler

import (
	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/api"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/service"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/validation"
)

// Conversions between generated API types and domain/service types.

func toNotificationRequest(b *api.NotificationCreate) validation.NotificationRequest {
	req := validation.NotificationRequest{
		Channel:        string(b.Channel),
		Recipient:      b.Recipient,
		Subject:        deref(b.Subject),
		Content:        deref(b.Content),
		ScheduledAt:    b.ScheduledAt,
		IdempotencyKey: deref(b.IdempotencyKey),
		MaxAttempts:    b.MaxAttempts,
		CallbackURL:    deref(b.CallbackUrl),
	}
	if b.Priority != nil {
		req.Priority = string(*b.Priority)
	}
	if b.TemplateId != nil {
		req.TemplateID = b.TemplateId.String()
	}
	if b.TemplateVars != nil {
		req.TemplateVars = *b.TemplateVars
	}
	if b.Metadata != nil {
		req.Metadata = *b.Metadata
	}
	return req
}

func toAPINotification(n *domain.Notification) api.Notification {
	out := api.Notification{
		Id:                n.ID,
		BatchId:           n.BatchID,
		Channel:           api.Channel(n.Channel),
		Priority:          api.Priority(n.Priority),
		Recipient:         n.Recipient,
		Subject:           nonZero(n.Subject),
		Content:           n.Content,
		Segments:          nonZero(n.Segments),
		TemplateId:        n.TemplateID,
		Status:            api.Status(n.Status),
		IdempotencyKey:    nonZero(n.IdempotencyKey),
		Attempts:          n.Attempts,
		MaxAttempts:       n.MaxAttempts,
		NextAttemptAt:     n.NextAttemptAt,
		ScheduledAt:       n.ScheduledAt,
		SentAt:            n.SentAt,
		FailedAt:          n.FailedAt,
		CancelledAt:       n.CancelledAt,
		ProviderMessageId: nonZero(n.ProviderMessageID),
		LastError:         nonZero(n.LastError),
		LastErrorCode:     nonZero(n.LastErrorCode),
		CallbackUrl:       nonZero(n.CallbackURL),
		Metadata:          n.Metadata,
		CreatedAt:         n.CreatedAt,
		UpdatedAt:         n.UpdatedAt,
	}
	if n.TemplateID != nil {
		out.TemplateVersion = &n.TemplateVersion
	}
	if out.Metadata == nil {
		out.Metadata = api.Metadata{}
	}
	return out
}

func toAPINotificationDetail(n *domain.Notification, attempts []domain.Attempt) api.NotificationDetail {
	b := toAPINotification(n)
	out := api.NotificationDetail{
		Id: b.Id, BatchId: b.BatchId, Channel: b.Channel, Priority: b.Priority, Recipient: b.Recipient,
		Subject: b.Subject, Content: b.Content, Segments: b.Segments, TemplateId: b.TemplateId,
		TemplateVersion: b.TemplateVersion, Status: b.Status, IdempotencyKey: b.IdempotencyKey,
		Attempts: b.Attempts, MaxAttempts: b.MaxAttempts, NextAttemptAt: b.NextAttemptAt,
		ScheduledAt: b.ScheduledAt, SentAt: b.SentAt, FailedAt: b.FailedAt, CancelledAt: b.CancelledAt,
		ProviderMessageId: b.ProviderMessageId, LastError: b.LastError, LastErrorCode: b.LastErrorCode,
		CallbackUrl: b.CallbackUrl, Metadata: b.Metadata, CreatedAt: b.CreatedAt, UpdatedAt: b.UpdatedAt,
		DeliveryAttempts: make([]api.DeliveryAttempt, len(attempts)),
	}
	for i := range attempts {
		a := &attempts[i]
		out.DeliveryAttempts[i] = api.DeliveryAttempt{
			AttemptNo:         a.AttemptNo,
			Outcome:           api.DeliveryAttemptOutcome(a.Outcome),
			StatusCode:        nonZero(a.StatusCode),
			ErrorCode:         nonZero(a.ErrorCode),
			Error:             nonZero(a.Error),
			ProviderMessageId: nonZero(a.ProviderMessageID),
			DurationMs:        int(a.Duration.Milliseconds()),
			CreatedAt:         a.CreatedAt,
		}
	}
	return out
}

func toAPINotificationPage(p postgres.Page[*domain.Notification]) api.NotificationPage {
	out := api.NotificationPage{
		Items:      make([]api.Notification, len(p.Items)),
		HasMore:    p.HasMore,
		NextCursor: nonZero(p.NextCursor),
	}
	for i, n := range p.Items {
		out.Items[i] = toAPINotification(n)
	}
	return out
}

func toAPIBatch(s *domain.BatchSummary) api.Batch {
	md := api.Metadata(s.Metadata)
	if md == nil {
		md = api.Metadata{}
	}
	return api.Batch{
		Id:    s.ID,
		Total: s.Total,
		Counts: api.BatchCounts{
			Pending:    s.Counts[domain.StatusPending],
			Scheduled:  s.Counts[domain.StatusScheduled],
			Processing: s.Counts[domain.StatusProcessing],
			Sent:       s.Counts[domain.StatusSent],
			Failed:     s.Counts[domain.StatusFailed],
			Cancelled:  s.Counts[domain.StatusCancelled],
		},
		Progress:    s.Progress(),
		CompletedAt: s.CompletedAt,
		Metadata:    md,
		CreatedAt:   s.CreatedAt,
	}
}

func toAPIBatchCreated(r *service.BatchResult) api.BatchCreated {
	out := api.BatchCreated{
		BatchId:       r.Batch.ID,
		Total:         r.Batch.Total,
		Notifications: make([]api.BatchCreatedItem, len(r.Items)),
	}
	for i, it := range r.Items {
		out.Notifications[i] = api.BatchCreatedItem{
			Id:        it.Notification.ID,
			Status:    api.Status(it.Notification.Status),
			Duplicate: it.Duplicate,
		}
	}
	return out
}

func toTemplateInput(b *api.TemplateWrite) validation.TemplateInput {
	return validation.TemplateInput{
		Name:    b.Name,
		Channel: string(b.Channel),
		Subject: deref(b.Subject),
		Body:    b.Body,
	}
}

func toAPITemplate(t *domain.Template) api.Template {
	return api.Template{
		Id:        t.ID,
		Name:      t.Name,
		Channel:   api.Channel(t.Channel),
		Subject:   nonZero(t.Subject),
		Body:      t.Body,
		Variables: t.Variables,
		Version:   t.Version,
		CreatedAt: t.CreatedAt,
		UpdatedAt: t.UpdatedAt,
	}
}

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}

// nonZero returns nil for a zero value so optional fields are omitted from
// JSON instead of being sent as "" or 0.
func nonZero[T comparable](v T) *T {
	var zero T
	if v == zero {
		return nil
	}
	return &v
}

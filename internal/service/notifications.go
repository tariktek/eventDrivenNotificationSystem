package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/msgtemplate"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/validation"
)

// CreateResult is a stored notification and whether it already existed
// under the same idempotency key.
type CreateResult struct {
	Notification *domain.Notification
	Duplicate    bool
}

// BatchResult is a stored batch and its items, in request order.
type BatchResult struct {
	Batch *domain.Batch
	Items []CreateResult
}

// CreateNotification validates, renders and stores one notification.
func (s *Service) CreateNotification(ctx context.Context, req *validation.NotificationRequest, meta RequestMeta) (CreateResult, error) {
	now := s.clock()
	p, err := validation.Notification(req, now, s.opts, "")
	if err != nil {
		return CreateResult{}, err
	}
	n, err := s.build(ctx, &p, "", nil, now, meta, newTemplateCache(s.store.Repo))
	if err != nil {
		return CreateResult{}, err
	}

	res, err := s.store.InsertNotifications(ctx, []*domain.Notification{n})
	if err != nil {
		return CreateResult{}, err
	}
	return CreateResult{Notification: res[0].Notification, Duplicate: res[0].Duplicate}, nil
}

// CreateBatch validates every item, then stores the batch and all its
// notifications in one transaction: either all are created or none.
func (s *Service) CreateBatch(ctx context.Context, items []validation.NotificationRequest, metadata map[string]any, meta RequestMeta) (BatchResult, error) {
	now := s.clock()
	parsed, itemErr := validation.Batch(items, now, s.opts)
	if err := validation.Merge(validation.Metadata(metadata, "metadata"), itemErr); err != nil {
		return BatchResult{}, err
	}

	batchID, err := uuid.NewV7()
	if err != nil {
		return BatchResult{}, fmt.Errorf("generate batch id: %w", err)
	}
	batch := &domain.Batch{
		ID:            batchID,
		Total:         len(parsed),
		Metadata:      metadata,
		CorrelationID: meta.CorrelationID,
		CreatedAt:     now,
	}

	// Render every item before touching the database, collecting all
	// template problems so the client sees them in one response.
	cache := newTemplateCache(s.store.Repo)
	ns := make([]*domain.Notification, len(parsed))
	var problems []error
	for i := range parsed {
		n, err := s.build(ctx, &parsed[i], fmt.Sprintf("items[%d]", i), &batchID, now, meta, cache)
		var verrs validation.Errors
		switch {
		case errors.As(err, &verrs):
			problems = append(problems, err)
		case err != nil:
			return BatchResult{}, err
		default:
			ns[i] = n
		}
	}
	if err := validation.Merge(problems...); err != nil {
		return BatchResult{}, err
	}

	var inserted []postgres.InsertResult
	err = s.store.InTx(ctx, func(r *postgres.Repo) error {
		if err := r.InsertBatch(ctx, batch); err != nil {
			return err
		}
		inserted, err = r.InsertNotifications(ctx, ns)
		return err
	})
	if err != nil {
		return BatchResult{}, err
	}

	out := BatchResult{Batch: batch, Items: make([]CreateResult, len(inserted))}
	for i, x := range inserted {
		out.Items[i] = CreateResult{Notification: x.Notification, Duplicate: x.Duplicate}
	}
	return out, nil
}

// build turns a validated request into a notification, rendering its
// template if it has one. field prefixes error paths ("items[3]").
func (s *Service) build(ctx context.Context, p *validation.ParsedNotification, field string, batchID *uuid.UUID,
	now time.Time, meta RequestMeta, cache *templateCache,
) (*domain.Notification, error) {
	params := domain.NewNotificationParams{
		BatchID:        batchID,
		Channel:        p.Channel,
		Priority:       p.Priority,
		Recipient:      p.Recipient,
		Subject:        p.Subject,
		Content:        p.Content,
		Segments:       p.Segments,
		IdempotencyKey: p.IdempotencyKey,
		MaxAttempts:    p.MaxAttempts,
		ScheduledAt:    p.ScheduledAt,
		CallbackURL:    p.CallbackURL,
		Metadata:       p.Metadata,
		CorrelationID:  meta.CorrelationID,
		TraceParent:    meta.TraceParent,
	}

	if p.TemplateID != nil {
		tpl, compiled, err := cache.get(ctx, *p.TemplateID, validation.Path(field, "template_id"))
		if err != nil {
			return nil, err
		}
		if tpl.Channel != p.Channel {
			return nil, validation.Field(validation.Path(field, "template_id"),
				"template %q is for %s, not %s", tpl.Name, tpl.Channel, p.Channel)
		}
		subject, content, segments, err := render(compiled, tpl.Channel, p.TemplateVars, field)
		if err != nil {
			return nil, err
		}
		params.Subject, params.Content, params.Segments = subject, content, segments
		params.TemplateID, params.TemplateVersion, params.TemplateVars = &tpl.ID, tpl.Version, p.TemplateVars
	}

	return domain.NewNotification(params, now)
}

// render executes a template and checks the result against channel limits.
// Every failure is a validation error on the request's fields.
func render(t *msgtemplate.Template, ch domain.Channel, vars map[string]any, field string) (subject, content string, segments int, err error) {
	subject, content, err = t.Render(vars)
	if err != nil {
		var missing *msgtemplate.MissingVariablesError
		if errors.As(err, &missing) || errors.Is(err, msgtemplate.ErrOutputTooLarge) {
			return "", "", 0, validation.Field(validation.Path(field, "template_vars"), "%s", err.Error())
		}
		// Execution errors stem from the supplied values (e.g. ranging over
		// a string), so they are the client's to fix.
		return "", "", 0, validation.Field(validation.Path(field, "template_vars"), "cannot render template: %s", err.Error())
	}
	segments, err = validation.Content(ch, subject, content, "")
	if err != nil {
		// The client never sent this content; report it against the values
		// that produced it, which is what they can change.
		var verrs validation.Errors
		if !errors.As(err, &verrs) {
			return "", "", 0, err
		}
		out := make(validation.Errors, len(verrs))
		for i, fe := range verrs {
			out[i] = validation.FieldError{
				Field:  validation.Path(field, "template_vars"),
				Reason: "rendered " + fe.Field + " " + fe.Reason,
			}
		}
		return "", "", 0, out
	}
	return subject, content, segments, nil
}

// GetNotification returns a notification and its delivery attempts.
func (s *Service) GetNotification(ctx context.Context, id uuid.UUID) (*domain.Notification, []domain.Attempt, error) {
	n, err := s.store.GetNotification(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	attempts, err := s.store.ListAttempts(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	return n, attempts, nil
}

// ListNotifications returns one page of notifications.
func (s *Service) ListNotifications(ctx context.Context, f postgres.NotificationFilter) (postgres.Page[*domain.Notification], error) {
	return s.store.ListNotifications(ctx, f)
}

// CancelNotification cancels a notification that no worker has picked up.
func (s *Service) CancelNotification(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	return s.store.CancelNotification(ctx, id, s.clock())
}

// RetryNotification re-queues a failed notification.
func (s *Service) RetryNotification(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	return s.store.RetryNotification(ctx, id, s.clock())
}

// GetBatch returns a batch with per-status counts.
func (s *Service) GetBatch(ctx context.Context, id uuid.UUID) (*domain.BatchSummary, error) {
	return s.store.GetBatchSummary(ctx, id)
}

// ListBatchNotifications lists a batch's notifications, or returns
// domain.ErrNotFound if the batch does not exist (rather than an empty page).
func (s *Service) ListBatchNotifications(ctx context.Context, id uuid.UUID, f postgres.NotificationFilter) (postgres.Page[*domain.Notification], error) {
	ok, err := s.store.BatchExists(ctx, id)
	if err != nil {
		return postgres.Page[*domain.Notification]{}, err
	}
	if !ok {
		return postgres.Page[*domain.Notification]{}, fmt.Errorf("batch %s: %w", id, domain.ErrNotFound)
	}
	f.BatchID = &id
	return s.store.ListNotifications(ctx, f)
}

// CancelBatch cancels the batch's pending and scheduled notifications.
func (s *Service) CancelBatch(ctx context.Context, id uuid.UUID) (int64, error) {
	return s.store.CancelBatch(ctx, id, s.clock())
}

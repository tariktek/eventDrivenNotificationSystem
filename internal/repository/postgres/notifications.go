package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres/sqlcgen"
)

// InsertResult is the outcome of inserting one notification.
type InsertResult struct {
	Notification *domain.Notification
	// Duplicate is true when a notification with the same idempotency key
	// already existed; Notification is then the existing one.
	Duplicate bool
}

// insertNotificationsSQL inserts many rows in one round trip by passing one
// array per column and expanding them with unnest. Unlike COPY, it supports
// ON CONFLICT, so a concurrent request with the same idempotency key makes
// that row a no-op instead of failing the whole batch.
const insertNotificationsSQL = `
INSERT INTO notifications (
    id, batch_id, channel, priority, recipient, subject, content, segments,
    template_id, template_version, template_vars, status, idempotency_key,
    max_attempts, next_attempt_at, scheduled_at, callback_url, metadata,
    correlation_id, trace_parent, created_at, updated_at)
SELECT
    t.id, t.batch_id, t.channel, t.priority, t.recipient, t.subject, t.content, t.segments,
    t.template_id, t.template_version, t.template_vars::jsonb, t.status, t.idempotency_key,
    t.max_attempts, t.next_attempt_at, t.scheduled_at, t.callback_url, t.metadata::jsonb,
    t.correlation_id, t.trace_parent, t.created_at, t.created_at
FROM unnest(
    $1::uuid[], $2::uuid[], $3::text[], $4::smallint[], $5::text[], $6::text[], $7::text[], $8::smallint[],
    $9::uuid[], $10::int[], $11::text[], $12::text[], $13::text[],
    $14::int[], $15::timestamptz[], $16::timestamptz[], $17::text[], $18::text[],
    $19::text[], $20::text[], $21::timestamptz[]
) AS t(id, batch_id, channel, priority, recipient, subject, content, segments,
       template_id, template_version, template_vars, status, idempotency_key,
       max_attempts, next_attempt_at, scheduled_at, callback_url, metadata,
       correlation_id, trace_parent, created_at)
ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING *`

// InsertNotifications stores notifications in one statement and reports,
// in input order, whether each was created or already existed (same
// idempotency key). Call it inside InTx together with InsertBatch so a
// batch is all-or-nothing.
func (r *Repo) InsertNotifications(ctx context.Context, ns []*domain.Notification) ([]InsertResult, error) {
	if len(ns) == 0 {
		return nil, nil
	}

	n := len(ns)
	var (
		ids, templateIDs, batchIDs   = make([]uuid.UUID, n), make([]*uuid.UUID, n), make([]*uuid.UUID, n)
		channels, recipients, bodies = make([]string, n), make([]string, n), make([]string, n)
		statuses, metadata           = make([]string, n), make([]string, n)
		subjects, templateVars, keys = make([]*string, n), make([]*string, n), make([]*string, n)
		callbacks, corrIDs, traces   = make([]*string, n), make([]*string, n), make([]*string, n)
		priorities                   = make([]int16, n)
		segments                     = make([]*int16, n)
		templateVersions             = make([]*int32, n)
		maxAttempts                  = make([]int32, n)
		nextAttempts, created        = make([]time.Time, n), make([]time.Time, n)
		scheduled                    = make([]*time.Time, n)
	)
	for i, x := range ns {
		md, err := marshalObject(x.Metadata)
		if err != nil {
			return nil, fmt.Errorf("encode metadata: %w", err)
		}
		if x.TemplateVars != nil {
			tv, err := marshalObject(x.TemplateVars)
			if err != nil {
				return nil, fmt.Errorf("encode template_vars: %w", err)
			}
			templateVars[i] = new(string(tv))
		}
		if x.TemplateID != nil {
			v, err := narrow[int32](x.TemplateVersion, "template_version")
			if err != nil {
				return nil, err
			}
			templateVersions[i] = &v
		}
		seg, err := narrow[int16](x.Segments, "segments")
		if err != nil {
			return nil, err
		}
		maxAtt, err := narrow[int32](x.MaxAttempts, "max_attempts")
		if err != nil {
			return nil, err
		}

		ids[i], batchIDs[i], templateIDs[i] = x.ID, x.BatchID, x.TemplateID
		channels[i], recipients[i], bodies[i] = string(x.Channel), x.Recipient, x.Content
		statuses[i], metadata[i] = string(x.Status), string(md)
		subjects[i], keys[i] = nilIfZero(x.Subject), nilIfZero(x.IdempotencyKey)
		callbacks[i], corrIDs[i], traces[i] = nilIfZero(x.CallbackURL), nilIfZero(x.CorrelationID), nilIfZero(x.TraceParent)
		priorities[i] = x.Priority.Rank()
		segments[i] = nilIfZero(seg)
		maxAttempts[i] = maxAtt
		nextAttempts[i], created[i], scheduled[i] = x.NextAttemptAt, x.CreatedAt, x.ScheduledAt
	}

	rows, err := r.db.Query(ctx, insertNotificationsSQL,
		ids, batchIDs, channels, priorities, recipients, subjects, bodies, segments,
		templateIDs, templateVersions, templateVars, statuses, keys,
		maxAttempts, nextAttempts, scheduled, callbacks, metadata,
		corrIDs, traces, created)
	if err != nil {
		return nil, fmt.Errorf("insert notifications: %w", err)
	}
	inserted, err := pgx.CollectRows(rows, pgx.RowToStructByPos[sqlcgen.Notification])
	if err != nil {
		return nil, fmt.Errorf("insert notifications: %w", err)
	}

	byID := make(map[uuid.UUID]*sqlcgen.Notification, len(inserted))
	for i := range inserted {
		byID[inserted[i].ID] = &inserted[i]
	}

	// Rows skipped by ON CONFLICT already exist under the same key.
	var existing map[string]*sqlcgen.Notification
	if len(inserted) < n {
		var dupKeys []string
		for _, x := range ns {
			if _, ok := byID[x.ID]; !ok && x.IdempotencyKey != "" {
				dupKeys = append(dupKeys, x.IdempotencyKey)
			}
		}
		found, err := r.q.GetNotificationsByIdempotencyKeys(ctx, dupKeys)
		if err != nil {
			return nil, fmt.Errorf("load existing notifications: %w", err)
		}
		existing = make(map[string]*sqlcgen.Notification, len(found))
		for i := range found {
			existing[*found[i].IdempotencyKey] = &found[i]
		}
	}

	results := make([]InsertResult, n)
	for i, x := range ns {
		row, dup := byID[x.ID], false
		if row == nil {
			row, dup = existing[x.IdempotencyKey], true
		}
		if row == nil {
			return nil, fmt.Errorf("notification %s was neither inserted nor found", x.ID)
		}
		dn, err := toNotification(row)
		if err != nil {
			return nil, err
		}
		results[i] = InsertResult{Notification: dn, Duplicate: dup}
	}
	return results, nil
}

// GetNotification returns domain.ErrNotFound if id does not exist.
func (r *Repo) GetNotification(ctx context.Context, id uuid.UUID) (*domain.Notification, error) {
	row, err := r.q.GetNotification(ctx, id)
	if err != nil {
		return nil, notFound(err, "get notification")
	}
	return toNotification(&row)
}

// ListAttempts returns a notification's delivery attempts, oldest first.
func (r *Repo) ListAttempts(ctx context.Context, id uuid.UUID) ([]domain.Attempt, error) {
	rows, err := r.q.ListAttempts(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("list attempts: %w", err)
	}
	out := make([]domain.Attempt, len(rows))
	for i := range rows {
		out[i] = toAttempt(&rows[i])
	}
	return out, nil
}

// CancelNotification cancels a pending or scheduled notification. It
// returns domain.ErrNotFound, or a *domain.TransitionError when the
// notification is in a status that cannot be cancelled.
func (r *Repo) CancelNotification(ctx context.Context, id uuid.UUID, now time.Time) (*domain.Notification, error) {
	row, err := r.q.CancelNotification(ctx, sqlcgen.CancelNotificationParams{ID: id, Now: &now})
	if err != nil {
		return nil, r.explainNoTransition(ctx, err, id, domain.StatusCancelled)
	}
	return toNotification(&row)
}

// RetryNotification moves a failed notification back to pending with a
// fresh attempt budget.
func (r *Repo) RetryNotification(ctx context.Context, id uuid.UUID, now time.Time) (*domain.Notification, error) {
	row, err := r.q.RetryNotification(ctx, sqlcgen.RetryNotificationParams{ID: id, Now: now})
	if err != nil {
		return nil, r.explainNoTransition(ctx, err, id, domain.StatusPending)
	}
	return toNotification(&row)
}

// explainNoTransition turns "the conditional UPDATE matched no row" into
// either ErrNotFound or a TransitionError naming the current status.
func (r *Repo) explainNoTransition(ctx context.Context, err error, id uuid.UUID, to domain.Status) error {
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("update notification: %w", err)
	}
	status, err := r.q.GetNotificationStatus(ctx, id)
	if err != nil {
		return notFound(err, "get notification status")
	}
	return &domain.TransitionError{From: domain.Status(status), To: to}
}

// NotificationFilter selects notifications for ListNotifications. Nil or
// zero fields do not filter.
type NotificationFilter struct {
	Status      *domain.Status
	Channel     *domain.Channel
	Priority    *domain.Priority
	BatchID     *uuid.UUID
	CreatedFrom *time.Time // inclusive
	CreatedTo   *time.Time // exclusive
	Limit       int
	Cursor      string
}

// ListNotifications returns one page, newest first.
//
// The query is assembled from fixed fragments; every value is a bind
// parameter, so the builder cannot introduce SQL injection.
func (r *Repo) ListNotifications(ctx context.Context, f NotificationFilter) (Page[*domain.Notification], error) {
	after, err := decodeCursor(f.Cursor)
	if err != nil {
		return Page[*domain.Notification]{}, err
	}
	limit := clampLimit(f.Limit)

	var (
		where []string
		args  []any
	)
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	if f.Status != nil {
		where = append(where, "status = "+arg(string(*f.Status)))
	}
	if f.Channel != nil {
		where = append(where, "channel = "+arg(string(*f.Channel)))
	}
	if f.Priority != nil {
		where = append(where, "priority = "+arg(f.Priority.Rank()))
	}
	if f.BatchID != nil {
		where = append(where, "batch_id = "+arg(*f.BatchID))
	}
	if f.CreatedFrom != nil {
		where = append(where, "created_at >= "+arg(*f.CreatedFrom))
	}
	if f.CreatedTo != nil {
		where = append(where, "created_at < "+arg(*f.CreatedTo))
	}
	if after != nil {
		where = append(where, fmt.Sprintf("(created_at, id) < (%s, %s)", arg(after.CreatedAt), arg(after.ID)))
	}

	var sb strings.Builder
	sb.WriteString("SELECT * FROM notifications")
	if len(where) > 0 {
		sb.WriteString(" WHERE " + strings.Join(where, " AND "))
	}
	sb.WriteString(" ORDER BY created_at DESC, id DESC LIMIT " + arg(limit+1))

	rows, err := r.db.Query(ctx, sb.String(), args...)
	if err != nil {
		return Page[*domain.Notification]{}, fmt.Errorf("list notifications: %w", err)
	}
	raw, err := pgx.CollectRows(rows, pgx.RowToStructByPos[sqlcgen.Notification])
	if err != nil {
		return Page[*domain.Notification]{}, fmt.Errorf("list notifications: %w", err)
	}
	items, err := toNotifications(raw)
	if err != nil {
		return Page[*domain.Notification]{}, err
	}
	return paginate(items, limit, func(n *domain.Notification) cursor {
		return cursor{CreatedAt: n.CreatedAt, ID: n.ID}
	}), nil
}

package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/events"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres/sqlcgen"
)

// outboxLockKey is the advisory lock that elects the publishing relay.
// Any constant works as long as nothing else uses it.
const outboxLockKey int64 = 0x6f7574626f78 // "outbox"

// TryOutboxLock takes the relay's lock for the current transaction and
// reports whether it got it. Only valid inside InTx.
func (r *Repo) TryOutboxLock(ctx context.Context) (bool, error) {
	ok, err := r.q.TryOutboxLock(ctx, outboxLockKey)
	if err != nil {
		return false, fmt.Errorf("try outbox lock: %w", err)
	}
	return ok, nil
}

// UnpublishedEvents returns up to limit unpublished events, oldest first.
func (r *Repo) UnpublishedEvents(ctx context.Context, limit int) ([]events.Event, error) {
	n, err := narrow[int32](limit, "limit")
	if err != nil {
		return nil, err
	}
	rows, err := r.q.ListUnpublishedEvents(ctx, n)
	if err != nil {
		return nil, fmt.Errorf("list unpublished events: %w", err)
	}
	out := make([]events.Event, len(rows))
	for i, row := range rows {
		out[i] = events.Event{
			ID:            row.ID,
			Type:          row.EventType,
			AggregateType: row.AggregateType,
			AggregateID:   row.AggregateID,
			Payload:       row.Payload,
			CreatedAt:     row.CreatedAt.UTC(),
		}
	}
	return out, nil
}

// MarkEventsPublished stamps events as published.
func (r *Repo) MarkEventsPublished(ctx context.Context, ids []int64, now time.Time) error {
	if err := r.q.MarkEventsPublished(ctx, sqlcgen.MarkEventsPublishedParams{Ids: ids, Now: &now}); err != nil {
		return fmt.Errorf("mark events published: %w", err)
	}
	return nil
}

// PrunePublishedEvents deletes up to limit events published before the
// given time and returns how many it deleted.
func (r *Repo) PrunePublishedEvents(ctx context.Context, before time.Time, limit int) (int64, error) {
	n, err := narrow[int32](limit, "limit")
	if err != nil {
		return 0, err
	}
	deleted, err := r.q.PrunePublishedEvents(ctx, sqlcgen.PrunePublishedEventsParams{Before: before, MaxRows: n})
	if err != nil {
		return 0, fmt.Errorf("prune published events: %w", err)
	}
	return deleted, nil
}

// OutboxBacklog reports how many events wait to be published and when the
// oldest of them was created (now, if none wait).
func (r *Repo) OutboxBacklog(ctx context.Context) (count int64, oldest time.Time, err error) {
	row, err := r.q.OutboxBacklog(ctx)
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("outbox backlog: %w", err)
	}
	return row.Events, row.Oldest, nil
}

package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres/sqlcgen"
)

// InsertBatch stores a batch row. Call it inside InTx before inserting the
// batch's notifications.
func (r *Repo) InsertBatch(ctx context.Context, b *domain.Batch) error {
	md, err := marshalObject(b.Metadata)
	if err != nil {
		return fmt.Errorf("encode batch metadata: %w", err)
	}
	total, err := narrow[int32](b.Total, "total")
	if err != nil {
		return err
	}
	err = r.q.InsertBatch(ctx, sqlcgen.InsertBatchParams{
		ID:            b.ID,
		Total:         total,
		Metadata:      md,
		CorrelationID: nilIfZero(b.CorrelationID),
		CreatedAt:     b.CreatedAt,
	})
	if err != nil {
		return fmt.Errorf("insert batch: %w", err)
	}
	return nil
}

// GetBatchSummary returns a batch with live per-status counts. Every status
// is present in Counts, zero if no notification has it.
func (r *Repo) GetBatchSummary(ctx context.Context, id uuid.UUID) (*domain.BatchSummary, error) {
	row, err := r.q.GetBatch(ctx, id)
	if err != nil {
		return nil, notFound(err, "get batch")
	}
	batch, err := toBatch(&row)
	if err != nil {
		return nil, err
	}

	counts, err := r.q.CountBatchStatuses(ctx, &id)
	if err != nil {
		return nil, fmt.Errorf("count batch statuses: %w", err)
	}
	s := &domain.BatchSummary{Batch: batch, Counts: make(map[domain.Status]int, len(domain.Statuses))}
	for _, st := range domain.Statuses {
		s.Counts[st] = 0
	}
	for _, c := range counts {
		s.Counts[domain.Status(c.Status)] = int(c.Count)
	}
	return s, nil
}

// CancelBatch cancels every pending or scheduled notification in a batch
// and returns how many were cancelled.
func (r *Repo) CancelBatch(ctx context.Context, id uuid.UUID, now time.Time) (int64, error) {
	exists, err := r.q.BatchExists(ctx, id)
	if err != nil {
		return 0, fmt.Errorf("check batch: %w", err)
	}
	if !exists {
		return 0, fmt.Errorf("cancel batch: %w", domain.ErrNotFound)
	}
	n, err := r.q.CancelBatchNotifications(ctx, sqlcgen.CancelBatchNotificationsParams{BatchID: &id, Now: &now})
	if err != nil {
		return 0, fmt.Errorf("cancel batch: %w", err)
	}
	return n, nil
}

// BatchExists reports whether a batch with the given id exists.
func (r *Repo) BatchExists(ctx context.Context, id uuid.UUID) (bool, error) {
	ok, err := r.q.BatchExists(ctx, id)
	if err != nil {
		return false, fmt.Errorf("check batch: %w", err)
	}
	return ok, nil
}

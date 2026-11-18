package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres/sqlcgen"
)

// Row types of the statistics queries, re-exported for package stats.
type (
	QueueDepthRow      = sqlcgen.QueueDepthRow
	DeliveryWindowsRow = sqlcgen.DeliveryWindowsRow
	DeliveryLatencyRow = sqlcgen.DeliveryLatencyRow
)

// QueueDepth counts pending, scheduled and processing notifications by
// channel, priority and status.
func (r *Repo) QueueDepth(ctx context.Context, now time.Time) ([]QueueDepthRow, error) {
	rows, err := r.q.QueueDepth(ctx, now)
	if err != nil {
		return nil, fmt.Errorf("queue depth: %w", err)
	}
	return rows, nil
}

// DeliveryWindows counts deliveries finished in the last 1/5/15 minutes by
// channel.
func (r *Repo) DeliveryWindows(ctx context.Context, now time.Time) ([]DeliveryWindowsRow, error) {
	rows, err := r.q.DeliveryWindows(ctx, now)
	if err != nil {
		return nil, fmt.Errorf("delivery windows: %w", err)
	}
	return rows, nil
}

// DeliveryLatency returns latency percentiles of the last 15 minutes by
// channel and for "all".
func (r *Repo) DeliveryLatency(ctx context.Context, now time.Time) ([]DeliveryLatencyRow, error) {
	rows, err := r.q.DeliveryLatency(ctx, now)
	if err != nil {
		return nil, fmt.Errorf("delivery latency: %w", err)
	}
	return rows, nil
}

// PendingCallbacks counts status callbacks not yet delivered or given up.
func (r *Repo) PendingCallbacks(ctx context.Context) (int64, error) {
	n, err := r.q.PendingCallbacks(ctx)
	if err != nil {
		return 0, fmt.Errorf("pending callbacks: %w", err)
	}
	return n, nil
}

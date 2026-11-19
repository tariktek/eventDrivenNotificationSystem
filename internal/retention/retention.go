// Package retention deletes data the system no longer needs, so the
// database does not grow without bound and personal data (recipients and
// message content) is not kept longer than necessary.
//
// Every worker runs a Job; a transaction-scoped advisory lock lets one of
// them work at a time, as for the outbox relay. Deletes are bounded chunks,
// each in its own short transaction, so retention never holds locks the
// delivery pipeline needs.
//
// What goes, every Interval:
//   - notifications in a final status (sent, failed, cancelled) for longer
//     than Notifications, with their attempts and callback deliveries
//     (ON DELETE CASCADE);
//   - batches older than that with no notification left;
//   - idempotency keys past their TTL.
//
// Published outbox events are pruned by the relay (OUTBOX_RETENTION).
package retention

import (
	"context"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
)

// Config tunes a Job.
type Config struct {
	// Notifications is how long a notification is kept after it reached a
	// final status. Zero keeps them forever (batches and expired
	// idempotency keys are still cleaned up).
	Notifications time.Duration
	// Interval separates rounds.
	Interval time.Duration
	// ChunkSize bounds the rows one delete statement removes.
	ChunkSize int
}

// Result counts what one round deleted.
type Result struct {
	Notifications   int64
	Batches         int64
	IdempotencyKeys int64
}

// Job runs retention rounds.
type Job struct {
	store   *postgres.Store
	cfg     Config
	deleted *prometheus.CounterVec // label: table
	log     *slog.Logger
	now     func() time.Time
}

// New returns a Job. deleted may be nil.
func New(store *postgres.Store, cfg Config, deleted *prometheus.CounterVec, log *slog.Logger) *Job {
	return &Job{store: store, cfg: cfg, deleted: deleted, log: log, now: time.Now}
}

// Run runs a round now and then every Interval until ctx is done.
func (j *Job) Run(ctx context.Context) {
	t := time.NewTicker(j.cfg.Interval)
	defer t.Stop()
	for {
		res, err := j.RunOnce(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			j.log.WarnContext(ctx, "retention round failed; will retry next interval", "error", err)
		case res.Notifications+res.Batches+res.IdempotencyKeys > 0:
			j.log.InfoContext(ctx, "retention deleted expired data",
				"notifications", res.Notifications, "batches", res.Batches, "idempotency_keys", res.IdempotencyKeys)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// RunOnce deletes everything currently expired, chunk by chunk. If another
// worker holds the retention lock it returns at once with nothing deleted.
func (j *Job) RunOnce(ctx context.Context) (Result, error) {
	var res Result
	now := j.now()
	steps := []struct {
		table string
		count *int64
		del   func(*postgres.Repo) (int64, error)
	}{
		{"notifications", &res.Notifications, func(r *postgres.Repo) (int64, error) {
			if j.cfg.Notifications <= 0 {
				return 0, nil
			}
			return r.DeleteExpiredNotifications(ctx, now.Add(-j.cfg.Notifications), j.cfg.ChunkSize)
		}},
		// After notifications, so batches emptied this round go too.
		{"batches", &res.Batches, func(r *postgres.Repo) (int64, error) {
			if j.cfg.Notifications <= 0 {
				return 0, nil
			}
			return r.DeleteEmptyBatches(ctx, now.Add(-j.cfg.Notifications), j.cfg.ChunkSize)
		}},
		{"idempotency_keys", &res.IdempotencyKeys, func(r *postgres.Repo) (int64, error) {
			return r.DeleteExpiredIdempotencyKeys(ctx, now, j.cfg.ChunkSize)
		}},
	}
	for _, s := range steps {
		for ctx.Err() == nil {
			n, leader, err := j.chunk(ctx, s.del)
			if err != nil || !leader {
				return res, err
			}
			*s.count += n
			if j.deleted != nil && n > 0 {
				j.deleted.WithLabelValues(s.table).Add(float64(n))
			}
			if n < int64(j.cfg.ChunkSize) {
				break // nothing more expired in this table
			}
		}
	}
	return res, ctx.Err()
}

// chunk runs one bounded delete in its own transaction, under the lock.
func (j *Job) chunk(ctx context.Context, del func(*postgres.Repo) (int64, error)) (n int64, leader bool, err error) {
	err = j.store.InTx(ctx, func(r *postgres.Repo) error {
		if leader, err = r.TryRetentionLock(ctx); err != nil || !leader {
			return err
		}
		n, err = del(r)
		return err
	})
	return n, leader, err
}

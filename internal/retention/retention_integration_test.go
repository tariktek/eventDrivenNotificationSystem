//go:build integration

package retention_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/retention"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/testutil/pgtest"
)

var (
	pool  *pgxpool.Pool
	store *postgres.Store
	quiet = slog.New(slog.NewTextHandler(io.Discard, nil))
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	pg, err := pgtest.Start(ctx)
	if err == nil {
		err = pgtest.MigrateUp(pg.DSN)
	}
	if err == nil {
		pool, err = pgxpool.New(ctx, pg.DSN)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	store = postgres.NewStore(pool)
	code := m.Run()
	pool.Close()
	_ = pg.Terminate()
	os.Exit(code)
}

const month = 30 * 24 * time.Hour

func reset(t *testing.T) {
	t.Helper()
	_, err := pool.Exec(t.Context(),
		`TRUNCATE notifications, batches, notification_attempts, outbox_events, callback_deliveries, idempotency_keys CASCADE`)
	require.NoError(t, err)
}

// notification stores an SMS and moves it to status, as if it got there
// `age` ago. updated_at is maintained by a trigger, so it is set with the
// trigger disabled.
func notification(t *testing.T, status domain.Status, age time.Duration, batch *uuid.UUID) uuid.UUID {
	t.Helper()
	ctx := t.Context()
	n, err := domain.NewNotification(domain.NewNotificationParams{
		Channel: domain.ChannelSMS, Recipient: "+905551234567", Content: "retention", BatchID: batch,
	}, time.Now().Add(-age))
	require.NoError(t, err)
	_, err = store.InsertNotifications(ctx, []*domain.Notification{n})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `ALTER TABLE notifications DISABLE TRIGGER notifications_set_updated_at`)
	require.NoError(t, err)
	defer func() {
		_, err := pool.Exec(context.WithoutCancel(ctx), `ALTER TABLE notifications ENABLE TRIGGER notifications_set_updated_at`)
		require.NoError(t, err)
	}()
	// Each final status requires its own timestamp (check constraints).
	_, err = pool.Exec(ctx, `UPDATE notifications SET status = $2, updated_at = $3,
		sent_at      = CASE WHEN $2 = 'sent'      THEN $3::timestamptz END,
		failed_at    = CASE WHEN $2 = 'failed'    THEN $3::timestamptz END,
		cancelled_at = CASE WHEN $2 = 'cancelled' THEN $3::timestamptz END
		WHERE id = $1`, n.ID, string(status), time.Now().Add(-age))
	require.NoError(t, err)
	return n.ID
}

func batch(t *testing.T, age time.Duration) uuid.UUID {
	t.Helper()
	id := uuid.New()
	require.NoError(t, store.InsertBatch(t.Context(), &domain.Batch{ID: id, Total: 1, CreatedAt: time.Now().Add(-age)}))
	return id
}

func idempotencyKey(t *testing.T, key string, expiresIn time.Duration) {
	t.Helper()
	_, err := pool.Exec(t.Context(), `INSERT INTO idempotency_keys (key, request_hash, response_status, response_body, expires_at, lock_token)
		VALUES ($1, 'h', 201, '\x7b7d', $2, gen_random_uuid())`, key, time.Now().Add(expiresIn))
	require.NoError(t, err)
}

func count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(t.Context(), query, args...).Scan(&n))
	return n
}

func job(cfg retention.Config, deleted *prometheus.CounterVec) *retention.Job {
	if cfg.Interval == 0 {
		cfg.Interval = time.Hour
	}
	if cfg.ChunkSize == 0 {
		cfg.ChunkSize = 100
	}
	return retention.New(store, cfg, deleted, quiet)
}

// TestRunOnce_DeletesOnlyExpiredData: final notifications past retention go
// with their attempts and callbacks, and so do batches they leave empty and
// expired idempotency keys. Recent, unfinished and still-used data stays.
func TestRunOnce_DeletesOnlyExpiredData(t *testing.T) {
	reset(t)
	ctx := t.Context()

	oldSent := notification(t, domain.StatusSent, 40*24*time.Hour, nil)
	_, err := pool.Exec(ctx, `INSERT INTO notification_attempts (notification_id, attempt_no, outcome, duration_ms, created_at)
		VALUES ($1, 1, 'success', 10, now())`, oldSent)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO callback_deliveries (notification_id, event_id, event_type, url, payload)
		VALUES ($1, 1, 'notification.sent', 'https://example.com/hook', '{}')`, oldSent)
	require.NoError(t, err)

	recentSent := notification(t, domain.StatusSent, 24*time.Hour, nil)
	oldPending := notification(t, domain.StatusPending, 40*24*time.Hour, nil) // not final: never deleted

	emptied := batch(t, 40*24*time.Hour)
	oldFailedInBatch := notification(t, domain.StatusFailed, 40*24*time.Hour, &emptied)
	stillUsed := batch(t, 40*24*time.Hour)
	notification(t, domain.StatusPending, 40*24*time.Hour, &stillUsed)

	idempotencyKey(t, "expired", -time.Minute)
	idempotencyKey(t, "valid", time.Hour)

	deleted := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "deleted"}, []string{"table"})
	res, err := job(retention.Config{Notifications: month}, deleted).RunOnce(ctx)
	require.NoError(t, err)

	assert.Equal(t, retention.Result{Notifications: 2, Batches: 1, IdempotencyKeys: 1}, res)
	for _, id := range []uuid.UUID{oldSent, oldFailedInBatch} {
		assert.Zero(t, count(t, `SELECT count(*) FROM notifications WHERE id = $1`, id))
	}
	for _, id := range []uuid.UUID{recentSent, oldPending} {
		assert.Equal(t, 1, count(t, `SELECT count(*) FROM notifications WHERE id = $1`, id))
	}
	assert.Zero(t, count(t, `SELECT count(*) FROM notification_attempts WHERE notification_id = $1`, oldSent), "attempts go with it")
	assert.Zero(t, count(t, `SELECT count(*) FROM callback_deliveries WHERE notification_id = $1`, oldSent), "callbacks go with it")
	assert.Zero(t, count(t, `SELECT count(*) FROM batches WHERE id = $1`, emptied))
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM batches WHERE id = $1`, stillUsed), "a batch with a notification left stays")
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM idempotency_keys`), "only the valid key is left")
	assert.InDelta(t, 2, testutil.ToFloat64(deleted.WithLabelValues("notifications")), 0)
}

// TestRunOnce_DeletesInChunksUntilDone: one round removes everything
// expired, however many chunks that takes.
func TestRunOnce_DeletesInChunksUntilDone(t *testing.T) {
	reset(t)
	for range 5 {
		notification(t, domain.StatusSent, 40*24*time.Hour, nil)
	}
	res, err := job(retention.Config{Notifications: month, ChunkSize: 2}, nil).RunOnce(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int64(5), res.Notifications)
	assert.Zero(t, count(t, `SELECT count(*) FROM notifications`))
}

// TestRunOnce_ZeroKeepsNotifications: retention 0 keeps notifications
// forever but still removes expired idempotency keys.
func TestRunOnce_ZeroKeepsNotifications(t *testing.T) {
	reset(t)
	notification(t, domain.StatusSent, 400*24*time.Hour, nil)
	idempotencyKey(t, "expired", -time.Minute)

	res, err := job(retention.Config{Notifications: 0}, nil).RunOnce(t.Context())
	require.NoError(t, err)
	assert.Equal(t, retention.Result{IdempotencyKeys: 1}, res)
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM notifications`))
}

// TestRunOnce_OneWorkerAtATime: while another worker holds the retention
// lock, a round deletes nothing and returns at once.
func TestRunOnce_OneWorkerAtATime(t *testing.T) {
	reset(t)
	notification(t, domain.StatusSent, 40*24*time.Hour, nil)

	tx, err := pool.Begin(t.Context())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.WithoutCancel(t.Context())) }()
	_, err = tx.Exec(t.Context(), `SELECT pg_advisory_xact_lock(x'726574656e74'::bigint)`)
	require.NoError(t, err)

	res, err := job(retention.Config{Notifications: month}, nil).RunOnce(t.Context())
	require.NoError(t, err)
	assert.Equal(t, retention.Result{}, res)
	assert.Equal(t, 1, count(t, `SELECT count(*) FROM notifications`))
}

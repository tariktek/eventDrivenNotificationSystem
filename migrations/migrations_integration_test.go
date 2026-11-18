//go:build integration

package migrations_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/testutil/pgtest"
)

const latestVersion = 13

var dsn string

func TestMain(m *testing.M) {
	ctx := context.Background()
	pg, err := pgtest.Start(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	dsn = pg.DSN

	code := m.Run()

	_ = pg.Terminate()
	os.Exit(code)
}

func connect(t *testing.T) *pgx.Conn {
	t.Helper()
	require.NoError(t, pgtest.MigrateUp(dsn))
	conn, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

func TestMigrations_UpDownUp(t *testing.T) {
	m, err := pgtest.NewMigrator(dsn)
	require.NoError(t, err)
	defer m.Close()

	require.NoError(t, ignoreNoChange(m.Up()))
	version, dirty, err := m.Version()
	require.NoError(t, err)
	assert.Equal(t, uint(latestVersion), version)
	assert.False(t, dirty)

	// Every down migration must cleanly undo its up migration.
	require.NoError(t, m.Down())
	conn, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	defer conn.Close(context.Background())
	var tables int
	require.NoError(t, conn.QueryRow(t.Context(),
		`SELECT count(*) FROM information_schema.tables
		 WHERE table_schema = 'public' AND table_name <> 'schema_migrations'`).Scan(&tables))
	assert.Zero(t, tables, "down migrations must drop every table")

	// And the schema must be rebuildable afterwards.
	require.NoError(t, m.Up())
	version, _, err = m.Version()
	require.NoError(t, err)
	assert.Equal(t, uint(latestVersion), version)
}

// TestNotifications_Constraints proves invariants hold in the database itself,
// independent of application code.
func TestNotifications_Constraints(t *testing.T) {
	conn := connect(t)

	insert := func(cols, vals string) error {
		_, err := conn.Exec(t.Context(), fmt.Sprintf(
			`INSERT INTO notifications (id, channel, recipient, content, status, max_attempts%s)
			 VALUES (gen_random_uuid(), %s)`, cols, vals))
		return err
	}

	tests := []struct {
		name           string
		cols, vals     string
		wantCode       string // SQLSTATE; "" means the insert must succeed
		wantConstraint string
	}{
		{"valid sms", "", `'sms', '+905551234567', 'hi', 'pending', 5`, "", ""},
		{"valid email", ", subject", `'email', 'a@b.co', 'hi', 'pending', 5, 'Hello'`, "", ""},
		{
			"valid processing with lease", ", locked_by, locked_until",
			`'push', 'tok', 'hi', 'processing', 5, 'w1', now() + interval '30s'`, "", "",
		},

		{
			"email without subject", "", `'email', 'a@b.co', 'hi', 'pending', 5`,
			"23514", "notifications_email_requires_subject",
		},
		{"unknown channel", "", `'fax', 'x', 'hi', 'pending', 5`, "23514", "notifications_channel_check"},
		{"unknown status", "", `'sms', 'x', 'hi', 'queued', 5`, "23514", "notifications_status_check"},
		{
			"priority out of range", ", priority", `'sms', 'x', 'hi', 'pending', 5, 3`,
			"23514", "notifications_priority_check",
		},
		{"zero max attempts", "", `'sms', 'x', 'hi', 'pending', 0`, "23514", "notifications_max_attempts_check"},
		{
			"processing without lease", "", `'sms', 'x', 'hi', 'processing', 5`,
			"23514", "notifications_processing_has_lease",
		},
		{"sent without sent_at", "", `'sms', 'x', 'hi', 'sent', 5`, "23514", "notifications_sent_has_timestamp"},
		{
			"failed without failed_at", "", `'sms', 'x', 'hi', 'failed', 5`,
			"23514", "notifications_failed_has_timestamp",
		},
		{
			"cancelled without cancelled_at", "", `'sms', 'x', 'hi', 'cancelled', 5`,
			"23514", "notifications_cancelled_has_timestamp",
		},
		{
			"template version without template", ", template_version", `'sms', 'x', 'hi', 'pending', 5, 1`,
			"23514", "notifications_template_version_with_template",
		},
		{
			"metadata not an object", ", metadata", `'sms', 'x', 'hi', 'pending', 5, '[]'`,
			"23514", "notifications_metadata_is_object",
		},
		{
			"unknown batch", ", batch_id", `'sms', 'x', 'hi', 'pending', 5, gen_random_uuid()`,
			"23503", "notifications_batch_id_fkey",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := insert(tt.cols, tt.vals)
			if tt.wantCode == "" {
				require.NoError(t, err)
				return
			}
			var pgErr *pgconn.PgError
			require.True(t, errors.As(err, &pgErr), "want a PostgreSQL error, got %v", err)
			assert.Equal(t, tt.wantCode, pgErr.Code)
			assert.Equal(t, tt.wantConstraint, pgErr.ConstraintName)
		})
	}
}

func TestNotifications_IdempotencyKeyIsUnique(t *testing.T) {
	conn := connect(t)
	const q = `INSERT INTO notifications (id, channel, recipient, content, status, max_attempts, idempotency_key)
	           VALUES (gen_random_uuid(), 'sms', 'x', 'hi', 'pending', 5, $1)`

	_, err := conn.Exec(t.Context(), q, "unique-key-1")
	require.NoError(t, err)

	_, err = conn.Exec(t.Context(), q, "unique-key-1")
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr))
	assert.Equal(t, "23505", pgErr.Code)
	assert.Equal(t, "notifications_idempotency_key_uidx", pgErr.ConstraintName)

	// Notifications without a key are not affected by the unique index.
	for range 2 {
		_, err = conn.Exec(t.Context(), q, nil)
		require.NoError(t, err)
	}
}

func TestUpdatedAtTrigger(t *testing.T) {
	conn := connect(t)
	old := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	var id string
	require.NoError(t, conn.QueryRow(t.Context(),
		`INSERT INTO notifications (id, channel, recipient, content, status, max_attempts, updated_at)
		 VALUES (gen_random_uuid(), 'sms', 'x', 'hi', 'pending', 5, $1) RETURNING id`, old).Scan(&id))

	_, err := conn.Exec(t.Context(),
		`UPDATE notifications SET status = 'cancelled', cancelled_at = now() WHERE id = $1`, id)
	require.NoError(t, err)

	var updated time.Time
	require.NoError(t, conn.QueryRow(t.Context(),
		`SELECT updated_at FROM notifications WHERE id = $1`, id).Scan(&updated))
	assert.True(t, updated.After(old), "trigger must refresh updated_at on UPDATE")
}

func TestTemplates_NameUniqueAmongLiveTemplates(t *testing.T) {
	conn := connect(t)
	const q = `INSERT INTO templates (id, name, channel, body) VALUES (gen_random_uuid(), $1, 'sms', 'hi')`

	_, err := conn.Exec(t.Context(), q, "welcome")
	require.NoError(t, err)
	_, err = conn.Exec(t.Context(), q, "welcome")
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr))
	assert.Equal(t, "23505", pgErr.Code)

	// After a soft delete the name is free again.
	_, err = conn.Exec(t.Context(), `UPDATE templates SET deleted_at = now() WHERE name = 'welcome'`)
	require.NoError(t, err)
	_, err = conn.Exec(t.Context(), q, "welcome")
	require.NoError(t, err)
}

func TestBatches_TotalWithinLimits(t *testing.T) {
	conn := connect(t)
	const q = `INSERT INTO batches (id, total) VALUES (gen_random_uuid(), $1)`

	for _, total := range []int{1, 1000} {
		_, err := conn.Exec(t.Context(), q, total)
		require.NoError(t, err, "total=%d", total)
	}
	for _, total := range []int{0, 1001} {
		_, err := conn.Exec(t.Context(), q, total)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr), "total=%d must be rejected", total)
		assert.Equal(t, "23514", pgErr.Code)
	}
}

func ignoreNoChange(err error) error {
	if errors.Is(err, migrate.ErrNoChange) {
		return nil
	}
	return err
}

//go:build integration

package postgres_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
)

// connect opens a pool whose connections carry the given application name,
// as a worker (or the API) would.
func connect(t *testing.T, appName string, conns int32) *pgxpool.Pool {
	t.Helper()
	cfg := pool.Config().Copy()
	cfg.ConnConfig.RuntimeParams["application_name"] = appName
	cfg.MinConns = conns
	p, err := pgxpool.NewWithConfig(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(p.Close)
	// Open them now: acquire all at once (so each is a separate
	// connection), then hand them back to the pool, where they stay open.
	held := make([]*pgxpool.Conn, 0, conns)
	for range conns {
		c, err := p.Acquire(t.Context())
		require.NoError(t, err)
		held = append(held, c)
	}
	for _, c := range held {
		c.Release()
	}
	return p
}

// TestCountLiveWorkers counts worker processes, not connections, ignores
// other applications, and forgets a worker once its connections close.
func TestCountLiveWorkers(t *testing.T) {
	connect(t, postgres.WorkerAppName+"a", 3) // one worker, several connections
	b := connect(t, postgres.WorkerAppName+"b", 1)
	connect(t, "notification-api", 2)

	n, err := store.CountLiveWorkers(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	// Worker b stops. Its sessions leave pg_stat_activity once their
	// server processes have exited, a moment after the client closed them
	// (the workers recount every few seconds, so the lag does not matter).
	b.Close()
	assert.Eventually(t, func() bool {
		n, err := store.CountLiveWorkers(t.Context())
		return err == nil && n == 1
	}, 5*time.Second, 20*time.Millisecond, "a stopped worker is no longer counted")
}

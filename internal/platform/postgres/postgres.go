// Package postgres creates the shared PostgreSQL connection pool.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/config"
)

const connectTimeout = 10 * time.Second

// NewPool opens a connection pool and verifies the database is reachable, so
// a wrong URL or unreachable server fails at startup rather than on the first
// request.
//
// appName names every connection (application_name), so pg_stat_activity
// shows which process each one belongs to; workers also count each other
// that way (see postgres.Store.CountLiveWorkers).
func NewPool(ctx context.Context, cfg config.DatabaseConfig, appName string) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		// Parse errors can echo the DSN, which contains the password.
		return nil, errors.New("parse DATABASE_URL: invalid connection string")
	}
	pc.MaxConns = cfg.MaxConns
	pc.MinConns = cfg.MinConns
	if appName != "" {
		pc.ConnConfig.RuntimeParams["application_name"] = appName
	}
	// Queries become spans inside the trace of the work running them (an
	// API request, a delivery). Background polls start no trace of their
	// own: see observability.SetupTracing.
	pc.ConnConfig.Tracer = otelpgx.NewTracer()

	ctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres at %s:%d: %w", pc.ConnConfig.Host, pc.ConnConfig.Port, err)
	}
	return pool, nil
}

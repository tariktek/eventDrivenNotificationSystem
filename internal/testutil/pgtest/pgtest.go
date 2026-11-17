//go:build integration

// Package pgtest starts a throwaway PostgreSQL container and applies the
// project's migrations, for integration tests.
package pgtest

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // registers the pgx5:// driver
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/tariktek/eventDrivenNotificationSystem/migrations"
)

// Image matches the version used in docker-compose.yml.
const Image = "postgres:16-alpine"

// Container is a running PostgreSQL test instance.
type Container struct {
	// DSN is a postgres:// connection string for pgx.
	DSN       string
	container *postgres.PostgresContainer
}

// Start runs a new PostgreSQL container and waits until it accepts
// connections.
func Start(ctx context.Context) (*Container, error) {
	c, err := postgres.Run(ctx, Image,
		postgres.WithDatabase("notification"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, fmt.Errorf("start postgres container: %w", err)
	}
	dsn, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = testcontainers.TerminateContainer(c)
		return nil, fmt.Errorf("postgres connection string: %w", err)
	}
	return &Container{DSN: dsn, container: c}, nil
}

// Terminate stops and removes the container.
func (c *Container) Terminate() error {
	return testcontainers.TerminateContainer(c.container)
}

// NewMigrator returns a migrator over the embedded migrations. The caller
// must Close it.
func NewMigrator(dsn string) (*migrate.Migrate, error) {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return nil, fmt.Errorf("open embedded migrations: %w", err)
	}
	// golang-migrate selects its pgx v5 driver by the pgx5:// scheme.
	dbURL := "pgx5" + strings.TrimPrefix(dsn, "postgres")
	m, err := migrate.NewWithSourceInstance("iofs", src, dbURL)
	if err != nil {
		return nil, fmt.Errorf("create migrator: %w", err)
	}
	return m, nil
}

// MigrateUp applies all pending migrations.
func MigrateUp(dsn string) error {
	m, err := NewMigrator(dsn)
	if err != nil {
		return err
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}

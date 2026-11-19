// Package postgres implements persistence on PostgreSQL.
//
// Static queries live in queries/*.sql and are compiled to Go by sqlc
// (package sqlcgen). Two queries are hand-written because sqlc cannot model
// them well: the bulk insert (unnest over typed arrays) and the filtered
// listing (dynamic WHERE clause).
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres/sqlcgen"
)

// Store is the entry point: its embedded Repo runs each call on its own
// connection, and InTx runs several calls atomically. Writes that create
// resources go through InTx, which also lets them join a caller's unit of
// work (the idempotency store relies on that).
type Store struct {
	*Repo
	pool *pgxpool.Pool
}

// NewStore wraps a connection pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{Repo: newRepo(pool), pool: pool}
}

// InTx runs fn in a transaction, committing if fn returns nil and rolling
// back otherwise (including on panic).
//
// If ctx carries a unit of work (see withUnit), fn joins its transaction
// instead, as a savepoint: fn's writes are undone if it fails, and
// otherwise commit or roll back with the unit as a whole.
func (s *Store) InTx(ctx context.Context, fn func(r *Repo) error) error {
	if u, ok := ctx.Value(unitKey{}).(*unit); ok && u.pool == s.pool {
		tx, err := u.begin(ctx)
		if err != nil {
			return err
		}
		return pgx.BeginFunc(ctx, tx, func(sp pgx.Tx) error { return fn(newRepo(sp)) })
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return fn(newRepo(tx))
	})
}

// unit is one transaction shared by every InTx call made with a context
// that carries it. It begins on first use, so whatever runs before the
// first write (validation, template reads) does not hold a connection.
// Not safe for concurrent use: it belongs to one request.
type unit struct {
	pool *pgxpool.Pool
	tx   pgx.Tx
}

type unitKey struct{}

func withUnit(ctx context.Context, u *unit) context.Context {
	return context.WithValue(ctx, unitKey{}, u)
}

func (u *unit) begin(ctx context.Context) (pgx.Tx, error) {
	if u.tx == nil {
		tx, err := u.pool.Begin(ctx)
		if err != nil {
			return nil, fmt.Errorf("begin unit of work: %w", err)
		}
		u.tx = tx
	}
	return u.tx, nil
}

// Repo holds the data-access methods. It runs on whatever it was built
// with: the pool, or a transaction inside InTx.
type Repo struct {
	db sqlcgen.DBTX
	q  *sqlcgen.Queries
}

func newRepo(db sqlcgen.DBTX) *Repo {
	return &Repo{db: db, q: sqlcgen.New(db)}
}

// PostgreSQL error codes we translate.
const (
	uniqueViolation = "23505"
)

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == uniqueViolation
}

// notFound maps pgx.ErrNoRows to domain.ErrNotFound and wraps other errors.
func notFound(err error, what string) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%s: %w", what, domain.ErrNotFound)
	}
	return fmt.Errorf("%s: %w", what, err)
}

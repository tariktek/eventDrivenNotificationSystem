package postgres

import (
	"context"
	"fmt"
)

// WorkerAppName is the application_name of a worker's database
// connections: this prefix plus the worker's ID.
const WorkerAppName = "notification-worker:"

// countLiveWorkersSQL counts the workers connected to this database. Every
// worker holds a connection for LISTEN at all times, so a running worker is
// always counted, and one that died disappears with its connections.
// pg_stat_activity shows application_name for every session, whoever runs
// the query.
const countLiveWorkersSQL = `
SELECT count(DISTINCT application_name)
FROM pg_stat_activity
WHERE datname = current_database() AND starts_with(application_name, $1)`

// CountLiveWorkers returns how many worker processes are connected now.
func (s *Store) CountLiveWorkers(ctx context.Context) (int, error) {
	var n int
	if err := s.pool.QueryRow(ctx, countLiveWorkersSQL, WorkerAppName).Scan(&n); err != nil {
		return 0, fmt.Errorf("count live workers: %w", err)
	}
	return n, nil
}

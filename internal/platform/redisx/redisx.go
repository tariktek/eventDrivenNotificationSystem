// Package redisx creates the shared Redis client.
package redisx

import (
	"errors"
	"fmt"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/config"
)

// NewClient creates a client without contacting Redis. Redis is a
// non-critical dependency (the services degrade instead of failing when it is
// unavailable), so callers decide whether an unreachable server at startup is
// fatal. The client reconnects on its own once Redis comes back.
func NewClient(cfg config.RedisConfig) (*redis.Client, error) {
	opts, err := redis.ParseURL(cfg.URL)
	if err != nil {
		// Parse errors can echo the URL, which may contain a password.
		return nil, errors.New("parse REDIS_URL: invalid connection string")
	}
	rdb := redis.NewClient(opts)
	// Commands become spans inside the trace of the work running them.
	if err := redisotel.InstrumentTracing(rdb, redisotel.WithDBStatement(false)); err != nil {
		return nil, fmt.Errorf("instrument redis: %w", err)
	}
	return rdb, nil
}

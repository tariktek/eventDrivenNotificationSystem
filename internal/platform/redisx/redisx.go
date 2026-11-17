// Package redisx creates the shared Redis client.
package redisx

import (
	"errors"

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
	return redis.NewClient(opts), nil
}

package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// reserveScript is gcra() run atomically inside Redis. It reads the clock
// from Redis (TIME), so every worker shares one timeline regardless of its
// own clock drift. Times are integer microseconds.
//
// KEYS[1] TAT key · ARGV[1] interval µs · ARGV[2] horizon µs · ARGV[3] n
// Returns {granted, first_us, wait_us, now_us}.
var reserveScript = redis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local interval = tonumber(ARGV[1])
local horizon = tonumber(ARGV[2])
local n = tonumber(ARGV[3])

local tat = tonumber(redis.call('GET', KEYS[1]) or now)
local offset = tat - now
if offset < 0 then offset = 0 end

local k = math.floor((horizon - offset) / interval)
if k > n then k = n end
if k < 0 then k = 0 end
if k == 0 then
  return {0, 0, offset + interval - horizon, now}
end

local newtat = now + offset + k * interval
-- Expire once the timeline is idle again; the key never outlives its use.
redis.call('SET', KEYS[1], newtat, 'PX', math.ceil((newtat - now) / 1000) + 1000)
return {k, offset, 0, now}
`)

// Redis is a Limiter shared by every process using the same Redis.
type Redis struct {
	client *redis.Client
	cfg    Config
	prefix string
}

// NewRedis returns a Redis-backed limiter. Keys are stored as
// "<prefix><key>".
func NewRedis(client *redis.Client, cfg Config, prefix string) *Redis {
	return &Redis{client: client, cfg: cfg, prefix: prefix}
}

// Reserve implements Limiter.
func (r *Redis) Reserve(ctx context.Context, key string, n int) (Reservation, error) {
	res, err := r.ReserveAt(ctx, key, n)
	return res.Reservation, err
}

// ServerReservation is a Reservation plus the Redis clock reading it was
// computed at, so tests can place slots on the shared timeline exactly.
type ServerReservation struct {
	Reservation
	ServerNow time.Time
}

// ReserveAt is Reserve that also returns the Redis clock reading.
func (r *Redis) ReserveAt(ctx context.Context, key string, n int) (ServerReservation, error) {
	interval := r.cfg.interval()
	vals, err := reserveScript.Run(ctx, r.client, []string{r.prefix + key},
		interval.Microseconds(), r.cfg.Horizon.Microseconds(), n).Int64Slice()
	if err != nil {
		return ServerReservation{}, fmt.Errorf("reserve rate limit slots: %w", err)
	}
	if len(vals) != 4 {
		return ServerReservation{}, fmt.Errorf("reserve rate limit slots: unexpected reply %v", vals)
	}
	us := func(v int64) time.Duration { return time.Duration(v) * time.Microsecond }
	return ServerReservation{
		Reservation: Reservation{
			Count:    int(vals[0]),
			First:    max(us(vals[1]), us(vals[2])),
			Interval: interval,
		},
		ServerNow: time.UnixMicro(vals[3]),
	}, nil
}

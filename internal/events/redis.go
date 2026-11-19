package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// Publisher appends events to a Redis stream.
type Publisher struct {
	rdb    redis.UniversalClient
	stream string
	maxLen int64
}

// NewPublisher returns a Publisher. The stream is trimmed to about maxLen
// entries so it cannot grow without bound; a consumer that falls further
// behind than that misses events (the outbox table still has them).
func NewPublisher(rdb redis.UniversalClient, stream string, maxLen int64) *Publisher {
	return &Publisher{rdb: rdb, stream: stream, maxLen: maxLen}
}

// Publish appends events in order, in one round trip. On error some of
// them may have been appended; the relay then publishes them all again
// and the WebSocket hub drops repeats per aggregate by Event.ID
// (realtime.dedup). Tail itself passes on whatever is in the stream.
func (p *Publisher) Publish(ctx context.Context, evs []Event) error {
	_, err := p.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i := range evs {
			pipe.XAdd(ctx, &redis.XAddArgs{
				Stream: p.stream,
				MaxLen: p.maxLen,
				Approx: true, // "~": trims whole nodes, much cheaper than exact
				Values: evs[i].values(),
			})
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("publish %d events: %w", len(evs), err)
	}
	return nil
}

// TailConfig configures Tail.
type TailConfig struct {
	Stream string
	// Count is the maximum number of entries per read.
	Count int64
	// Block is how long one read waits for new entries.
	Block time.Duration
	// RetryEvery is the pause after a Redis error.
	RetryEvery time.Duration
	Log        *slog.Logger
}

// Tail calls handle for every event appended to the stream after Tail
// starts, until ctx is done. Every Tail sees every event (fan-out). After
// a Redis error it resumes from the last event it saw, so an outage
// shorter than the stream's retention loses nothing.
func Tail(ctx context.Context, rdb redis.UniversalClient, cfg TailConfig, handle func(Event)) {
	// Start after the current last entry. Not "$": every XREAD with "$"
	// means "after whatever is last now", so events appended between one
	// read timing out and the next one starting would be skipped.
	last := ""
	for ctx.Err() == nil && last == "" {
		msgs, err := rdb.XRevRangeN(ctx, cfg.Stream, "+", "-", 1).Result()
		switch {
		case err != nil:
			cfg.Log.WarnContext(ctx, "read event stream position", "stream", cfg.Stream, "error", err)
			sleep(ctx, cfg.RetryEvery)
		case len(msgs) == 0:
			last = "0-0" // empty or missing stream: everything that comes is new
		default:
			last = msgs[0].ID
		}
	}
	for ctx.Err() == nil {
		res, err := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{cfg.Stream, last},
			Count:   cfg.Count,
			Block:   cfg.Block,
		}).Result()
		switch {
		case errors.Is(err, redis.Nil): // nothing new within Block
			continue
		case err != nil:
			if ctx.Err() == nil {
				cfg.Log.WarnContext(ctx, "read event stream", "stream", cfg.Stream, "error", err)
				sleep(ctx, cfg.RetryEvery)
			}
			continue
		}
		for _, s := range res {
			for _, m := range s.Messages {
				last = m.ID
				e, err := Decode(m)
				if err != nil {
					cfg.Log.ErrorContext(ctx, "skip malformed event", "error", err)
					continue
				}
				handle(e)
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

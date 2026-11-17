package events

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
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
// and consumers deduplicate by Event.ID.
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

// GroupConfig configures a Group consumer.
type GroupConfig struct {
	Stream   string
	Group    string
	Consumer string // unique per process
	Count    int64
	Block    time.Duration
	// MinIdle is how long an event may stay unacknowledged before another
	// consumer takes it over (its consumer is presumed dead).
	MinIdle    time.Duration
	RetryEvery time.Duration
	Log        *slog.Logger
}

// Group consumes a stream as a member of a consumer group.
type Group struct {
	rdb redis.UniversalClient
	cfg GroupConfig
}

// NewGroup returns a Group consumer.
func NewGroup(rdb redis.UniversalClient, cfg GroupConfig) *Group {
	return &Group{rdb: rdb, cfg: cfg}
}

// Ensure creates the consumer group if it does not exist. A new group
// starts at the beginning of the stream, so events published before the
// first consumer started are not skipped.
func (g *Group) Ensure(ctx context.Context) error {
	err := g.rdb.XGroupCreateMkStream(ctx, g.cfg.Stream, g.cfg.Group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create consumer group %s: %w", g.cfg.Group, err)
	}
	return nil
}

// Run calls handle for each event and acknowledges it when handle returns
// nil. An event whose handle failed stays pending and is retried after
// MinIdle, by this or another consumer, as is one whose consumer died.
// Run returns when ctx is done.
func (g *Group) Run(ctx context.Context, handle func(context.Context, Event) error) {
	var (
		nextClaim time.Time
		ensured   bool
	)
	for ctx.Err() == nil {
		if !ensured {
			if err := g.Ensure(ctx); err != nil {
				g.fail(ctx, "ensure consumer group", err)
				continue
			}
			ensured = true
		}
		if time.Now().After(nextClaim) {
			if err := g.claimStale(ctx, handle); err != nil {
				ensured = !strings.HasPrefix(err.Error(), "NOGROUP")
				g.fail(ctx, "claim stale events", err)
				continue
			}
			nextClaim = time.Now().Add(g.cfg.MinIdle / 2)
		}
		res, err := g.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    g.cfg.Group,
			Consumer: g.cfg.Consumer,
			Streams:  []string{g.cfg.Stream, ">"},
			Count:    g.cfg.Count,
			Block:    g.cfg.Block,
		}).Result()
		switch {
		case errors.Is(err, redis.Nil):
			continue
		case err != nil:
			// NOGROUP: Redis lost the group (restart without persistence,
			// stream deleted); recreate it on the next round.
			ensured = !strings.HasPrefix(err.Error(), "NOGROUP")
			g.fail(ctx, "read consumer group", err)
			continue
		}
		for _, s := range res {
			g.process(ctx, s.Messages, handle)
		}
	}
}

// claimStale takes over events left unacknowledged for MinIdle: their
// consumer crashed, or their handler failed and they are due a retry.
func (g *Group) claimStale(ctx context.Context, handle func(context.Context, Event) error) error {
	// XAUTOCLAIM pages through the pending list; "0-0" means it wrapped.
	for start := "0-0"; ctx.Err() == nil; {
		msgs, next, err := g.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   g.cfg.Stream,
			Group:    g.cfg.Group,
			Consumer: g.cfg.Consumer,
			MinIdle:  g.cfg.MinIdle,
			Start:    start,
			Count:    g.cfg.Count,
		}).Result()
		if err != nil {
			return err
		}
		g.process(ctx, msgs, handle)
		if next == "0-0" {
			break
		}
		start = next
	}
	return nil
}

func (g *Group) process(ctx context.Context, msgs []redis.XMessage, handle func(context.Context, Event) error) {
	for _, m := range msgs {
		e, err := Decode(m)
		if err != nil {
			// Retrying cannot fix it: acknowledge so it does not come back.
			g.cfg.Log.ErrorContext(ctx, "skip malformed event", "error", err)
			g.ack(ctx, m.ID)
			continue
		}
		if err := handle(ctx, e); err != nil {
			if ctx.Err() == nil {
				g.cfg.Log.WarnContext(ctx, "event handler failed; will retry", "event_id", e.ID, "type", e.Type, "error", err)
			}
			continue
		}
		g.ack(ctx, m.ID)
	}
}

func (g *Group) ack(ctx context.Context, id string) {
	if err := g.rdb.XAck(ctx, g.cfg.Stream, g.cfg.Group, id).Err(); err != nil && ctx.Err() == nil {
		// Not fatal: the event is redelivered after MinIdle and handled
		// again, which handlers tolerate (at-least-once).
		g.cfg.Log.WarnContext(ctx, "acknowledge event", "stream_id", id, "error", err)
	}
}

func (g *Group) fail(ctx context.Context, what string, err error) {
	if ctx.Err() == nil {
		g.cfg.Log.WarnContext(ctx, what, "group", g.cfg.Group, "error", err)
		sleep(ctx, g.cfg.RetryEvery)
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

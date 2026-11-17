//go:build integration

package ratelimit_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/ratelimit"
)

var redisClient *goredis.Client

func TestMain(m *testing.M) {
	ctx := context.Background()
	c, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	url, err := c.ConnectionString(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	opts, err := goredis.ParseURL(url)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	redisClient = goredis.NewClient(opts)

	code := m.Run()
	_ = redisClient.Close()
	_ = c.Terminate(ctx)
	os.Exit(code)
}

// TestRedis_ConcurrentWorkersShareOneTimeline has eight workers reserve
// slots concurrently for ~3 seconds. Slot times are taken on Redis's own
// clock, so the check is exact: no two slots may coincide and no one-second
// window may hold more than 100, which a per-process limiter could not
// guarantee.
func TestRedis_ConcurrentWorkersShareOneTimeline(t *testing.T) {
	limiter := ratelimit.NewRedis(redisClient, cfg, "test:shared:")
	const workers = 8
	deadline := time.Now().Add(3 * time.Second)

	var (
		mu    sync.Mutex
		slots []time.Time
		wg    sync.WaitGroup
	)
	for w := range workers {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(w), 7))
			for time.Now().Before(deadline) {
				r, err := limiter.ReserveAt(context.Background(), "sms", 1+rng.IntN(30))
				if !assert.NoError(t, err) {
					return
				}
				mu.Lock()
				for i := range r.Count {
					slots = append(slots, r.ServerNow.Add(r.Delay(i)))
				}
				mu.Unlock()
				if r.Count == 0 {
					time.Sleep(r.First)
				}
			}
		})
	}
	wg.Wait()

	require.Greater(t, len(slots), 250, "the limiter must still let ~100/s through")
	assertRate(t, slots, cfg)
	t.Logf("%d slots reserved by %d concurrent workers in ~3s", len(slots), workers)
}

// TestRedis_MeasuredSendRate actually sends on the reserved slots, as the
// worker will, and measures throughput on the local clock.
func TestRedis_MeasuredSendRate(t *testing.T) {
	limiter := ratelimit.NewRedis(redisClient, cfg, "test:measured:")
	const workers = 3
	start := time.Now()
	deadline := start.Add(3 * time.Second)

	var (
		mu    sync.Mutex
		sends []time.Time
		wg    sync.WaitGroup
	)
	for range workers {
		wg.Go(func() {
			for time.Now().Before(deadline) {
				r, err := limiter.Reserve(context.Background(), "push", 20)
				if !assert.NoError(t, err) {
					return
				}
				base := time.Now()
				if r.Count == 0 {
					time.Sleep(r.First)
					continue
				}
				for i := range r.Count {
					time.Sleep(time.Until(base.Add(r.Delay(i))))
					mu.Lock()
					sends = append(sends, time.Now())
					mu.Unlock()
				}
			}
		})
	}
	wg.Wait()

	elapsed := time.Since(start).Seconds()
	rate := float64(len(sends)) / elapsed
	t.Logf("%d sends in %.2fs = %.1f/s across %d workers", len(sends), elapsed, rate, workers)
	assert.InDelta(t, 100, rate, 10, "throughput should sit at the limit")
}

func TestRedis_KeyExpiresWhenIdle(t *testing.T) {
	limiter := ratelimit.NewRedis(redisClient, cfg, "test:ttl:")
	_, err := limiter.Reserve(t.Context(), "email", 5)
	require.NoError(t, err)
	ttl, err := redisClient.PTTL(t.Context(), "test:ttl:email").Result()
	require.NoError(t, err)
	assert.Greater(t, ttl, time.Duration(0))
	assert.LessOrEqual(t, ttl, 2*time.Second, "state lives only as long as the reserved timeline")
}

func TestFallback_RealRedisOutage(t *testing.T) {
	dead := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond, MaxRetries: -1})
	defer dead.Close()
	f := ratelimit.NewFallback(ratelimit.FallbackConfig{
		Primary:    ratelimit.NewRedis(dead, cfg, "x:"),
		Rate:       cfg,
		Instances:  1,
		Log:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		RetryEvery: time.Minute,
	})

	r, err := f.Reserve(t.Context(), "sms", 10)
	require.NoError(t, err)
	assert.Equal(t, 10, r.Count)
	assert.True(t, f.Degraded())

	start := time.Now()
	_, _ = f.Reserve(t.Context(), "sms", 10)
	assert.Less(t, time.Since(start), 50*time.Millisecond, "no dial timeout while known to be down")
}

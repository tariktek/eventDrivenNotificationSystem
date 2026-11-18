// Command worker consumes the notification queue and delivers messages to
// the external provider.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/breaker"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/callback"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/config"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/events"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/metrics"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/observability"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/outbox"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/buildinfo"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/health"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/httpserver"
	pgplatform "github.com/tariktek/eventDrivenNotificationSystem/internal/platform/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/redisx"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/provider"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/ratelimit"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/retry"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/worker"
)

const (
	serviceName        = "worker"
	healthCheckTimeout = time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", serviceName, err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	log := observability.NewLogger(os.Stdout, cfg.Log, serviceName, buildinfo.Version)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Info("starting", "env", cfg.Env, "commit", buildinfo.Commit)

	shutdownTracing, err := observability.SetupTracing(ctx, cfg.Tracing, serviceName, buildinfo.Version)
	if err != nil {
		return err
	}
	defer func() {
		// Flush buffered spans; ctx is already cancelled at this point.
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(flushCtx); err != nil {
			log.Warn("flush traces", "error", err)
		}
	}()

	pool, err := pgplatform.NewPool(ctx, cfg.Database)
	if err != nil {
		return err
	}
	defer pool.Close()

	rdb, err := redisx.NewClient(cfg.Redis)
	if err != nil {
		return err
	}
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Warn("redis unreachable at startup, continuing in degraded mode", "error", err)
	}

	checks := health.NewRegistry(healthCheckTimeout)
	checks.Register("postgres", true, pool.Ping)
	checks.Register("redis", false, func(ctx context.Context) error { return rdb.Ping(ctx).Err() })
	defer context.AfterFunc(ctx, checks.SetDraining)()

	store := postgres.NewStore(pool)
	m := metrics.New()
	rate := ratelimit.Config{Rate: cfg.Worker.RateLimit, Horizon: cfg.Worker.RateHorizon}
	limiter := ratelimit.NewFallback(ratelimit.FallbackConfig{
		Primary:    ratelimit.NewRedis(rdb, rate, "ratelimit:"),
		Rate:       rate,
		Instances:  cfg.Worker.ExpectedInstances,
		Log:        log,
		RetryEvery: 5 * time.Second,
	})
	m.Registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "rate_limiter_fallback_active",
		Help: "1 while Redis is unreachable and the local fallback limiter is in use.",
	}, func() float64 {
		if limiter.Degraded() {
			return 1
		}
		return 0
	}))
	checks.Register("rate_limiter", false, func(context.Context) error {
		if limiter.Degraded() {
			return errors.New("shared limiter unavailable; using local fallback")
		}
		return nil
	})

	id := workerID(cfg.Worker.ID)
	urls := map[domain.Channel]string{}
	for _, ch := range domain.Channels {
		urls[ch] = cfg.Provider.URLFor(string(ch))
	}
	w := worker.New(worker.Config{
		ID:            id,
		Channels:      domain.Channels,
		Concurrency:   cfg.Worker.Concurrency,
		BatchSize:     cfg.Worker.BatchSize,
		PollInterval:  cfg.Worker.PollInterval,
		Lease:         cfg.Worker.Lease,
		ReapInterval:  cfg.Worker.ReapInterval,
		Aging:         cfg.Worker.PriorityAging,
		ShutdownGrace: cfg.ShutdownTimeout,
		Breaker: breaker.Config{
			Window:       cfg.Breaker.Window,
			MinRequests:  uint32(cfg.Breaker.MinRequests), //nolint:gosec // validated >= 1 at startup
			FailureRatio: cfg.Breaker.FailureRatio,
			OpenTimeout:  cfg.Breaker.OpenTimeout,
			Probes:       uint32(cfg.Breaker.Probes), //nolint:gosec // validated >= 1 at startup
		},
	}, worker.Deps{
		Queue:    store.Queue(),
		Limiter:  limiter,
		Sender:   provider.NewHTTPClient(provider.HTTPConfig{URLs: urls, Timeout: cfg.Provider.Timeout}),
		Policy:   retry.Policy{Base: cfg.Retry.BaseDelay, Max: cfg.Retry.MaxDelay},
		Notifier: store.Queue(),
		Metrics:  m,
		Log:      log,
	})

	// An open breaker means a provider is failing: the worker still runs
	// (other channels deliver, the failing one waits), so it is "degraded".
	checks.Register("circuit_breakers", false, func(context.Context) error {
		var open []string
		for _, ch := range domain.Channels {
			if s := w.BreakerStates()[ch]; s != breaker.StateClosed {
				open = append(open, string(ch)+"="+string(s))
			}
		}
		if len(open) > 0 {
			return fmt.Errorf("provider failing, deliveries paused: %s", strings.Join(open, ", "))
		}
		return nil
	})

	// Every worker runs a relay; an advisory lock lets one publish at a time.
	relay := outbox.New(store,
		events.NewPublisher(rdb, cfg.Events.Stream, cfg.Events.StreamMaxLen),
		outbox.Config{
			BatchSize:     cfg.Outbox.BatchSize,
			PollInterval:  cfg.Outbox.PollInterval,
			RetryEvery:    time.Second,
			Retention:     cfg.Outbox.Retention,
			PruneInterval: cfg.Outbox.PruneInterval,
			OnPublished:   m.ObserveEvents, // status counters: see package metrics
		}, log)

	// Status callbacks: one consumer group shared by all workers turns
	// final-status events into callback_deliveries rows; dispatchers on
	// every worker send them.
	enqueuer := &callback.Enqueuer{Store: store}
	callbackEvents := events.NewGroup(rdb, events.GroupConfig{
		Stream: cfg.Events.Stream, Group: "callbacks", Consumer: id,
		Count: 100, Block: 5 * time.Second, MinIdle: 30 * time.Second, RetryEvery: time.Second, Log: log,
	})
	callbacks := callback.NewDispatcher(store, callback.DispatcherConfig{
		Client: callback.ClientConfig{
			Timeout: cfg.Callback.Timeout, AllowInsecure: cfg.Callback.AllowInsecure, AllowPrivate: cfg.Callback.AllowPrivate,
		},
		Secret:       []byte(cfg.Callback.SigningSecret),
		Concurrency:  cfg.Callback.Concurrency,
		MaxAttempts:  cfg.Callback.MaxAttempts,
		Backoff:      retry.Policy{Base: cfg.Callback.RetryBaseDelay, Max: cfg.Callback.RetryMaxDelay},
		Lease:        cfg.Callback.Lease,
		PollInterval: cfg.Callback.PollInterval,
		Metrics:      m,
	}, log)

	// The worker has no public API; this small "ops" server exposes probes
	// and Prometheus metrics for the orchestrator and the scraper.
	mux := http.NewServeMux()
	mux.Handle("GET /health", health.LivenessHandler())
	mux.Handle("GET /ready", checks.ReadinessHandler())
	mux.Handle("GET /metrics", m.Handler())
	srv := httpserver.New(cfg.HTTP, mux)

	// If either part fails (e.g. the ops port is taken), stop the other too
	// rather than run half alive.
	g, gctx := errgroup.WithContext(ctx)
	// Create the callback group before this worker's relay publishes, so a
	// first deployment does not skip the first events. If Redis is down,
	// the consumer retries on its own.
	if err := callbackEvents.Ensure(ctx); err != nil {
		log.Warn("create callback consumer group; will retry", "error", err)
	}
	g.Go(func() error { return w.Run(gctx) })
	g.Go(func() error { relay.Run(gctx); return nil })
	g.Go(func() error { callbackEvents.Run(gctx, enqueuer.Handle); return nil })
	g.Go(func() error { callbacks.Run(gctx); return nil })
	g.Go(func() error {
		log.Info("ops server listening", "addr", cfg.HTTP.Addr)
		return httpserver.ListenAndServe(gctx, srv, cfg.ShutdownTimeout)
	})
	if err := g.Wait(); err != nil {
		return err
	}

	log.Info("stopped gracefully")
	return nil
}

// workerID returns the configured ID, or hostname-plus-random so replicas
// of the same container image never share one.
func workerID(configured string) string {
	if configured != "" {
		return configured
	}
	host, err := os.Hostname()
	if err != nil {
		host = "worker"
	}
	return host + "-" + uuid.NewString()[:8]
}

// Command api serves the notification management HTTP API.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/config"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/events"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/handler"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/router"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/metrics"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/observability"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/buildinfo"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/health"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/httpserver"
	pgplatform "github.com/tariktek/eventDrivenNotificationSystem/internal/platform/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/redisx"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/realtime"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/service"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/stats"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/validation"
)

const (
	serviceName        = "api"
	healthCheckTimeout = time.Second
	statsInterval      = 5 * time.Second
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
	// Report not-ready as soon as shutdown starts so traffic drains away.
	defer context.AfterFunc(ctx, checks.SetDraining)()

	store := postgres.NewStore(pool)
	m := metrics.New()
	// System-wide statistics for GET /api/v1/metrics and the queue-depth
	// and outbox gauges, sampled from the database in the background.
	sampler := stats.New(store, m, statsInterval, log)
	go sampler.Run(ctx)
	svc := service.New(store, time.Now, validation.Options{
		AllowInsecureCallbacks: cfg.Callback.AllowInsecure,
		AllowPrivateCallbacks:  cfg.Callback.AllowPrivate,
	}).WithStats(sampler)
	meta := func(ctx context.Context) service.RequestMeta {
		// The traceparent is stored with the notification, so its delivery
		// (later, in a worker) can link back to this request's trace.
		return service.RequestMeta{CorrelationID: observability.CorrelationID(ctx), TraceParent: observability.TraceParent(ctx)}
	}
	// Live events: this instance tails the event stream and pushes matching
	// events to its WebSocket clients.
	hub := realtime.NewHub(realtime.Config{MaxClients: cfg.WS.MaxClients, BufferSize: cfg.WS.BufferSize, Clients: m.WebSocketClients}, log)
	go hub.Run(ctx, func(ctx context.Context, publish func(events.Event)) {
		events.Tail(ctx, rdb, events.TailConfig{
			Stream: cfg.Events.Stream, Count: 500, Block: 5 * time.Second, RetryEvery: time.Second, Log: log,
		}, publish)
	})
	routes := router.New(router.Deps{
		Health:         checks,
		API:            handler.New(svc, meta),
		Log:            log,
		MaxBodyBytes:   cfg.HTTP.MaxBodyBytes,
		APIKeys:        cfg.Auth.APIKeys,
		Idempotency:    store.Idempotency(),
		IdempotencyTTL: cfg.Auth.IdempotencyTTL,
		Now:            time.Now,
		Metrics:        m,
		Realtime: &realtime.Handler{
			Hub:            hub,
			OriginPatterns: cfg.WS.AllowedOrigins,
			Log:            log,
			Shutdown:       ctx,
		},
	})
	srv := httpserver.New(cfg.HTTP, routes)

	log.Info("http server listening", "addr", cfg.HTTP.Addr)
	if err := httpserver.ListenAndServe(ctx, srv, cfg.ShutdownTimeout); err != nil {
		return err
	}

	log.Info("stopped gracefully")
	return nil
}

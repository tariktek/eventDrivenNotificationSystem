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
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/handler"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/router"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/observability"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/buildinfo"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/health"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/httpserver"
	pgplatform "github.com/tariktek/eventDrivenNotificationSystem/internal/platform/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/redisx"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/repository/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/service"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/validation"
)

const (
	serviceName        = "api"
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
	svc := service.New(store, time.Now, validation.Options{
		AllowInsecureCallbacks: cfg.Callback.AllowInsecure,
		AllowPrivateCallbacks:  cfg.Callback.AllowPrivate,
	})
	meta := func(ctx context.Context) service.RequestMeta {
		return service.RequestMeta{CorrelationID: observability.CorrelationID(ctx)}
	}
	routes := router.New(router.Deps{
		Health:         checks,
		API:            handler.New(svc, meta),
		Log:            log,
		MaxBodyBytes:   cfg.HTTP.MaxBodyBytes,
		APIKeys:        cfg.Auth.APIKeys,
		Idempotency:    store.Idempotency(),
		IdempotencyTTL: cfg.Auth.IdempotencyTTL,
		Now:            time.Now,
	})
	srv := httpserver.New(cfg.HTTP, routes)

	log.Info("http server listening", "addr", cfg.HTTP.Addr)
	if err := httpserver.ListenAndServe(ctx, srv, cfg.ShutdownTimeout); err != nil {
		return err
	}

	log.Info("stopped gracefully")
	return nil
}

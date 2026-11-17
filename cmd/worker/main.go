// Command worker consumes the notification queue and delivers messages to
// the external provider.
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/config"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/observability"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/buildinfo"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/health"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/httpserver"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/postgres"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/redisx"
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

	pool, err := postgres.NewPool(ctx, cfg.Database)
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

	// The worker has no public API; this small "ops" server exposes probes
	// (and, later, Prometheus metrics) for the orchestrator.
	mux := http.NewServeMux()
	mux.Handle("GET /health", health.LivenessHandler())
	mux.Handle("GET /ready", checks.ReadinessHandler())
	srv := httpserver.New(cfg.HTTP, mux)

	log.Info("ops server listening", "addr", cfg.HTTP.Addr)
	if err := httpserver.ListenAndServe(ctx, srv, cfg.ShutdownTimeout); err != nil {
		return err
	}

	log.Info("stopped gracefully")
	return nil
}

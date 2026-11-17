// Command api serves the notification management HTTP API.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/config"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/observability"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/buildinfo"
)

const serviceName = "api"

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

	log.Info("starting", "env", cfg.Env, "commit", buildinfo.Commit, "http_addr", cfg.HTTP.Addr)

	<-ctx.Done()

	log.Info("shutdown signal received")
	return nil
}

// Command mockprovider runs a fault-injecting stand-in for the external
// notification provider. See package mockprovider.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/caarlos0/env/v11"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/config"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/mockprovider"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/httpserver"
)

type settings struct {
	Addr          string        `env:"MOCK_ADDR"                   envDefault:":9000"`
	FailureRate   float64       `env:"MOCK_FAILURE_RATE"           envDefault:"0"`
	RateLimitRate float64       `env:"MOCK_RATE_LIMIT_RATE"        envDefault:"0"`
	PermanentRate float64       `env:"MOCK_PERMANENT_FAILURE_RATE" envDefault:"0"`
	Latency       time.Duration `env:"MOCK_LATENCY"                envDefault:"20ms"`
	LatencyJitter time.Duration `env:"MOCK_LATENCY_JITTER"         envDefault:"30ms"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "mockprovider: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	s, err := env.ParseAs[settings]()
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "mockprovider")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mock := mockprovider.New(mockprovider.Config{
		FailureRate:   s.FailureRate,
		RateLimitRate: s.RateLimitRate,
		PermanentRate: s.PermanentRate,
		Latency:       mockprovider.Duration(s.Latency),
		LatencyJitter: mockprovider.Duration(s.LatencyJitter),
	}, nil)

	srv := httpserver.New(config.HTTPConfig{
		Addr: s.Addr, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second,
	}, mock.Handler())

	log.Info("mock provider listening", "addr", s.Addr,
		"failure_rate", s.FailureRate, "rate_limit_rate", s.RateLimitRate, "permanent_rate", s.PermanentRate)
	return httpserver.ListenAndServe(ctx, srv, 10*time.Second)
}

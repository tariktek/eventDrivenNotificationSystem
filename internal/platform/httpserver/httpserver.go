// Package httpserver runs an http.Server with explicit timeouts and graceful
// shutdown.
package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/config"
)

// New builds a server with the configured timeouts. Leaving any of them at
// zero (the net/http default) lets a slow or malicious client hold a
// connection open indefinitely.
func New(cfg config.HTTPConfig, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           handler,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}
}

// ListenAndServe binds srv.Addr and serves until ctx is cancelled; see Serve.
func ListenAndServe(ctx context.Context, srv *http.Server, shutdownTimeout time.Duration) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", srv.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", srv.Addr, err)
	}
	return Serve(ctx, srv, ln, shutdownTimeout)
}

// Serve serves on ln until ctx is cancelled or the server fails.
//
// On cancellation it stops accepting new connections and waits up to
// shutdownTimeout for in-flight requests to complete. Requests still running
// after that are cut off and an error is returned, so the caller can tell a
// clean shutdown from a forced one.
func Serve(ctx context.Context, srv *http.Server, ln net.Listener, shutdownTimeout time.Duration) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err := <-serveErr:
		// Serve only returns before Shutdown on a real failure.
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	// ctx is already cancelled; derive a fresh deadline that keeps its values.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		_ = srv.Close()
		return fmt.Errorf("graceful shutdown did not finish within %s: %w", shutdownTimeout, err)
	}
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}
	return nil
}

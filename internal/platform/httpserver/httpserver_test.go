package httpserver_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/config"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/httpserver"
)

func testConfig() config.HTTPConfig {
	return config.HTTPConfig{
		ReadHeaderTimeout: time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       5 * time.Second,
	}
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	return ln
}

func get(t *testing.T, url string) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, http.NoBody)
	require.NoError(t, err)
	return http.DefaultClient.Do(req)
}

func TestServe_InFlightRequestCompletesDuringShutdown(t *testing.T) {
	started := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		time.Sleep(200 * time.Millisecond) // simulate work in progress at SIGTERM
		_, _ = io.WriteString(w, "done")
	})

	ln := listen(t)
	srv := httpserver.New(testConfig(), handler)
	ctx, cancel := context.WithCancel(context.Background())

	serveDone := make(chan error, 1)
	go func() { serveDone <- httpserver.Serve(ctx, srv, ln, 2*time.Second) }()

	type result struct {
		body string
		err  error
	}
	reqDone := make(chan result, 1)
	go func() {
		resp, err := get(t, "http://"+ln.Addr().String())
		if err != nil {
			reqDone <- result{err: err}
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		reqDone <- result{body: string(b), err: err}
	}()

	<-started
	cancel() // shutdown begins while the request is in flight

	res := <-reqDone
	require.NoError(t, res.err)
	assert.Equal(t, "done", res.body, "in-flight request must finish, not be cut off")
	assert.NoError(t, <-serveDone)

	resp, err := get(t, "http://"+ln.Addr().String())
	if err == nil {
		_ = resp.Body.Close()
	}
	assert.Error(t, err, "server must not accept connections after shutdown")
}

func TestServe_ShutdownTimeoutIsReported(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	handler := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release // never finishes on its own within the shutdown window
	})

	ln := listen(t)
	srv := httpserver.New(testConfig(), handler)
	ctx, cancel := context.WithCancel(context.Background())

	serveDone := make(chan error, 1)
	go func() { serveDone <- httpserver.Serve(ctx, srv, ln, 50*time.Millisecond) }()
	go func() {
		if resp, err := get(t, "http://"+ln.Addr().String()); err == nil {
			_ = resp.Body.Close()
		}
	}()

	<-started
	cancel()

	err := <-serveDone
	require.Error(t, err)
	assert.Contains(t, err.Error(), "graceful shutdown did not finish")
}

func TestListenAndServe_BindError(t *testing.T) {
	ln := listen(t) // occupy a port
	defer ln.Close()

	cfg := testConfig()
	cfg.Addr = ln.Addr().String()
	err := httpserver.ListenAndServe(context.Background(), httpserver.New(cfg, http.NotFoundHandler()), time.Second)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "listen on")
}

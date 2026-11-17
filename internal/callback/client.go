package callback

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"syscall"
	"time"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/validation"
)

// ErrBlockedAddress is returned when a callback URL resolves to an address
// we must not connect to (private, loopback, link-local, ...).
var ErrBlockedAddress = errors.New("callback address is not public")

// ClientConfig tunes the HTTP client.
type ClientConfig struct {
	Timeout time.Duration
	// AllowInsecure permits http:// URLs; AllowPrivate permits private and
	// loopback addresses. Development only (refused in production).
	AllowInsecure bool
	AllowPrivate  bool
}

// NewHTTPClient returns a client hardened against SSRF:
//
//   - The address is checked when connecting, after DNS resolution, in
//     the dialer. Checking the URL when the notification was created is
//     not enough: a name can resolve to a public address then and to
//     169.254.169.254 (cloud metadata) or 10.x later (DNS rebinding).
//   - Redirects are not followed: a public URL could redirect inward.
//   - Environment proxies are ignored: the proxy would make the connection,
//     bypassing the check.
func NewHTTPClient(cfg ClientConfig) *http.Client {
	dialer := &net.Dialer{
		Timeout: 5 * time.Second,
		Control: func(_, address string, _ syscall.RawConn) error {
			if cfg.AllowPrivate {
				return nil
			}
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			ip, err := netip.ParseAddr(host)
			if err != nil || !validation.IsPublicIP(ip) {
				return fmt.Errorf("%w: %s", ErrBlockedAddress, host)
			}
			return nil
		},
	}
	return &http.Client{
		Timeout: cfg.Timeout,
		Transport: &http.Transport{
			Proxy:                 nil,
			DialContext:           dialer.DialContext,
			TLSHandshakeTimeout:   5 * time.Second,
			ResponseHeaderTimeout: cfg.Timeout,
			MaxIdleConnsPerHost:   4,
			IdleConnTimeout:       90 * time.Second,
			ForceAttemptHTTP2:     true,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// Result is the outcome of one POST.
type Result struct {
	StatusCode int // 0 if there was no response
	Err        error
	Retryable  bool
}

// Post sends one signed callback.
func Post(ctx context.Context, client *http.Client, cfg ClientConfig, secret []byte, d Request, now time.Time) Result {
	u, err := url.Parse(d.URL)
	if err != nil || (u.Scheme != "https" && (u.Scheme != "http" || !cfg.AllowInsecure)) {
		return Result{Err: fmt.Errorf("callback URL must use https: %q", d.URL)}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, bytes.NewReader(d.Body))
	if err != nil {
		return Result{Err: err}
	}
	ts := now.Unix()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "notification-system-callbacks/1")
	req.Header.Set(HeaderEventID, strconv.FormatInt(d.EventID, 10))
	req.Header.Set(HeaderEventType, d.EventType)
	req.Header.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	req.Header.Set(HeaderSignature, Sign(secret, ts, d.Body))

	resp, err := client.Do(req)
	if err != nil {
		// Blocked addresses will not unblock themselves; anything else
		// (timeouts, refused connections, DNS) may be temporary.
		return Result{Err: err, Retryable: !errors.Is(err, ErrBlockedAddress)}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10)) // reuse the connection

	code := resp.StatusCode
	switch {
	case code >= 200 && code < 300:
		return Result{StatusCode: code}
	case code == http.StatusRequestTimeout || code == http.StatusTooManyRequests || code >= 500:
		return Result{StatusCode: code, Err: fmt.Errorf("receiver answered %d", code), Retryable: true}
	default:
		// 3xx (redirects are not followed) and other 4xx: the receiver
		// rejected it; retrying the same request will not help.
		return Result{StatusCode: code, Err: fmt.Errorf("receiver answered %d", code)}
	}
}

// Request is one callback to POST.
type Request struct {
	URL       string
	EventID   int64
	EventType string
	Body      []byte
}

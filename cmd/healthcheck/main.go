// Command healthcheck probes an HTTP endpoint and exits 0 only on a 2xx
// response. The distroless runtime image has no shell, curl or wget, so
// Docker HEALTHCHECK needs this tiny static binary instead.
//
// Usage: healthcheck [url]   (default http://127.0.0.1:8080/health)
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	url := "http://127.0.0.1:8080/health"
	if len(os.Args) > 1 {
		url = os.Args[1]
	}
	if err := probe(url); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func probe(url string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// The URL comes from the image's own HEALTHCHECK instruction, not from
	// untrusted input, so the SSRF warning does not apply.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody) //nolint:gosec // see above
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // see above
	if err != nil {
		return fmt.Errorf("request %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("%s returned %d", url, resp.StatusCode)
	}
	return nil
}

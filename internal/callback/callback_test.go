package callback_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/callback"
)

var secret = []byte("test-secret-at-least-32-characters!!")

func TestSignAndVerify(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	body := []byte(`{"event_id":1,"type":"notification.sent"}`)
	ts := strconv.FormatInt(now.Unix(), 10)
	sig := callback.Sign(secret, now.Unix(), body)
	assert.Regexp(t, `^sha256=[0-9a-f]{64}$`, sig)

	require.NoError(t, callback.Verify(secret, sig, ts, body, now.Add(time.Minute), 5*time.Minute))

	tests := map[string]struct {
		secret, body []byte
		sig, ts      string
		at           time.Time
		want         error
	}{
		"tampered body":   {secret, []byte(`{"event_id":2}`), sig, ts, now, callback.ErrBadSignature},
		"wrong secret":    {[]byte("other"), body, sig, ts, now, callback.ErrBadSignature},
		"timestamp swap":  {secret, body, sig, strconv.FormatInt(now.Unix()+1, 10), now, callback.ErrBadSignature},
		"replayed later":  {secret, body, sig, ts, now.Add(10 * time.Minute), callback.ErrStale},
		"from the future": {secret, body, sig, ts, now.Add(-10 * time.Minute), callback.ErrStale},
		"garbage ts":      {secret, body, sig, "yesterday", now, callback.ErrBadSignature},
		"missing prefix":  {secret, body, sig[len("sha256="):], ts, now, callback.ErrBadSignature},
		"empty signature": {secret, body, "", ts, now, callback.ErrBadSignature},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			assert.ErrorIs(t, callback.Verify(tt.secret, tt.sig, tt.ts, tt.body, tt.at, 5*time.Minute), tt.want)
		})
	}
}

// receiver is a client's endpoint that verifies what it gets.
func receiver(t *testing.T, status int) (*httptest.Server, *[]error) {
	t.Helper()
	var results []error
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		results = append(results, callback.Verify(secret, r.Header.Get(callback.HeaderSignature),
			r.Header.Get(callback.HeaderTimestamp), body, time.Now(), 5*time.Minute))
		assert.Equal(t, "42", r.Header.Get(callback.HeaderEventID))
		assert.Equal(t, "notification.sent", r.Header.Get(callback.HeaderEventType))
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, &results
}

func post(srvURL string, cfg callback.ClientConfig) callback.Result {
	return callback.Post(context.Background(), callback.NewHTTPClient(cfg), cfg, secret,
		callback.Request{URL: srvURL, EventID: 42, EventType: "notification.sent", Body: []byte(`{"ok":true}`)}, time.Now())
}

var devCfg = callback.ClientConfig{Timeout: 2 * time.Second, AllowInsecure: true, AllowPrivate: true}

func TestPost_SignedAndVerifiable(t *testing.T) {
	srv, results := receiver(t, http.StatusNoContent)
	res := post(srv.URL, devCfg)
	require.NoError(t, res.Err)
	assert.Equal(t, http.StatusNoContent, res.StatusCode)
	require.Len(t, *results, 1)
	assert.NoError(t, (*results)[0], "the receiver could verify the signature")
}

func TestPost_ClassifiesResponses(t *testing.T) {
	for code, retry := range map[int]bool{500: true, 503: true, 429: true, 408: true, 400: false, 404: false, 410: false} {
		srv, _ := receiver(t, code)
		res := post(srv.URL, devCfg)
		assert.Error(t, res.Err, "%d", code)
		assert.Equal(t, code, res.StatusCode)
		assert.Equal(t, retry, res.Retryable, "%d", code)
	}
}

func TestPost_BlocksPrivateAddressesAtConnectTime(t *testing.T) {
	srv, results := receiver(t, http.StatusOK)
	strict := callback.ClientConfig{Timeout: 2 * time.Second, AllowInsecure: true}

	// httptest listens on 127.0.0.1; "localhost" resolves there too, as a
	// public-looking name would after DNS rebinding.
	for _, u := range []string{srv.URL, "http://localhost:" + srv.URL[len("http://127.0.0.1:"):]} {
		res := post(u, strict)
		assert.ErrorIs(t, res.Err, callback.ErrBlockedAddress, u)
		assert.False(t, res.Retryable, "a blocked address will not unblock itself")
	}
	assert.Empty(t, *results, "no request reached the private address")
}

func TestPost_RequiresHTTPS(t *testing.T) {
	srv, results := receiver(t, http.StatusOK)
	res := post(srv.URL, callback.ClientConfig{Timeout: time.Second, AllowPrivate: true})
	assert.ErrorContains(t, res.Err, "https")
	assert.Empty(t, *results)
}

func TestPost_DoesNotFollowRedirects(t *testing.T) {
	inner, results := receiver(t, http.StatusOK)
	outer := httptest.NewServer(http.RedirectHandler(inner.URL, http.StatusTemporaryRedirect))
	t.Cleanup(outer.Close)

	res := post(outer.URL, devCfg)
	assert.Equal(t, http.StatusTemporaryRedirect, res.StatusCode)
	assert.Error(t, res.Err)
	assert.False(t, res.Retryable)
	assert.Empty(t, *results, "a public URL must not bounce us to an internal one")
}

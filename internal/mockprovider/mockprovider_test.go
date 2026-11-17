package mockprovider_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/domain"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/mockprovider"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/provider"
)

func setup(t *testing.T, cfg mockprovider.Config) (*httptest.Server, *provider.HTTPClient) {
	t.Helper()
	srv := httptest.NewServer(mockprovider.New(cfg, nil).Handler())
	t.Cleanup(srv.Close)
	c := provider.NewHTTPClient(provider.HTTPConfig{
		URLs:    map[domain.Channel]string{domain.ChannelSMS: srv.URL + "/send", domain.ChannelPush: srv.URL + "/send"},
		Timeout: 2 * time.Second,
	})
	return srv, c
}

func msg(ch domain.Channel) *provider.Message {
	return &provider.Message{NotificationID: uuid.New(), Channel: ch, To: "+905551234567", Content: "hi"}
}

func stats(t *testing.T, srv *httptest.Server) mockprovider.Stats {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/admin/stats", http.NoBody)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var s mockprovider.Stats
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&s))
	return s
}

// TestMock_SpeaksTheProviderContract: our real client must accept the mock's
// responses exactly as it accepts the real provider's.
func TestMock_SpeaksTheProviderContract(t *testing.T) {
	_, c := setup(t, mockprovider.Config{})
	res := c.Send(t.Context(), msg(domain.ChannelSMS))
	assert.Equal(t, domain.OutcomeSuccess, res.Outcome)
	assert.Equal(t, http.StatusAccepted, res.StatusCode)
	assert.NotEmpty(t, res.MessageID)
}

func TestMock_InjectedFaultsAreClassifiedCorrectly(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg     mockprovider.Config
		outcome domain.AttemptOutcome
		code    string
	}{
		"503": {mockprovider.Config{FailureRate: 1}, domain.OutcomeTransientError, "http_503"},
		"429": {mockprovider.Config{RateLimitRate: 1}, domain.OutcomeTransientError, "http_429"},
		"400": {mockprovider.Config{PermanentRate: 1}, domain.OutcomePermanentError, "http_400"},
	} {
		t.Run(name, func(t *testing.T) {
			_, c := setup(t, tc.cfg)
			res := c.Send(t.Context(), msg(domain.ChannelSMS))
			assert.Equal(t, tc.outcome, res.Outcome)
			assert.Equal(t, tc.code, res.ErrorCode)
			if name == "429" {
				assert.Equal(t, time.Second, res.RetryAfter)
			}
		})
	}
}

func TestMock_ConfigCanChangeAtRuntime(t *testing.T) {
	srv, c := setup(t, mockprovider.Config{})
	body, _ := json.Marshal(mockprovider.Config{FailureRate: 1})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, srv.URL+"/admin/config", bytes.NewReader(body))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()

	assert.Equal(t, "http_503", c.Send(t.Context(), msg(domain.ChannelSMS)).ErrorCode, "provider outage switched on live")
}

func TestMock_Stats(t *testing.T) {
	srv, c := setup(t, mockprovider.Config{})
	m := msg(domain.ChannelSMS)
	c.Send(t.Context(), m)
	c.Send(t.Context(), m) // the same notification again: a duplicate delivery
	c.Send(t.Context(), msg(domain.ChannelPush))

	s := stats(t, srv)
	assert.Equal(t, 3, s.Received)
	assert.Equal(t, 3, s.Accepted)
	assert.Equal(t, 1, s.Duplicates)
	assert.Equal(t, map[string]int{"sms": 2, "push": 1}, s.ByChannel)
	assert.Equal(t, 2, s.MaxPerSecond["sms"])
	assert.Equal(t, 3, s.ByStatus["202"])
}

func TestMock_ConfigUsesReadableDurations(t *testing.T) {
	var c mockprovider.Config
	require.NoError(t, json.Unmarshal([]byte(`{"latency":"250ms","failure_rate":0.5}`), &c))
	assert.Equal(t, mockprovider.Duration(250*time.Millisecond), c.Latency)

	b, err := json.Marshal(c)
	require.NoError(t, err)
	assert.Contains(t, string(b), `"latency":"250ms"`)

	assert.Error(t, json.Unmarshal([]byte(`{"latency":250}`), &c), "bare numbers are ambiguous")
}

func TestMock_RejectsMalformedRequests(t *testing.T) {
	srv, _ := setup(t, mockprovider.Config{})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/send", bytes.NewReader([]byte(`{"to":""}`)))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

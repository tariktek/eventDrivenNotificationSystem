package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/health"
)

func ok(context.Context) error { return nil }

func failing(context.Context) error { return errors.New("connection refused") }

func TestRegistry_Check(t *testing.T) {
	tests := []struct {
		name          string
		critical      health.Check
		nonCritical   health.Check
		wantStatus    health.Status
		wantDBStatus  health.Status
		wantErrString string
	}{
		{"all healthy", ok, ok, health.StatusOK, health.StatusOK, ""},
		{"non-critical down degrades", ok, failing, health.StatusDegraded, health.StatusOK, ""},
		{"critical down", failing, ok, health.StatusDown, health.StatusDown, "connection refused"},
		{"both down", failing, failing, health.StatusDown, health.StatusDown, "connection refused"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := health.NewRegistry(time.Second)
			reg.Register("postgres", true, tt.critical)
			reg.Register("redis", false, tt.nonCritical)

			report := reg.Check(context.Background())

			assert.Equal(t, tt.wantStatus, report.Status)
			require.Contains(t, report.Checks, "postgres")
			require.Contains(t, report.Checks, "redis")
			assert.Equal(t, tt.wantDBStatus, report.Checks["postgres"].Status)
			assert.True(t, report.Checks["postgres"].Critical)
			assert.False(t, report.Checks["redis"].Critical)
			assert.Equal(t, tt.wantErrString, report.Checks["postgres"].Error)
		})
	}
}

func TestRegistry_CheckTimesOut(t *testing.T) {
	reg := health.NewRegistry(20 * time.Millisecond)
	reg.Register("slow", true, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	start := time.Now()
	report := reg.Check(context.Background())

	assert.Less(t, time.Since(start), time.Second, "check must be bounded by the timeout")
	assert.Equal(t, health.StatusDown, report.Status)
	assert.Contains(t, report.Checks["slow"].Error, "deadline exceeded")
}

func TestRegistry_ChecksRunConcurrently(t *testing.T) {
	reg := health.NewRegistry(time.Second)
	sleep := func(ctx context.Context) error {
		select {
		case <-time.After(100 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, name := range []string{"a", "b", "c", "d"} {
		reg.Register(name, true, sleep)
	}

	start := time.Now()
	report := reg.Check(context.Background())

	assert.Equal(t, health.StatusOK, report.Status)
	assert.Less(t, time.Since(start), 300*time.Millisecond, "4 x 100ms checks should overlap")
}

func TestRegistry_Draining(t *testing.T) {
	reg := health.NewRegistry(time.Second)
	reg.Register("postgres", true, ok)
	reg.SetDraining()

	report := reg.Check(context.Background())

	assert.Equal(t, health.StatusDown, report.Status)
	assert.Equal(t, "shutting down", report.Reason)
}

func TestReadinessHandler_StatusCodes(t *testing.T) {
	tests := []struct {
		name     string
		critical health.Check
		other    health.Check
		drain    bool
		wantCode int
		wantBody health.Status
	}{
		{"ok", ok, ok, false, http.StatusOK, health.StatusOK},
		{"degraded is still ready", ok, failing, false, http.StatusOK, health.StatusDegraded},
		{"critical down", failing, ok, false, http.StatusServiceUnavailable, health.StatusDown},
		{"draining", ok, ok, true, http.StatusServiceUnavailable, health.StatusDown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := health.NewRegistry(time.Second)
			reg.Register("postgres", true, tt.critical)
			reg.Register("redis", false, tt.other)
			if tt.drain {
				reg.SetDraining()
			}

			rec := httptest.NewRecorder()
			reg.ReadinessHandler()(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/ready", http.NoBody))

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			var report health.Report
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &report))
			assert.Equal(t, tt.wantBody, report.Status)
		})
	}
}

func TestLivenessHandler(t *testing.T) {
	rec := httptest.NewRecorder()
	health.LivenessHandler()(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health", http.NoBody))

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.JSONEq(t, `{"status":"ok"}`, rec.Body.String())
}

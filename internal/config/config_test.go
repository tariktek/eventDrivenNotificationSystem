package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/config"
)

func TestLoadFrom_Defaults(t *testing.T) {
	cfg, err := config.LoadFrom(map[string]string{})
	require.NoError(t, err)

	assert.Equal(t, config.EnvDevelopment, cfg.Env)
	assert.Equal(t, 20*time.Second, cfg.ShutdownTimeout)
	assert.Equal(t, "info", cfg.Log.Level)
	assert.Equal(t, "json", cfg.Log.Format)
	assert.Equal(t, ":8080", cfg.HTTP.Addr)
	assert.Equal(t, 5*time.Second, cfg.HTTP.ReadHeaderTimeout)
	assert.Equal(t, int32(20), cfg.Database.MaxConns)
	assert.Equal(t, "redis://localhost:6379/0", cfg.Redis.URL)
	assert.Equal(t, []string{config.DevAPIKey}, cfg.Auth.APIKeys)
	assert.Equal(t, 24*time.Hour, cfg.Auth.IdempotencyTTL)
}

func TestLoadFrom_MultipleAPIKeysForRotation(t *testing.T) {
	cfg, err := config.LoadFrom(map[string]string{"API_KEYS": "key-one, key-two "})
	require.NoError(t, err)
	assert.Equal(t, []string{"key-one", "key-two"}, cfg.Auth.APIKeys)
}

func TestLoadFrom_Overrides(t *testing.T) {
	cfg, err := config.LoadFrom(map[string]string{
		"APP_ENV":            "production",
		"LOG_LEVEL":          "debug",
		"LOG_FORMAT":         "text",
		"HTTP_ADDR":          ":9999",
		"DATABASE_URL":       "postgresql://u:p@db:5432/app",
		"DATABASE_MAX_CONNS": "50",
		"REDIS_URL":          "rediss://cache:6380/1",
		"SHUTDOWN_TIMEOUT":   "45s",
		"API_KEYS":           "a-production-key-that-is-long-enough",
	})
	require.NoError(t, err)

	assert.Equal(t, config.EnvProduction, cfg.Env)
	assert.Equal(t, "debug", cfg.Log.Level)
	assert.Equal(t, "text", cfg.Log.Format)
	assert.Equal(t, ":9999", cfg.HTTP.Addr)
	assert.Equal(t, "postgresql://u:p@db:5432/app", cfg.Database.URL)
	assert.Equal(t, int32(50), cfg.Database.MaxConns)
	assert.Equal(t, "rediss://cache:6380/1", cfg.Redis.URL)
	assert.Equal(t, 45*time.Second, cfg.ShutdownTimeout)
}

func TestLoadFrom_Invalid(t *testing.T) {
	tests := []struct {
		name    string
		vars    map[string]string
		wantErr string
	}{
		{"unknown env", map[string]string{"APP_ENV": "staging"}, "APP_ENV"},
		{"bad log level", map[string]string{"LOG_LEVEL": "verbose"}, "LOG_LEVEL"},
		{"bad log format", map[string]string{"LOG_FORMAT": "xml"}, "LOG_FORMAT"},
		{"zero shutdown timeout", map[string]string{"SHUTDOWN_TIMEOUT": "0s"}, "SHUTDOWN_TIMEOUT"},
		{"negative http timeout", map[string]string{"HTTP_READ_TIMEOUT": "-1s"}, "HTTP_READ_TIMEOUT"},
		{"wrong db scheme", map[string]string{"DATABASE_URL": "mysql://localhost/db"}, "DATABASE_URL"},
		{"db without host", map[string]string{"DATABASE_URL": "postgres:///db"}, "DATABASE_URL"},
		{"min conns above max", map[string]string{"DATABASE_MIN_CONNS": "10", "DATABASE_MAX_CONNS": "5"}, "DATABASE_MIN_CONNS"},
		{"wrong redis scheme", map[string]string{"REDIS_URL": "http://localhost:6379"}, "REDIS_URL"},
		{"empty api key", map[string]string{"API_KEYS": "a,,b"}, "API_KEYS"},
		{"dev key in production", map[string]string{"APP_ENV": "production"}, "API_KEYS"},
		{"short key in production", map[string]string{"APP_ENV": "production", "API_KEYS": "short"}, "API_KEYS"},
		{"zero idempotency ttl", map[string]string{"IDEMPOTENCY_TTL": "0s"}, "IDEMPOTENCY_TTL"},
		{"zero body limit", map[string]string{"HTTP_MAX_BODY_BYTES": "0"}, "HTTP_MAX_BODY_BYTES"},
		{"insecure callbacks in production", map[string]string{"APP_ENV": "production", "CALLBACK_ALLOW_PRIVATE": "true"}, "CALLBACK_ALLOW"},
		// Parse errors come from the env library and name the struct field and
		// the offending value rather than the variable.
		{"unparseable duration", map[string]string{"SHUTDOWN_TIMEOUT": "soon"}, `ShutdownTimeout" of type "time.Duration": unable to parse duration: time: invalid duration "soon"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := config.LoadFrom(tt.vars)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestLoadFrom_ReportsAllErrorsAtOnce(t *testing.T) {
	_, err := config.LoadFrom(map[string]string{
		"APP_ENV":   "staging",
		"LOG_LEVEL": "verbose",
		"REDIS_URL": "http://x",
	})
	require.Error(t, err)
	for _, want := range []string{"APP_ENV", "LOG_LEVEL", "REDIS_URL"} {
		assert.Contains(t, err.Error(), want)
	}
}

func TestLoadFrom_DoesNotLeakDatabasePassword(t *testing.T) {
	_, err := config.LoadFrom(map[string]string{"DATABASE_URL": "postgres://user:s3cret@[bad"})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "s3cret")
}

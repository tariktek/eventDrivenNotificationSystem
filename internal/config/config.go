// Package config loads and validates process configuration from environment
// variables. Configuration is validated once at startup so that a
// misconfigured service fails fast with a readable error instead of
// misbehaving later at runtime.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Environment names the deployment environment the process runs in.
type Environment string

// Supported environments.
const (
	EnvDevelopment Environment = "development"
	EnvTest        Environment = "test"
	EnvProduction  Environment = "production"
)

// Config is the root configuration shared by all binaries.
type Config struct {
	Env             Environment   `env:"APP_ENV"          envDefault:"development"`
	ShutdownTimeout time.Duration `env:"SHUTDOWN_TIMEOUT" envDefault:"20s"`

	Log      LogConfig      `envPrefix:"LOG_"`
	HTTP     HTTPConfig     `envPrefix:"HTTP_"`
	Database DatabaseConfig `envPrefix:"DATABASE_"`
	Redis    RedisConfig    `envPrefix:"REDIS_"`
	Callback CallbackConfig `envPrefix:"CALLBACK_"`
	Auth     AuthConfig
	Provider ProviderConfig `envPrefix:"PROVIDER_"`
	Worker   WorkerConfig   `envPrefix:"WORKER_"`
	Retry    RetryConfig    `envPrefix:"RETRY_"`
	Breaker  BreakerConfig  `envPrefix:"BREAKER_"`
	Events   EventsConfig   `envPrefix:"EVENTS_"`
	Outbox   OutboxConfig   `envPrefix:"OUTBOX_"`
	WS       WSConfig       `envPrefix:"WS_"`
	Tracing  TracingConfig  `envPrefix:"TRACING_"`
}

// TracingConfig controls OpenTelemetry tracing.
type TracingConfig struct {
	Enabled bool `env:"ENABLED" envDefault:"false"`
	// Endpoint is the OTLP/HTTP collector base URL (Jaeger accepts OTLP).
	Endpoint string `env:"ENDPOINT" envDefault:"http://localhost:4318"`
	// SampleRatio is the share of new traces kept, in [0, 1].
	SampleRatio float64 `env:"SAMPLE_RATIO" envDefault:"1"`
}

// WSConfig tunes the WebSocket endpoint.
type WSConfig struct {
	MaxClients int `env:"MAX_CLIENTS" envDefault:"1000"`
	// BufferSize is how many events may queue for one client before it is
	// disconnected as too slow.
	BufferSize int `env:"BUFFER_SIZE" envDefault:"256"`
	// AllowedOrigins are extra browser origins (host patterns such as
	// "app.example.com" or "localhost:*"); the API's own host is always
	// allowed.
	AllowedOrigins []string `env:"ALLOWED_ORIGINS" envSeparator:","`
}

// EventsConfig locates the domain event stream.
type EventsConfig struct {
	Stream string `env:"STREAM" envDefault:"stream:notification-events"`
	// StreamMaxLen trims the stream to about this many entries (~0.5 KB
	// each). A consumer further behind than that misses events.
	StreamMaxLen int64 `env:"STREAM_MAX_LEN" envDefault:"100000"`
}

// OutboxConfig tunes the relay from the outbox table to the stream.
type OutboxConfig struct {
	BatchSize     int           `env:"BATCH_SIZE"     envDefault:"500"`
	PollInterval  time.Duration `env:"POLL_INTERVAL"  envDefault:"100ms"`
	Retention     time.Duration `env:"RETENTION"      envDefault:"24h"`
	PruneInterval time.Duration `env:"PRUNE_INTERVAL" envDefault:"10m"`
}

// WorkerConfig tunes the delivery pipeline.
type WorkerConfig struct {
	// ID identifies this worker in leases and logs; generated if empty.
	ID string `env:"ID"`
	// Concurrency is the max in-flight deliveries per channel.
	Concurrency  int           `env:"CONCURRENCY"   envDefault:"100"`
	BatchSize    int           `env:"BATCH_SIZE"    envDefault:"50"`
	PollInterval time.Duration `env:"POLL_INTERVAL" envDefault:"1s"`
	Lease        time.Duration `env:"LEASE"         envDefault:"30s"`
	ReapInterval time.Duration `env:"REAP_INTERVAL" envDefault:"10s"`
	// PriorityAging promotes waiting work one priority level per interval.
	PriorityAging time.Duration `env:"PRIORITY_AGING" envDefault:"5m"`
	// RateLimit is messages per second per channel, across all workers.
	RateLimit   int           `env:"RATE_LIMIT"   envDefault:"100"`
	RateHorizon time.Duration `env:"RATE_HORIZON" envDefault:"1s"`
	// ExpectedInstances sizes the local fallback limiter: if Redis is down,
	// each worker sends at RateLimit / ExpectedInstances.
	ExpectedInstances int `env:"EXPECTED_INSTANCES" envDefault:"1"`
}

// RetryConfig shapes the backoff between delivery attempts.
type RetryConfig struct {
	BaseDelay time.Duration `env:"BASE_DELAY" envDefault:"2s"`
	MaxDelay  time.Duration `env:"MAX_DELAY"  envDefault:"5m"`
}

// BreakerConfig tunes the per-channel circuit breaker in front of the
// provider: it opens when at least MIN_REQUESTS deliveries in WINDOW
// failed at FAILURE_RATIO or more, and probes again after OPEN_TIMEOUT.
type BreakerConfig struct {
	Window       time.Duration `env:"WINDOW"        envDefault:"10s"`
	MinRequests  int           `env:"MIN_REQUESTS"  envDefault:"20"`
	FailureRatio float64       `env:"FAILURE_RATIO" envDefault:"0.5"`
	OpenTimeout  time.Duration `env:"OPEN_TIMEOUT"  envDefault:"30s"`
	Probes       int           `env:"PROBES"        envDefault:"5"`
}

// ProviderConfig locates the external notification provider. URL serves
// every channel unless a channel-specific URL overrides it (real systems
// often use a different vendor per channel).
type ProviderConfig struct {
	URL      string        `env:"URL"       envDefault:"http://localhost:9000/send"`
	URLSMS   string        `env:"URL_SMS"`
	URLEmail string        `env:"URL_EMAIL"`
	URLPush  string        `env:"URL_PUSH"`
	Timeout  time.Duration `env:"TIMEOUT"   envDefault:"10s"`
}

// URLFor returns the endpoint for a channel ("sms", "email" or "push").
func (p ProviderConfig) URLFor(channel string) string {
	override := map[string]string{"sms": p.URLSMS, "email": p.URLEmail, "push": p.URLPush}[channel]
	if override != "" {
		return override
	}
	return p.URL
}

// DevAPIKey is the default key for local development. It is rejected when
// APP_ENV=production so it can never protect a real deployment.
const DevAPIKey = "dev-api-key-change-me" //nolint:gosec // public on purpose; rejected in production

// AuthConfig controls API authentication and request idempotency.
type AuthConfig struct {
	// APIKeys are accepted values of the X-API-Key header. Several keys allow
	// rotation: add the new key, migrate clients, remove the old one.
	APIKeys []string `env:"API_KEYS" envDefault:"dev-api-key-change-me" envSeparator:","`
	// IdempotencyTTL is how long an Idempotency-Key response is replayable.
	IdempotencyTTL time.Duration `env:"IDEMPOTENCY_TTL" envDefault:"24h"`
}

// LogConfig controls structured logging output.
type LogConfig struct {
	Level  string `env:"LEVEL"  envDefault:"info"`
	Format string `env:"FORMAT" envDefault:"json"`
}

// HTTPConfig controls the API server. Explicit timeouts protect the server
// against slow clients (e.g. Slowloris) holding connections open.
type HTTPConfig struct {
	Addr              string        `env:"ADDR"                envDefault:":8080"`
	ReadHeaderTimeout time.Duration `env:"READ_HEADER_TIMEOUT" envDefault:"5s"`
	ReadTimeout       time.Duration `env:"READ_TIMEOUT"        envDefault:"15s"`
	WriteTimeout      time.Duration `env:"WRITE_TIMEOUT"       envDefault:"15s"`
	IdleTimeout       time.Duration `env:"IDLE_TIMEOUT"        envDefault:"60s"`
	// MaxBodyBytes caps request bodies; 5 MiB fits a 1000-item batch.
	MaxBodyBytes int64 `env:"MAX_BODY_BYTES" envDefault:"5242880"`
}

// DatabaseConfig controls the PostgreSQL connection pool.
type DatabaseConfig struct {
	URL      string `env:"URL"       envDefault:"postgres://notification:notification@localhost:5432/notification?sslmode=disable"`
	MaxConns int32  `env:"MAX_CONNS" envDefault:"20"`
	MinConns int32  `env:"MIN_CONNS" envDefault:"2"`
}

// RedisConfig controls the Redis client.
type RedisConfig struct {
	URL string `env:"URL" envDefault:"redis://localhost:6379/0"`
}

// CallbackConfig relaxes callback URL rules for local development. Both
// must stay false in production: they would let clients make the service
// call internal endpoints (SSRF).
type CallbackConfig struct {
	AllowInsecure bool `env:"ALLOW_INSECURE" envDefault:"false"` // permit http://
	AllowPrivate  bool `env:"ALLOW_PRIVATE"  envDefault:"false"` // permit localhost/private IPs
	// SigningSecret keys the HMAC in X-Signature; clients verify with it.
	SigningSecret  string        `env:"SIGNING_SECRET"   envDefault:"dev-callback-secret-change-me"`
	Timeout        time.Duration `env:"TIMEOUT"          envDefault:"5s"`
	MaxAttempts    int           `env:"MAX_ATTEMPTS"     envDefault:"5"`
	Concurrency    int           `env:"CONCURRENCY"      envDefault:"20"`
	RetryBaseDelay time.Duration `env:"RETRY_BASE_DELAY" envDefault:"5s"`
	RetryMaxDelay  time.Duration `env:"RETRY_MAX_DELAY"  envDefault:"10m"`
	// Lease hides a claimed callback from other workers while it is sent.
	Lease        time.Duration `env:"LEASE"         envDefault:"1m"`
	PollInterval time.Duration `env:"POLL_INTERVAL" envDefault:"1s"`
}

// DevCallbackSecret is the default signing secret for local development;
// rejected in production. It must match CallbackConfig.SigningSecret's
// envDefault (TestDevCallbackSecretIsTheDefault guards that).
const DevCallbackSecret = "dev-callback-secret-change-me" //nolint:gosec // public on purpose; rejected in production

// Load reads configuration from the process environment and validates it.
func Load() (Config, error) {
	return load(env.Options{})
}

// LoadFrom reads configuration from the given variables instead of the
// process environment. It exists so tests do not depend on global state.
func LoadFrom(vars map[string]string) (Config, error) {
	return load(env.Options{Environment: vars})
}

func load(opts env.Options) (Config, error) {
	cfg, err := env.ParseAsWithOptions[Config](opts)
	if err != nil {
		return Config{}, fmt.Errorf("parse environment: %w", err)
	}
	// "key-a, key-b" is a natural way to write a list; HTTP header values
	// are trimmed too, so untrimmed keys could never match.
	for i, k := range cfg.Auth.APIKeys {
		cfg.Auth.APIKeys[i] = strings.TrimSpace(k)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

// Validate reports every invalid setting at once rather than stopping at the
// first, so an operator can fix the whole configuration in one pass.
func (c Config) Validate() error {
	var errs []error

	if !slices.Contains([]Environment{EnvDevelopment, EnvTest, EnvProduction}, c.Env) {
		errs = append(errs, fmt.Errorf("APP_ENV: must be one of development, test, production; got %q", c.Env))
	}
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("SHUTDOWN_TIMEOUT: must be positive"))
	}

	if !slices.Contains([]string{"debug", "info", "warn", "error"}, c.Log.Level) {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: must be one of debug, info, warn, error; got %q", c.Log.Level))
	}
	if !slices.Contains([]string{"json", "text"}, c.Log.Format) {
		errs = append(errs, fmt.Errorf("LOG_FORMAT: must be json or text; got %q", c.Log.Format))
	}

	if c.HTTP.Addr == "" {
		errs = append(errs, errors.New("HTTP_ADDR: must not be empty"))
	}
	for _, t := range []struct {
		name string
		d    time.Duration
	}{
		{"HTTP_READ_HEADER_TIMEOUT", c.HTTP.ReadHeaderTimeout},
		{"HTTP_READ_TIMEOUT", c.HTTP.ReadTimeout},
		{"HTTP_WRITE_TIMEOUT", c.HTTP.WriteTimeout},
		{"HTTP_IDLE_TIMEOUT", c.HTTP.IdleTimeout},
	} {
		if t.d <= 0 {
			errs = append(errs, fmt.Errorf("%s: must be positive", t.name))
		}
	}

	if c.HTTP.MaxBodyBytes <= 0 {
		errs = append(errs, errors.New("HTTP_MAX_BODY_BYTES: must be positive"))
	}

	if len(c.Auth.APIKeys) == 0 {
		errs = append(errs, errors.New("API_KEYS: at least one key is required"))
	}
	for _, k := range c.Auth.APIKeys {
		switch {
		case k == "":
			errs = append(errs, errors.New("API_KEYS: keys must not be empty"))
		case c.Env == EnvProduction && (k == DevAPIKey || len(k) < 32):
			errs = append(errs, errors.New("API_KEYS: production keys must be at least 32 characters and not the development default"))
		}
	}
	if c.Auth.IdempotencyTTL <= 0 {
		errs = append(errs, errors.New("IDEMPOTENCY_TTL: must be positive"))
	}

	errs = append(errs, c.Worker.validate()...)
	if c.Retry.BaseDelay <= 0 || c.Retry.MaxDelay < c.Retry.BaseDelay {
		errs = append(errs, errors.New("RETRY_BASE_DELAY/RETRY_MAX_DELAY: need 0 < base <= max"))
	}

	if c.Breaker.Window <= 0 || c.Breaker.OpenTimeout <= 0 {
		errs = append(errs, errors.New("BREAKER_WINDOW/BREAKER_OPEN_TIMEOUT: must be positive"))
	}
	if c.Breaker.MinRequests < 1 || c.Breaker.Probes < 1 {
		errs = append(errs, errors.New("BREAKER_MIN_REQUESTS/BREAKER_PROBES: must be at least 1"))
	}
	if c.Breaker.FailureRatio <= 0 || c.Breaker.FailureRatio > 1 {
		errs = append(errs, errors.New("BREAKER_FAILURE_RATIO: must be in (0, 1]"))
	}

	if c.Events.Stream == "" || c.Events.StreamMaxLen < 1000 {
		errs = append(errs, errors.New("EVENTS_STREAM/EVENTS_STREAM_MAX_LEN: need a stream name and a max length of at least 1000"))
	}
	if c.Outbox.BatchSize < 1 || c.Outbox.BatchSize > 10000 {
		errs = append(errs, errors.New("OUTBOX_BATCH_SIZE: must be between 1 and 10000"))
	}
	if c.Outbox.PollInterval <= 0 || c.Outbox.Retention <= 0 || c.Outbox.PruneInterval <= 0 {
		errs = append(errs, errors.New("OUTBOX_POLL_INTERVAL/OUTBOX_RETENTION/OUTBOX_PRUNE_INTERVAL: must be positive"))
	}

	if c.Tracing.SampleRatio < 0 || c.Tracing.SampleRatio > 1 {
		errs = append(errs, errors.New("TRACING_SAMPLE_RATIO: must be between 0 and 1"))
	}
	if c.Tracing.Enabled {
		if err := validateURL(c.Tracing.Endpoint, "http", "https"); err != nil {
			errs = append(errs, fmt.Errorf("TRACING_ENDPOINT: %w", err))
		}
	}
	if c.WS.MaxClients < 1 || c.WS.BufferSize < 1 {
		errs = append(errs, errors.New("WS_MAX_CLIENTS/WS_BUFFER_SIZE: must be at least 1"))
	}

	cb := c.Callback
	switch {
	case cb.SigningSecret == "":
		errs = append(errs, errors.New("CALLBACK_SIGNING_SECRET: must be set"))
	case c.Env == EnvProduction && (cb.SigningSecret == DevCallbackSecret || len(cb.SigningSecret) < 32):
		errs = append(errs, errors.New("CALLBACK_SIGNING_SECRET: production secrets must be at least 32 characters and not the development default"))
	}
	if cb.Timeout <= 0 || cb.Lease <= cb.Timeout {
		errs = append(errs, errors.New("CALLBACK_TIMEOUT/CALLBACK_LEASE: need 0 < timeout < lease"))
	}
	if cb.MaxAttempts < 1 || cb.MaxAttempts > 20 || cb.Concurrency < 1 {
		errs = append(errs, errors.New("CALLBACK_MAX_ATTEMPTS/CALLBACK_CONCURRENCY: need 1..20 attempts and concurrency >= 1"))
	}
	if cb.RetryBaseDelay <= 0 || cb.RetryMaxDelay < cb.RetryBaseDelay || cb.PollInterval <= 0 {
		errs = append(errs, errors.New("CALLBACK_RETRY_BASE_DELAY/CALLBACK_RETRY_MAX_DELAY/CALLBACK_POLL_INTERVAL: need 0 < base <= max and a positive poll interval"))
	}

	if c.Env == EnvProduction && (c.Callback.AllowInsecure || c.Callback.AllowPrivate) {
		errs = append(errs, errors.New("CALLBACK_ALLOW_INSECURE/CALLBACK_ALLOW_PRIVATE: must be false in production"))
	}

	for name, u := range map[string]string{
		"PROVIDER_URL": c.Provider.URL, "PROVIDER_URL_SMS": c.Provider.URLSMS,
		"PROVIDER_URL_EMAIL": c.Provider.URLEmail, "PROVIDER_URL_PUSH": c.Provider.URLPush,
	} {
		if u == "" && name != "PROVIDER_URL" {
			continue // optional override
		}
		if err := validateURL(u, "http", "https"); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		}
	}
	if c.Provider.Timeout <= 0 {
		errs = append(errs, errors.New("PROVIDER_TIMEOUT: must be positive"))
	}

	if err := validateURL(c.Database.URL, "postgres", "postgresql"); err != nil {
		errs = append(errs, fmt.Errorf("DATABASE_URL: %w", err))
	}
	if c.Database.MinConns < 0 || c.Database.MaxConns < 1 || c.Database.MinConns > c.Database.MaxConns {
		errs = append(errs, fmt.Errorf("DATABASE_MIN_CONNS/DATABASE_MAX_CONNS: need 0 <= min <= max and max >= 1; got min=%d max=%d",
			c.Database.MinConns, c.Database.MaxConns))
	}

	if err := validateURL(c.Redis.URL, "redis", "rediss"); err != nil {
		errs = append(errs, fmt.Errorf("REDIS_URL: %w", err))
	}

	return errors.Join(errs...)
}

func (w WorkerConfig) validate() []error {
	var errs []error
	for _, v := range []struct {
		name string
		val  int
	}{
		{"WORKER_CONCURRENCY", w.Concurrency},
		{"WORKER_BATCH_SIZE", w.BatchSize},
		{"WORKER_RATE_LIMIT", w.RateLimit},
		{"WORKER_EXPECTED_INSTANCES", w.ExpectedInstances},
	} {
		if v.val < 1 {
			errs = append(errs, fmt.Errorf("%s: must be at least 1", v.name))
		}
	}
	for _, d := range []struct {
		name string
		val  time.Duration
	}{
		{"WORKER_POLL_INTERVAL", w.PollInterval},
		{"WORKER_LEASE", w.Lease},
		{"WORKER_REAP_INTERVAL", w.ReapInterval},
		{"WORKER_PRIORITY_AGING", w.PriorityAging},
		{"WORKER_RATE_HORIZON", w.RateHorizon},
	} {
		if d.val <= 0 {
			errs = append(errs, fmt.Errorf("%s: must be positive", d.name))
		}
	}
	if w.BatchSize > w.Concurrency {
		errs = append(errs, errors.New("WORKER_BATCH_SIZE: must not exceed WORKER_CONCURRENCY"))
	}
	if w.RateLimit > 0 && w.RateHorizon < time.Second/time.Duration(w.RateLimit) {
		errs = append(errs, errors.New("WORKER_RATE_HORIZON: must fit at least one send slot (1s / WORKER_RATE_LIMIT)"))
	}
	return errs
}

func validateURL(raw string, schemes ...string) error {
	if raw == "" {
		return errors.New("must not be empty")
	}
	u, err := url.Parse(raw)
	if err != nil {
		// url.Parse errors echo the input, which may contain a password.
		return errors.New("is not a valid URL")
	}
	if !slices.Contains(schemes, u.Scheme) {
		return fmt.Errorf("scheme must be one of %v; got %q", schemes, u.Scheme)
	}
	if u.Host == "" {
		return errors.New("must include a host")
	}
	return nil
}

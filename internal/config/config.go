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
}

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

// Package router wires HTTP routes for the API process.
package router

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/api"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/apierror"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/docs"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/handler"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/middleware"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/idempotency"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/observability"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/health"
)

// Deps are the collaborators the routes need.
type Deps struct {
	Health         *health.Registry
	API            *handler.Handler
	Log            *slog.Logger
	MaxBodyBytes   int64
	APIKeys        []string
	Idempotency    idempotency.Store
	IdempotencyTTL time.Duration
	Now            func() time.Time
}

// idempotentRoutes are the operations whose spec declares the
// Idempotency-Key header: the ones that create resources.
var idempotentRoutes = map[string]bool{
	"/api/v1/notifications":       true,
	"/api/v1/notifications/batch": true,
}

// New returns the API's root handler.
func New(d Deps) http.Handler {
	r := chi.NewRouter()
	errs := apierror.Writer{
		Log:           d.Log,
		CorrelationID: func(req *http.Request) string { return observability.CorrelationID(req.Context()) },
	}

	// Order matters: the correlation ID first so every log line carries it;
	// the request log outside recovery so requests that panic are logged too
	// (as the 500 that recovery turns them into).
	r.Use(
		middleware.CorrelationID,
		middleware.RequestLog(d.Log),
		middleware.Recovery(d.Log, errs.Write),
		middleware.SecureHeaders,
	)

	r.NotFound(func(w http.ResponseWriter, req *http.Request) {
		errs.Write(w, req, apierror.New(http.StatusNotFound, api.ErrorCodeNotFound, "No route matches "+req.Method+" "+req.URL.Path+"."))
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, req *http.Request) {
		errs.Write(w, req, apierror.New(http.StatusMethodNotAllowed, api.ErrorCodeBadRequest, "Method "+req.Method+" is not allowed on "+req.URL.Path+"."))
	})

	// Ops endpoints: no authentication, no body.
	r.Get("/health", health.LivenessHandler())
	r.Get("/ready", d.Health.ReadinessHandler())
	r.Get("/openapi.yaml", docs.SpecHandler())
	r.Get("/docs", docs.UIHandler())

	// API routes generated from the OpenAPI spec. Authentication runs first
	// so unauthenticated requests never reach body parsing or the database.
	apiRoutes := r.With(
		middleware.APIKeyAuth(d.APIKeys, errs.Write),
		middleware.BufferBody(d.MaxBodyBytes, errs.Write),
		middleware.Idempotency(middleware.IdempotencyConfig{
			Store: d.Idempotency,
			TTL:   d.IdempotencyTTL,
			Now:   d.Now,
			Log:   d.Log,
			Applies: func(req *http.Request) bool {
				return req.Method == http.MethodPost && idempotentRoutes[req.URL.Path]
			},
		}, errs.Write),
	)
	handler.HTTPHandler(d.API, api.ChiServerOptions{BaseRouter: apiRoutes}, errs.Write)

	return r
}

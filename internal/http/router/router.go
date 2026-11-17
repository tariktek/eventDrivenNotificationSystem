// Package router wires HTTP routes for the API process.
package router

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/tariktek/eventDrivenNotificationSystem/internal/http/docs"
	"github.com/tariktek/eventDrivenNotificationSystem/internal/platform/health"
)

// Deps are the collaborators the routes need.
type Deps struct {
	Health *health.Registry
}

// New returns the API's root handler.
func New(d Deps) http.Handler {
	r := chi.NewRouter()

	r.Get("/health", health.LivenessHandler())
	r.Get("/ready", d.Health.ReadinessHandler())

	r.Get("/openapi.yaml", docs.SpecHandler())
	r.Get("/docs", docs.UIHandler())

	return r
}

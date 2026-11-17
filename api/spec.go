// Package apispec embeds the OpenAPI specification so the API can serve it
// at /openapi.yaml for Swagger UI and client generators.
package apispec

import _ "embed"

// YAML is the raw contents of openapi.yaml.
//
//go:embed openapi.yaml
var YAML []byte

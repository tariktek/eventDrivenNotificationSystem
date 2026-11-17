// Package migrations embeds the versioned SQL schema migrations.
//
// In Docker Compose the migrate/migrate container applies them from disk;
// embedding them as well lets integration tests build the exact same schema
// without depending on the working directory.
package migrations

import "embed"

// FS holds every *.up.sql and *.down.sql file in this directory.
//
//go:embed *.sql
var FS embed.FS

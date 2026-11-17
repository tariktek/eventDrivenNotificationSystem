// Package buildinfo exposes version metadata injected at build time via
// -ldflags, e.g.:
//
//	go build -ldflags "-X github.com/tariktek/eventDrivenNotificationSystem/internal/platform/buildinfo.Version=v1.0.0"
package buildinfo

// Values are overwritten by the linker; the defaults identify local builds.
var (
	Version = "dev"
	Commit  = "none"
)

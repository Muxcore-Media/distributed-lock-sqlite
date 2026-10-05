package internal

import (
	modulesdk "github.com/Muxcore-Media/core/sdk/go/module"
	manifest "github.com/Muxcore-Media/distributed-lock-sqlite"
)

// Version is an optional build-time override set by cmd/module from -ldflags.
// When empty or "dev"/"0.0.0-dev", the version from muxcore.json is used (ADR-0021).
var Version = ""

func moduleVersion() string {
	if Version != "" && Version != "dev" && Version != "0.0.0-dev" {
		return Version
	}
	return modulesdk.ManifestVersion(manifest.ManifestJSON)
}

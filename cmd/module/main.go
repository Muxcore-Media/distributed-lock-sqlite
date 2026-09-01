package main

import (
	"log/slog"
	"os"

	modulesdk "github.com/Muxcore-Media/core/sdk/go/module"

	"github.com/Muxcore-Media/distributed-lock-sqlite/internal"
)

var version = "0.0.0-dev"

func main() {
	internal.Version = version
	mod, err := internal.NewModule(internal.Config{})
	if err != nil {
		slog.Error("invalid module config", "error", err)
		os.Exit(1)
	}
	insecure := os.Getenv("MUXCORE_INSECURE_DISABLE_TLS") == "true" || os.Getenv("MUXCORE_GRPC_INSECURE") == "true"
	if err := modulesdk.Run(modulesdk.Config{
		Module:   mod,
		Insecure: insecure,
	}); err != nil {
		slog.Error("module exited", "error", err)
		os.Exit(1)
	}
}

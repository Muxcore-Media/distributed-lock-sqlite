# Changelog

## [0.1.3] — 2026-08-10

### Fixed

- Self-hosted CI (`runs-on: self-hosted`; `go test` without `-race` for laptop runners)

### Changed

- `muxcore.json` / `Info()` align `minCoreVersion` and contract pin to **0.5.0** (matches `go.mod` core **v0.5.8**)

## [0.1.2] — 2026-08-10

### Added

- Advertise `settings` capability so admin-ui discovers SettingsProvider without ListAll probing.

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0/).

## [Unreleased]

### Added

- `pkg/client` implementing `DistributedLockProvider` with bufconn tests
- Restart persistence test; gRPC Serve round-trip test
- Settings persistence (`db_path`, `sweep_interval`) beside the database
- Input validation (`InvalidArgument`) for empty keys/tokens and non-positive TTL
- SQLite `busy_timeout(5000)` pragma; loopback default `LOCK_GRPC_ADDR=127.0.0.1:9604`

### Fixed

- Stop lock theft via holder_id re-acquire; conflict errors no longer leak holder IDs
- Hold DB read lock for entire Acquire/Unlock/Renew/sweep operations during reopen
- `filepath.Dir` for database directory creation; invalid `LOCK_SWEEP_INTERVAL` fails startup
- Wire `var version` in `cmd/module/main.go` for Makefile `-X main.version`

## [0.1.1] — 2026-08-10

### Added

- `RegisterSettings` / `SettingsUpdater` for live `db_path` and `sweep_interval`
- Pin `core` / contracts / `sdk/go/module` to **v0.5.2**

## [0.1.0] — 2026-08-09

### Added

- SQLite-backed `DistributedLockProvider` sidecar (`distributed.lock`) with TTL, sweeper, renew, and token unlock.
- COMPATIBILITY: single-node / shared-volume / multi-writer limits.

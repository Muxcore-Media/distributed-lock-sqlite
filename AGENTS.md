# AGENTS.md — distributed-lock-sqlite

MuxCore sidecar module (`distributed-lock-sqlite`). Workspace deploy and SSH: [`../AGENTS.md`](../AGENTS.md). Default ports: [`_mvp/PORTS.md`](../_mvp/PORTS.md).

## Module identity

| Field | Value |
|-------|-------|
| Directory | `distributed-lock-sqlite` |
| Capabilities | `distributed.lock`, `settings` |
| Contracts | `DistributedLockProvider` |

## Agent rules

- Modules run as gRPC sidecars; capabilities are the security boundary.
- TLS required in production (`MUXCORE_INSECURE_DISABLE_TLS` / `MUXCORE_GRPC_INSECURE` is dev-only).
- Match existing Go patterns; run `gofmt` and package tests before finishing.
- Cross-module events: prefer `github.com/Muxcore-Media/contracts-media/events` over deprecated `core/pkg/contracts` aliases.
- Do not edit polluted workspace dumps (see `MASTER-ROADMAP.md` Appendix H).
- Optional in MVP: `MVP_ENABLE_DISTRIBUTED_LOCK=1` in `_mvp/run-host.sh`.

## Build

```bash
cd distributed-lock-sqlite
nix-shell -p go --run 'go test ./...'
```

## Client

`pkg/client` implements `DistributedLockProvider` over `DistributedLockService` gRPC. Map `Acquire` not-acquired responses to `contracts.ErrLockHeld`.

## Settings

Live `db_path` and `sweep_interval` via admin-ui (`settings` capability). Updates persist to `distributed-lock-sqlite-settings.json` beside the database and survive restart.

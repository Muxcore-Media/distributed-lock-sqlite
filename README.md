# Distributed Lock SQLite

SQLite-backed lock provider for mesh-wide coordination on a **single MuxCore host** (or carefully constrained shared volume).

A gRPC sidecar that implements `DistributedLockProvider` using a local SQLite database with WAL mode. TTL-based lock expiration, automatic expired lock sweeping, and token-based unlock/renew. Without this module, core has no durable lock capability.

Use `pkg/client` for in-process callers that need the `DistributedLockProvider` contract over gRPC.

> **Not multi-writer HA.** See [COMPATIBILITY.md](COMPATIBILITY.md) for single-node vs shared-volume vs multi-module limits.

## Single-node expectations

This module is the **laptop / single-host** lock provider:

| Expectation | Detail |
|-------------|--------|
| Topology | **One** `distributed-lock-sqlite` process per mesh, `LOCK_DB_PATH` on **local disk** |
| Coordination | Callers on that mesh dial this module; locks are **not** shared across separate SQLite files |
| Restart | Rows survive process restart; abandoned holders expire via **TTL + sweeper** |
| Not supported | Multi-writer HA, multiple lock modules on one DB, or NFS/shared-volume multi-host writers |

For cluster-wide locks, use an external store (Redis `SET NX`, etcd, Postgres advisory locks). Full matrix: [COMPATIBILITY.md](COMPATIBILITY.md).

## Key Features

- **TTL-based locks** — Locks auto-expire after a configurable duration
- **Periodic sweeper** — Background goroutine purges expired locks at `LOCK_SWEEP_INTERVAL` (default 10s)
- **Token-based unlock** — Each lock acquisition returns a unique cryptographically random token required to unlock or renew
- **Renew API** — Extend a held lock's TTL with the current token (re-acquire by holder ID alone is rejected)
- **Settings** — Live `db_path` and `sweep_interval` via admin-ui; persisted beside the database
- **Go client** — `pkg/client` implements `DistributedLockProvider` over gRPC

## Configuration

| Env var | Default | Description |
|---------|---------|-------------|
| `LOCK_GRPC_ADDR` | `127.0.0.1:9604` | gRPC listen address (loopback by default; set explicitly for non-loopback) |
| `LOCK_DB_PATH` | `/var/lib/distributed-lock-sqlite/locks.db` | SQLite database path |
| `LOCK_SWEEP_INTERVAL` | `10s` | Expired lock sweep interval (Go duration; invalid values fail startup) |
| `MUXCORE_INSECURE_DISABLE_TLS` | unset | Dev-only: disable TLS on inbound and outbound gRPC |
| `MUXCORE_GRPC_INSECURE` | unset | Alias for `MUXCORE_INSECURE_DISABLE_TLS` |
| `LOCK_TLS_CERT` / `_KEY` / `_CA` / `_DIR` | — | Optional TLS material (auto-generated when unset) |

### MVP stack

Enable in `_mvp/run-host.sh` with `MVP_ENABLE_DISTRIBUTED_LOCK=1`. Data lands under `$DATA/locks/locks.db` (port **9604**, see `_mvp/PORTS.md`).

## Usage

```bash
distributed-lock-sqlite
```

Production uses TLS by default on the module gRPC listener. For local dev without TLS, set `MUXCORE_INSECURE_DISABLE_TLS=true` on both core and this module.

### Go client

```go
// Dev only — use TLS credentials in production (see LOCK_TLS_* / auto-generated CA).
conn, _ := grpc.NewClient("127.0.0.1:9604", grpc.WithTransportCredentials(insecure.NewCredentials()))
lock := client.New(conn, "my-module")
handle, err := lock.Acquire(ctx, "migration", 30*time.Second)
if errors.Is(err, contracts.ErrLockHeld) {
    // another holder has the lock
}
defer handle.Unlock(ctx)
```

## Contract

Implements `DistributedLockProvider` (`pkg/contracts`). Registers capabilities: `distributed.lock`, `settings`.

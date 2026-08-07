# Distributed Lock SQLite

SQLite-backed distributed lock provider for cluster-wide coordination in MuxCore.

A gRPC sidecar module that provides distributed mutexes using a local SQLite database with WAL mode. TTL-based lock expiration, automatic expired lock sweeping, and re-acquire by the same holder. Without this module, core has no durable distributed lock capability.

## Key Features

- **TTL-based locks** — Locks auto-expire after a configurable duration
- **Periodic sweeper** — Background goroutine purges expired locks at `LOCK_SWEEP_INTERVAL` (default 10s)
- **Re-acquire support** — Same holder can re-acquire without releasing first (issues a new token)
- **Token-based unlock** — Each lock acquisition returns a unique cryptographically random token required to unlock
- **Renew API** — Extend a held lock's TTL before it expires

## Configuration

| Env var | Default | Description |
|---------|---------|-------------|
| `LOCK_GRPC_ADDR` | `:9612` | gRPC listen address |
| `LOCK_DB_PATH` | `/var/lib/distributed-lock-sqlite/locks.db` | SQLite database path |
| `LOCK_SWEEP_INTERVAL` | `10s` | Expired lock sweep interval |
| `MUXCORE_INSECURE_DISABLE_TLS` | unset | Set `true` to disable TLS for the module SDK |

## Usage

```bash
export MUXCORE_INSECURE_DISABLE_TLS=true
distributed-lock-sqlite
```

## Contract

Implements `DistributedLockProvider` (`pkg/contracts`). Registers capability: `distributed.lock`.

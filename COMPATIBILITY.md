# Compatibility

## Core Version

| Module Version | Core Version | Status |
|----------------|-------------|--------|
| v0.1.0         | v0.4.0+     | Current |

## Contract

| Contract | Capability | Status |
|----------|-----------|--------|
| DistributedLockProvider | `distributed.lock` | Current |

## Deployment limits (read carefully)

Despite the “distributed” name, this module is **SQLite-backed** and is **not** a multi-writer cluster lock service.

| Topology | Supported? | Notes |
|----------|------------|-------|
| **Single node** — one `distributed-lock-sqlite` process, local disk `LOCK_DB_PATH` | **Yes** | Preferred. Locks coordinate callers on that mesh that dial this module. |
| **Shared volume** — one lock module, DB on NFS/CIFS/shared FS | **Risky** | SQLite + network filesystems are fragile (locking, WAL, split brain). Prefer a local disk or a purpose-built lock store. If you must, use a single writer host and a filesystem with working POSIX locks; still expect edge-case corruption under failover. |
| **Multiple lock modules** each with their **own** DB | **No** | Separate SQLite files do **not** share lock state. Clients will see inconsistent Acquire/Unlock. |
| **Multiple lock modules** writing the **same** DB file | **No** | Do not run concurrent SQLite writers against one file (even on a “shared” volume). |
| **Multi-host HA** without a shared consensus store | **No** | Use an external lock backend (e.g. Redis/`SET NX`, etcd, Postgres advisory locks) when you outgrow single-node SQLite. |

### Operational expectations

- **TTL + sweeper** reclaim abandoned locks; clock skew between clients and the lock module affects expiry accuracy.
- Process restart keeps rows in SQLite; expired rows are swept. A crashed holder’s lock remains until TTL/sweep.
- **One** module advertising `distributed.lock` per mesh is the intended setup.

## Breaking Changes

This is a pre-1.0 module. Interfaces may change without notice.

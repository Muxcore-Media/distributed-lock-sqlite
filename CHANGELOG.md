# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0/).

## [Unreleased]

## [0.1.0] — 2026-08-09

### Added

- SQLite-backed `DistributedLockProvider` sidecar (`distributed.lock`) with TTL, sweeper, renew, and token unlock.
- COMPATIBILITY: single-node / shared-volume / multi-writer limits.

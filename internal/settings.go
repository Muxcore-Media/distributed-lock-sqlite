package internal

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Muxcore-Media/core/pkg/contracts"
)

func (m *Module) Settings() []contracts.SettingDef {
	return m.settingsDefs()
}

func (m *Module) UpdateSetting(key, value string) error {
	return m.updateSetting(key, value)
}

func (m *Module) settingsDefs() []contracts.SettingDef {
	m.mu.RLock()
	path := m.dbPath
	interval := m.sweepInterval
	m.mu.RUnlock()
	return []contracts.SettingDef{
		{
			Key:         "db_path",
			Label:       "SQLite Database Path",
			Type:        contracts.SettingTypeString,
			Value:       path,
			Default:     "/var/lib/distributed-lock-sqlite/locks.db",
			Description: "Lock database path (LOCK_DB_PATH); updates reopen SQLite live",
			Group:       "Storage",
		},
		{
			Key:         "sweep_interval",
			Label:       "Sweep Interval",
			Type:        contracts.SettingTypeString,
			Value:       interval.String(),
			Default:     "10s",
			Description: "Expired-lock sweeper period (LOCK_SWEEP_INTERVAL, Go duration)",
			Group:       "Maintenance",
		},
	}
}

func (m *Module) updateSetting(key, value string) error {
	value = strings.TrimSpace(value)
	switch key {
	case "db_path", "LOCK_DB_PATH":
		if value == "" {
			return fmt.Errorf("db_path must not be empty")
		}
		return m.reopenDB(context.Background(), value)
	case "sweep_interval", "LOCK_SWEEP_INTERVAL":
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return fmt.Errorf("invalid sweep_interval %q (positive Go duration)", value)
		}
		m.mu.Lock()
		m.sweepInterval = d
		if m.sweeper != nil {
			m.sweeper.Reset(d)
		}
		m.mu.Unlock()
		return nil
	default:
		return fmt.Errorf("unknown setting %q", key)
	}
}

func (m *Module) reopenDB(ctx context.Context, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("create db directory: %w", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return fmt.Errorf("open sqlite: %w", err)
	}
	if _, err := db.ExecContext(ctx, `PRAGMA journal_mode=WAL`); err != nil {
		_ = db.Close()
		return fmt.Errorf("enable WAL: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS locks (
			key        TEXT PRIMARY KEY,
			holder_id  TEXT NOT NULL,
			token      TEXT NOT NULL,
			expires_at INTEGER NOT NULL,
			created_at INTEGER NOT NULL
		)
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("create locks table: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE INDEX IF NOT EXISTS idx_locks_expires ON locks(expires_at)
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("create index: %w", err)
	}

	m.mu.Lock()
	old := m.db
	m.db = db
	m.dbPath = path
	m.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return nil
}

package internal

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/Muxcore-Media/core/pkg/contracts"
	distributedlockv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/distributedlock/v1"
	modulesdk "github.com/Muxcore-Media/core/sdk/go/module"
	_ "modernc.org/sqlite"
)

type Module struct {
	distributedlockv1.UnimplementedDistributedLockServiceServer

	mu      sync.RWMutex
	db      *sql.DB
	sweeper *time.Ticker
	done    chan struct{}

	id            string
	dbPath        string
	grpcAddr      string
	sweepInterval time.Duration
	grpcSrv       *grpc.Server
	lis           net.Listener
}

type Config struct {
	ID            string
	DBPath        string
	GRPCAddr      string
	SweepInterval time.Duration
}

func NewModule(cfg Config) *Module {
	if cfg.ID == "" {
		cfg.ID = "distributed-lock-sqlite"
	}
	if cfg.DBPath == "" {
		cfg.DBPath = "/var/lib/distributed-lock-sqlite/locks.db"
	}
	if cfg.GRPCAddr == "" {
		cfg.GRPCAddr = ":9604"
	}
	if cfg.SweepInterval == 0 {
		cfg.SweepInterval = 10 * time.Second
	}
	if v := os.Getenv("LOCK_DB_PATH"); v != "" {
		cfg.DBPath = v
	}
	if v := os.Getenv("LOCK_GRPC_ADDR"); v != "" {
		cfg.GRPCAddr = v
	}
	if v := os.Getenv("LOCK_SWEEP_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.SweepInterval = d
		}
	}
	return &Module{
		id:            cfg.ID,
		dbPath:        cfg.DBPath,
		grpcAddr:      cfg.GRPCAddr,
		sweepInterval: cfg.SweepInterval,
	}
}

func (m *Module) Info() contracts.ModuleInfo {
	return contracts.ModuleInfo{
		ID:           m.id,
		Name:         "Distributed Lock SQLite",
		Version:      "0.1.3",
		Roles:        []string{"infrastructure"},
		Description:  "SQLite-backed lock provider (single-node; see COMPATIBILITY for shared-volume limits)",
		Author:       "MuxCore",
		Capabilities: []string{contracts.CapabilityDistributedLock, "settings"},
		Contracts: []contracts.ContractDeclaration{
			{
				Repo:      "github.com/Muxcore-Media/core/pkg/contracts",
				Interface: "DistributedLockProvider",
				Version:   "v0.5.0",
			},
		},
		MinCoreVersion: "0.5.0",
		HTTPAddr:       m.grpcAddr,
	}
}

func (m *Module) Init(ctx context.Context) error {
	dir := m.dbPath
	for i := len(dir) - 1; i >= 0; i-- {
		if dir[i] == '/' {
			dir = dir[:i]
			break
		}
	}
	if dir != m.dbPath {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return fmt.Errorf("create db directory %s: %w", dir, err)
		}
	}

	db, err := sql.Open("sqlite", m.dbPath)
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
	m.db = db
	m.mu.Unlock()

	lis, err := net.Listen("tcp", m.grpcAddr)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("listen %s: %w", m.grpcAddr, err)
	}
	m.lis = lis

	slog.Info("distributed-lock-sqlite initialized",
		"db", m.dbPath,
		"addr", m.grpcAddr,
		"sweep_interval", m.sweepInterval,
	)
	return nil
}

func (m *Module) Start(ctx context.Context) error {
	m.grpcSrv = grpc.NewServer()
	distributedlockv1.RegisterDistributedLockServiceServer(m.grpcSrv, m)
	modulesdk.RegisterSettings(m.grpcSrv, m.id, m)

	m.done = make(chan struct{})
	m.sweeper = time.NewTicker(m.sweepInterval)
	go m.sweepLoop()

	go func() {
		slog.Info("distributed-lock-sqlite gRPC service started", "addr", m.grpcAddr)
		if err := m.grpcSrv.Serve(m.lis); err != nil {
			slog.Error("distributed-lock-sqlite gRPC serve error", "error", err)
		}
	}()
	return nil
}

func (m *Module) Stop(ctx context.Context) error {
	if m.sweeper != nil {
		m.sweeper.Stop()
	}
	if m.done != nil {
		close(m.done)
	}
	if m.grpcSrv != nil {
		m.grpcSrv.GracefulStop()
	}
	m.mu.Lock()
	if m.db != nil {
		_ = m.db.Close()
		m.db = nil
	}
	m.mu.Unlock()
	slog.Info("distributed-lock-sqlite stopped")
	return nil
}

func (m *Module) Health(ctx context.Context) error {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return errors.New("database not initialized")
	}
	return db.PingContext(ctx)
}

func (m *Module) Acquire(ctx context.Context, req *distributedlockv1.AcquireRequest) (*distributedlockv1.AcquireResponse, error) {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, errors.New("not initialized")
	}

	now := time.Now().UnixNano()
	expiresAt := now + (req.GetTtlMs() * 1_000_000)
	token, err := newToken()
	if err != nil {
		return nil, fmt.Errorf("new token: %w", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `DELETE FROM locks WHERE expires_at <= ?`, now); err != nil {
		return nil, fmt.Errorf("prune expired: %w", err)
	}

	res, err := tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO locks (key, holder_id, token, expires_at, created_at) VALUES (?, ?, ?, ?, ?)`,
		req.GetKey(), req.GetHolderId(), token, expiresAt, now,
	)
	if err != nil {
		return nil, fmt.Errorf("acquire lock: %w", err)
	}

	n, _ := res.RowsAffected()
	if n > 0 {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit acquire: %w", err)
		}
		return &distributedlockv1.AcquireResponse{
			Acquired:  true,
			LockToken: token,
		}, nil
	}

	var existingHolder string
	var existingExpires int64
	err = tx.QueryRowContext(ctx,
		`SELECT holder_id, expires_at FROM locks WHERE key = ?`, req.GetKey(),
	).Scan(&existingHolder, &existingExpires)
	if err != nil {
		return nil, fmt.Errorf("check existing lock: %w", err)
	}

	if existingHolder == req.GetHolderId() {
		_, err = tx.ExecContext(ctx,
			`UPDATE locks SET token = ?, expires_at = ?, created_at = ? WHERE key = ?`,
			token, expiresAt, now, req.GetKey(),
		)
		if err != nil {
			return nil, fmt.Errorf("re-acquire: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit re-acquire: %w", err)
		}
		return &distributedlockv1.AcquireResponse{
			Acquired:  true,
			LockToken: token,
		}, nil
	}

	remaining := (existingExpires - now) / 1_000_000
	if remaining < 0 {
		remaining = 0
	}
	return &distributedlockv1.AcquireResponse{
		Acquired:  false,
		LockToken: "",
		Error:     fmt.Sprintf("held by %s for ~%dms", existingHolder, remaining),
	}, nil
}

func (m *Module) Unlock(ctx context.Context, req *distributedlockv1.UnlockRequest) (*distributedlockv1.UnlockResponse, error) {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, errors.New("not initialized")
	}

	res, err := db.ExecContext(ctx,
		`DELETE FROM locks WHERE key = ? AND token = ?`,
		req.GetKey(), req.GetLockToken(),
	)
	if err != nil {
		return nil, fmt.Errorf("unlock: %w", err)
	}

	n, _ := res.RowsAffected()
	return &distributedlockv1.UnlockResponse{Released: n > 0}, nil
}

func (m *Module) Renew(ctx context.Context, req *distributedlockv1.RenewRequest) (*distributedlockv1.RenewResponse, error) {
	m.mu.RLock()
	db := m.db
	m.mu.RUnlock()
	if db == nil {
		return nil, errors.New("not initialized")
	}

	now := time.Now().UnixNano()
	newExpires := now + (req.GetTtlMs() * 1_000_000)

	res, err := db.ExecContext(ctx,
		`UPDATE locks SET expires_at = ? WHERE key = ? AND token = ? AND expires_at > ?`,
		newExpires, req.GetKey(), req.GetLockToken(), now,
	)
	if err != nil {
		return nil, fmt.Errorf("renew: %w", err)
	}

	n, _ := res.RowsAffected()
	return &distributedlockv1.RenewResponse{Renewed: n > 0}, nil
}

func (m *Module) sweepLoop() {
	for {
		select {
		case <-m.done:
			return
		case <-m.sweeper.C:
			m.mu.RLock()
			db := m.db
			m.mu.RUnlock()
			if db == nil {
				continue
			}
			now := time.Now().UnixNano()
			res, err := db.ExecContext(context.Background(),
				`DELETE FROM locks WHERE expires_at <= ?`, now,
			)
			if err != nil {
				slog.Error("sweep expired locks", "error", err)
				continue
			}
			if n, _ := res.RowsAffected(); n > 0 {
				slog.Debug("swept expired locks", "count", n)
			}
		}
	}
}

func newToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

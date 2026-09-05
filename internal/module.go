package internal

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/Muxcore-Media/core/pkg/contracts"
	distributedlockv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/distributedlock/v1"
	modulesdk "github.com/Muxcore-Media/core/sdk/go/module"
	"github.com/Muxcore-Media/distributed-lock-sqlite/internal/grpctls"
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

type persistedSettings struct {
	DBPath        string `json:"db_path"`
	SweepInterval string `json:"sweep_interval"`
}

func NewModule(cfg Config) (*Module, error) {
	if cfg.ID == "" {
		cfg.ID = "distributed-lock-sqlite"
	}
	if cfg.DBPath == "" {
		cfg.DBPath = "/var/lib/distributed-lock-sqlite/locks.db"
	}
	if cfg.GRPCAddr == "" {
		cfg.GRPCAddr = "127.0.0.1:9604"
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
	cfg.GRPCAddr = resolveGRPCAddr(cfg.GRPCAddr)
	if v := os.Getenv("LOCK_SWEEP_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("invalid LOCK_SWEEP_INTERVAL %q", v)
		}
		cfg.SweepInterval = d
	}
	return &Module{
		id:            cfg.ID,
		dbPath:        cfg.DBPath,
		grpcAddr:      cfg.GRPCAddr,
		sweepInterval: cfg.SweepInterval,
	}, nil
}

func (m *Module) Info() contracts.ModuleInfo {
	return contracts.ModuleInfo{
		ID:           m.id,
		Name:         "Distributed Lock SQLite",
		Version:      Version,
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
	if err := m.loadPersistedSettings(); err != nil {
		return err
	}

	dir := filepath.Dir(m.dbPath)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return fmt.Errorf("create db directory %s: %w", dir, err)
		}
	}

	db, err := openSQLite(ctx, m.dbPath)
	if err != nil {
		return err
	}

	m.mu.Lock()
	m.db = db
	m.mu.Unlock()

	slog.Info("distributed-lock-sqlite initialized",
		"db", m.dbPath,
		"addr", m.grpcAddr,
		"sweep_interval", m.sweepInterval,
	)
	return nil
}

func (m *Module) Start(ctx context.Context) error {
	if m.lis == nil {
		lis, err := net.Listen("tcp", m.grpcAddr)
		if err != nil {
			return fmt.Errorf("listen %s: %w", m.grpcAddr, err)
		}
		m.lis = lis
	}

	var grpcOpts []grpc.ServerOption
	tlsCfg, err := grpctls.ServerConfig()
	if err != nil {
		return fmt.Errorf("gRPC TLS: %w", err)
	}
	if tlsCfg != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(tlsCfg)))
		slog.Info("distributed-lock-sqlite gRPC TLS enabled", "addr", m.grpcAddr)
	} else {
		slog.Warn("distributed-lock-sqlite gRPC listening without TLS (dev only)",
			"addr", m.grpcAddr,
			"hint", "unset MUXCORE_INSECURE_DISABLE_TLS for production",
		)
	}
	m.grpcSrv = grpc.NewServer(grpcOpts...)
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
	defer m.mu.RUnlock()
	if m.db == nil {
		return errors.New("database not initialized")
	}
	return m.db.PingContext(ctx)
}

// SetListener attaches a net.Listener before Start (tests).
func (m *Module) SetListener(lis net.Listener) {
	m.lis = lis
}

func (m *Module) Acquire(ctx context.Context, req *distributedlockv1.AcquireRequest) (*distributedlockv1.AcquireResponse, error) {
	if err := validateAcquire(req); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.db == nil {
		return nil, errors.New("not initialized")
	}

	now := time.Now().UnixNano()
	expiresAt := now + (req.GetTtlMs() * 1_000_000)
	token, err := newToken()
	if err != nil {
		return nil, fmt.Errorf("new token: %w", err)
	}

	tx, err := m.db.BeginTx(ctx, nil)
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

	var existingExpires int64
	err = tx.QueryRowContext(ctx,
		`SELECT expires_at FROM locks WHERE key = ?`, req.GetKey(),
	).Scan(&existingExpires)
	if err != nil {
		return nil, fmt.Errorf("check existing lock: %w", err)
	}

	remaining := (existingExpires - now) / 1_000_000
	if remaining < 0 {
		remaining = 0
	}
	return &distributedlockv1.AcquireResponse{
		Acquired: false,
		Error:    fmt.Sprintf("lock held for ~%dms", remaining),
	}, nil
}

func (m *Module) Unlock(ctx context.Context, req *distributedlockv1.UnlockRequest) (*distributedlockv1.UnlockResponse, error) {
	if err := validateUnlock(req); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.db == nil {
		return nil, errors.New("not initialized")
	}

	res, err := m.db.ExecContext(ctx,
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
	if err := validateRenew(req); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.db == nil {
		return nil, errors.New("not initialized")
	}

	now := time.Now().UnixNano()
	newExpires := now + (req.GetTtlMs() * 1_000_000)

	res, err := m.db.ExecContext(ctx,
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
			if m.db == nil {
				m.mu.RUnlock()
				continue
			}
			now := time.Now().UnixNano()
			res, err := m.db.ExecContext(context.Background(),
				`DELETE FROM locks WHERE expires_at <= ?`, now,
			)
			m.mu.RUnlock()
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

func openSQLite(ctx context.Context, path string) (*sql.DB, error) {
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

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
		return nil, fmt.Errorf("create locks table: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE INDEX IF NOT EXISTS idx_locks_expires ON locks(expires_at)
	`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create index: %w", err)
	}

	db.SetMaxOpenConns(1)
	return db, nil
}

func validateAcquire(req *distributedlockv1.AcquireRequest) error {
	if strings.TrimSpace(req.GetKey()) == "" {
		return status.Error(codes.InvalidArgument, "key is required")
	}
	if strings.TrimSpace(req.GetHolderId()) == "" {
		return status.Error(codes.InvalidArgument, "holder_id is required")
	}
	if req.GetTtlMs() <= 0 {
		return status.Error(codes.InvalidArgument, "ttl_ms must be positive")
	}
	return nil
}

func validateUnlock(req *distributedlockv1.UnlockRequest) error {
	if strings.TrimSpace(req.GetKey()) == "" {
		return status.Error(codes.InvalidArgument, "key is required")
	}
	if strings.TrimSpace(req.GetLockToken()) == "" {
		return status.Error(codes.InvalidArgument, "lock_token is required")
	}
	return nil
}

func validateRenew(req *distributedlockv1.RenewRequest) error {
	if strings.TrimSpace(req.GetKey()) == "" {
		return status.Error(codes.InvalidArgument, "key is required")
	}
	if strings.TrimSpace(req.GetLockToken()) == "" {
		return status.Error(codes.InvalidArgument, "lock_token is required")
	}
	if req.GetTtlMs() <= 0 {
		return status.Error(codes.InvalidArgument, "ttl_ms must be positive")
	}
	return nil
}

func (m *Module) settingsFilePath() string {
	dir := filepath.Dir(m.dbPath)
	if dir == "." || dir == "" {
		dir = "."
	}
	return filepath.Join(dir, "distributed-lock-sqlite-settings.json")
}

func (m *Module) loadPersistedSettings() error {
	data, err := os.ReadFile(m.settingsFilePath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read settings: %w", err)
	}
	var ps persistedSettings
	if err := json.Unmarshal(data, &ps); err != nil {
		return fmt.Errorf("parse settings: %w", err)
	}
	if ps.DBPath != "" {
		m.dbPath = ps.DBPath
	}
	if ps.SweepInterval != "" {
		d, err := time.ParseDuration(ps.SweepInterval)
		if err != nil || d <= 0 {
			return fmt.Errorf("invalid persisted sweep_interval %q", ps.SweepInterval)
		}
		m.sweepInterval = d
	}
	return nil
}

func (m *Module) savePersistedSettings() error {
	m.mu.RLock()
	ps := persistedSettings{
		DBPath:        m.dbPath,
		SweepInterval: m.sweepInterval.String(),
	}
	m.mu.RUnlock()

	data, err := json.Marshal(ps)
	if err != nil {
		return fmt.Errorf("marshal settings: %w", err)
	}
	path := m.settingsFilePath()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("create settings directory: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return fmt.Errorf("write settings: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename settings: %w", err)
	}
	return nil
}

// resolveGRPCAddr prefers loopback when plaintext is explicitly enabled and the
// bind address would otherwise listen on all interfaces.
func resolveGRPCAddr(addr string) string {
	if !grpctls.InsecureAllowed() {
		return addr
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		if strings.HasPrefix(addr, ":") {
			return "127.0.0.1" + addr
		}
		return addr
	}
	if host == "" || host == "0.0.0.0" {
		return "127.0.0.1:" + port
	}
	return addr
}

func newToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

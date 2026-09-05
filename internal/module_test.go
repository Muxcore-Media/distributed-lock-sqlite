package internal

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Muxcore-Media/core/pkg/contracts"
	distributedlockv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/distributedlock/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20

func TestModuleInfo(t *testing.T) {
	Version = "0.1.3"
	m, err := NewModule(Config{})
	if err != nil {
		t.Fatal(err)
	}
	info := m.Info()
	if info.ID == "" {
		t.Error("module ID must not be empty")
	}
	if info.Version != "0.1.3" {
		t.Errorf("version = %q want 0.1.3", info.Version)
	}
	if info.MinCoreVersion == "" {
		t.Error("MinCoreVersion must not be empty")
	}
	if len(info.Contracts) == 0 {
		t.Error("Contracts must not be empty")
	}
	if info.Contracts[0].Interface != "DistributedLockProvider" {
		t.Errorf("expected DistributedLockProvider contract, got %s", info.Contracts[0].Interface)
	}
	if len(info.Capabilities) == 0 {
		t.Error("Capabilities must not be empty")
	}
	if info.Capabilities[0] != "distributed.lock" {
		t.Errorf("expected distributed.lock capability, got %s", info.Capabilities[0])
	}
}

func TestDefaultGRPCAddr(t *testing.T) {
	t.Setenv("MUXCORE_INSECURE_DISABLE_TLS", "true")
	m, err := NewModule(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if m.grpcAddr != "127.0.0.1:9604" {
		t.Fatalf("grpcAddr=%q want 127.0.0.1:9604", m.grpcAddr)
	}
}

func TestResolveGRPCAddr_InsecureLoopback(t *testing.T) {
	t.Setenv("MUXCORE_INSECURE_DISABLE_TLS", "true")
	if got := resolveGRPCAddr(":9604"); got != "127.0.0.1:9604" {
		t.Fatalf("got %q", got)
	}
	if got := resolveGRPCAddr("0.0.0.0:9604"); got != "127.0.0.1:9604" {
		t.Fatalf("got %q", got)
	}
	if got := resolveGRPCAddr("192.168.1.1:9604"); got != "192.168.1.1:9604" {
		t.Fatalf("got %q", got)
	}
}

func TestInvalidSweepIntervalEnv(t *testing.T) {
	t.Setenv("LOCK_SWEEP_INTERVAL", "not-a-duration")
	_, err := NewModule(Config{})
	if err == nil {
		t.Fatal("expected error for invalid LOCK_SWEEP_INTERVAL")
	}
}

func newTestModule(t *testing.T) *Module {
	t.Helper()
	m, err := NewModule(Config{
		DBPath:        filepath.Join(t.TempDir(), "test.db"),
		GRPCAddr:      "127.0.0.1:0",
		SweepInterval: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewModule: %v", err)
	}
	ctx := context.Background()
	if err := m.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() {
		_ = m.Stop(ctx)
	})
	return m
}

func TestAcquireRelease(t *testing.T) {
	m := newTestModule(t)
	ctx := context.Background()

	resp, err := m.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key:      "test-key",
		TtlMs:    60_000,
		HolderId: "holder-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Acquired {
		t.Fatal("expected lock acquired")
	}
	if resp.LockToken == "" {
		t.Fatal("expected non-empty lock token")
	}

	unlock, err := m.Unlock(ctx, &distributedlockv1.UnlockRequest{
		Key:       "test-key",
		LockToken: resp.LockToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !unlock.Released {
		t.Fatal("expected lock released")
	}
}

func TestAcquireConflict(t *testing.T) {
	m := newTestModule(t)
	ctx := context.Background()

	resp, err := m.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key:      "conflict-key",
		TtlMs:    60_000,
		HolderId: "holder-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Acquired {
		t.Fatal("expected first acquire to succeed")
	}

	resp, err = m.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key:      "conflict-key",
		TtlMs:    60_000,
		HolderId: "holder-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Acquired {
		t.Fatal("expected second acquire to fail")
	}
	if resp.Error == "" {
		t.Fatal("expected error description")
	}
	if strings.Contains(resp.Error, "holder-1") {
		t.Fatalf("conflict error must not leak holder_id: %q", resp.Error)
	}
}

func TestHolderIDCannotStealLock(t *testing.T) {
	m := newTestModule(t)
	ctx := context.Background()

	resp, err := m.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key:      "steal-key",
		TtlMs:    60_000,
		HolderId: "holder-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Acquired {
		t.Fatal("expected first acquire to succeed")
	}

	resp2, err := m.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key:      "steal-key",
		TtlMs:    60_000,
		HolderId: "holder-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp2.Acquired {
		t.Fatal("same holder_id must not re-acquire without token")
	}

	resp3, err := m.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key:      "steal-key",
		TtlMs:    60_000,
		HolderId: "holder-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp3.Acquired {
		t.Fatal("expected conflict for different holder")
	}
	if strings.Contains(resp3.Error, "holder-1") {
		t.Fatalf("conflict error must not leak holder_id: %q", resp3.Error)
	}
}

func TestTTLExpiry(t *testing.T) {
	m := newTestModule(t)
	ctx := context.Background()

	resp, err := m.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key:      "ttl-key",
		TtlMs:    50,
		HolderId: "holder-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Acquired {
		t.Fatal("expected lock acquired")
	}

	time.Sleep(100 * time.Millisecond)

	resp, err = m.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key:      "ttl-key",
		TtlMs:    60_000,
		HolderId: "holder-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Acquired {
		t.Fatal("expected lock acquired after TTL expiry")
	}
}

func TestRenew(t *testing.T) {
	m := newTestModule(t)
	ctx := context.Background()

	resp, err := m.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key:      "renew-key",
		TtlMs:    60_000,
		HolderId: "holder-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Acquired {
		t.Fatal("expected lock acquired")
	}

	renew, err := m.Renew(ctx, &distributedlockv1.RenewRequest{
		Key:       "renew-key",
		LockToken: resp.LockToken,
		TtlMs:     120_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !renew.Renewed {
		t.Fatal("expected renew to succeed")
	}
}

func TestRenewExpired(t *testing.T) {
	m := newTestModule(t)
	ctx := context.Background()

	resp, err := m.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key:      "renew-expired-key",
		TtlMs:    20,
		HolderId: "holder-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Acquired {
		t.Fatal("expected lock acquired")
	}

	time.Sleep(50 * time.Millisecond)

	renew, err := m.Renew(ctx, &distributedlockv1.RenewRequest{
		Key:       "renew-expired-key",
		LockToken: resp.LockToken,
		TtlMs:     60_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if renew.Renewed {
		t.Fatal("expected renew to fail after expiry")
	}
}

func TestTokenProtection(t *testing.T) {
	m := newTestModule(t)
	ctx := context.Background()

	resp, err := m.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key:      "token-protect-key",
		TtlMs:    60_000,
		HolderId: "holder-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Acquired {
		t.Fatal("expected lock acquired")
	}

	unlock, err := m.Unlock(ctx, &distributedlockv1.UnlockRequest{
		Key:       "token-protect-key",
		LockToken: "wrong-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	if unlock.Released {
		t.Fatal("expected unlock with wrong token to fail")
	}
}

func TestSameHolderUsesRenewNotReacquire(t *testing.T) {
	m := newTestModule(t)
	ctx := context.Background()

	resp, err := m.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key:      "reentrant-key",
		TtlMs:    60_000,
		HolderId: "holder-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Acquired {
		t.Fatal("expected first acquire to succeed")
	}

	resp2, err := m.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key:      "reentrant-key",
		TtlMs:    60_000,
		HolderId: "holder-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp2.Acquired {
		t.Fatal("expected re-acquire without token to fail")
	}

	renew, err := m.Renew(ctx, &distributedlockv1.RenewRequest{
		Key:       "reentrant-key",
		LockToken: resp.LockToken,
		TtlMs:     120_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !renew.Renewed {
		t.Fatal("expected renew to succeed for same holder")
	}

	unlock, err := m.Unlock(ctx, &distributedlockv1.UnlockRequest{
		Key:       "reentrant-key",
		LockToken: resp.LockToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !unlock.Released {
		t.Fatal("expected unlock after renew to succeed")
	}
}

func TestConcurrent(t *testing.T) {
	m := newTestModule(t)
	ctx := context.Background()

	const numGoroutines = 50

	var wg sync.WaitGroup
	results := make(chan string, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := "concurrent-key"
			if id%2 == 0 {
				key = "concurrent-key-b"
			}
			holder := fmt.Sprintf("holder-%d", id)
			resp, err := m.Acquire(ctx, &distributedlockv1.AcquireRequest{
				Key:      key,
				TtlMs:    60_000,
				HolderId: holder,
			})
			if err != nil {
				results <- fmt.Sprintf("err: %v", err)
				return
			}
			if resp.Acquired {
				time.Sleep(5 * time.Millisecond)
				unlock, err := m.Unlock(ctx, &distributedlockv1.UnlockRequest{
					Key:       key,
					LockToken: resp.LockToken,
				})
				if err != nil {
					results <- fmt.Sprintf("unlock-err: %v", err)
					return
				}
				if !unlock.Released {
					results <- "unlock-fail"
					return
				}
				results <- "ok"
			} else {
				results <- "conflict"
			}
		}(i)
	}

	wg.Wait()
	close(results)

	var okCount, conflictCount, otherCount int
	others := make([]string, 0)
	for r := range results {
		switch r {
		case "ok":
			okCount++
		case "conflict":
			conflictCount++
		default:
			otherCount++
			others = append(others, r)
		}
	}
	if otherCount > 0 {
		t.Fatalf("unexpected results (%d): %v", otherCount, others)
	}
	if okCount == 0 {
		t.Fatal("expected at least one successful acquire")
	}
	if conflictCount == 0 {
		t.Fatal("expected at least one conflict")
	}
}

func TestSweep(t *testing.T) {
	m := newTestModule(t)
	ctx := context.Background()

	resp, err := m.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key:      "sweep-key",
		TtlMs:    20,
		HolderId: "holder-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Acquired {
		t.Fatal("expected lock acquired")
	}

	time.Sleep(300 * time.Millisecond)

	resp, err = m.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key:      "sweep-key",
		TtlMs:    60_000,
		HolderId: "holder-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Acquired {
		t.Fatal("expected lock acquired after sweeper cleared expired lock")
	}
}

func TestHealth(t *testing.T) {
	m := newTestModule(t)
	ctx := context.Background()

	if err := m.Health(ctx); err != nil {
		t.Fatal("expected health to pass after init")
	}
}

func TestLifecycle(t *testing.T) {
	t.Setenv("LOCK_TLS_DIR", t.TempDir())
	m, err := NewModule(Config{
		DBPath:   filepath.Join(t.TempDir(), "lifecycle.db"),
		GRPCAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Health(ctx); err != nil {
		t.Fatal("expected health to pass after start")
	}
	if err := m.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestGRPCServe(t *testing.T) {
	t.Setenv("MUXCORE_INSECURE_DISABLE_TLS", "true")
	m, err := NewModule(Config{
		DBPath:   filepath.Join(t.TempDir(), "grpc.db"),
		GRPCAddr: "127.0.0.1:0",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	lis := bufconn.Listen(bufSize)
	m.SetListener(lis)
	if err := m.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.Stop(ctx) })

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := distributedlockv1.NewDistributedLockServiceClient(conn)
	acq, err := client.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key: "grpc-key", HolderId: "h1", TtlMs: 60_000,
	})
	if err != nil || !acq.GetAcquired() {
		t.Fatalf("Acquire: %v %+v", err, acq)
	}
	renew, err := client.Renew(ctx, &distributedlockv1.RenewRequest{
		Key: "grpc-key", LockToken: acq.GetLockToken(), TtlMs: 120_000,
	})
	if err != nil || !renew.GetRenewed() {
		t.Fatalf("Renew: %v %+v", err, renew)
	}
	unlock, err := client.Unlock(ctx, &distributedlockv1.UnlockRequest{
		Key: "grpc-key", LockToken: acq.GetLockToken(),
	})
	if err != nil || !unlock.GetReleased() {
		t.Fatalf("Unlock: %v %+v", err, unlock)
	}
}

func TestRestartPersistence(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "persist.db")
	ctx := context.Background()

	m1, err := NewModule(Config{DBPath: dbPath, GRPCAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m1.Init(ctx); err != nil {
		t.Fatal(err)
	}
	acq, err := m1.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key: "persist-key", HolderId: "holder-1", TtlMs: 60_000,
	})
	if err != nil || !acq.GetAcquired() {
		t.Fatalf("first acquire: %v %+v", err, acq)
	}
	if err := m1.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	m2, err := NewModule(Config{DBPath: dbPath, GRPCAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	if err := m2.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m2.Stop(ctx) }()

	acq2, err := m2.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key: "persist-key", HolderId: "holder-2", TtlMs: 60_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if acq2.GetAcquired() {
		t.Fatal("expected lock still held after process restart until TTL expires")
	}
}

func TestValidationErrors(t *testing.T) {
	m := newTestModule(t)
	ctx := context.Background()

	cases := []struct {
		name string
		run  func() error
	}{
		{"acquire empty key", func() error {
			_, err := m.Acquire(ctx, &distributedlockv1.AcquireRequest{HolderId: "h", TtlMs: 1000})
			return err
		}},
		{"acquire empty holder", func() error {
			_, err := m.Acquire(ctx, &distributedlockv1.AcquireRequest{Key: "k", TtlMs: 1000})
			return err
		}},
		{"acquire zero ttl", func() error {
			_, err := m.Acquire(ctx, &distributedlockv1.AcquireRequest{Key: "k", HolderId: "h", TtlMs: 0})
			return err
		}},
		{"unlock empty token", func() error {
			_, err := m.Unlock(ctx, &distributedlockv1.UnlockRequest{Key: "k"})
			return err
		}},
		{"renew empty token", func() error {
			_, err := m.Renew(ctx, &distributedlockv1.RenewRequest{Key: "k", TtlMs: 1000})
			return err
		}},
		{"renew zero ttl", func() error {
			_, err := m.Renew(ctx, &distributedlockv1.RenewRequest{Key: "k", LockToken: "t", TtlMs: 0})
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.run()
			if err == nil {
				t.Fatal("expected error")
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("code=%v want InvalidArgument", status.Code(err))
			}
		})
	}
}

func TestSettingsDBPathAndSweep(t *testing.T) {
	dir := t.TempDir()
	pathA := filepath.Join(dir, "a.db")
	pathB := filepath.Join(dir, "b.db")
	m, err := NewModule(Config{DBPath: pathA, GRPCAddr: "127.0.0.1:0", SweepInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := m.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Stop(ctx) }()
	defs := m.Settings()
	if len(defs) != 2 || defs[0].Key != "db_path" {
		t.Fatalf("Settings=%+v", defs)
	}
	if err := m.UpdateSetting("sweep_interval", "250ms"); err != nil {
		t.Fatal(err)
	}
	m.mu.RLock()
	got := m.sweepInterval
	m.mu.RUnlock()
	if got != 250*time.Millisecond {
		t.Fatalf("sweepInterval=%v", got)
	}
	acq, err := m.Acquire(ctx, &distributedlockv1.AcquireRequest{Key: "k", HolderId: "h", TtlMs: 5000})
	if err != nil || !acq.GetAcquired() {
		t.Fatalf("acquire before reopen: %v %+v", err, acq)
	}
	if err := m.UpdateSetting("db_path", pathB); err != nil {
		t.Fatal(err)
	}
	acq2, err := m.Acquire(ctx, &distributedlockv1.AcquireRequest{Key: "k", HolderId: "h2", TtlMs: 5000})
	if err != nil || !acq2.GetAcquired() {
		t.Fatalf("acquire after reopen should succeed on empty DB: %v %+v", err, acq2)
	}

	settingsPath := filepath.Join(dir, "distributed-lock-sqlite-settings.json")
	data, err := os.ReadFile(settingsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), pathB) || !strings.Contains(string(data), "250ms") {
		t.Fatalf("settings not persisted: %s", data)
	}

	m3, err := NewModule(Config{DBPath: pathA, GRPCAddr: "127.0.0.1:0", SweepInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := m3.Init(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m3.Stop(ctx) }()
	m3.mu.RLock()
	loadedPath := m3.dbPath
	loadedSweep := m3.sweepInterval
	m3.mu.RUnlock()
	if loadedPath != pathB {
		t.Fatalf("db_path after restart=%q want %q", loadedPath, pathB)
	}
	if loadedSweep != 250*time.Millisecond {
		t.Fatalf("sweep_interval after restart=%v want 250ms", loadedSweep)
	}
}

func TestErrLockHeldContract(t *testing.T) {
	if contracts.ErrLockHeld == nil {
		t.Fatal("ErrLockHeld must be defined")
	}
}

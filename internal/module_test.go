package internal

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	distributedlockv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/distributedlock/v1"
)

func TestModuleInfo(t *testing.T) {
	m := NewModule(Config{})
	info := m.Info()
	if info.ID == "" {
		t.Error("module ID must not be empty")
	}
	if info.Version == "" {
		t.Error("module version must not be empty")
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

func newTestModule(t *testing.T) *Module {
	t.Helper()
	m := NewModule(Config{
		DBPath:        filepath.Join(t.TempDir(), "test.db"),
		GRPCAddr:      ":0",
		SweepInterval: 100 * time.Millisecond,
	})
	ctx := context.Background()
	if err := m.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() {
		m.Stop(ctx)
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

func TestReentrant(t *testing.T) {
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
	if !resp2.Acquired {
		t.Fatal("expected reentrant acquire to succeed")
	}

	unlock, err := m.Unlock(ctx, &distributedlockv1.UnlockRequest{
		Key:       "reentrant-key",
		LockToken: resp2.LockToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !unlock.Released {
		t.Fatal("expected unlock after reentrant to succeed")
	}

	unlock, err = m.Unlock(ctx, &distributedlockv1.UnlockRequest{
		Key:       "reentrant-key",
		LockToken: resp.LockToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if unlock.Released {
		t.Fatal("expected old token unlock to fail after re-acquire")
	}
}

func TestConcurrent(t *testing.T) {
	m := newTestModule(t)
	ctx := context.Background()

	const numKeys = 10
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
	m := NewModule(Config{
		DBPath:   filepath.Join(t.TempDir(), "lifecycle.db"),
		GRPCAddr: ":0",
	})
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

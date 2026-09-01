package client_test

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/Muxcore-Media/core/pkg/contracts"
	distributedlockv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/distributedlock/v1"
	"github.com/Muxcore-Media/distributed-lock-sqlite/internal"
	lockclient "github.com/Muxcore-Media/distributed-lock-sqlite/pkg/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20

func startTestModule(t *testing.T) (*internal.Module, *bufconn.Listener) {
	t.Helper()
	mod, err := internal.NewModule(internal.Config{
		DBPath:        filepath.Join(t.TempDir(), "client.db"),
		GRPCAddr:      "127.0.0.1:0",
		SweepInterval: time.Second,
	})
	if err != nil {
		t.Fatalf("NewModule: %v", err)
	}
	ctx := context.Background()
	if err := mod.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	lis := bufconn.Listen(bufSize)
	mod.SetListener(lis)
	if err := mod.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		_ = mod.Stop(ctx)
		_ = lis.Close()
	})
	return mod, lis
}

func dialClient(t *testing.T, lis *bufconn.Listener, holderID string) *lockclient.Client {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return lockclient.New(conn, holderID)
}

func TestClientAcquireUnlock(t *testing.T) {
	_, lis := startTestModule(t)
	c := dialClient(t, lis, "client-a")
	ctx := context.Background()

	handle, err := c.Acquire(ctx, "client-key", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestClientAcquireConflict(t *testing.T) {
	_, lis := startTestModule(t)
	c1 := dialClient(t, lis, "client-1")
	c2 := dialClient(t, lis, "client-2")
	ctx := context.Background()

	handle, err := c1.Acquire(ctx, "conflict-key", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = handle.Unlock(ctx) }()

	_, err = c2.Acquire(ctx, "conflict-key", time.Minute)
	if !errors.Is(err, contracts.ErrLockHeld) {
		t.Fatalf("err=%v want ErrLockHeld", err)
	}
}

func TestClientRenew(t *testing.T) {
	_, lis := startTestModule(t)
	c := dialClient(t, lis, "client-renew")
	ctx := context.Background()

	handle, err := c.Acquire(ctx, "renew-key", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Renew(ctx, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := handle.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestClientNotAcquiredMapsToErrLockHeld(t *testing.T) {
	mod, lis := startTestModule(t)
	ctx := context.Background()

	// Hold lock in-process so client sees conflict.
	resp, err := mod.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key: "held", HolderId: "server", TtlMs: 60_000,
	})
	if err != nil || !resp.GetAcquired() {
		t.Fatalf("setup acquire: %v %+v", err, resp)
	}

	c := dialClient(t, lis, "client-blocked")
	_, err = c.Acquire(ctx, "held", time.Minute)
	if !errors.Is(err, contracts.ErrLockHeld) {
		t.Fatalf("err=%v want ErrLockHeld", err)
	}
}

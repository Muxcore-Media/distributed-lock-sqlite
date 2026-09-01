package client

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Muxcore-Media/core/pkg/contracts"
	distributedlockv1 "github.com/Muxcore-Media/core/proto/gen/muxcore/distributedlock/v1"
	"google.golang.org/grpc"
)

// Client implements contracts.DistributedLockProvider over DistributedLockService gRPC.
type Client struct {
	rpc      distributedlockv1.DistributedLockServiceClient
	holderID string
}

// New returns a DistributedLockProvider backed by conn. holderID identifies this
// caller to the lock service; when empty, a host+pid default is used.
func New(conn grpc.ClientConnInterface, holderID string) *Client {
	if holderID == "" {
		holderID = defaultHolderID()
	}
	return &Client{
		rpc:      distributedlockv1.NewDistributedLockServiceClient(conn),
		holderID: holderID,
	}
}

var _ contracts.DistributedLockProvider = (*Client)(nil)

// Acquire attempts to acquire key with ttl. Returns ErrLockHeld when not acquired.
func (c *Client) Acquire(ctx context.Context, key string, ttl time.Duration) (contracts.DistributedLockHandle, error) {
	resp, err := c.rpc.Acquire(ctx, &distributedlockv1.AcquireRequest{
		Key:      key,
		HolderId: c.holderID,
		TtlMs:    ttl.Milliseconds(),
	})
	if err != nil {
		return nil, err
	}
	if !resp.GetAcquired() {
		return nil, contracts.ErrLockHeld
	}
	return &handle{
		rpc:   c.rpc,
		key:   key,
		token: resp.GetLockToken(),
	}, nil
}

type handle struct {
	rpc   distributedlockv1.DistributedLockServiceClient
	key   string
	token string
}

func (h *handle) Unlock(ctx context.Context) error {
	resp, err := h.rpc.Unlock(ctx, &distributedlockv1.UnlockRequest{
		Key:       h.key,
		LockToken: h.token,
	})
	if err != nil {
		return err
	}
	if !resp.GetReleased() {
		return contracts.ErrLockHeld
	}
	return nil
}

func (h *handle) Renew(ctx context.Context, ttl time.Duration) error {
	resp, err := h.rpc.Renew(ctx, &distributedlockv1.RenewRequest{
		Key:       h.key,
		LockToken: h.token,
		TtlMs:     ttl.Milliseconds(),
	})
	if err != nil {
		return err
	}
	if !resp.GetRenewed() {
		return contracts.ErrLockHeld
	}
	return nil
}

func defaultHolderID() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

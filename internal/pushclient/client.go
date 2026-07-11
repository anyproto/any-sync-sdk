// The DRPC transport — a thin shim over the secure-channel pool, one
// wrapper per pushproto.Push RPC. Whole-RPC errors are unwrapped with
// rpcerr so callers can errors.Is against the pushapi sentinels
// (ErrSpaceExists, ErrNoValidTopics, ErrInvalidSignature, …).

package pushclient

import (
	"context"
	"fmt"

	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/rpc/rpcerr"
	"github.com/anyproto/any-sync/net/secureservice"
	"github.com/anyproto/anytype-push-server/pushclient/pushapi"
	"storj.io/drpc"
)

// Client speaks pushproto.Push to a single configured push node. Safe
// for concurrent use.
type Client struct {
	pool   pool.Pool
	peerId string
}

// NewClient builds a Client bound to the push node's peerId. The
// peer's dial addresses must already be registered on the peer service
// (App.SetPeerAddrs) — the push node is not in the nodeconf.
func NewClient(p pool.Pool, peerId string) *Client {
	return &Client{pool: p, peerId: peerId}
}

// do dials the push node through the pool and runs fn on a fresh RPC
// client. The context is upgraded with CtxAllowAccountCheck: the
// server authorizes by ACCOUNT key (peer.CtxPubKey(ctx).Account()), so
// the handshake must be allowed to carry the account identity — same
// as anytype-heart's doClient.
func (c *Client) do(ctx context.Context, fn func(cl pushapi.DRPCPushClient) error) error {
	ctx = secureservice.CtxAllowAccountCheck(ctx)
	pr, err := c.pool.Get(ctx, c.peerId)
	if err != nil {
		return fmt.Errorf("pushclient: dial push node: %w", err)
	}
	return pr.DoDrpc(ctx, func(conn drpc.Conn) error {
		return fn(pushapi.NewDRPCPushClient(conn))
	})
}

// SetToken registers this device's platform token.
func (c *Client) SetToken(ctx context.Context, req *pushapi.SetTokenRequest) error {
	return c.do(ctx, func(cl pushapi.DRPCPushClient) error {
		if _, err := cl.SetToken(ctx, req); err != nil {
			return fmt.Errorf("pushclient: set token: %w", rpcerr.Unwrap(err))
		}
		return nil
	})
}

// RevokeToken drops this device's token.
func (c *Client) RevokeToken(ctx context.Context) error {
	return c.do(ctx, func(cl pushapi.DRPCPushClient) error {
		if _, err := cl.RevokeToken(ctx, &pushapi.Ok{}); err != nil {
			return fmt.Errorf("pushclient: revoke token: %w", rpcerr.Unwrap(err))
		}
		return nil
	})
}

// CreateSpace registers a space push key with the server.
func (c *Client) CreateSpace(ctx context.Context, req *pushapi.CreateSpaceRequest) error {
	return c.do(ctx, func(cl pushapi.DRPCPushClient) error {
		if _, err := cl.CreateSpace(ctx, req); err != nil {
			return fmt.Errorf("pushclient: create space: %w", rpcerr.Unwrap(err))
		}
		return nil
	})
}

// RemoveSpace unregisters a space push key.
func (c *Client) RemoveSpace(ctx context.Context, req *pushapi.RemoveSpaceRequest) error {
	return c.do(ctx, func(cl pushapi.DRPCPushClient) error {
		if _, err := cl.RemoveSpace(ctx, req); err != nil {
			return fmt.Errorf("pushclient: remove space: %w", rpcerr.Unwrap(err))
		}
		return nil
	})
}

// SubscribeAll replaces the account's entire topic set.
func (c *Client) SubscribeAll(ctx context.Context, req *pushapi.SubscribeAllRequest) error {
	return c.do(ctx, func(cl pushapi.DRPCPushClient) error {
		if _, err := cl.SubscribeAll(ctx, req); err != nil {
			return fmt.Errorf("pushclient: subscribe all: %w", rpcerr.Unwrap(err))
		}
		return nil
	})
}

// Subscriptions lists the account's current topic set.
func (c *Client) Subscriptions(ctx context.Context) (resp *pushapi.SubscriptionsResponse, err error) {
	err = c.do(ctx, func(cl pushapi.DRPCPushClient) error {
		resp, err = cl.Subscriptions(ctx, &pushapi.SubscriptionsRequest{})
		if err != nil {
			return fmt.Errorf("pushclient: subscriptions: %w", rpcerr.Unwrap(err))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Notify publishes an encrypted notification to a set of topics.
func (c *Client) Notify(ctx context.Context, req *pushapi.NotifyRequest) error {
	return c.do(ctx, func(cl pushapi.DRPCPushClient) error {
		if _, err := cl.Notify(ctx, req); err != nil {
			return fmt.Errorf("pushclient: notify: %w", rpcerr.Unwrap(err))
		}
		return nil
	})
}

// NotifySilent publishes a data-only wakeup (no message body).
func (c *Client) NotifySilent(ctx context.Context, req *pushapi.NotifyRequest) error {
	return c.do(ctx, func(cl pushapi.DRPCPushClient) error {
		if _, err := cl.NotifySilent(ctx, req); err != nil {
			return fmt.Errorf("pushclient: notify silent: %w", rpcerr.Unwrap(err))
		}
		return nil
	})
}

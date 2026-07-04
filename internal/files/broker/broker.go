// Package broker is the fileprotov2 client of the files subsystem
// (SYN-27): batch RPCs against the space's responsible fileV2 nodes
// (routing by nodeconf.FileV2Peers with NotResponsible failover), the
// presigned HTTP upload, and durable-custody receipt verification
// against the fleet.
//
// Per-item outcomes (fileprotov2.ErrCode in the response body) are the
// caller's business — this package only routes whole-RPC errors.
package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/anyproto/any-sync/commonfile/fileproto/fileprotov2"
	"github.com/anyproto/any-sync/commonfile/fileproto/fileprotov2/fileprotov2err"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/rpc/rpcerr"
	"github.com/ipfs/go-cid"
	"storj.io/drpc"
)

// ErrNoFileNodes — the network configuration lists no fileV2 nodes;
// files can be registered but never made durable.
var ErrNoFileNodes = errors.New("filebroker: no fileV2 nodes in network config")

// Client speaks fileprotov2 to the network's fileV2 fleet. Safe for
// concurrent use.
type Client struct {
	pool          pool.Pool
	peers         func() []string
	networkId     string
	fileNetworkId func() string
	hc            *http.Client
}

// New builds a Client. peers must return the current fileV2 fleet
// (nodeconf.FileV2Peers) and fileNetworkId the fleet's receipt-signing
// identity (nodeconf.Configuration().FileNetworkId) — both funcs so
// nodeconf reloads are picked up.
func New(p pool.Pool, peers func() []string, networkId string, fileNetworkId func() string) *Client {
	return &Client{pool: p, peers: peers, networkId: networkId, fileNetworkId: fileNetworkId, hc: http.DefaultClient}
}

// NetworkId returns the network the client verifies receipts against.
func (c *Client) NetworkId() string { return c.networkId }

// do runs fn against the fleet: peers are tried in order, failing over
// on dial errors and on the whole-RPC NotResponsible routing signal;
// any other RPC error is final (auth / malformed — another peer would
// answer the same).
func (c *Client) do(ctx context.Context, fn func(cl fileprotov2.DRPCFileV2Client) error) error {
	peers := c.peers()
	if len(peers) == 0 {
		return ErrNoFileNodes
	}
	var lastErr error
	for _, peerId := range peers {
		pr, err := c.pool.Get(ctx, peerId)
		if err != nil {
			lastErr = err
			continue
		}
		err = pr.DoDrpc(ctx, func(conn drpc.Conn) error {
			return fn(fileprotov2.NewDRPCFileV2Client(conn))
		})
		if err == nil {
			return nil
		}
		lastErr = err
		if errors.Is(rpcerr.Unwrap(err), fileprotov2err.ErrNotResponsible) {
			continue
		}
		return err
	}
	return lastErr
}

// Info fetches the node-level metadata (publicReadBaseUrl et al) from
// any responsible node.
func (c *Client) Info(ctx context.Context) (resp *fileprotov2.InfoResponse, err error) {
	err = c.do(ctx, func(cl fileprotov2.DRPCFileV2Client) error {
		resp, err = cl.Info(ctx, &fileprotov2.InfoRequest{})
		return err
	})
	return resp, err
}

// Upload requests presigned upload targets for a batch of roots.
func (c *Client) Upload(ctx context.Context, spaceId string, items []*fileprotov2.UploadRequestItem) (resp *fileprotov2.UploadResponse, err error) {
	err = c.do(ctx, func(cl fileprotov2.DRPCFileV2Client) error {
		resp, err = cl.Upload(ctx, &fileprotov2.UploadRequest{SpaceId: spaceId, Items: items})
		return err
	})
	return resp, err
}

// RequestSign requests durable-custody receipts for a batch of roots.
func (c *Client) RequestSign(ctx context.Context, spaceId string, rootCids [][]byte) (resp *fileprotov2.RequestSignResponse, err error) {
	err = c.do(ctx, func(cl fileprotov2.DRPCFileV2Client) error {
		resp, err = cl.RequestSign(ctx, &fileprotov2.RequestSignRequest{SpaceId: spaceId, RootCids: rootCids})
		return err
	})
	return resp, err
}

// SpaceInfo fetches quota state for a batch of spaces.
func (c *Client) SpaceInfo(ctx context.Context, spaceIds []string) (resp *fileprotov2.SpaceInfoResponse, err error) {
	err = c.do(ctx, func(cl fileprotov2.DRPCFileV2Client) error {
		resp, err = cl.SpaceInfo(ctx, &fileprotov2.SpaceInfoRequest{SpaceIds: spaceIds})
		return err
	})
	return resp, err
}

// Put performs the presigned upload: the exact body bytes, provider
// fields sent verbatim as headers, Content-Length set explicitly (the
// presign enforces the cap server-side; we fail fast locally).
func (c *Client) Put(ctx context.Context, up *fileprotov2.PresignedUpload, body io.Reader, size int64) error {
	if up == nil || up.Url == "" {
		return errors.New("filebroker: empty presigned upload")
	}
	if up.MaxContentLength > 0 && uint64(size) > up.MaxContentLength {
		return fmt.Errorf("filebroker: body %d bytes exceeds presign cap %d", size, up.MaxContentLength)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, up.Url, body)
	if err != nil {
		return err
	}
	req.ContentLength = size
	for _, f := range up.Fields {
		req.Header.Set(f.Key, f.Value)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("filebroker: presigned PUT: %d %s", resp.StatusCode, string(msg))
	}
	return nil
}

// VerifyReceipt validates a durable-custody receipt for (spaceId,
// root, objectSize) against this network's fleet key and returns the
// networkSign row value ("{fileNetworkId}/{base64(signature)}").
func (c *Client) VerifyReceipt(rcpt *fileprotov2.NetworkSignReceipt, spaceId string, root cid.Cid, objectSize uint64) (string, error) {
	return VerifyReceipt(rcpt, VerifyParams{
		NetworkId:     c.networkId,
		SpaceId:       spaceId,
		Root:          root,
		ObjectSize:    objectSize,
		FileNetworkId: c.fileNetworkId(),
	})
}

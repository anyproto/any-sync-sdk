package anysyncx

import (
	"context"
	"errors"
	"fmt"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	anynet "github.com/anyproto/any-sync/net"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/rpc/rpcerr"
	"github.com/anyproto/any-sync/node/nodeclient"
	"github.com/anyproto/any-sync/nodeconf"
	"storj.io/drpc"
)

// DecodeSpaceHeader parses a raw space header as storage and the wire
// carry it; nil when it does not parse.
func DecodeSpaceHeader(rawHeader []byte) *spacesyncproto.SpaceHeader {
	var raw spacesyncproto.RawSpaceHeader
	if err := raw.UnmarshalVT(rawHeader); err != nil {
		return nil
	}
	hdr := &spacesyncproto.SpaceHeader{}
	if err := hdr.UnmarshalVT(raw.SpaceHeader); err != nil {
		return nil
	}
	return hdr
}

// NodeClient is any-sync's node client with the read of a space header.
type NodeClient interface {
	nodeclient.NodeClient
	// SpaceHeader returns the header the nodes serve for spaceId, as
	// they sent it.
	SpaceHeader(ctx context.Context, spaceId string) (*spacesyncproto.RawSpaceHeaderWithId, error)
}

type nodeClient struct {
	nodeclient.NodeClient
	pool     pool.Pool
	nodeConf nodeconf.Service
}

func newNodeClient() NodeClient {
	return &nodeClient{NodeClient: nodeclient.New()}
}

func (c *nodeClient) Init(a *app.App) error {
	c.pool = a.MustComponent(pool.CName).(pool.Pool)
	c.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	return c.NodeClient.Init(a)
}

// SpaceHeader asks the nodes responsible for spaceId one after another:
// a node the space has not reached answers that it is missing. The
// error is spacesyncproto.ErrSpaceMissing when each of them says so,
// and anynet.ErrUnableToConnect when one of them is out of reach.
func (c *nodeClient) SpaceHeader(ctx context.Context, spaceId string) (*spacesyncproto.RawSpaceHeaderWithId, error) {
	nodeIds := c.nodeConf.NodeIds(spaceId)
	var answer error
	reached := 0
	for _, nodeId := range nodeIds {
		p, err := c.pool.Get(ctx, nodeId)
		if err != nil {
			continue
		}
		reached++
		var res *spacesyncproto.SpacePullResponse
		err = p.DoDrpc(ctx, func(conn drpc.Conn) (e error) {
			res, e = spacesyncproto.NewDRPCSpaceSyncClient(conn).SpacePull(ctx, &spacesyncproto.SpacePullRequest{Id: spaceId})
			return e
		})
		if err == nil {
			return res.GetPayload().GetSpaceHeader(), nil
		}
		if err = rpcerr.Unwrap(err); answer == nil || !errors.Is(err, spacesyncproto.ErrSpaceMissing) {
			answer = err
		}
	}
	if answer == nil || reached < len(nodeIds) {
		return nil, anynet.ErrUnableToConnect
	}
	return nil, answer
}

// SpaceHeader reads the header of spaceId from the nodes and stores
// nothing: the device need not hold the space. A space id is the hash
// of its header, so a header of another space is refused.
func (a *App) SpaceHeader(ctx context.Context, spaceId string) (*spacesyncproto.SpaceHeader, error) {
	served, err := a.nodes.SpaceHeader(ctx, spaceId)
	if err != nil {
		return nil, fmt.Errorf("anysyncx: space header %q: %w", spaceId, err)
	}
	raw := &spacesyncproto.RawSpaceHeaderWithId{RawHeader: served.GetRawHeader(), Id: spaceId}
	if _, err := spacepayloads.ValidateSpaceHeader(raw, nil, nil, nil); err != nil {
		return nil, fmt.Errorf("anysyncx: space header %q: %w", spaceId, err)
	}
	return DecodeSpaceHeader(raw.RawHeader), nil
}

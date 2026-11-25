package anysyncx

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	anynet "github.com/anyproto/any-sync/net"
	"github.com/anyproto/any-sync/net/rpc/rpctest"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pullNode answers SpacePull with the header it holds, and that any
// other space is missing.
type pullNode struct {
	spacesyncproto.DRPCSpaceSyncUnimplementedServer
	header *spacesyncproto.RawSpaceHeaderWithId
	asked  atomic.Int32
}

func (n *pullNode) SpacePull(_ context.Context, req *spacesyncproto.SpacePullRequest) (*spacesyncproto.SpacePullResponse, error) {
	n.asked.Add(1)
	if n.header == nil || n.header.Id != req.Id {
		return nil, spacesyncproto.ErrSpaceMissing
	}
	return &spacesyncproto.SpacePullResponse{Payload: &spacesyncproto.SpacePayload{SpaceHeader: n.header}}, nil
}

type responsibleNodes struct {
	nodeconf.Service
	ids []string
}

func (n responsibleNodes) NodeIds(string) []string { return n.ids }

// The header comes from the first responsible node that holds the
// space: a node out of reach, and one the space has not reached, are
// passed over. The space is missing when every responsible node says
// so, and the nodes are out of reach when one that could hold it is.
func TestNodeClient_SpaceHeader(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	header := &spacesyncproto.RawSpaceHeaderWithId{Id: "space.1", RawHeader: []byte("header")}

	peers := rpctest.NewTestPool()
	// connect puts node in the pool and returns its peer id.
	connect := func(name string, node *pullNode) string {
		server := rpctest.NewTestServer()
		require.NoError(t, spacesyncproto.DRPCRegisterSpaceSync(server, node))
		p, err := server.Dial(name)
		require.NoError(t, err)
		t.Cleanup(func() { _ = p.Close() })
		require.NoError(t, peers.AddPeer(ctx, p))
		return p.Id()
	}
	without, holder, last := &pullNode{}, &pullNode{header: header}, &pullNode{header: header}
	inReach := []string{connect("without", without), connect("holder", holder), connect("last", last)}
	client := &nodeClient{pool: peers, nodeConf: responsibleNodes{ids: append([]string{"out of reach"}, inReach...)}}

	got, err := client.SpaceHeader(ctx, header.Id)
	require.NoError(t, err)
	assert.Equal(t, header.RawHeader, got.GetRawHeader())
	assert.EqualValues(t, 1, without.asked.Load())
	assert.EqualValues(t, 1, holder.asked.Load())
	assert.Zero(t, last.asked.Load())

	// No node in reach holds the space, and one is out of reach.
	_, err = client.SpaceHeader(ctx, "other.1")
	require.ErrorIs(t, err, anynet.ErrUnableToConnect)

	// No responsible node holds the space.
	client.nodeConf = responsibleNodes{ids: inReach}
	_, err = client.SpaceHeader(ctx, "other.1")
	require.ErrorIs(t, err, spacesyncproto.ErrSpaceMissing)

	// No responsible node is in reach.
	client.nodeConf = responsibleNodes{ids: []string{"out of reach"}}
	_, err = client.SpaceHeader(ctx, header.Id)
	require.ErrorIs(t, err, anynet.ErrUnableToConnect)
}

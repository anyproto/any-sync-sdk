package components

import (
	"context"
	"fmt"

	"storj.io/drpc"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/peermanager"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/commonspace/sync/objectsync/objectmessages"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/nodeconf"
)

// PeerManagerProvider creates per-space peer managers.
type PeerManagerProvider struct{}

func NewPeerManagerProvider() *PeerManagerProvider {
	return &PeerManagerProvider{}
}

func (p *PeerManagerProvider) Init(_ *app.App) error {
	return nil
}

func (p *PeerManagerProvider) Name() string {
	return peermanager.CName
}

func (p *PeerManagerProvider) NewPeerManager(_ context.Context, spaceId string) (peermanager.PeerManager, error) {
	return &spacePeerManager{spaceId: spaceId}, nil
}

// spacePeerManager is a per-space peer manager that resolves peers via nodeconf and pool.
type spacePeerManager struct {
	spaceId  string
	nodeConf nodeconf.Service
	pool     pool.Pool
}

func (m *spacePeerManager) Init(a *app.App) error {
	m.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	m.pool = a.MustComponent(pool.CName).(pool.Pool)
	return nil
}

func (m *spacePeerManager) Name() string {
	return peermanager.CName
}

func (m *spacePeerManager) GetResponsiblePeers(ctx context.Context) ([]peer.Peer, error) {
	return m.GetNodePeers(ctx)
}

func (m *spacePeerManager) GetNodePeers(ctx context.Context) ([]peer.Peer, error) {
	nodeIds := m.nodeConf.NodeIds(m.spaceId)
	var peers []peer.Peer
	for _, id := range nodeIds {
		p, err := m.pool.Get(ctx, id)
		if err != nil {
			continue
		}
		peers = append(peers, p)
	}
	return peers, nil
}

func (m *spacePeerManager) BroadcastMessage(ctx context.Context, msg drpc.Message) error {
	objMsg, err := toObjectSyncMessage(msg)
	if err != nil {
		return err
	}
	peers, err := m.GetNodePeers(ctx)
	if err != nil {
		return err
	}
	for _, p := range peers {
		_ = p.DoDrpc(ctx, func(conn drpc.Conn) error {
			cl := spacesyncproto.NewDRPCSpaceSyncClient(conn)
			_, err := cl.ObjectSync(ctx, objMsg)
			return err
		})
	}
	return nil
}

func (m *spacePeerManager) SendMessage(ctx context.Context, peerId string, msg drpc.Message) error {
	objMsg, err := toObjectSyncMessage(msg)
	if err != nil {
		return err
	}
	p, err := m.pool.Get(ctx, peerId)
	if err != nil {
		return err
	}
	return p.DoDrpc(ctx, func(conn drpc.Conn) error {
		cl := spacesyncproto.NewDRPCSpaceSyncClient(conn)
		_, err := cl.ObjectSync(ctx, objMsg)
		return err
	})
}

func (m *spacePeerManager) KeepAlive(_ context.Context) {
	// no-op for SDK client
}

// toObjectSyncMessage converts a drpc.Message to an ObjectSyncMessage for sending via RPC.
func toObjectSyncMessage(msg drpc.Message) (*spacesyncproto.ObjectSyncMessage, error) {
	switch m := msg.(type) {
	case *spacesyncproto.ObjectSyncMessage:
		return m, nil
	case *objectmessages.HeadUpdate:
		protoMsg, err := m.ProtoMessage()
		if err != nil {
			return nil, err
		}
		osm, ok := protoMsg.(*spacesyncproto.ObjectSyncMessage)
		if !ok {
			return nil, fmt.Errorf("unexpected proto type: %T", protoMsg)
		}
		return osm, nil
	default:
		return nil, fmt.Errorf("unsupported message type: %T", msg)
	}
}

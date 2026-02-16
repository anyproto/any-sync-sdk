package components

import (
	"context"

	"storj.io/drpc"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/peermanager"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/streampool"
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

// spacePeerManager is a per-space peer manager that resolves peers via nodeconf
// and sends messages through the StreamPool for reactive sync.
type spacePeerManager struct {
	spaceId    string
	nodeConf   nodeconf.Service
	pool       pool.Pool
	streamPool streampool.StreamPool
}

func (m *spacePeerManager) Init(a *app.App) error {
	m.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	m.pool = a.MustComponent(pool.CName).(pool.Pool)
	m.streamPool = a.MustComponent(streampool.CName).(streampool.StreamPool)
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
	return m.streamPool.Send(ctx, msg, func(ctx context.Context) ([]peer.Peer, error) {
		return m.GetNodePeers(ctx)
	})
}

func (m *spacePeerManager) SendMessage(ctx context.Context, peerId string, msg drpc.Message) error {
	return m.streamPool.Send(ctx, msg, func(ctx context.Context) ([]peer.Peer, error) {
		p, err := m.pool.Get(ctx, peerId)
		if err != nil {
			return nil, err
		}
		return []peer.Peer{p}, nil
	})
}

func (m *spacePeerManager) KeepAlive(_ context.Context) {
	// no-op for SDK client
}

package components

import (
	"context"

	"storj.io/drpc"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/peermanager"
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
	nodeIds := m.nodeConf.NodeIds(m.spaceId)
	if len(nodeIds) == 0 {
		return nil, nil
	}
	p, err := m.pool.GetOneOf(ctx, nodeIds)
	if err != nil {
		return nil, err
	}
	return []peer.Peer{p}, nil
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

func (m *spacePeerManager) BroadcastMessage(_ context.Context, _ drpc.Message) error {
	// no-op for SDK client; sync tree handles its own messaging
	return nil
}

func (m *spacePeerManager) SendMessage(_ context.Context, _ string, _ drpc.Message) error {
	// no-op for SDK client; sync tree handles its own messaging
	return nil
}

func (m *spacePeerManager) KeepAlive(_ context.Context) {
	// no-op for SDK client
}

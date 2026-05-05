package anysyncx

import (
	"context"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/peermanager"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/streampool"
	"github.com/anyproto/any-sync/nodeconf"
	"storj.io/drpc"
)

// peerManagerProvider creates per-space peer managers. any-sync calls
// NewPeerManager once per space at NewSpace time.
type peerManagerProvider struct{}

func newPeerManagerProvider() *peerManagerProvider { return &peerManagerProvider{} }

func (p *peerManagerProvider) Init(_ *app.App) error { return nil }
func (p *peerManagerProvider) Name() string          { return peermanager.CName }

func (p *peerManagerProvider) NewPeerManager(_ context.Context, spaceId string) (peermanager.PeerManager, error) {
	return &spacePeerManager{spaceId: spaceId}, nil
}

// spacePeerManager resolves nodes via nodeconf and ships messages
// through the StreamPool for reactive push-based sync.
type spacePeerManager struct {
	spaceId         string
	nodeConf        nodeconf.Service
	pool            pool.Pool
	streamPool      streampool.StreamPool
	subscribeMsgRaw []byte

	runCtx    context.Context
	runCancel context.CancelFunc
}

func (m *spacePeerManager) Init(a *app.App) error {
	m.nodeConf = a.MustComponent(nodeconf.CName).(nodeconf.Service)
	m.pool = a.MustComponent(pool.CName).(pool.Pool)
	m.streamPool = a.MustComponent(streampool.CName).(streampool.StreamPool)
	sub := &spacesyncproto.SpaceSubscription{
		SpaceIds: []string{m.spaceId},
		Action:   spacesyncproto.SpaceSubscriptionAction_Subscribe,
	}
	payload, err := sub.MarshalVT()
	if err != nil {
		return err
	}
	m.subscribeMsgRaw = payload
	m.runCtx, m.runCancel = context.WithCancel(context.Background())
	return nil
}

// Run / Close make spacePeerManager an app.ComponentRunnable so the
// per-space app starts the subscribe loop on Start and tears it down
// on Close.
func (m *spacePeerManager) Run(_ context.Context) error {
	go m.subscribeLoop()
	return nil
}

func (m *spacePeerManager) Close(_ context.Context) error {
	if m.runCancel != nil {
		m.runCancel()
	}
	return nil
}

// subscribeLoop fast-retries SpaceSubscription_Subscribe to sync nodes
// until peers are reachable, then settles to a slow refresh tick.
//
// Why we need this beyond diffsyncer's KeepAlive:
//   - any-sync's diffsyncer calls KeepAlive only at the END of a
//     successful Sync round. The first periodic tick fires immediately
//     on space load — but at that moment DNS / dial often hasn't
//     settled yet, so GetResponsiblePeers errors out and Sync returns
//     before reaching KeepAlive.
//   - Without our own loop, the next subscribe attempt is the second
//     headsync tick at SyncPeriod (~30s), so any ACL push (e.g. a
//     joiner's RequestJoin) sent in that window is missed and only
//     surfaces via the periodic pull.
//
// The loop is best-effort: BroadcastMessage failures are silently
// ignored — if peers aren't connected yet, the next iteration retries.
// Once peers are reachable, the message hits all of them via the
// streampool's existing-or-newly-opened streams.
func (m *spacePeerManager) subscribeLoop() {
	delays := []time.Duration{
		200 * time.Millisecond,
		500 * time.Millisecond,
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
	}
	const slowTick = 30 * time.Second
	for i := 0; ; i++ {
		var d time.Duration
		if i < len(delays) {
			d = delays[i]
		} else {
			d = slowTick
		}
		select {
		case <-m.runCtx.Done():
			return
		case <-time.After(d):
		}
		ctx, cancel := context.WithTimeout(m.runCtx, 10*time.Second)
		m.KeepAlive(ctx)
		cancel()
	}
}

func (m *spacePeerManager) Name() string { return peermanager.CName }

// GetResponsiblePeers returns a single node peer per call. As a client we
// only need to diff-sync against one node per cycle; pool.GetOneOf reuses
// a live connection when possible and otherwise dials a random node from
// the configured set.
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
	peers := make([]peer.Peer, 0, len(nodeIds))
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

// KeepAlive (re)broadcasts a SpaceSubscription_Subscribe so sync nodes
// keep our spaceId tagged on their outbound streams to us. Without this
// we only get push for spaces that were registered when the stream
// first opened — a fresh-Created space loaded after the stream was
// opened (e.g. by coordinator/nodeconf traffic) would otherwise wait
// for the next headsync periodic pull (SyncPeriod=30s) to see ACL
// updates. Called by diffsyncer at the end of every headsync cycle.
func (m *spacePeerManager) KeepAlive(ctx context.Context) {
	if len(m.subscribeMsgRaw) == 0 {
		return
	}
	msg := &spacesyncproto.ObjectSyncMessage{Payload: m.subscribeMsgRaw}
	_ = m.BroadcastMessage(ctx, msg)
}

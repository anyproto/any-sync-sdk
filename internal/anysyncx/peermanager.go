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
// NewPeerManager once per space at NewSpace time. Local-only spaces
// (see localOnlySpaces) get an inert manager instead of the node-backed
// one, so nothing about them ever reaches the network.
type peerManagerProvider struct {
	localOnly *localOnlySpaces
}

func newPeerManagerProvider(localOnly *localOnlySpaces) *peerManagerProvider {
	return &peerManagerProvider{localOnly: localOnly}
}

func (p *peerManagerProvider) Init(_ *app.App) error { return nil }
func (p *peerManagerProvider) Name() string          { return peermanager.CName }

func (p *peerManagerProvider) NewPeerManager(_ context.Context, spaceId string) (peermanager.PeerManager, error) {
	if p.localOnly.has(spaceId) {
		return &localPeerManager{}, nil
	}
	return &spacePeerManager{spaceId: spaceId}, nil
}

// localPeerManager is the peer manager of a local-only space: it
// resolves no peers and sends nothing, so headsync has nobody to diff
// against — no node subscribe, no SpaceMissing, no push. All sends
// succeed as no-ops (the write is durable locally; there is simply no
// audience).
type localPeerManager struct{}

func (m *localPeerManager) Init(_ *app.App) error { return nil }
func (m *localPeerManager) Name() string          { return peermanager.CName }

func (m *localPeerManager) GetResponsiblePeers(_ context.Context) ([]peer.Peer, error) {
	return nil, nil
}
func (m *localPeerManager) GetNodePeers(_ context.Context) ([]peer.Peer, error) { return nil, nil }
func (m *localPeerManager) BroadcastMessage(_ context.Context, _ drpc.Message) error {
	return nil
}
func (m *localPeerManager) SendMessage(_ context.Context, _ string, _ drpc.Message) error {
	return nil
}
func (m *localPeerManager) KeepAlive(_ context.Context) {}

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

// subscribeRamp is the cold-start retry schedule: an eager send on Run
// (before the ramp), then these delays. Covers the case where the eager
// send loses on DNS / dial — once any peer becomes reachable the next
// iteration hits it.
var subscribeRamp = []time.Duration{
	200 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
}

const (
	// subscribeRefresh is the steady-state cadence once a node stream is
	// up. A live stream keeps push flowing, so this only re-tags us in
	// case a node forgets our subscription; it deliberately does not pin
	// the periodic-pull cadence.
	subscribeRefresh = 30 * time.Second
	// subscribeChurn is the cadence used while no node stream is open. A
	// dropped stream (node restart / connection reset) is our push
	// channel going down; re-broadcasting this fast forces an immediate
	// reopen — streamHandler.OpenStream re-primes the fresh stream with
	// every registered space — instead of waiting a full subscribeRefresh
	// "next tick" while pushes are silently missed.
	subscribeChurn = 2 * time.Second
)

// subscribeLoop publishes SpaceSubscription_Subscribe to sync nodes:
// once immediately on Run, then a fast-retry ramp, then a health-driven
// steady state.
//
// The eager first send is critical for cold-start convergence. Without
// it, the order is "headsync.Sync → diff → KeepAlive at end of Sync":
// if the diff happens to query a node that hasn't yet replicated our
// ACL activation (e.g. a joiner whose accept-record is still in
// flight across the node cluster), it returns newIds=0 and we wait
// the next SyncPeriod tick (~30s) for another attempt. Firing
// Subscribe up front tags us on every reachable node before the diff
// runs, so the diff response includes our missing trees on the first
// try. Documented separately because it converts what looks like a
// 30s/3.4s flake into a deterministic ~3s convergence.
//
// Beyond cold-start the cadence adapts to stream health (see
// nextSubscribeDelay): a slow refresh while a node stream is live, a
// fast churn cadence while none is — so a reopen+resubscribe follows a
// dropped stream within subscribeChurn rather than a full
// subscribeRefresh. KeepAlive failures are silently swallowed; the loop
// keeps trying.
func (m *spacePeerManager) subscribeLoop() {
	m.broadcastSubscribe()

	for i := 0; ; i++ {
		d := nextSubscribeDelay(i, subscribeRamp, m.hasNodeStream())
		select {
		case <-m.runCtx.Done():
			return
		case <-time.After(d):
		}
		m.broadcastSubscribe()
	}
}

// nextSubscribeDelay picks the wait before the next Subscribe broadcast.
// During the cold-start ramp (attempt < len(ramp)) it follows the fixed
// schedule regardless of health. After the ramp it returns
// subscribeRefresh while a node stream is live and subscribeChurn while
// none is. Pure so the cadence is unit-testable without a stream pool.
func nextSubscribeDelay(attempt int, ramp []time.Duration, healthy bool) time.Duration {
	if attempt < len(ramp) {
		return ramp[attempt]
	}
	if healthy {
		return subscribeRefresh
	}
	return subscribeChurn
}

// hasNodeStream reports whether at least one outbound stream to a sync
// node is currently open. streamHandler.OpenStream tags every node
// stream with nodeStreamTag; a zero count means our push channel is
// down (the stream was removed on read/write failure), so we should
// re-broadcast Subscribe on the fast churn cadence to force a reopen.
func (m *spacePeerManager) hasNodeStream() bool {
	return len(m.streamPool.Streams(nodeStreamTag)) > 0
}

// broadcastSubscribe is one Subscribe broadcast against m.runCtx with
// a 10s deadline. Best-effort: errors are swallowed because the
// subscribeLoop retries on a backoff.
func (m *spacePeerManager) broadcastSubscribe() {
	ctx, cancel := context.WithTimeout(m.runCtx, 10*time.Second)
	defer cancel()
	m.KeepAlive(ctx)
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

// BroadcastMessage queues msg for delivery to every node peer in this
// space. Detaches from the caller's ctx on purpose: streamPool.Send
// hands the actual peer-fetch + write off to a worker goroutine
// (ExecPool.TryAdd), so by the time that worker dequeues the closure
// the caller's ctx is typically already cancelled — most callers of
// the underlying SyncTree.AddContent / SyncAcl.AddRawRecord pass an
// HTTP request context that ends as soon as the response is written.
// The symptom was every joiner-side write succeeding locally but
// never reaching the sync node: streampool logged "send peer error:
// context canceled" because pool.Get / openStream saw the dead ctx,
// and Bob's change just sat on disk. Headsync's periodic pull picks
// up incoming changes but does not push outgoing ones — that's the
// broadcast's job, and it has to outlive the caller.
//
// The runCtx is the per-space manager lifetime, so shutdown still
// cancels in-flight broadcasts. This mirrors any-sync's own
// synctest.TestPeerManager, which uses context.Background() for the
// same reason.
func (m *spacePeerManager) BroadcastMessage(_ context.Context, msg drpc.Message) error {
	return m.streamPool.Send(m.runCtx, msg, func(ctx context.Context) ([]peer.Peer, error) {
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

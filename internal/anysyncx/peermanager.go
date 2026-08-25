package anysyncx

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/commonspace/peermanager"
	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/commonspace/sync/objectsync/objectmessages"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/streampool"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/cheggaaa/mb/v3"
	"go.uber.org/zap"
	"storj.io/drpc"

	"github.com/anyproto/any-sync-sdk/internal/p2p"
)

var pmLog = logger.NewNamed("anysyncx.peermanager")

// peerManagerProvider creates per-space peer managers. any-sync calls
// NewPeerManager once per space at NewSpace time. Local-only spaces
// (see localOnlySpaces) get an inert manager instead of the node-backed
// one, so nothing about them ever reaches the network.
type peerManagerProvider struct {
	localOnly    *localOnlySpaces
	localPeers   localPeerSource
	globalPeers  globalPeerSource
	globalFanout int
}

// localPeerSource is the slice of p2p.PeerStore the peer manager needs;
// an interface so manager tests can fake it.
type localPeerSource interface {
	LocalPeerIds(spaceId string) []string
	RemoveLocalPeer(peerId string)
}

// globalPeerSource lists the global peers sharing a space, best first.
// nil when the global layer is off. Global peers are never dialed
// here — only picked from the pool while already connected.
type globalPeerSource interface {
	GlobalPeerIds(spaceId string) []string
}

func newPeerManagerProvider(localOnly *localOnlySpaces, localPeers localPeerSource, globalPeers globalPeerSource, globalFanout int) *peerManagerProvider {
	return &peerManagerProvider{localOnly: localOnly, localPeers: localPeers, globalPeers: globalPeers, globalFanout: globalFanout}
}

func (p *peerManagerProvider) Init(_ *app.App) error { return nil }
func (p *peerManagerProvider) Name() string          { return peermanager.CName }

func (p *peerManagerProvider) NewPeerManager(_ context.Context, spaceId string) (peermanager.PeerManager, error) {
	if p.localOnly.has(spaceId) {
		return &localPeerManager{}, nil
	}
	return &spacePeerManager{spaceId: spaceId, localPeers: p.localPeers, globalPeers: p.globalPeers, globalFanout: p.globalFanout}, nil
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

// sendPool is the streamPool slice the manager uses — an interface so
// tests can fake queue overflow without a real stream pool.
type sendPool interface {
	Send(ctx context.Context, msg drpc.Message, target streampool.PeerGetter) error
	Streams(tags ...string) []drpc.Stream
}

// spacePeerManager resolves nodes via nodeconf and ships messages
// through the StreamPool for reactive push-based sync. Local-network
// peers that share this space (from the p2p peer store) are folded
// into the responsible/broadcast sets, so head-sync and pushes run
// over the LAN too — including while every node is unreachable.
//
// Global peers (internet-wide, from key-value records) join only while
// no node stream is up: with a node reachable they receive everything
// through it, so broadcasting to them would multiply upload for
// nothing. Without a node they take over — head updates coalesced per
// object over a short window and fanned out to at most globalFanout
// already-connected peers, the periodic diff against one connected
// peer per tick, rotating. They are never dialed from here.
type spacePeerManager struct {
	spaceId         string
	nodeConf        nodeconf.Service
	pool            pool.Pool
	streamPool      sendPool
	localPeers      localPeerSource
	globalPeers     globalPeerSource
	globalFanout    int
	subscribeMsgRaw []byte

	// globalRotation picks the next connected global peer for the
	// periodic diff.
	globalMu       sync.Mutex
	globalRotation int
	coalesced      map[string]drpc.Message
	coalesceOrder  []string
	coalesceSeq    int
	coalesceTimer  *time.Timer

	runCtx    context.Context
	runCancel context.CancelFunc

	strikesMu   sync.Mutex
	dialStrikes map[string]int

	// Overflowed-broadcast park buffer (see BroadcastMessage). The
	// retry worker is started lazily on first park: the space-pull path
	// builds a short-lived throwaway peer manager, which must not spawn
	// goroutines it never needs.
	parkMu      sync.Mutex
	parked      []drpc.Message
	parkedBytes int
	parkWake    chan struct{}
	parkStart   sync.Once
	parkDone    chan struct{}
	parkStarted bool
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
	m.parkWake = make(chan struct{}, 1)
	m.parkDone = make(chan struct{})
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
	m.globalMu.Lock()
	if m.coalesceTimer != nil {
		m.coalesceTimer.Stop()
		m.coalesceTimer = nil
	}
	m.globalMu.Unlock()
	// The retry worker never blocks outside runCtx selects (Send is a
	// non-blocking TryAdd), so this wait is prompt.
	m.parkMu.Lock()
	started := m.parkStarted
	m.parkMu.Unlock()
	if started {
		<-m.parkDone
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

// GetResponsiblePeers returns a single node peer (as a client we only
// need to diff-sync against one node per cycle; pool.GetOneOf reuses a
// live connection when possible) plus every connectable local-network
// peer that shares this space. When all nodes are unreachable but a
// local peer is up, the local peers alone are returned — that is what
// keeps a space syncing over the LAN while offline. With no node
// stream open one connected global peer joins too (rotating across
// ticks). The node error only surfaces when there is nobody at all to
// sync with.
func (m *spacePeerManager) GetResponsiblePeers(ctx context.Context) ([]peer.Peer, error) {
	var (
		peers   []peer.Peer
		nodeErr error
	)
	if nodeIds := m.nodeConf.NodeIds(m.spaceId); len(nodeIds) > 0 {
		p, err := m.pool.GetOneOf(ctx, nodeIds)
		if err != nil {
			nodeErr = err
		} else {
			peers = append(peers, p)
		}
	}
	peers = append(peers, m.getLocalPeers(ctx)...)
	if m.globalFallback() {
		if gp := m.nextGlobalPeer(ctx); gp != nil {
			peers = append(peers, gp)
		}
	}
	if len(peers) == 0 && nodeErr != nil {
		return nil, nodeErr
	}
	return peers, nil
}

// globalFallback reports whether global peers take part in this
// space's sync right now: the layer is on and no node stream is up.
func (m *spacePeerManager) globalFallback() bool {
	return m.globalPeers != nil && !m.hasNodeStream()
}

// connectedGlobalPeers picks the global peers sharing this space that
// are connected right now, best first, at most limit (0 = all). Each
// lookup is bounded by p2p.PickTimeout and never dials.
func (m *spacePeerManager) connectedGlobalPeers(ctx context.Context, limit int) []peer.Peer {
	if m.globalPeers == nil {
		return nil
	}
	var out []peer.Peer
	for _, id := range m.globalPeers.GlobalPeerIds(m.spaceId) {
		if limit > 0 && len(out) >= limit {
			break
		}
		if p, err := p2p.PickLive(ctx, m.pool, id); err == nil {
			out = append(out, p)
		}
	}
	return out
}

// isGlobalOnly reports a peer known to this space through records only:
// such a peer is never dialed from a sync path.
func (m *spacePeerManager) isGlobalOnly(peerId string) bool {
	if m.globalPeers == nil || !slices.Contains(m.globalPeers.GlobalPeerIds(m.spaceId), peerId) {
		return false
	}
	return m.localPeers == nil || !slices.Contains(m.localPeers.LocalPeerIds(m.spaceId), peerId)
}

// nextGlobalPeer rotates over the connected global peers so successive
// diff ticks spread across them instead of always hitting the first.
func (m *spacePeerManager) nextGlobalPeer(ctx context.Context) peer.Peer {
	peers := m.connectedGlobalPeers(ctx, 0)
	if len(peers) == 0 {
		return nil
	}
	m.globalMu.Lock()
	i := m.globalRotation % len(peers)
	m.globalRotation++
	m.globalMu.Unlock()
	return peers[i]
}

// localDialStrikes is how many CONSECUTIVE dial failures a LAN peer
// must accumulate before we drop it from the peer store. A single miss
// (the peer briefly restarting its QUIC session on an interface change)
// must not evict it — the re-handshake resweep only re-adds peers still
// in the store, so a premature eviction can strand a peer until its
// next mDNS re-announce.
const localDialStrikes = 3

// getLocalPeers dials the local-network peers known to share this
// space. A peer is dropped from the store only after localDialStrikes
// consecutive failures; a success resets its counter.
func (m *spacePeerManager) getLocalPeers(ctx context.Context) []peer.Peer {
	if m.localPeers == nil {
		return nil
	}
	var out []peer.Peer
	for _, id := range m.localPeers.LocalPeerIds(m.spaceId) {
		p, err := m.pool.Get(ctx, id)
		if err != nil {
			// Don't punish the peer for our own cancelled context.
			if ctx.Err() != nil {
				continue
			}
			if m.strike(id) >= localDialStrikes {
				m.localPeers.RemoveLocalPeer(id)
				m.clearStrikes(id)
			}
			continue
		}
		m.clearStrikes(id)
		out = append(out, p)
	}
	return out
}

func (m *spacePeerManager) strike(peerId string) int {
	m.strikesMu.Lock()
	defer m.strikesMu.Unlock()
	if m.dialStrikes == nil {
		m.dialStrikes = map[string]int{}
	}
	m.dialStrikes[peerId]++
	return m.dialStrikes[peerId]
}

func (m *spacePeerManager) clearStrikes(peerId string) {
	m.strikesMu.Lock()
	defer m.strikesMu.Unlock()
	delete(m.dialStrikes, peerId)
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
//
// streamPool.Send is a non-blocking TryAdd into the shared dial queue;
// when the queue is full it returns mb.ErrOverflowed and the message
// would be silently lost — every producer (keyvaluestorage.Set,
// synctree, syncacl) just logs the error, and nothing upstream ever
// retries a broadcast (headsync pulls, it doesn't push). Overflowed
// broadcasts are therefore parked in a bounded buffer and re-sent with
// backoff — a full queue means delay, not loss. A parked message
// reports success: from the caller's perspective it is queued.
func (m *spacePeerManager) BroadcastMessage(_ context.Context, msg drpc.Message) error {
	err := m.streamPool.Send(m.runCtx, msg, m.getBroadcastPeers)
	if errors.Is(err, mb.ErrOverflowed) {
		m.park(msg)
		err = nil
	}
	if m.globalFallback() {
		m.coalesce(msg)
	}
	return err
}

// globalCoalesceWindow is how long head updates are held before one
// batch goes to the global peers: the latest update per object wins,
// so a burst of edits costs one relay send per peer.
const globalCoalesceWindow = time.Second

// coalesce queues msg for the global peers. A tree head update
// replaces the older queued one for the same object (a tree receiver
// fetches whatever it misses); every other message — key-value rows,
// ACL records, whose payloads are not cumulative — is queued in order
// under its own key. Nothing is queued while no global peer shares the
// space.
func (m *spacePeerManager) coalesce(msg drpc.Message) {
	if len(m.globalPeers.GlobalPeerIds(m.spaceId)) == 0 {
		return
	}
	m.globalMu.Lock()
	var key string
	if hu, ok := msg.(*objectmessages.HeadUpdate); ok && isTreeUpdate(hu) && hu.Meta.ObjectId != "" {
		key = "object:" + hu.Meta.ObjectId
	} else {
		m.coalesceSeq++
		key = "seq:" + strconv.Itoa(m.coalesceSeq)
	}
	if m.coalesced == nil {
		m.coalesced = map[string]drpc.Message{}
	}
	if _, ok := m.coalesced[key]; !ok {
		m.coalesceOrder = append(m.coalesceOrder, key)
	}
	m.coalesced[key] = msg
	if m.coalesceTimer == nil {
		m.coalesceTimer = time.AfterFunc(globalCoalesceWindow, m.flushCoalesced)
	}
	m.globalMu.Unlock()
}

// isTreeUpdate reports a head update of an object tree. An outbound
// update carries its type on the inner update; a decoded one on the
// message itself.
func isTreeUpdate(hu *objectmessages.HeadUpdate) bool {
	if hu.Update != nil {
		return hu.Update.ObjectType() == spacesyncproto.ObjectType_Tree
	}
	return hu.ObjectType() == spacesyncproto.ObjectType_Tree
}

// flushCoalesced sends the held batch to at most globalFanout connected
// global peers. Overflow is not parked: the node / LAN copy of every
// message already has the park buffer, and the next diff tick against
// a global peer converges anyway.
func (m *spacePeerManager) flushCoalesced() {
	m.globalMu.Lock()
	batch := make([]drpc.Message, 0, len(m.coalesceOrder))
	for _, key := range m.coalesceOrder {
		batch = append(batch, m.coalesced[key])
	}
	m.coalesced = nil
	m.coalesceOrder = nil
	m.coalesceTimer = nil
	m.globalMu.Unlock()
	if m.runCtx.Err() != nil {
		return
	}
	for _, msg := range batch {
		if err := m.streamPool.Send(m.runCtx, msg, m.getGlobalPeers); err != nil {
			pmLog.Debug("global broadcast", zap.String("spaceId", m.spaceId), zap.Error(err))
		}
	}
}

// getGlobalPeers is the fan-out audience of one coalesced batch.
func (m *spacePeerManager) getGlobalPeers(ctx context.Context) ([]peer.Peer, error) {
	return m.connectedGlobalPeers(ctx, m.globalFanout), nil
}

// getBroadcastPeers is the push audience: every node peer plus every
// connectable local peer sharing this space. GetNodePeers stays
// nodes-only on purpose — callers asking for "the nodes" (e.g. the
// space-delete flow) must not get LAN devices.
func (m *spacePeerManager) getBroadcastPeers(ctx context.Context) ([]peer.Peer, error) {
	peers, err := m.GetNodePeers(ctx)
	if err != nil {
		return nil, err
	}
	return append(peers, m.getLocalPeers(ctx)...), nil
}

// Park-buffer bounds. Broadcast messages keep their payload (raw
// changes / KV values) alive until sent, so the buffer is capped by
// bytes as well as count; when full the OLDEST message drops — KV rows
// are LWW per (key, peer) and head updates are idempotent, so a newer
// parked message supersedes an older one for the same object.
const (
	parkedMaxCount = 256
	parkedMaxBytes = 8 << 20

	// outgoingQueueSize sizes both shared outgoing queues: the
	// process-wide dial queue (GetStreamConfig) and each stream's write
	// queue (streamHandler.OpenStream). Both drop on overflow, so they
	// are sized to make overflow rare at real space counts; matches
	// heart's DialQueueSize.
	outgoingQueueSize = 300
)

// retryRamp is the flush schedule after an overflow: quick first
// retries while the startup burst drains, then a slow steady tick.
var retryRamp = []time.Duration{
	100 * time.Millisecond,
	500 * time.Millisecond,
	2 * time.Second,
}

const retrySteady = 10 * time.Second

// nextRetryDelay picks the wait before the next parked-broadcast flush
// attempt. Pure, same shape as nextSubscribeDelay.
func nextRetryDelay(attempt int) time.Duration {
	if attempt < len(retryRamp) {
		return retryRamp[attempt]
	}
	return retrySteady
}

// msgSize is the byte estimate for the park bound. Broadcast payloads
// (objectmessages.HeadUpdate) expose MsgSize; anything else counts 0
// and is bounded by parkedMaxCount alone.
func msgSize(msg drpc.Message) int {
	if s, ok := msg.(interface{ MsgSize() uint64 }); ok {
		return int(s.MsgSize())
	}
	return 0
}

func (m *spacePeerManager) park(msg drpc.Message) {
	size := msgSize(msg)
	m.parkMu.Lock()
	dropped := 0
	for len(m.parked) > 0 &&
		(len(m.parked) >= parkedMaxCount || m.parkedBytes+size > parkedMaxBytes) {
		m.parkedBytes -= msgSize(m.parked[0])
		m.parked = m.parked[1:]
		dropped++
	}
	m.parked = append(m.parked, msg)
	m.parkedBytes += size
	count := len(m.parked)
	if !m.parkStarted {
		m.parkStarted = true
		m.parkStart.Do(func() { go m.retryLoop() })
	}
	m.parkMu.Unlock()

	if dropped > 0 {
		pmLog.Warn("park buffer full; dropped oldest broadcasts",
			zap.String("spaceId", m.spaceId), zap.Int("dropped", dropped))
	}
	pmLog.Warn("broadcast overflowed; parked for retry",
		zap.String("spaceId", m.spaceId), zap.Int("parked", count))
	select {
	case m.parkWake <- struct{}{}:
	default:
	}
}

// retryLoop flushes the park buffer on a backoff ramp; once the buffer
// drains it sleeps until the next park. Exits with runCtx.
func (m *spacePeerManager) retryLoop() {
	defer close(m.parkDone)
	attempt := 0
	for {
		select {
		case <-m.runCtx.Done():
			return
		case <-time.After(nextRetryDelay(attempt)):
		}
		if m.flushParked() {
			attempt = 0
			select {
			case <-m.runCtx.Done():
				return
			case <-m.parkWake:
			}
		} else {
			attempt++
		}
	}
}

// flushParked re-sends parked broadcasts FIFO until the first
// re-overflow. Reports whether the buffer is empty afterwards.
func (m *spacePeerManager) flushParked() bool {
	delivered := 0
	defer func() {
		if delivered > 0 {
			pmLog.Info("parked broadcasts delivered",
				zap.String("spaceId", m.spaceId), zap.Int("count", delivered))
		}
	}()
	for {
		m.parkMu.Lock()
		if len(m.parked) == 0 {
			m.parkMu.Unlock()
			return true
		}
		msg := m.parked[0]
		m.parked = m.parked[1:]
		m.parkedBytes -= msgSize(msg)
		m.parkMu.Unlock()

		err := m.streamPool.Send(m.runCtx, msg, m.getBroadcastPeers)
		if errors.Is(err, mb.ErrOverflowed) {
			// Back to the front; FIFO order is preserved for the next
			// flush. May exceed the bound by one message — fine.
			m.parkMu.Lock()
			m.parked = append([]drpc.Message{msg}, m.parked...)
			m.parkedBytes += msgSize(msg)
			m.parkMu.Unlock()
			return false
		}
		// Queued, or terminal (pool closed / shutdown): either way the
		// message leaves the buffer.
		if err == nil {
			delivered++
		}
	}
}

// SendMessage is the unicast path (headsync diffsyncer's subscribe).
// Deliberately no park-on-overflow: the diffsyncer re-subscribes on its
// own cadence, so a lost send heals within a sync period. A global-only
// peer is picked, never dialed; nodes and LAN peers are dialed as
// before.
func (m *spacePeerManager) SendMessage(ctx context.Context, peerId string, msg drpc.Message) error {
	return m.streamPool.Send(ctx, msg, func(ctx context.Context) ([]peer.Peer, error) {
		var (
			p   peer.Peer
			err error
		)
		if m.isGlobalOnly(peerId) {
			p, err = p2p.PickLive(ctx, m.pool, peerId)
		} else {
			p, err = m.pool.Get(ctx, peerId)
		}
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
//
// Targets NODES ONLY: the subscribe keeps node streams tagged, but LAN
// peer streams are already subscribed by streamHandler.OpenStream's
// preamble when they open. Sending it to local peers would flood them
// on the fast churn cadence (which runs whenever no node stream is up —
// i.e. exactly the offline-LAN scenario) for no benefit.
func (m *spacePeerManager) KeepAlive(ctx context.Context) {
	if len(m.subscribeMsgRaw) == 0 {
		return
	}
	msg := &spacesyncproto.ObjectSyncMessage{Payload: m.subscribeMsgRaw}
	_ = m.streamPool.Send(m.runCtx, msg, func(ctx context.Context) ([]peer.Peer, error) {
		return m.GetNodePeers(ctx)
	})
}

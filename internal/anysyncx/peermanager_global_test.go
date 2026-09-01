package anysyncx

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/commonspace/sync/objectsync/objectmessages"
	"github.com/anyproto/any-sync/net/peer"
	netpool "github.com/anyproto/any-sync/net/pool"
	"github.com/anyproto/any-sync/net/streampool"
	"github.com/stretchr/testify/require"
	"storj.io/drpc"
)

// hangingPool never completes a dial: Get blocks until ctx is done and
// counts the call; Pick answers from the connected map at once. Any
// Get for a global peer is a guard violation.
type hangingPool struct {
	fakePool
	mu   sync.Mutex
	gets []string
}

func (h *hangingPool) Get(ctx context.Context, id string) (peer.Peer, error) {
	h.mu.Lock()
	h.gets = append(h.gets, id)
	h.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (h *hangingPool) GetOneOf(ctx context.Context, ids []string) (peer.Peer, error) {
	return h.Get(ctx, ids[0])
}

func (h *hangingPool) getCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.gets)
}

type fakeGlobalPeers struct{ ids []string }

func (f *fakeGlobalPeers) GlobalPeerIds(string) []string { return f.ids }

// recordingSendPool resolves the PeerGetter of every Send and records
// the audience; nodeUp toggles the node-stream signal.
type recordingSendPool struct {
	mu        sync.Mutex
	audiences [][]string
	messages  []drpc.Message
	nodeUp    bool
}

func (r *recordingSendPool) Send(ctx context.Context, msg drpc.Message, getter streampool.PeerGetter) error {
	peers, err := getter(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.audiences = append(r.audiences, peerIds(peers))
	r.messages = append(r.messages, msg)
	r.mu.Unlock()
	return nil
}

func (r *recordingSendPool) Streams(tags ...string) []drpc.Stream {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []drpc.Stream
	for _, tag := range tags {
		if tag == nodeStreamTag && r.nodeUp {
			out = append(out, nil)
		}
	}
	return out
}

func (r *recordingSendPool) setNodeUp(up bool) {
	r.mu.Lock()
	r.nodeUp = up
	r.mu.Unlock()
}

// fakeSubs is a global subscription registry with fixed contents.
type fakeSubs struct{ ids map[string][]string }

func (f *fakeSubs) Subscribers(spaceId string) []string { return f.ids[spaceId] }

// sentPayloads lists the ObjectSyncMessage payloads sent so far, one
// per Send, nil for other message kinds.
func (r *recordingSendPool) sentPayloads() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]byte, len(r.messages))
	for i, m := range r.messages {
		if osm, ok := m.(*spacesyncproto.ObjectSyncMessage); ok {
			out[i] = osm.Payload
		}
	}
	return out
}

func (r *recordingSendPool) sent() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]string(nil), r.audiences...)
}

type objMsg struct {
	drpc.Message
	objectId string
}

func (m objMsg) ObjectId() string { return m.objectId }

// fakeInner is an outbound inner head update of a given object type.
type fakeInner struct {
	objectmessages.InnerHeadUpdate
	typ spacesyncproto.ObjectType
}

func (f fakeInner) ObjectType() spacesyncproto.ObjectType { return f.typ }

func headUpdate(objectId string, typ spacesyncproto.ObjectType) *objectmessages.HeadUpdate {
	return &objectmessages.HeadUpdate{Meta: objectmessages.ObjectMeta{ObjectId: objectId, SpaceId: "space1"}, Update: fakeInner{typ: typ}}
}

// blockingPickPool: Pick waits for ctx (an in-flight pool load for the
// same id) and Get hangs like hangingPool.
type blockingPickPool struct {
	hangingPool
}

func (b *blockingPickPool) Pick(ctx context.Context, _ string) (peer.Peer, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func newGlobalTestManager(pool netpool.Pool, nodes []string, global []string, sendPool *recordingSendPool) *spacePeerManager {
	m := &spacePeerManager{
		spaceId:      "space1",
		nodeConf:     &fakeNodeConf{nodeIds: nodes},
		pool:         pool,
		localPeers:   &fakeLocalPeers{},
		globalPeers:  &fakeGlobalPeers{ids: global},
		globalFanout: 2,
		streamPool:   sendPool,
	}
	m.runCtx, m.runCancel = context.WithCancel(context.Background())
	m.parkWake = make(chan struct{}, 1)
	m.parkDone = make(chan struct{})
	m.globalAskWake = make(chan struct{}, 1)
	return m
}

func TestGlobalPeersNeverDialedOnSyncPath(t *testing.T) {
	pool := &hangingPool{fakePool: fakePool{peers: map[string]peer.Peer{"g1": fakePeer{id: "g1"}}}}
	send := &recordingSendPool{}
	m := newGlobalTestManager(pool, nil, []string{"g1", "g2"}, send)
	defer func() { require.NoError(t, m.Close(context.Background())) }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	peers, err := m.GetResponsiblePeers(ctx)
	require.NoError(t, err)
	require.Less(t, time.Since(start), 500*time.Millisecond, "must not wait on a global dial")
	require.Equal(t, []string{"g1"}, peerIds(peers), "only the connected global peer, g2 is skipped")
	require.Equal(t, 0, pool.getCount(), "pool.Get never reaches a global peer")

	// The periodic diff rotates over connected global peers.
	pool.peers["g2"] = fakePeer{id: "g2"}
	first, _ := m.GetResponsiblePeers(ctx)
	second, _ := m.GetResponsiblePeers(ctx)
	require.NotEqual(t, peerIds(first), peerIds(second))
	require.Equal(t, 0, pool.getCount())
}

func TestGlobalPeersExcludedWhileNodeStreamUp(t *testing.T) {
	pool := &hangingPool{fakePool: fakePool{peers: map[string]peer.Peer{"g1": fakePeer{id: "g1"}}}}
	send := &recordingSendPool{nodeUp: true}
	m := newGlobalTestManager(pool, nil, []string{"g1"}, send)
	defer func() { require.NoError(t, m.Close(context.Background())) }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	peers, err := m.GetResponsiblePeers(ctx)
	require.NoError(t, err)
	require.Empty(t, peers, "a node stream carries the diff; no global peer joins")

	require.NoError(t, m.BroadcastMessage(ctx, objMsg{objectId: "o1"}))
	time.Sleep(globalCoalesceWindow + 100*time.Millisecond)
	sent := send.sent()
	require.Len(t, sent, 1, "one node/LAN send, no global fan-out")
	require.Empty(t, sent[0])
	require.Equal(t, 0, pool.getCount())
}

func TestGlobalBroadcastCoalescesWithoutNode(t *testing.T) {
	pool := &hangingPool{fakePool: fakePool{peers: map[string]peer.Peer{
		"g1": fakePeer{id: "g1"}, "g2": fakePeer{id: "g2"}, "g3": fakePeer{id: "g3"},
	}}}
	send := &recordingSendPool{}
	m := newGlobalTestManager(pool, nil, []string{"g1", "g2", "g3"}, send)
	defer func() { require.NoError(t, m.Close(context.Background())) }()

	ctx := context.Background()
	// Three tree updates of one object and one of another within the
	// window → two messages to the global peers, latest per object,
	// fan-out capped at globalFanout (2 of 3 connected).
	require.NoError(t, m.BroadcastMessage(ctx, headUpdate("o1", spacesyncproto.ObjectType_Tree)))
	require.NoError(t, m.BroadcastMessage(ctx, headUpdate("o1", spacesyncproto.ObjectType_Tree)))
	require.NoError(t, m.BroadcastMessage(ctx, headUpdate("o2", spacesyncproto.ObjectType_Tree)))
	require.NoError(t, m.BroadcastMessage(ctx, headUpdate("o1", spacesyncproto.ObjectType_Tree)))
	require.Eventually(t, func() bool { return len(send.sent()) == 6 }, 3*time.Second, 10*time.Millisecond)
	sent := send.sent()
	// First four sends are the immediate node/LAN broadcasts (empty
	// audiences here); the last two are the coalesced global batch.
	require.Equal(t, []string{"g1", "g2"}, sent[4])
	require.Equal(t, []string{"g1", "g2"}, sent[5])
	require.Equal(t, 0, pool.getCount())

	// Key-value and ACL updates carry non-cumulative payloads: two rows
	// of the same store object both reach the global peers.
	require.NoError(t, m.BroadcastMessage(ctx, headUpdate("kv", spacesyncproto.ObjectType_KeyValue)))
	require.NoError(t, m.BroadcastMessage(ctx, headUpdate("kv", spacesyncproto.ObjectType_KeyValue)))
	require.Eventually(t, func() bool { return len(send.sent()) == 10 }, 3*time.Second, 10*time.Millisecond)
}

// A space with no global peer queues nothing for the global fan-out.
func TestGlobalBroadcastSkipsWithoutGlobalPeers(t *testing.T) {
	pool := &hangingPool{fakePool: fakePool{peers: map[string]peer.Peer{}}}
	send := &recordingSendPool{}
	m := newGlobalTestManager(pool, nil, nil, send)
	defer func() { require.NoError(t, m.Close(context.Background())) }()

	require.NoError(t, m.BroadcastMessage(context.Background(), headUpdate("o1", spacesyncproto.ObjectType_Tree)))
	time.Sleep(globalCoalesceWindow + 100*time.Millisecond)
	require.Len(t, send.sent(), 1, "node/LAN send only")
	m.globalMu.Lock()
	require.Nil(t, m.coalesceTimer)
	m.globalMu.Unlock()
}

// A pool whose Pick waits on an in-flight load must not stall the sync
// path: global lookups are bounded by p2p.PickTimeout.
func TestGlobalPeersNeverWaitOnPick(t *testing.T) {
	pool := &blockingPickPool{hangingPool{fakePool: fakePool{peers: map[string]peer.Peer{}}}}
	send := &recordingSendPool{}
	m := newGlobalTestManager(pool, nil, []string{"g1", "g2"}, send)
	defer func() { require.NoError(t, m.Close(context.Background())) }()

	start := time.Now()
	peers, err := m.GetResponsiblePeers(context.Background())
	require.NoError(t, err)
	require.Empty(t, peers)
	require.Less(t, time.Since(start), 500*time.Millisecond)

	start = time.Now()
	err = m.SendMessage(context.Background(), "g1", msg("x"))
	require.Error(t, err, "a global-only peer is picked, never dialed")
	require.Less(t, time.Since(start), 500*time.Millisecond)
	require.Equal(t, 0, pool.getCount())
}

func TestGlobalBroadcastKeepsNonObjectMessages(t *testing.T) {
	pool := &hangingPool{fakePool: fakePool{peers: map[string]peer.Peer{"g1": fakePeer{id: "g1"}}}}
	send := &recordingSendPool{}
	m := newGlobalTestManager(pool, nil, []string{"g1"}, send)
	defer func() { require.NoError(t, m.Close(context.Background())) }()

	ctx := context.Background()
	require.NoError(t, m.BroadcastMessage(ctx, msg("kv1")))
	require.NoError(t, m.BroadcastMessage(ctx, msg("kv2")))
	require.NoError(t, m.BroadcastMessage(ctx, msg("same")))
	require.NoError(t, m.BroadcastMessage(ctx, msg("same")))
	require.Eventually(t, func() bool { return len(send.sent()) == 8 }, 3*time.Second, 10*time.Millisecond, "identical messages are not collapsed")
}

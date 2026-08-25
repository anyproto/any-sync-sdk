package anysyncx

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/net/peer"
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
// the audience; streams toggles the node-stream signal.
type recordingSendPool struct {
	mu        sync.Mutex
	audiences [][]string
	nodeUp    bool
}

func (r *recordingSendPool) Send(ctx context.Context, _ drpc.Message, getter streampool.PeerGetter) error {
	peers, err := getter(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.audiences = append(r.audiences, peerIds(peers))
	r.mu.Unlock()
	return nil
}

func (r *recordingSendPool) Streams(...string) []drpc.Stream {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nodeUp {
		return []drpc.Stream{nil}
	}
	return nil
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

func newGlobalTestManager(pool *hangingPool, nodes []string, global []string, sendPool *recordingSendPool) *spacePeerManager {
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
	// Three updates of one object and one of another within the window
	// → two messages to the global peers, latest per object, fan-out
	// capped at globalFanout (2 of 3 connected).
	require.NoError(t, m.BroadcastMessage(ctx, objMsg{objectId: "o1"}))
	require.NoError(t, m.BroadcastMessage(ctx, objMsg{objectId: "o1"}))
	require.NoError(t, m.BroadcastMessage(ctx, objMsg{objectId: "o2"}))
	require.NoError(t, m.BroadcastMessage(ctx, objMsg{objectId: "o1"}))
	require.Eventually(t, func() bool { return len(send.sent()) == 6 }, 3*time.Second, 10*time.Millisecond)
	sent := send.sent()
	// First four sends are the immediate node/LAN broadcasts (empty
	// audiences here); the last two are the coalesced global batch.
	require.Equal(t, []string{"g1", "g2"}, sent[4])
	require.Equal(t, []string{"g1", "g2"}, sent[5])
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
	require.Eventually(t, func() bool { return len(send.sent()) == 4 }, 3*time.Second, 10*time.Millisecond)
}

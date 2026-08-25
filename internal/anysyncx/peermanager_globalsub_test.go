package anysyncx

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/stretchr/testify/require"
)

func subscriptionPayloads(t *testing.T, spaceId string) (subscribe, unsubscribe []byte) {
	t.Helper()
	sub := &spacesyncproto.SpaceSubscription{SpaceIds: []string{spaceId}, Action: spacesyncproto.SpaceSubscriptionAction_Subscribe}
	subscribe, err := sub.MarshalVT()
	require.NoError(t, err)
	sub.Action = spacesyncproto.SpaceSubscriptionAction_Unsubscribe
	unsubscribe, err = sub.MarshalVT()
	require.NoError(t, err)
	return
}

func newAskingManager(t *testing.T, pool *fakePool, global []string, send *recordingSendPool) *spacePeerManager {
	t.Helper()
	m := newGlobalTestManager(pool, nil, global, send)
	m.subscribeMsgRaw, m.unsubscribeMsgRaw = subscriptionPayloads(t, "space1")
	return m
}

// A device with no node stream asks its connected global peers for
// pushes on every ask round and withdraws the ask once a node is back.
func TestGlobalAskWhileNodeless(t *testing.T) {
	pool := &fakePool{peers: map[string]peer.Peer{"g1": fakePeer{id: "g1"}}}
	send := &recordingSendPool{}
	m := newAskingManager(t, pool, []string{"g1", "g2"}, send)

	m.globalAskStep()
	sent, payloads := send.sent(), send.sentPayloads()
	require.Len(t, sent, 1)
	require.Equal(t, []string{"g1"}, sent[0], "only connected global peers, never dialed")
	require.Equal(t, m.subscribeMsgRaw, payloads[0])

	// still node-less: asked again (a peer connected later is covered)
	m.globalAskStep()
	require.Len(t, send.sent(), 2)
	require.Equal(t, m.subscribeMsgRaw, send.sentPayloads()[1])

	// node back: one unsubscribe, then nothing
	send.setNodeUp(true)
	m.globalAskStep()
	sent, payloads = send.sent(), send.sentPayloads()
	require.Len(t, sent, 3)
	require.Equal(t, []string{"g1"}, sent[2])
	require.Equal(t, m.unsubscribeMsgRaw, payloads[2])
	m.globalAskStep()
	require.Len(t, send.sent(), 3, "nothing while a node stream is up")

	require.NoError(t, m.Close(context.Background()))
	require.Len(t, send.sent(), 3, "nothing to withdraw")
}

// A node stream that flaps yields exactly one withdrawal per
// up-transition and a fresh ask after every drop.
func TestGlobalAskNodeStreamFlapping(t *testing.T) {
	pool := &fakePool{peers: map[string]peer.Peer{"g1": fakePeer{id: "g1"}}}
	send := &recordingSendPool{}
	m := newAskingManager(t, pool, []string{"g1"}, send)

	var want [][]byte
	for _, up := range []bool{false, true, true, false, false, true} {
		send.setNodeUp(up)
		m.globalAskStep()
		switch {
		case !up:
			want = append(want, m.subscribeMsgRaw)
		case up && (len(want) > 0 && string(want[len(want)-1]) == string(m.subscribeMsgRaw)):
			want = append(want, m.unsubscribeMsgRaw)
		}
	}
	require.Equal(t, want, send.sentPayloads())
	require.Len(t, want, 5, "3 asks, 2 withdrawals")
}

// KeepAlive never sends the global ask itself: it wakes the ask loop
// only when the node stream state and the ask disagree.
func TestNoteNodeStateWakesOnTransition(t *testing.T) {
	pool := &fakePool{peers: map[string]peer.Peer{"g1": fakePeer{id: "g1"}}}
	send := &recordingSendPool{}
	m := newAskingManager(t, pool, []string{"g1"}, send)
	woke := func() bool {
		select {
		case <-m.globalAskWake:
			return true
		default:
			return false
		}
	}

	m.KeepAlive(context.Background())
	require.Len(t, send.sent(), 1, "node subscribe only")
	require.True(t, woke(), "node-less and not yet asked")

	m.globalAskStep()
	m.KeepAlive(context.Background())
	require.False(t, woke(), "already asked")

	send.setNodeUp(true)
	m.KeepAlive(context.Background())
	require.True(t, woke(), "node back while asked")
	m.globalAskStep()
	m.KeepAlive(context.Background())
	require.False(t, woke())
}

// No ask is queued for a space without global peers, and a peer
// reachable over the LAN is left to the LAN path.
func TestGlobalAskAudience(t *testing.T) {
	pool := &fakePool{peers: map[string]peer.Peer{"g1": fakePeer{id: "g1"}, "g2": fakePeer{id: "g2"}}}
	send := &recordingSendPool{}
	m := newAskingManager(t, pool, nil, send)
	m.globalAskStep()
	require.Empty(t, send.sent(), "no global peers: nothing queued")

	m = newAskingManager(t, pool, []string{"g1", "g2"}, send)
	m.localPeers = &fakeLocalPeers{ids: []string{"g2"}}
	m.globalAskStep()
	require.Equal(t, [][]string{{"g1"}}, send.sent())
}

// Close withdraws an outstanding ask with a context that outlives the
// call: the send pool runs the audience lookup later.
func TestCloseWithdrawsGlobalAsk(t *testing.T) {
	pool := &fakePool{peers: map[string]peer.Peer{"g1": fakePeer{id: "g1"}}}
	send := &recordingSendPool{}
	m := newAskingManager(t, pool, []string{"g1"}, send)
	m.globalAskStep()
	require.NoError(t, m.Close(context.Background()))
	payloads := send.sentPayloads()
	require.Len(t, payloads, 2)
	require.Equal(t, m.unsubscribeMsgRaw, payloads[1])
	require.Equal(t, []string{"g1"}, send.sent()[1])
}

// A device with a node stream pushes only to the global peers that
// asked: an ask from a peer the records do not name for the space, or
// from a LAN-reachable peer, buys nothing.
func TestGlobalPushWithNodeTargetsSubscribers(t *testing.T) {
	pool := &fakePool{peers: map[string]peer.Peer{
		"g1": fakePeer{id: "g1"}, "g2": fakePeer{id: "g2"}, "g3": fakePeer{id: "g3"}, "l1": fakePeer{id: "l1"},
	}}
	send := &recordingSendPool{nodeUp: true}
	m := newGlobalTestManager(pool, nil, []string{"g1", "g2", "l1"}, send)
	m.localPeers = &fakeLocalPeers{ids: []string{"l1"}}
	subs := &fakeSubs{ids: map[string][]string{}}
	m.subs = subs
	defer func() { require.NoError(t, m.Close(context.Background())) }()

	require.False(t, m.globalPushActive(), "nobody asked")
	require.NoError(t, m.BroadcastMessage(context.Background(), headUpdate("o1", spacesyncproto.ObjectType_Tree)))
	m.globalMu.Lock()
	require.Empty(t, m.coalesced, "no global push without a subscriber")
	m.globalMu.Unlock()

	subs.ids["space1"] = []string{"g1", "g3", "l1"} // g3 unknown to the records, l1 on the LAN
	require.True(t, m.globalPushActive())
	peers, err := m.getGlobalPeers(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"g1"}, peerIds(peers))

	require.NoError(t, m.BroadcastMessage(context.Background(), headUpdate("o1", spacesyncproto.ObjectType_Tree)))
	m.flushCoalesced()
	sent := send.sent()
	require.NotEmpty(t, sent)
	require.Equal(t, []string{"g1"}, sent[len(sent)-1])

	// the subscriber left the records: its ask no longer keeps the
	// space pushing
	subs.ids["space1"] = []string{"g3"}
	require.False(t, m.globalPushActive())
	require.NoError(t, m.BroadcastMessage(context.Background(), headUpdate("o2", spacesyncproto.ObjectType_Tree)))
	m.globalMu.Lock()
	require.Empty(t, m.coalesced)
	m.globalMu.Unlock()
}

// Without a node stream every connected global peer is pushed to,
// subscribed or not: they are the only path.
func TestGlobalPushWithoutNodeTargetsAllConnected(t *testing.T) {
	pool := &fakePool{peers: map[string]peer.Peer{"g1": fakePeer{id: "g1"}, "g2": fakePeer{id: "g2"}}}
	send := &recordingSendPool{}
	m := newGlobalTestManager(pool, nil, []string{"g1", "g2"}, send)
	defer func() { require.NoError(t, m.Close(context.Background())) }()

	require.True(t, m.globalPushActive())
	peers, err := m.getGlobalPeers(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"g1", "g2"}, peerIds(peers))
}

// The subscriber audience is picked, never dialed: with a pool whose
// dial hangs the lookup returns at once with zero dials.
func TestSubscribedGlobalPeersNeverDial(t *testing.T) {
	pool := &hangingPool{fakePool: fakePool{peers: map[string]peer.Peer{}}}
	send := &recordingSendPool{nodeUp: true}
	m := newGlobalTestManager(pool, nil, []string{"g1"}, send)
	m.subs = &fakeSubs{ids: map[string][]string{"space1": {"g1"}}}
	defer func() { require.NoError(t, m.Close(context.Background())) }()

	start := time.Now()
	require.True(t, m.globalPushActive())
	peers, err := m.getGlobalPeers(context.Background())
	require.NoError(t, err)
	require.Empty(t, peers, "not connected")
	require.Less(t, time.Since(start), time.Second)
	require.Zero(t, pool.getCount())
}

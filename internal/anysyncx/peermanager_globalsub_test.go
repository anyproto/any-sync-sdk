package anysyncx

import (
	"context"
	"testing"

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

// A device with no node stream asks its connected global peers for
// pushes on every KeepAlive and withdraws the ask once a node is back.
func TestKeepAliveSubscribesGlobalPeersWhileNodeless(t *testing.T) {
	pool := &fakePool{peers: map[string]peer.Peer{"g1": fakePeer{id: "g1"}}}
	send := &recordingSendPool{}
	m := newGlobalTestManager(pool, nil, []string{"g1", "g2"}, send)
	m.subscribeMsgRaw, m.unsubscribeMsgRaw = subscriptionPayloads(t, "space1")
	ctx := context.Background()

	m.KeepAlive(ctx)
	sent, payloads := send.sent(), send.sentPayloads()
	require.Len(t, sent, 2, "node subscribe + global subscribe")
	require.Equal(t, []string{"g1"}, sent[1], "only connected global peers, never dialed")
	require.Equal(t, m.subscribeMsgRaw, payloads[1])

	// still node-less: asked again (a peer connected later is covered)
	m.KeepAlive(ctx)
	require.Len(t, send.sent(), 4)
	require.Equal(t, m.subscribeMsgRaw, send.sentPayloads()[3])

	// node back: one unsubscribe, then nothing
	send.mu.Lock()
	send.nodeUp = true
	send.mu.Unlock()
	m.KeepAlive(ctx)
	sent, payloads = send.sent(), send.sentPayloads()
	require.Len(t, sent, 6)
	require.Equal(t, []string{"g1"}, sent[5])
	require.Equal(t, m.unsubscribeMsgRaw, payloads[5])
	m.KeepAlive(ctx)
	require.Len(t, send.sent(), 7, "node subscribe only")

	require.NoError(t, m.Close(ctx))
}

// A device with a node stream pushes only to the global peers that
// asked: a subscription from a peer the records do not name for the
// space, or from a LAN-reachable peer, buys nothing.
func TestGlobalPushWithNodeTargetsSubscribers(t *testing.T) {
	pool := &fakePool{peers: map[string]peer.Peer{
		"g1": fakePeer{id: "g1"}, "g2": fakePeer{id: "g2"}, "g3": fakePeer{id: "g3"}, "l1": fakePeer{id: "l1"},
	}}
	send := &recordingSendPool{nodeUp: true}
	m := newGlobalTestManager(pool, nil, []string{"g1", "g2", "l1"}, send)
	m.localPeers = &fakeLocalPeers{ids: []string{"l1"}}
	defer func() { require.NoError(t, m.Close(context.Background())) }()

	require.False(t, m.globalPushActive(), "nobody asked")
	require.NoError(t, m.BroadcastMessage(context.Background(), headUpdate("o1", spacesyncproto.ObjectType_Tree)))
	m.globalMu.Lock()
	require.Empty(t, m.coalesced, "no global push without a subscriber")
	m.globalMu.Unlock()

	send.subscribe("space1", "g1")
	send.subscribe("space1", "g3") // unknown to the records
	send.subscribe("space1", "l1") // pushed to by the LAN path
	require.True(t, m.globalPushActive())
	peers, err := m.getGlobalPeers(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"g1"}, peerIds(peers))

	require.NoError(t, m.BroadcastMessage(context.Background(), headUpdate("o1", spacesyncproto.ObjectType_Tree)))
	m.flushCoalesced()
	sent := send.sent()
	require.NotEmpty(t, sent)
	require.Equal(t, []string{"g1"}, sent[len(sent)-1])
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

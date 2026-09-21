package filep2p

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync-sdk/internal/files/fetch"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	"storj.io/drpc"
)

type globalOnlyPeers struct{ global []string }

func (g globalOnlyPeers) LocalPeerIds(string) []string  { return nil }
func (g globalOnlyPeers) GlobalPeerIds(string) []string { return g.global }

// stubPeer answers every RPC with an error, so holdsFull reports false.
type stubPeer struct {
	peer.Peer
	id string
}

func (s stubPeer) Id() string { return s.id }
func (s stubPeer) DoDrpc(context.Context, func(conn drpc.Conn) error) error {
	return errors.New("no rpc in test")
}

// hangingDialPool: Get blocks until ctx is done (a relay dial that
// never answers), Pick serves the connected map at once.
type hangingDialPool struct {
	mu        sync.Mutex
	gets      int
	connected map[string]peer.Peer
}

func (h *hangingDialPool) Get(ctx context.Context, _ string) (peer.Peer, error) {
	h.mu.Lock()
	h.gets++
	h.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (h *hangingDialPool) Pick(_ context.Context, id string) (peer.Peer, error) {
	if p, ok := h.connected[id]; ok {
		return p, nil
	}
	return nil, errors.New("not connected")
}

func TestSourceGlobalPeersPickOnly(t *testing.T) {
	pool := &hangingDialPool{connected: map[string]peer.Peer{"g1": stubPeer{id: "g1"}}}
	src := NewSource(pool, globalOnlyPeers{global: []string{"g1", "g2"}})
	root, err := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	require.NoError(t, err)

	start := time.Now()
	_, _, ok := src.SourceFor(context.Background(), "s", root)
	require.False(t, ok)
	require.Less(t, time.Since(start), perPeerTimeout, "a disconnected global peer is skipped, never dialed")
	require.Equal(t, 0, pool.gets, "pool.Get never reaches a global peer")

	// A disconnected global peer is not banned: it is simply not
	// connected right now.
	src.banMu.Lock()
	_, banned := src.banned["g2"]
	src.banMu.Unlock()
	require.False(t, banned)
}

// blockingPickPool: Pick waits on its ctx (an in-flight pool load).
type blockingPickPool struct{ hangingDialPool }

func (b *blockingPickPool) Pick(ctx context.Context, _ string) (peer.Peer, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestSourceGlobalPeersBoundedPick(t *testing.T) {
	pool := &blockingPickPool{}
	src := NewSource(pool, globalOnlyPeers{global: []string{"g1", "g2"}})
	root, err := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	require.NoError(t, err)

	start := time.Now()
	_, _, ok := src.SourceFor(context.Background(), "s", root)
	require.False(t, ok)
	require.Less(t, time.Since(start), 2*perPeerTimeout, "each global lookup is bounded well below the per-peer budget")
	require.Equal(t, 0, pool.gets)
}

// slowCheckPeer answers FileCheck after `delay`, modelling the
// round trip to a global peer across the internet through a relay.
type slowCheckPeer struct {
	peer.Peer
	id    string
	delay time.Duration
}

func (s slowCheckPeer) Id() string { return s.id }
func (s slowCheckPeer) DoDrpc(ctx context.Context, _ func(conn drpc.Conn) error) error {
	select {
	case <-time.After(s.delay):
		return errors.New("answered, but this stub reports nothing held")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// A global candidate gets a budget sized for a relayed round trip, not
// the LAN one. At the LAN figure a peer that holds the file in full is
// never asked, the file reads as unavailable, and nothing names the
// cause — measured across two machines, a 1.5 MB file would not
// transfer at all until the budget was raised.
func TestSourceGlobalPeerGetsARelaySizedBudget(t *testing.T) {
	require.Greater(t, perGlobalPeerTimeout, perPeerTimeout,
		"a relayed FileCheck cannot share the LAN budget")

	// A peer answering just past the LAN budget must still be asked.
	slow := slowCheckPeer{id: "g1", delay: perPeerTimeout * 2}
	pool := &hangingDialPool{connected: map[string]peer.Peer{"g1": slow}}
	s := NewSource(pool, globalOnlyPeers{global: []string{"g1"}})

	start := time.Now()
	_, _, ok := s.SourceFor(context.Background(), "space1", testCid(t))
	elapsed := time.Since(start)

	require.False(t, ok, "the stub holds nothing, so selection still fails")
	require.Greater(t, elapsed, perPeerTimeout,
		"the check was cut off at the LAN budget instead of being awaited")
	require.Less(t, elapsed, perGlobalPeerTimeout, "and it must still be bounded")
}

// The LAN budget is unchanged: a slow LAN peer is still cut off fast so
// the sweep reaches the peers behind it.
func TestSourceLanPeerKeepsTheShortBudget(t *testing.T) {
	slow := slowCheckPeer{id: "l1", delay: time.Hour}
	pool := &hangingDialPool{connected: map[string]peer.Peer{"l1": slow}}
	s := NewSource(pool, lanOnlyPeers{lan: []string{"l1"}})

	start := time.Now()
	_, _, ok := s.SourceFor(context.Background(), "space1", testCid(t))

	require.False(t, ok)
	require.Less(t, time.Since(start), perGlobalPeerTimeout,
		"a LAN peer must not hold the sweep for the global budget")
}

type lanOnlyPeers struct{ lan []string }

func (l lanOnlyPeers) LocalPeerIds(string) []string  { return l.lan }
func (l lanOnlyPeers) GlobalPeerIds(string) []string { return nil }

func testCid(t *testing.T) cid.Cid {
	t.Helper()
	root, err := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	require.NoError(t, err)
	return root
}

// A global peerCar states a read budget of its own; a LAN one defers to
// the fetch package's default. A range served across a relay runs at a
// few MB/s, so the LAN figure expires mid-range — and with no durable
// copy to fall back on, that expiry fails the whole fetch.
func TestPeerCarReadDeadlineIsPerLayer(t *testing.T) {
	global := &peerCar{global: true}
	lan := &peerCar{global: false}
	require.Equal(t, globalReadDeadline, global.PeerReadDeadline())
	require.Zero(t, lan.PeerReadDeadline(), "a LAN read keeps the package default")
	var _ fetch.PeerReadDeadliner = global
}

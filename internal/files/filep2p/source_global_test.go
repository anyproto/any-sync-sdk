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
	picks     int
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
	h.mu.Lock()
	h.picks++
	h.mu.Unlock()
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
	_, banned := src.banned[banKey{"g2", true}]
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
// the LAN one: at the LAN figure a peer holding the file in full is
// never asked and the file reads as unavailable.
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

// The LAN FileCheck budget is unchanged: a LAN peer that DIALS fine
// and then answers slowly is still cut off at the short budget, so the
// sweep reaches the peers behind it. The dial has to succeed or this
// measures the dial timeout instead.
func TestSourceLanPeerKeepsTheShortBudget(t *testing.T) {
	slow := slowCheckPeer{id: "l1", delay: time.Hour}
	pool := &answeringDialPool{peers: map[string]peer.Peer{"l1": slow}}
	s := NewSource(pool, lanOnlyPeers{lan: []string{"l1"}})

	start := time.Now()
	_, _, ok := s.SourceFor(context.Background(), "space1", testCid(t))

	require.False(t, ok)
	require.Less(t, time.Since(start), 2*perPeerTimeout,
		"a LAN FileCheck must not get the relayed budget")
}

// answeringDialPool dials successfully, so the FileCheck budget is
// what bounds the candidate.
type answeringDialPool struct{ peers map[string]peer.Peer }

func (a *answeringDialPool) Get(_ context.Context, id string) (peer.Peer, error) {
	if p, ok := a.peers[id]; ok {
		return p, nil
	}
	return nil, errors.New("no such peer")
}

func (a *answeringDialPool) Pick(ctx context.Context, id string) (peer.Peer, error) {
	return a.Get(ctx, id)
}

type lanOnlyPeers struct{ lan []string }

func (l lanOnlyPeers) LocalPeerIds(string) []string  { return l.lan }
func (l lanOnlyPeers) GlobalPeerIds(string) []string { return nil }

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

// A LAN failure must not remove the same device's relayed path: one
// peer id, two paths, two very different budgets.
func TestSourceBansArePerLayer(t *testing.T) {
	pool := &hangingDialPool{connected: map[string]peer.Peer{}}
	s := NewSource(pool, lanOnlyPeers{lan: []string{"d1"}})
	// A LAN dial that never answers bans the LAN entry.
	_, _, ok := s.SourceFor(context.Background(), "space1", testCid(t))
	require.False(t, ok)

	s.banMu.Lock()
	_, lanBanned := s.banned[banKey{"d1", false}]
	_, globalBanned := s.banned[banKey{"d1", true}]
	s.banMu.Unlock()
	require.True(t, lanBanned, "the failing LAN path is banned")
	require.False(t, globalBanned, "the relayed path to the same device must survive")
}

// The caller giving up must not be recorded against the peer: banning
// on an aborted download would hide the one device holding the file
// for banTTL. The cancel has to land MID-DIAL — a context already dead
// when the sweep starts never reaches the ban path at all.
func TestSourceDoesNotBanOnCallerCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := &cancellingDialPool{cancel: cancel}
	s := NewSource(pool, lanOnlyPeers{lan: []string{"d1"}})

	_, _, ok := s.SourceFor(ctx, "space1", testCid(t))
	require.False(t, ok)
	s.banMu.Lock()
	n := len(s.banned)
	s.banMu.Unlock()
	require.Zero(t, n, "a cancelled caller says nothing about the peer")
}

// cancellingDialPool cancels the caller while the dial is in flight.
type cancellingDialPool struct {
	hangingDialPool
	cancel context.CancelFunc
}

func (c *cancellingDialPool) Get(ctx context.Context, _ string) (peer.Peer, error) {
	c.cancel()
	<-ctx.Done()
	return nil, ctx.Err()
}

// A layer that exhausts its own clock must not spend the next layer's:
// stale LAN ids used to consume a shared budget, so the relayed peer
// holding the file was never consulted.
func TestSourceLayersDoNotStarveEachOther(t *testing.T) {
	live := stubPeer{id: "g1"}
	pool := &hangingDialPool{connected: map[string]peer.Peer{"g1": live}}
	var lan []string
	for i := 0; i < 12; i++ {
		lan = append(lan, "stale"+string(rune('a'+i)))
	}
	s := NewSource(pool, bothLayerPeers{lan: lan, global: []string{"g1"}})

	_, _, ok := s.SourceFor(context.Background(), "space1", testCid(t))
	require.False(t, ok, "the stub reports nothing held")
	require.LessOrEqual(t, pool.gets, maxCandidates, "the LAN layer must stay bounded")
	require.Positive(t, pool.picks, "the global layer must still be reached")
}

type bothLayerPeers struct{ lan, global []string }

func (b bothLayerPeers) LocalPeerIds(string) []string  { return b.lan }
func (b bothLayerPeers) GlobalPeerIds(string) []string { return b.global }

// The global layer is bounded on its own terms: at most
// maxCandidates consulted, and its clock is not spent by the LAN one.
func TestSourceGlobalLayerIsBoundedSeparately(t *testing.T) {
	pool := &hangingDialPool{connected: map[string]peer.Peer{}}
	var global []string
	for i := 0; i < 20; i++ {
		global = append(global, "g"+string(rune('a'+i)))
	}
	s := NewSource(pool, globalOnlyPeers{global: global})

	start := time.Now()
	_, _, ok := s.SourceFor(context.Background(), "space1", testCid(t))
	require.False(t, ok)
	require.LessOrEqual(t, pool.picks, maxCandidates, "twenty stale ids must not all be consulted")
	require.Less(t, time.Since(start), maxGlobalSweep, "and the layer's clock bounds it")
}

// The head probe cannot be tighter than the budget selection admitted
// the peer under: both are one relayed round trip, so a peer accepted
// at perGlobalPeerTimeout would otherwise fail its first read.
func TestGlobalProbeBudgetCoversSelection(t *testing.T) {
	require.GreaterOrEqual(t, globalProbeDeadline, perGlobalPeerTimeout)
	require.GreaterOrEqual(t, globalReadDeadline, globalProbeDeadline)
}

func testCid(t *testing.T) cid.Cid {
	t.Helper()
	root, err := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	require.NoError(t, err)
	return root
}

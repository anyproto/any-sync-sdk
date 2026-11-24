package filep2p

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonfile/fileproto/filep2p"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	"storj.io/drpc"

	"github.com/anyproto/any-sync-sdk/internal/files/fetch"
)

// testBudgets are the production clocks divided by twenty, so every
// proportion the sweeps depend on (how many per-peer budgets fit a
// layer clock, LAN against global) holds without sleeping out real
// seconds.
const testScale = 20

var testBudgets = budgets{
	lanPeer:     perPeerTimeout / testScale,
	globalPeer:  perGlobalPeerTimeout / testScale,
	lanSweep:    maxLanSweep / testScale,
	globalSweep: maxGlobalSweep / testScale,
	ban:         banTTL,
	backoffMin:  backoffMin,
	backoffMax:  backoffMax,
}

func newTestSource(pool DialPool, peers LocalPeers) *Source {
	s := NewSource(pool, peers, nil)
	s.b = testBudgets
	return s
}

// query asks for the test file in a space, as a fetch that has a public
// object to fall back on.
func query(t *testing.T, spaceId string) fetch.PeerQuery {
	t.Helper()
	return fetch.PeerQuery{SpaceId: spaceId, Root: testCid(t)}
}

// isBanned reports a peer that selection skips right now, banned or
// backing off.
func (s *Source) isBanned(peerId string, global bool) bool {
	s.banMu.Lock()
	defer s.banMu.Unlock()
	e, ok := s.banned[banKey{peerId, global}]
	return ok && time.Now().Before(e.until)
}

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

// holderPeer answers FileCheck with Availability_Full for every root.
type holderPeer struct {
	peer.Peer
	id string
}

func (h holderPeer) Id() string { return h.id }
func (h holderPeer) DoDrpc(_ context.Context, do func(conn drpc.Conn) error) error {
	return do(holderConn{})
}

type holderConn struct{ drpc.Conn }

func (holderConn) Invoke(_ context.Context, _ string, _ drpc.Encoding, in, out drpc.Message) error {
	req := in.(*filep2p.FileCheckRequest)
	resp := out.(*filep2p.FileCheckResponse)
	for _, root := range req.RootCids {
		resp.Files = append(resp.Files, &filep2p.FileAvailability{RootCid: root, Have: filep2p.Availability_Full})
	}
	return nil
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
	src := newTestSource(pool, globalOnlyPeers{global: []string{"g1", "g2"}})

	start := time.Now()
	_, _, err := src.SourceFor(context.Background(), query(t, "s"))
	require.ErrorIs(t, err, fetch.ErrNotAvailable)
	require.Less(t, time.Since(start), testBudgets.globalPeer, "a disconnected global peer is skipped, never dialed")
	require.Equal(t, 0, pool.gets, "pool.Get never reaches a global peer")

	// A disconnected global peer is not sidelined: it is simply not
	// connected right now.
	require.False(t, src.isBanned("g2", true))
}

// blockingPickPool: Pick waits on its ctx (an in-flight pool load).
type blockingPickPool struct{ hangingDialPool }

func (b *blockingPickPool) Pick(ctx context.Context, _ string) (peer.Peer, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestSourceGlobalPeersBoundedPick(t *testing.T) {
	pool := &blockingPickPool{}
	src := newTestSource(pool, globalOnlyPeers{global: []string{"g1", "g2"}})

	start := time.Now()
	_, _, err := src.SourceFor(context.Background(), query(t, "s"))
	require.ErrorIs(t, err, fetch.ErrNotAvailable)
	require.Less(t, time.Since(start), testBudgets.globalPeer, "each global lookup is bounded well below the per-peer budget")
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
	slow := slowCheckPeer{id: "g1", delay: testBudgets.lanPeer * 2}
	pool := &hangingDialPool{connected: map[string]peer.Peer{"g1": slow}}
	s := newTestSource(pool, globalOnlyPeers{global: []string{"g1"}})

	start := time.Now()
	_, _, err := s.SourceFor(context.Background(), query(t, "space1"))
	elapsed := time.Since(start)

	require.ErrorIs(t, err, fetch.ErrNotAvailable, "the stub holds nothing, so selection still fails")
	require.GreaterOrEqual(t, elapsed, slow.delay,
		"the check was cut off at the LAN budget instead of being awaited")
	require.Less(t, elapsed, testBudgets.globalPeer, "and it must still be bounded")
}

// The LAN FileCheck budget is unchanged: a LAN peer that DIALS fine
// and then answers slowly is still cut off at the short budget, so the
// sweep reaches the peers behind it. Two stalled peers both fit in the
// LAN clock at the short budget; at the relayed one the first eats the
// whole clock and the second is never asked. The dial has to succeed
// or this measures the dial timeout instead.
func TestSourceLanPeerKeepsTheShortBudget(t *testing.T) {
	require.Less(t, 2*testBudgets.lanPeer, testBudgets.lanSweep)
	require.Greater(t, testBudgets.globalPeer, testBudgets.lanSweep)
	stall := time.Hour
	pool := &answeringDialPool{peers: map[string]peer.Peer{
		"l1": slowCheckPeer{id: "l1", delay: stall},
		"l2": slowCheckPeer{id: "l2", delay: stall},
	}}
	s := newTestSource(pool, lanOnlyPeers{lan: []string{"l1", "l2"}})

	_, _, err := s.SourceFor(context.Background(), query(t, "space1"))

	require.ErrorIs(t, err, fetch.ErrNotAvailable)
	require.Equal(t, []string{"l1", "l2"}, pool.asked,
		"a LAN FileCheck must not get the relayed budget")
}

// answeringDialPool dials successfully, so the FileCheck budget is
// what bounds the candidate.
type answeringDialPool struct {
	peers map[string]peer.Peer
	asked []string
}

func (a *answeringDialPool) Get(_ context.Context, id string) (peer.Peer, error) {
	if p, ok := a.peers[id]; ok {
		a.asked = append(a.asked, id)
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
func TestPeerCarReadBudgetIsPerLayer(t *testing.T) {
	global := &peerCar{global: true}
	lan := &peerCar{global: false}
	rt, rate := global.PeerReadBudget()
	require.Equal(t, time.Duration(globalRoundTrip), rt)
	require.Equal(t, int64(globalMinRate), rate)
	rt, rate = lan.PeerReadBudget()
	require.Zero(t, rt, "a LAN read keeps the package round trip")
	require.Equal(t, int64(lanMinRate), rate, "but still earns time per byte")
	var _ fetch.PeerReadBudgeter = global
}

// A LAN failure must not remove the same device's relayed path: one
// peer id, two paths, two very different budgets.
func TestSourceBansArePerLayer(t *testing.T) {
	pool := &hangingDialPool{connected: map[string]peer.Peer{}}
	s := newTestSource(pool, bothLayerPeers{lan: []string{"d1"}, global: []string{"d1"}})
	// A LAN dial that never answers backs the LAN entry off.
	_, _, err := s.SourceFor(context.Background(), query(t, "space1"))
	require.ErrorIs(t, err, fetch.ErrNotAvailable)
	require.Equal(t, 1, pool.gets)
	require.Equal(t, 1, pool.picks, "the global layer still consults the device")
	require.True(t, s.isBanned("d1", false), "the failing LAN path is backed off")
	require.False(t, s.isBanned("d1", true), "the relayed path to the same device must survive")

	// The next selection skips the LAN dial and still picks the device.
	_, _, err = s.SourceFor(context.Background(), query(t, "space1"))
	require.ErrorIs(t, err, fetch.ErrNotAvailable)
	require.Equal(t, 1, pool.gets, "the LAN path is not dialed again inside its backoff")
	require.Equal(t, 2, pool.picks, "the relayed path is still consulted")
}

// The caller giving up must not be recorded against the peer: an
// aborted download would hide the one device holding the file. The
// cancel has to land MID-DIAL — a context already dead when the sweep
// starts never reaches the verdict at all.
func TestSourceDoesNotBanOnCallerCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := &cancellingDialPool{cancel: cancel}
	s := newTestSource(pool, lanOnlyPeers{lan: []string{"d1"}})

	_, _, err := s.SourceFor(ctx, query(t, "space1"))
	require.ErrorIs(t, err, fetch.ErrNotAvailable)
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

// The layer clock ending mid-FileCheck says nothing about that peer:
// only a candidate's OWN budget or a real RPC error backs it off.
// Otherwise one stalling candidate ahead of the holder gets it
// sidelined when the sweep runs out of time on it.
func TestSourceDoesNotBanOnLayerClock(t *testing.T) {
	stall := time.Hour
	pool := &hangingDialPool{connected: map[string]peer.Peer{
		"g1": slowCheckPeer{id: "g1", delay: stall},
		"g2": slowCheckPeer{id: "g2", delay: stall},
	}}
	// One full per-peer budget fits in the layer clock; the second
	// candidate is cut off by the clock, not by its own budget.
	require.Less(t, testBudgets.globalPeer, testBudgets.globalSweep)
	require.Greater(t, 2*testBudgets.globalPeer, testBudgets.globalSweep)
	s := newTestSource(pool, globalOnlyPeers{global: []string{"g1", "g2"}})

	_, _, err := s.SourceFor(context.Background(), query(t, "space1"))
	require.ErrorIs(t, err, fetch.ErrNotAvailable)
	require.True(t, s.isBanned("g1", true), "exhausted its own budget")
	require.False(t, s.isBanned("g2", true), "cut off by the layer clock, not by its own budget")
}

// A dial error that carries another caller's cancel is not this peer's
// fault: the pool shares an in-flight dial's outcome with every waiter,
// so a fetch aborted elsewhere must not sideline a healthy holder. The
// dial's OWN timeout, surfacing as a deadline error while the
// candidate's budget is still running, is the peer's fault.
func TestSourceDoesNotBanOnInheritedCancel(t *testing.T) {
	pool := &dialErrPool{err: context.Canceled}
	s := newTestSource(pool, lanOnlyPeers{lan: []string{"d1"}})
	_, _, err := s.SourceFor(context.Background(), query(t, "space1"))
	require.ErrorIs(t, err, fetch.ErrNotAvailable)
	require.False(t, s.isBanned("d1", false), "someone else's cancel says nothing about the peer")

	pool = &dialErrPool{err: context.DeadlineExceeded}
	s = newTestSource(pool, lanOnlyPeers{lan: []string{"d1"}})
	_, _, err = s.SourceFor(context.Background(), query(t, "space1"))
	require.ErrorIs(t, err, fetch.ErrNotAvailable)
	require.True(t, s.isBanned("d1", false), "the dial's own timeout means the peer is unreachable")
}

// dialErrPool fails the dial at once with a fixed error, the caller's
// context untouched.
type dialErrPool struct {
	hangingDialPool
	err error
}

func (p *dialErrPool) Get(context.Context, string) (peer.Peer, error) { return nil, p.err }

// Each layer has its own clock: a LAN layer that exhausts its budget on
// stale ids must not consume the global layer's, or the relayed peer
// holding the file is never consulted.
func TestSourceLayersDoNotStarveEachOther(t *testing.T) {
	live := stubPeer{id: "g1"}
	pool := &hangingDialPool{connected: map[string]peer.Peer{"g1": live}}
	var lan []string
	for i := 0; i < 12; i++ {
		lan = append(lan, "stale"+string(rune('a'+i)))
	}
	s := newTestSource(pool, bothLayerPeers{lan: lan, global: []string{"g1"}})

	_, _, err := s.SourceFor(context.Background(), query(t, "space1"))
	require.ErrorIs(t, err, fetch.ErrNotAvailable, "the stub reports nothing held")
	require.Less(t, pool.gets, len(lan), "the LAN clock bounds the stale dials")
	require.Positive(t, pool.picks, "the global layer must still be reached")
}

type bothLayerPeers struct{ lan, global []string }

func (b bothLayerPeers) LocalPeerIds(string) []string  { return b.lan }
func (b bothLayerPeers) GlobalPeerIds(string) []string { return b.global }

// maxCandidates counts peers that were ASKED: with twenty connected
// peers answering nothing useful, the sweep stops at maxCandidates.
func TestSourceGlobalLayerAsksAtMostMaxCandidates(t *testing.T) {
	connected := map[string]peer.Peer{}
	var global []string
	for i := 0; i < 20; i++ {
		id := "g" + string(rune('a'+i))
		global = append(global, id)
		connected[id] = stubPeer{id: id}
	}
	pool := &hangingDialPool{connected: connected}
	s := newTestSource(pool, globalOnlyPeers{global: global})

	_, _, err := s.SourceFor(context.Background(), query(t, "space1"))
	require.ErrorIs(t, err, fetch.ErrNotAvailable)
	require.Equal(t, maxCandidates, pool.picks, "twenty answering peers must not all be asked")
}

// The global ranking is by last seen, not connectivity, so offline
// devices can sit ahead of the one that holds the file. Skipping them
// is cheap and must not consume the candidate count, or the holder is
// never asked.
func TestSourceGlobalOfflineIdsDoNotHideTheHolder(t *testing.T) {
	pool := &hangingDialPool{connected: map[string]peer.Peer{"g4": holderPeer{id: "g4"}}}
	s := newTestSource(pool, globalOnlyPeers{global: []string{"g1", "g2", "g3", "g4"}})

	src, _, err := s.SourceFor(context.Background(), query(t, "space1"))
	require.NoError(t, err, "the connected holder ranked behind three offline ids must be asked")
	require.Equal(t, "g4", src.(*peerCar).peerId)
	for _, id := range []string{"g1", "g2", "g3"} {
		require.False(t, s.isBanned(id, true), "an offline id is skipped with no verdict")
	}
}

func testCid(t *testing.T) cid.Cid {
	t.Helper()
	root, err := cid.Decode("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy")
	require.NoError(t, err)
	return root
}

// One failed FileCheck must not hide the only holder for long: inside
// the backoff the peer is skipped without a probe, and right after it
// the peer is asked again and selected.
func TestSourceAsksAFailedPeerAgainAfterItsBackoff(t *testing.T) {
	pool := &hangingDialPool{connected: map[string]peer.Peer{"g1": stubPeer{id: "g1"}}}
	s := newTestSource(pool, globalOnlyPeers{global: []string{"g1"}})
	s.b.backoffMin, s.b.backoffMax = 40*time.Millisecond, time.Second

	_, _, err := s.SourceFor(context.Background(), query(t, "space1"))
	require.ErrorIs(t, err, fetch.ErrNotAvailable)
	require.ErrorContains(t, err, "1 peer(s) that may hold it did not answer",
		"the failure names the holder that did not answer")
	require.Equal(t, 1, pool.picks)

	_, _, err = s.SourceFor(context.Background(), query(t, "space1"))
	require.ErrorContains(t, err, "1 peer(s) that may hold it did not answer")
	require.Equal(t, 1, pool.picks, "a burst of fetches probes the failed peer once")

	// The peer is back.
	pool.connected["g1"] = holderPeer{id: "g1"}
	time.Sleep(2 * s.b.backoffMin)
	src, _, err := s.SourceFor(context.Background(), query(t, "space1"))
	require.NoError(t, err, "past the backoff the holder is asked again")
	require.Equal(t, "g1", src.(*peerCar).peerId)
	s.banMu.Lock()
	n := len(s.banned)
	s.banMu.Unlock()
	require.Zero(t, n, "an answer clears the backoff")
}

// expire ends a peer's current wait, as time passing would.
func (s *Source) expire(peerId string, global bool) {
	s.banMu.Lock()
	defer s.banMu.Unlock()
	e := s.banned[banKey{peerId, global}]
	e.until = time.Now().Add(-time.Millisecond)
	s.banned[banKey{peerId, global}] = e
}

func (s *Source) waitOf(peerId string, global bool) time.Duration {
	s.banMu.Lock()
	defer s.banMu.Unlock()
	return time.Until(s.banned[banKey{peerId, global}].until)
}

// The backoff doubles per consecutive failure up to its cap, and an
// answer starts it over.
func TestSourceBackoffGrowsAndResets(t *testing.T) {
	s := newTestSource(&hangingDialPool{}, globalOnlyPeers{})
	for i, want := range []time.Duration{backoffMin, 2 * backoffMin, 4 * backoffMin, 8 * backoffMin, backoffMax, backoffMax} {
		s.unreachable("p", true)
		require.InDelta(t, want, s.waitOf("p", true), float64(time.Second), "failure %d", i+1)
		s.expire("p", true)
	}
	require.LessOrEqual(t, 8*backoffMin, backoffMax, "the table above assumes four doublings fit under the cap")

	s.answered("p", true)
	s.unreachable("p", true)
	require.InDelta(t, backoffMin, s.waitOf("p", true), float64(time.Second), "an answer starts the backoff over")
}

// Fetches that ask one peer together lose the same round trip: the
// burst counts as one failure, not as one per fetch.
func TestSourceBurstOfFailuresCountsOnce(t *testing.T) {
	pool := &hangingDialPool{connected: map[string]peer.Peer{"g1": stubPeer{id: "g1"}}}
	s := newTestSource(pool, globalOnlyPeers{global: []string{"g1"}})
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = s.SourceFor(context.Background(), query(t, "space1"))
		}()
	}
	wg.Wait()
	require.InDelta(t, backoffMin, s.waitOf("g1", true), float64(time.Second))
}

// A peer that served invalid content stays out for banTTL: neither a
// failed check nor an answered one in the meantime lets it back in.
func TestSourceBanSurvivesChecks(t *testing.T) {
	s := newTestSource(&hangingDialPool{}, globalOnlyPeers{})
	s.ban("p", true)
	s.unreachable("p", true)
	s.answered("p", true)
	require.True(t, s.isBanned("p", true))
	require.InDelta(t, banTTL, s.waitOf("p", true), float64(time.Second))

	// Also when the ban followed a backoff.
	s.unreachable("q", true)
	s.ban("q", true)
	s.answered("q", true)
	require.InDelta(t, banTTL, s.waitOf("q", true), float64(time.Second))
}

// With no candidate waiting out a backoff, the failure says that nobody
// reachable holds the file.
func TestSourceWithoutCandidatesSaysNobodyHoldsIt(t *testing.T) {
	s := newTestSource(&hangingDialPool{}, globalOnlyPeers{global: []string{"g1"}})
	_, _, err := s.SourceFor(context.Background(), query(t, "space1"))
	require.ErrorIs(t, err, fetch.ErrNotAvailable)
	require.ErrorContains(t, err, "no reachable peer holds it")
}

// fakeDialer connects the next scripted peer on each call, as the
// connector would, and records what it was asked. A nil entry is a
// dial that found nobody home.
type fakeDialer struct {
	pool    *hangingDialPool
	connect []peer.Peer
	calls   []dialCall
}

type dialCall struct {
	spaceId, prefer string
	skip            []string
}

func (d *fakeDialer) ConnectOne(_ context.Context, spaceId, prefer string, skip []string) (string, bool) {
	d.calls = append(d.calls, dialCall{spaceId, prefer, append([]string(nil), skip...)})
	if len(d.connect) == 0 {
		return "", false
	}
	p := d.connect[0]
	d.connect = d.connect[1:]
	if p == nil {
		return "nobody-home", false
	}
	d.pool.mu.Lock()
	d.pool.connected[p.Id()] = p
	d.pool.mu.Unlock()
	return p.Id(), true
}

func soleQuery(t *testing.T, author string) fetch.PeerQuery {
	t.Helper()
	return fetch.PeerQuery{SpaceId: "space1", Root: testCid(t), Author: author, Sole: true}
}

// A file nothing else can serve, held by a peer that is not connected:
// the source has the connector dial for it — the peers it already asked
// excluded, the author named — and selects the holder that comes up.
// The source itself still never dials a global peer.
func TestSourceDialsAnUnconnectedHolder(t *testing.T) {
	pool := &hangingDialPool{connected: map[string]peer.Peer{"g1": stubPeer{id: "g1"}}}
	s := newTestSource(pool, globalOnlyPeers{global: []string{"g1", "g2", "g3"}})
	dialer := &fakeDialer{pool: pool, connect: []peer.Peer{notFullPeer{id: "g2"}, holderPeer{id: "g3"}}}
	s.dialer = dialer

	src, ban, err := s.SourceFor(context.Background(), soleQuery(t, "author"))
	require.NoError(t, err)
	require.Equal(t, "g3", src.(*peerCar).peerId)
	require.True(t, src.(*peerCar).global)
	require.Equal(t, []dialCall{
		{"space1", "author", []string{"g1"}},
		{"space1", "author", []string{"g1", "g2"}},
	}, dialer.calls)
	require.Zero(t, pool.gets, "pool.Get never reaches a global peer")

	ban()
	require.True(t, s.isBanned("g3", true), "the ban hook covers a dialed holder")
}

// With a public object to fall back on, nothing is dialed: global peers
// are used only while connected.
func TestSourceDoesNotDialWhenAnotherSourceExists(t *testing.T) {
	pool := &hangingDialPool{connected: map[string]peer.Peer{}}
	s := newTestSource(pool, globalOnlyPeers{global: []string{"g1"}})
	dialer := &fakeDialer{pool: pool, connect: []peer.Peer{holderPeer{id: "g1"}}}
	s.dialer = dialer

	_, _, err := s.SourceFor(context.Background(), query(t, "space1"))
	require.ErrorIs(t, err, fetch.ErrNotAvailable)
	require.Empty(t, dialer.calls)
}

// One fetch has at most maxDemandDials peers brought up, a dial in vain
// included, and a peer sitting out a backoff is not handed back by the
// dialer.
func TestSourceBoundsItsDemandDials(t *testing.T) {
	pool := &hangingDialPool{connected: map[string]peer.Peer{}}
	global := []string{"down"}
	connect := []peer.Peer{nil}
	for i := 0; i < maxDemandDials+2; i++ {
		id := "g" + string(rune('a'+i))
		global = append(global, id)
		connect = append(connect, notFullPeer{id: id})
	}
	s := newTestSource(pool, globalOnlyPeers{global: global})
	s.unreachable("down", true)
	dialer := &fakeDialer{pool: pool, connect: connect}
	s.dialer = dialer

	_, _, err := s.SourceFor(context.Background(), soleQuery(t, ""))
	require.ErrorIs(t, err, fetch.ErrNotAvailable)
	require.Len(t, dialer.calls, maxDemandDials)
	for _, call := range dialer.calls {
		require.Contains(t, call.skip, "down")
	}
	require.Contains(t, dialer.calls[1].skip, "nobody-home", "a peer dialed in vain is not asked for again")
}

// notFullPeer answers FileCheck with nothing held.
type notFullPeer struct {
	peer.Peer
	id string
}

func (n notFullPeer) Id() string { return n.id }
func (n notFullPeer) DoDrpc(_ context.Context, do func(conn drpc.Conn) error) error {
	return do(emptyConn{})
}

type emptyConn struct{ drpc.Conn }

func (emptyConn) Invoke(context.Context, string, drpc.Encoding, drpc.Message, drpc.Message) error {
	return nil
}

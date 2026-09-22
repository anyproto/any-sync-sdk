package filep2p

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/anyproto/any-sync/commonfile/fileproto/filep2p"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/ipfs/go-cid"
	"storj.io/drpc"

	"github.com/anyproto/any-sync-sdk/internal/files/fetch"
	"github.com/anyproto/any-sync-sdk/internal/p2p"
)

const (
	// probeLen mirrors the fetch package's head probe: enough for the
	// CARv2 pragma + header + root, and the response's totalSize gives
	// the object size for the index-tail read.
	probeLen = 4096
	// perPeerTimeout bounds EACH LAN candidate's dial+FileCheck during
	// selection — a per-peer budget, NOT a shared one, so a single slow
	// peer can't exhaust the sweep and cause healthy peers behind it to
	// time out and be banned. A LAN round trip is sub-millisecond, so
	// this is already generous there.
	perPeerTimeout = 700 * time.Millisecond
	// perGlobalPeerTimeout is the budget for a global candidate, whose
	// FileCheck crosses the internet and usually a relay. The LAN
	// figure sits at the edge of a relayed round trip, so the check
	// times out and a peer holding the file in full is never asked.
	// What this bounds is the FileCheck RPC: the pick itself is
	// already capped at p2p.PickTimeout.
	perGlobalPeerTimeout = 5 * time.Second
	// maxLanSweep / maxGlobalSweep bound each layer's whole sweep. Per
	// layer, not shared: a shared clock lets stale LAN ids spend it all
	// before any relayed peer is consulted.
	maxLanSweep    = 3 * time.Second
	maxGlobalSweep = 8 * time.Second
	// globalRoundTrip is the fixed part of a global read's budget: one
	// relayed round trip, like the FileCheck that selected the peer, so
	// it cannot be smaller than the budget selection passed under or a
	// peer admitted at that figure would fail its first read.
	globalRoundTrip = perGlobalPeerTimeout
	// globalMinRate / lanMinRate are the transfer rates a read on each
	// layer is expected to sustain; the budget grows by length/rate on
	// top of the round trip, so a 4 KiB probe and a multi-MiB range are
	// each sized for what they move. Below the rate the peer is treated
	// as stalled, and with an HTTP fallback that bans it, so each floor
	// sits well under what its medium delivers: a relayed path runs at
	// a few MB/s, a congested Wi-Fi still tens of Mbit/s.
	globalMinRate = 1 << 20
	lanMinRate    = 2 << 20
	// maxCandidates caps how many peers of ONE layer are ASKED (a
	// FileCheck was attempted on a resolved peer) before giving up. A
	// candidate that never answers a dial or has no live connection
	// does not count: the layer clock already bounds those, and
	// counting them lets a few offline ids ranked ahead hide the one
	// connected holder.
	maxCandidates = 3
	// banTTL keeps a peer that failed to dial/serve out of selection.
	banTTL = 5 * time.Minute
)

// budgets are the selection clocks; a field so tests run them in
// milliseconds.
type budgets struct {
	lanPeer, globalPeer   time.Duration
	lanSweep, globalSweep time.Duration
}

var defaultBudgets = budgets{
	lanPeer:     perPeerTimeout,
	globalPeer:  perGlobalPeerTimeout,
	lanSweep:    maxLanSweep,
	globalSweep: maxGlobalSweep,
}

// LocalPeers is the slice of the p2p peer store this source needs: LAN
// peers are dialed (bounded), global peers only used while connected.
type LocalPeers interface {
	LocalPeerIds(spaceId string) []string
	GlobalPeerIds(spaceId string) []string
}

// DialPool is the slice of the any-sync pool this source needs. Get
// dials LAN peers; Pick is the only call ever made for a global peer.
type DialPool interface {
	Get(ctx context.Context, id string) (peer.Peer, error)
	Pick(ctx context.Context, id string) (peer.Peer, error)
}

// Source is the fetch.PeerSource: it finds a peer that holds a file in
// full — on the LAN first, then among the connected global peers — and
// hands back a Car reader over the FileP2P ObjectRead RPC. Each layer
// sweeps under its own clock and per-candidate budget, and failures ban
// the peer on that layer, so selection never stalls a fetch when no
// peer has the file.
type Source struct {
	pool  DialPool
	peers LocalPeers
	b     budgets

	banMu  sync.Mutex
	banned map[banKey]time.Time
}

func NewSource(pool DialPool, peers LocalPeers) *Source {
	return &Source{pool: pool, peers: peers, b: defaultBudgets, banned: map[banKey]time.Time{}}
}

var (
	_ fetch.PeerSource       = (*Source)(nil)
	_ fetch.PeerReadBudgeter = (*peerCar)(nil)
)

// SourceFor returns a peer-backed Car reader for (spaceId, root) when a
// non-banned peer reports holding the file in full, plus a hook to ban
// that peer (called by the fetcher if it serves invalid bytes).
// ok=false when no peer holds the file.
//
// Each layer gets its own try count AND its own clock. Sharing either
// starves the global layer: LAN ids come first and a handful of stale
// ones exhaust a shared budget before any relayed peer is asked.
func (s *Source) SourceFor(ctx context.Context, spaceId string, root cid.Cid) (fetch.CarSource, func(), bool) {
	for _, layer := range []struct {
		ids    func() []string
		global bool
		sweep  time.Duration
	}{
		{ids: func() []string { return s.filterBanned(s.peers.LocalPeerIds(spaceId), false) }, sweep: s.b.lanSweep},
		// Built lazily: resolving the global set unions in every
		// account peer and ranks them, all wasted when a LAN peer
		// answers first.
		{ids: func() []string { return s.filterBanned(s.peers.GlobalPeerIds(spaceId), true) }, global: true, sweep: s.b.globalSweep},
	} {
		lctx, cancel := context.WithTimeout(ctx, layer.sweep)
		src, ban, ok := s.sweepLayer(lctx, layer.ids(), layer.global, spaceId, root)
		cancel()
		if ok {
			return src, ban, true
		}
	}
	return nil, nil, false
}

// sweepLayer asks up to maxCandidates peers of one layer, under lctx,
// this layer's clock. Candidates that are never reached — a dead LAN
// dial, a global id with no live connection — are skipped without
// counting: the clock bounds them, and the global ranking is by last
// seen, not connectivity, so a connected holder may sit behind several
// offline ids.
func (s *Source) sweepLayer(lctx context.Context, ids []string, global bool, spaceId string, root cid.Cid) (fetch.CarSource, func(), bool) {
	asked := 0
	for _, id := range ids {
		if asked >= maxCandidates || lctx.Err() != nil {
			break
		}
		reached, full := s.holdsFull(lctx, id, global, spaceId, root)
		if reached {
			asked++
		}
		if full {
			peerId := id
			car := &peerCar{pool: s.pool, peerId: peerId, global: global, spaceId: spaceId, root: root}
			return car, func() { s.ban(peerId, global) }, true
		}
	}
	return nil, nil, false
}

// holdsFull reaches one peer (under its own budget) and asks whether it
// holds the file in full. reached reports that the peer was resolved
// and a FileCheck attempted on it. LAN peers are dialed; a global peer
// is only picked from the pool and skipped, not banned, when it has no
// live connection. A dial/RPC failure bans the peer — it is unreachable
// — but a plain "not full" answer does not, and neither does the layer
// clock (or the caller) ending first: that cut-off says nothing about
// the peer, and banTTL is long enough to hide the only device holding
// the file. Nor does a dial error carrying ANOTHER caller's cancel: the
// pool shares an in-flight dial's outcome with every waiter (retrying a
// few times, but the last verdict can still be that cancel), so a fetch
// aborted elsewhere must not ban a healthy LAN holder. A deadline error
// with pctx alive is NOT inherited: it is the dial's own timeout, and
// the peer is unreachable.
func (s *Source) holdsFull(lctx context.Context, peerId string, global bool, spaceId string, root cid.Cid) (reached, full bool) {
	budget := s.b.lanPeer
	if global {
		budget = s.b.globalPeer
	}
	pctx, cancel := context.WithTimeout(lctx, budget)
	defer cancel()
	p, err := s.peer(pctx, peerId, global)
	if err != nil {
		inherited := pctx.Err() == nil && errors.Is(err, context.Canceled)
		if !global && lctx.Err() == nil && !inherited {
			s.ban(peerId, global)
		}
		return false, false
	}
	err = p.DoDrpc(pctx, func(conn drpc.Conn) error {
		resp, derr := filep2p.NewDRPCFileP2PClient(conn).FileCheck(pctx, &filep2p.FileCheckRequest{
			SpaceId:  spaceId,
			RootCids: [][]byte{root.Bytes()},
		})
		if derr != nil {
			return derr
		}
		for _, f := range resp.Files {
			if bytes.Equal(f.RootCid, root.Bytes()) && f.Have == filep2p.Availability_Full {
				full = true
			}
		}
		return nil
	})
	if err != nil {
		if lctx.Err() == nil {
			s.ban(peerId, global)
		}
		return true, false
	}
	return true, full
}

// peer resolves a candidate: dial for LAN, live connection only for
// global.
func (s *Source) peer(ctx context.Context, peerId string, global bool) (peer.Peer, error) {
	if global {
		return p2p.PickLive(ctx, s.pool, peerId)
	}
	return s.pool.Get(ctx, peerId)
}

// ban sidelines a peer ON ONE LAYER. A device reachable both on the
// LAN and through a relay is one peer id with two very different
// paths and two very different budgets, so a LAN failure — client
// isolation on the Wi-Fi, say — must not also remove the relayed path
// that works.
func (s *Source) ban(peerId string, global bool) {
	s.banMu.Lock()
	s.banned[banKey{peerId, global}] = time.Now().Add(banTTL)
	s.banMu.Unlock()
}

// banKey is a peer on one layer.
type banKey struct {
	peerId string
	global bool
}

func (s *Source) filterBanned(ids []string, global bool) []string {
	s.banMu.Lock()
	defer s.banMu.Unlock()
	now := time.Now()
	// Drop every expired ban, not only the ones whose id is in this
	// list: a peer that left the network is never handed back, so a
	// lazy per-id cleanup would keep its entry for the process life.
	for k, until := range s.banned {
		if !now.Before(until) {
			delete(s.banned, k)
		}
	}
	out := ids[:0:0]
	for _, id := range ids {
		if _, banned := s.banned[banKey{id, global}]; banned {
			continue
		}
		out = append(out, id)
	}
	return out
}

// peerCar reads a file's CAR object from one peer, LAN or global, via
// ObjectRead. Implements fetch.CarSource so seed and the block fetcher
// use it exactly like the HTTP source. It does NOT ban on its own: the fetcher decides
// (via the ban hook SourceFor returned) whether a failure was invalid
// content — ban — or merely transient — demote for this fetch only.
type peerCar struct {
	pool   DialPool
	peerId string
	// global peers are picked, never dialed.
	global  bool
	spaceId string
	root    cid.Cid
}

func (c *peerCar) read(ctx context.Context, off, length int64) (data []byte, total int64, err error) {
	var p peer.Peer
	if c.global {
		p, err = p2p.PickLive(ctx, c.pool, c.peerId)
	} else {
		p, err = c.pool.Get(ctx, c.peerId)
	}
	if err != nil {
		return nil, 0, err
	}
	err = p.DoDrpc(ctx, func(conn drpc.Conn) error {
		resp, rerr := filep2p.NewDRPCFileP2PClient(conn).ObjectRead(ctx, &filep2p.ObjectReadRequest{
			SpaceId: c.spaceId,
			RootCid: c.root.Bytes(),
			Offset:  uint64(off),
			Length:  uint32(length),
		})
		if rerr != nil {
			return rerr
		}
		data = resp.Data
		total = int64(resp.TotalSize)
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return data, total, nil
}

// PeerReadBudget implements fetch.PeerReadBudgeter: a global peer's
// read crosses the internet; a LAN peer's keeps the package round trip
// but still earns time per byte, or a coalesced multi-MiB range would
// have to land inside one LAN round trip.
func (c *peerCar) PeerReadBudget() (time.Duration, int64) {
	if c.global {
		return globalRoundTrip, globalMinRate
	}
	return 0, lanMinRate
}

func (c *peerCar) ReadRange(ctx context.Context, off, length int64) ([]byte, error) {
	data, _, err := c.read(ctx, off, length)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != length {
		// A mid-object short read is a protocol error (the caller knows
		// the geometry) — surface it so the ladder demotes to HTTP.
		return nil, fmt.Errorf("filep2p: range [%d,%d): got %d bytes", off, off+length, len(data))
	}
	return data, nil
}

func (c *peerCar) ReadProbe(ctx context.Context) (head []byte, total int64, err error) {
	return c.read(ctx, 0, probeLen)
}

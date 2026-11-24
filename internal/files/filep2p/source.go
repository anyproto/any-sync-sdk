package filep2p

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/anyproto/any-sync/commonfile/fileproto/filep2p"
	"github.com/anyproto/any-sync/commonfile/fileproto/filep2p/filep2perr"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/rpc/rpcerr"
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
	// banTTL keeps a peer that served invalid content, or stalled a read
	// that has a fallback, out of selection.
	banTTL = 5 * time.Minute
	// maxDemandDials caps the peers one fetch has the connector bring
	// up, dialed or not, for a file nothing else can serve.
	maxDemandDials = 3
	// backoffMin / backoffMax bound how long a peer that failed a dial or
	// a FileCheck is skipped; the wait doubles per consecutive failure. A
	// burst of fetches probes a dead peer once, and the only holder of a
	// file is asked again seconds after one lost round trip.
	backoffMin = 5 * time.Second
	backoffMax = time.Minute
)

// budgets are the selection clocks; a field so tests run them in
// milliseconds.
type budgets struct {
	lanPeer, globalPeer   time.Duration
	lanSweep, globalSweep time.Duration
	ban                   time.Duration
	backoffMin            time.Duration
	backoffMax            time.Duration
}

var defaultBudgets = budgets{
	lanPeer:     perPeerTimeout,
	globalPeer:  perGlobalPeerTimeout,
	lanSweep:    maxLanSweep,
	globalSweep: maxGlobalSweep,
	ban:         banTTL,
	backoffMin:  backoffMin,
	backoffMax:  backoffMax,
}

// LocalPeers is the slice of the p2p peer store this source needs: LAN
// peers are dialed (bounded), global peers used while connected.
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

// Dialer brings up one more global peer of a space on request, with at
// most one dial: a peer outside skip, devices of the prefer identity
// first. connected reports a live peer; a peer id with connected false
// was dialed in vain, and no peer id means nothing was dialed. The
// global layer's connector implements it; it stays the only dialer.
type Dialer interface {
	ConnectOne(ctx context.Context, spaceId, prefer string, skip []string) (peerId string, connected bool)
}

// Source is the fetch.PeerSource: it finds a peer that holds a file in
// full — on the LAN first, then among the connected global peers, then,
// for a file nothing else can serve, among global peers it has the
// connector dial — and hands back a Car reader over the FileP2P
// ObjectRead RPC. Each layer sweeps under its own clock and
// per-candidate budget, and a peer that fails is skipped on that layer
// for a while, so selection never stalls a fetch when no peer has the
// file.
type Source struct {
	pool   DialPool
	peers  LocalPeers
	dialer Dialer
	b      budgets

	banMu  sync.Mutex
	banned map[banKey]exclusion
}

// NewSource builds the source. dialer is nil when the global layer is
// off: global peers are then used only while connected.
func NewSource(pool DialPool, peers LocalPeers, dialer Dialer) *Source {
	return &Source{pool: pool, peers: peers, dialer: dialer, b: defaultBudgets, banned: map[banKey]exclusion{}}
}

var (
	_ fetch.PeerSource       = (*Source)(nil)
	_ fetch.PeerReadBudgeter = (*peerCar)(nil)
)

// SourceFor returns a peer-backed Car reader for the queried file when
// a selectable peer reports holding it in full, plus a hook to ban that
// peer (called by the fetcher if it serves invalid bytes). The error
// wraps fetch.ErrNotAvailable and says whether peers that may hold the
// file are only waiting out a backoff.
//
// Each layer gets its own try count AND its own clock. Sharing either
// starves the global layer: LAN ids come first and a handful of stale
// ones exhaust a shared budget before any relayed peer is asked.
func (s *Source) SourceFor(ctx context.Context, q fetch.PeerQuery) (fetch.CarSource, func(), error) {
	layers := []struct {
		ids    func() []string
		global bool
		sweep  time.Duration
	}{
		{ids: func() []string { return s.peers.LocalPeerIds(q.SpaceId) }, sweep: s.b.lanSweep},
		// Built lazily: resolving the global set unions in every
		// account peer and ranks them, all wasted when a LAN peer
		// answers first.
		{ids: func() []string { return s.peers.GlobalPeerIds(q.SpaceId) }, global: true, sweep: s.b.globalSweep},
	}
	var candidates []banKey
	for _, layer := range layers {
		ids := layer.ids()
		for _, id := range ids {
			candidates = append(candidates, banKey{id, layer.global})
		}
		selectable := s.filterBanned(ids, layer.global)
		lctx, cancel := context.WithTimeout(ctx, layer.sweep)
		car, asked := s.sweepLayer(lctx, selectable, layer.global, q)
		if car == nil && layer.global {
			// asked peers and the ones sitting out a backoff are not
			// handed back by the dialer
			car = s.dialHolder(lctx, q, append(asked, excluded(ids, selectable)...))
		}
		cancel()
		if car != nil {
			return car, func() { s.ban(car.peerId, car.global) }, nil
		}
	}
	return nil, nil, s.unavailable(candidates)
}

// excluded returns the ids filterBanned dropped.
func excluded(ids, selectable []string) []string {
	var out []string
	for _, id := range ids {
		if !slices.Contains(selectable, id) {
			out = append(out, id)
		}
	}
	return out
}

// dialHolder has the connector bring up global peers that are not
// connected, one at a time, and asks each for the file — only for a
// file nothing else can serve, and inside the global layer's clock. A
// dial still in flight when the clock ends completes on its own, so the
// next fetch finds that peer connected.
func (s *Source) dialHolder(lctx context.Context, q fetch.PeerQuery, skip []string) *peerCar {
	if !q.Sole || s.dialer == nil {
		return nil
	}
	for range maxDemandDials {
		if lctx.Err() != nil {
			return nil
		}
		id, connected := s.dialer.ConnectOne(lctx, q.SpaceId, q.Author, skip)
		if id == "" {
			return nil
		}
		skip = append(skip, id)
		if !connected {
			continue
		}
		if _, full := s.holdsFull(lctx, id, true, q.SpaceId, q.Root); full {
			return &peerCar{pool: s.pool, peerId: id, global: true, spaceId: q.SpaceId, root: q.Root}
		}
	}
	return nil
}

// unavailable is the selection's failure: a plain "nobody holds it", or
// a count of the candidates that failed to answer and are skipped until
// their backoff ends.
func (s *Source) unavailable(candidates []banKey) error {
	now := time.Now()
	var (
		waiting int
		next    time.Time
	)
	s.banMu.Lock()
	for _, k := range candidates {
		e, ok := s.banned[k]
		if !ok || e.banned || !now.Before(e.until) {
			continue
		}
		waiting++
		if next.IsZero() || e.until.Before(next) {
			next = e.until
		}
	}
	s.banMu.Unlock()
	if waiting == 0 {
		return fmt.Errorf("%w: no reachable peer holds it", fetch.ErrNotAvailable)
	}
	// rounded up: "0s" would read as "now"
	wait := (next.Sub(now) + time.Second - 1).Truncate(time.Second)
	return fmt.Errorf("%w: %d peer(s) that may hold it did not answer and are asked again in %s",
		fetch.ErrNotAvailable, waiting, wait)
}

// sweepLayer asks up to maxCandidates peers of one layer, under lctx,
// this layer's clock, and returns the holder's reader (nil when none)
// with the peers it asked. Candidates that are never reached — a dead
// LAN dial, a global id with no live connection — are skipped without
// counting: the clock bounds them, and the global ranking is by last
// seen, not connectivity, so a connected holder may sit behind several
// offline ids.
func (s *Source) sweepLayer(lctx context.Context, ids []string, global bool, q fetch.PeerQuery) (car *peerCar, asked []string) {
	for _, id := range ids {
		if len(asked) >= maxCandidates || lctx.Err() != nil {
			break
		}
		reached, full := s.holdsFull(lctx, id, global, q.SpaceId, q.Root)
		if reached {
			asked = append(asked, id)
		}
		if full {
			return &peerCar{pool: s.pool, peerId: id, global: global, spaceId: q.SpaceId, root: q.Root}, asked
		}
	}
	return nil, asked
}

// holdsFull reaches one peer (under its own budget) and asks whether it
// holds the file in full. reached reports that the peer was resolved
// and a FileCheck attempted on it. LAN peers are dialed; a global peer
// is only picked from the pool and skipped, with no verdict, when it has
// no live connection. A dial/RPC failure backs the peer off — it is
// unreachable — and any answer clears that, a refusal included. The
// layer clock (or the
// caller) ending first is no verdict: that cut-off says nothing about
// the peer. Nor is a dial error carrying ANOTHER caller's cancel: the
// pool shares an in-flight dial's outcome with every waiter (retrying a
// few times, but the last verdict can still be that cancel), so a fetch
// aborted elsewhere must not sideline a healthy LAN holder. A deadline
// error with pctx alive is NOT inherited: it is the dial's own timeout,
// and the peer is unreachable.
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
			s.unreachable(peerId, global)
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
		switch {
		case lctx.Err() != nil:
		case refused(err):
			// The peer is there. It turns the check down until it lists
			// this device with the space, which may be a moment away.
			s.answered(peerId, global)
		default:
			s.unreachable(peerId, global)
		}
		return true, false
	}
	s.answered(peerId, global)
	return true, full
}

// refused reports a check the peer's handler turned down: an answer
// from a live peer, not a failure to reach it.
func refused(err error) bool {
	switch rpcerr.Unwrap(err) {
	case filep2perr.ErrForbidden, filep2perr.ErrInvalidRequest, filep2perr.ErrFileNotFound, filep2perr.ErrUnexpected:
		return true
	}
	return false
}

// peer resolves a candidate: dial for LAN, live connection only for
// global.
func (s *Source) peer(ctx context.Context, peerId string, global bool) (peer.Peer, error) {
	if global {
		return p2p.PickLive(ctx, s.pool, peerId)
	}
	return s.pool.Get(ctx, peerId)
}

// ban sidelines a peer ON ONE LAYER for banTTL: it served invalid
// content, or stalled a read another source can serve. A device
// reachable both on the LAN and through a relay is one peer id with two
// very different paths and two very different budgets, so a LAN failure
// — client isolation on the Wi-Fi, say — must not also remove the
// relayed path that works.
func (s *Source) ban(peerId string, global bool) {
	s.banMu.Lock()
	s.banned[banKey{peerId, global}] = exclusion{until: time.Now().Add(s.b.ban), banned: true}
	s.banMu.Unlock()
}

// unreachable backs a peer off on one layer after a failed dial or
// FileCheck: backoffMin, doubled per consecutive failure, up to
// backoffMax. A failure seen while the peer is already skipped adds
// nothing: fetches that asked it together lost the same round trip,
// and a ban is not shortened into a backoff.
func (s *Source) unreachable(peerId string, global bool) {
	k := banKey{peerId, global}
	now := time.Now()
	s.banMu.Lock()
	defer s.banMu.Unlock()
	e := s.banned[k]
	if now.Before(e.until) {
		return
	}
	e.failures++
	e.banned = false
	wait := s.b.backoffMax
	// the shift is bounded so a long outage cannot overflow it
	if shift := e.failures - 1; shift < 16 {
		wait = min(s.b.backoffMin<<shift, s.b.backoffMax)
	}
	e.until = now.Add(wait)
	s.banned[k] = e
}

// answered clears a peer's backoff: it replied to a FileCheck. A ban
// for invalid content stays, also one that followed a backoff.
func (s *Source) answered(peerId string, global bool) {
	k := banKey{peerId, global}
	s.banMu.Lock()
	if e, ok := s.banned[k]; ok && !e.banned {
		delete(s.banned, k)
	}
	s.banMu.Unlock()
}

// banKey is a peer on one layer.
type banKey struct {
	peerId string
	global bool
}

// exclusion keeps a peer out of one layer's selection until `until`:
// a ban, or a backoff after `failures` consecutive unreachable
// verdicts.
type exclusion struct {
	until    time.Time
	failures int
	banned   bool
}

func (s *Source) filterBanned(ids []string, global bool) []string {
	s.banMu.Lock()
	defer s.banMu.Unlock()
	now := time.Now()
	// Drop every stale entry, not only the ones whose id is in this
	// list: a peer that left the network is never handed back, so a
	// lazy per-id cleanup would keep its entry for the process life. A
	// backoff outlives its wait by backoffMax, so that a peer failing
	// again right after it keeps climbing instead of starting over.
	for k, e := range s.banned {
		end := e.until
		if !e.banned {
			end = end.Add(s.b.backoffMax)
		}
		if !now.Before(end) {
			delete(s.banned, k)
		}
	}
	out := ids[:0:0]
	for _, id := range ids {
		if e, ok := s.banned[banKey{id, global}]; ok && now.Before(e.until) {
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

package filep2p

import (
	"bytes"
	"context"
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
	// perGlobalPeerTimeout is the same budget for a global candidate,
	// whose FileCheck crosses the internet and usually a relay. The LAN
	// figure sits right at the edge of a relayed round trip: the check
	// times out, the file is reported unavailable, and a peer that holds
	// it in full is never asked — so files simply do not transfer over
	// the global layer, intermittently and with no error naming the
	// cause. Kept under the connector's own global dial timeout, since a
	// global candidate is picked from the pool rather than dialed.
	perGlobalPeerTimeout = 5 * time.Second
	// maxSweep bounds the WHOLE selection sweep, not just each
	// candidate. The per-candidate budgets multiply by the number of
	// candidates, and a fetch that has an HTTP fallback must reach it
	// in reasonable time rather than spending a minute discovering
	// that every peer is stalled.
	maxSweep = 8 * time.Second
	// globalReadDeadline is the budget for ONE range read from a global
	// peer, replacing the fetch package's LAN default. The fetcher
	// coalesces at most maxFetchSpan (4 MiB) per read and a relayed
	// path runs at a few MB/s, so the LAN figure expires mid-range;
	// with no durable copy to fall back on, that expiry fails the whole
	// fetch.
	globalReadDeadline = 10 * time.Second
	// globalProbeDeadline is the budget for the 4 KiB head probe, which
	// is one round trip rather than a transfer. Giving it the range
	// budget would make a silently stalled peer cost globalReadDeadline
	// before the ladder demotes to HTTP.
	globalProbeDeadline = 2 * time.Second
	// maxCandidates caps how many peers we FileCheck before giving up
	// and using HTTP — keeps selection bounded on a busy LAN, and bounds
	// the worst case once global candidates carry the larger budget.
	maxCandidates = 3
	// banTTL keeps a peer that failed to dial/serve out of selection.
	banTTL = 5 * time.Minute
)

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

// Source is the fetch.PeerSource: it finds a LAN peer that holds a file
// in full and hands back a Car reader over the FileP2P ObjectRead RPC.
// Selection is bounded (one FileCheck sweep) and failures ban the peer,
// so it never stalls a fetch when no peer has the file.
type Source struct {
	pool  DialPool
	peers LocalPeers

	banMu  sync.Mutex
	banned map[banKey]time.Time
}

func NewSource(pool DialPool, peers LocalPeers) *Source {
	return &Source{pool: pool, peers: peers, banned: map[banKey]time.Time{}}
}

var (
	_ fetch.PeerSource         = (*Source)(nil)
	_ fetch.PeerReadDeadliner  = (*peerCar)(nil)
	_ fetch.PeerProbeDeadliner = (*peerCar)(nil)
)

// SourceFor returns a peer-backed Car reader for (spaceId, root) when a
// non-banned peer reports holding the file in full, plus a hook to ban
// that peer (called by the fetcher if it serves invalid bytes).
// ok=false when no peer holds the file.
//
// The two layers get separate try budgets. They used to share one, so
// three LAN peers that did not have the file could spend it before any
// global peer was asked — a file no LAN peer holds and a relayed peer
// does would never be found. The whole sweep is bounded as well as each
// candidate: a fetch with an HTTP fallback must reach it in reasonable
// time, and the per-candidate budgets multiply.
func (s *Source) SourceFor(ctx context.Context, spaceId string, root cid.Cid) (fetch.CarSource, func(), bool) {
	sctx, cancel := context.WithTimeout(ctx, maxSweep)
	defer cancel()
	for _, layer := range []struct {
		ids    []string
		global bool
	}{
		{ids: s.filterBanned(s.peers.LocalPeerIds(spaceId), false)},
		{ids: s.filterBanned(s.peers.GlobalPeerIds(spaceId), true), global: true},
	} {
		tried := 0
		for _, id := range layer.ids {
			if tried >= maxCandidates || sctx.Err() != nil {
				break
			}
			full, reached := s.holdsFull(sctx, id, layer.global, spaceId, root)
			if reached {
				// Only a candidate we actually reached spends a slot.
				// Counting unreachable ones let stale global ids — the
				// peer store ranks by last-seen, not by connectedness —
				// shadow the connected peer that has the file.
				tried++
			}
			if full {
				peerId, global := id, layer.global
				car := &peerCar{pool: s.pool, peerId: peerId, global: global, spaceId: spaceId, root: root}
				return car, func() { s.ban(peerId, global) }, true
			}
		}
	}
	return nil, nil, false
}

// holdsFull reaches one peer (under its own timeout) and asks whether
// it holds the file in full. LAN peers are dialed; a global peer is
// only picked from the pool and skipped, not banned, when it has no
// live connection. A dial/RPC failure bans the peer — it is
// unreachable — but a plain "not full" answer does not, and neither
// does the CALLER's context ending: an aborted download or an expired
// request deadline says nothing about the peer, and banning on it
// would drop the one device holding the file for banTTL.
//
// reached reports whether the peer answered at all, so the caller can
// charge a try only for a candidate that was really consulted.
func (s *Source) holdsFull(ctx context.Context, peerId string, global bool, spaceId string, root cid.Cid) (full, reached bool) {
	budget := perPeerTimeout
	if global {
		budget = perGlobalPeerTimeout
	}
	pctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	p, err := s.peer(pctx, peerId, global)
	if err != nil {
		if !global && !callerDone(ctx) {
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
		if !callerDone(ctx) {
			s.ban(peerId, global)
		}
		return false, false
	}
	return full, true
}

// callerDone reports that the sweep's own context ended — the caller
// gave up or ran out of budget — as opposed to this peer failing.
func callerDone(ctx context.Context) bool { return ctx.Err() != nil }

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
	out := ids[:0:0]
	for _, id := range ids {
		k := banKey{id, global}
		if until, ok := s.banned[k]; ok {
			if now.Before(until) {
				continue
			}
			delete(s.banned, k) // lazy cleanup of an expired ban
		}
		out = append(out, id)
	}
	return out
}

// peerCar reads a file's CAR object from one LAN peer via ObjectRead.
// Implements fetch.CarSource so seed and the block fetcher use it exactly
// like the HTTP source. It does NOT ban on its own: the fetcher decides
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

// PeerReadDeadline implements fetch.PeerReadDeadliner: a global peer's
// range crosses the internet, a LAN peer's does not.
func (c *peerCar) PeerReadDeadline() time.Duration {
	if c.global {
		return globalReadDeadline
	}
	return 0 // the fetch package's LAN default
}

// PeerProbeDeadline implements fetch.PeerProbeDeadliner.
func (c *peerCar) PeerProbeDeadline() time.Duration {
	if c.global {
		return globalProbeDeadline
	}
	return 0
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

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
	banned map[string]time.Time
}

func NewSource(pool DialPool, peers LocalPeers) *Source {
	return &Source{pool: pool, peers: peers, banned: map[string]time.Time{}}
}

var _ fetch.PeerSource = (*Source)(nil)

// SourceFor returns a peer-backed Car reader for (spaceId, root) when a
// non-banned LAN peer reports holding the file in full, plus a hook to
// ban that peer (called by the fetcher if it serves invalid bytes).
// ok=false when no peer holds the file. Each candidate gets its own
// bounded budget, so a slow first peer never poisons the rest.
func (s *Source) SourceFor(ctx context.Context, spaceId string, root cid.Cid) (fetch.CarSource, func(), bool) {
	type candidate struct {
		id     string
		global bool
	}
	var cands []candidate
	for _, id := range s.filterBanned(s.peers.LocalPeerIds(spaceId)) {
		cands = append(cands, candidate{id: id})
	}
	for _, id := range s.filterBanned(s.peers.GlobalPeerIds(spaceId)) {
		cands = append(cands, candidate{id: id, global: true})
	}
	if len(cands) == 0 {
		return nil, nil, false
	}
	tried := 0
	for _, c := range cands {
		if tried >= maxCandidates {
			break
		}
		tried++
		if s.holdsFull(ctx, c.id, c.global, spaceId, root) {
			peerId := c.id
			car := &peerCar{pool: s.pool, peerId: peerId, global: c.global, spaceId: spaceId, root: root}
			return car, func() { s.ban(peerId) }, true
		}
	}
	return nil, nil, false
}

// holdsFull reaches one peer (under its own timeout) and asks whether
// it holds the file in full. LAN peers are dialed; a global peer is
// only picked from the pool and skipped, not banned, when it has no
// live connection. A dial/RPC failure bans the peer — it is
// unreachable — but a plain "not full" answer does not.
func (s *Source) holdsFull(ctx context.Context, peerId string, global bool, spaceId string, root cid.Cid) bool {
	budget := perPeerTimeout
	if global {
		budget = perGlobalPeerTimeout
	}
	pctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	p, err := s.peer(pctx, peerId, global)
	if err != nil {
		if !global {
			s.ban(peerId)
		}
		return false
	}
	var full bool
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
		s.ban(peerId)
		return false
	}
	return full
}

// peer resolves a candidate: dial for LAN, live connection only for
// global.
func (s *Source) peer(ctx context.Context, peerId string, global bool) (peer.Peer, error) {
	if global {
		return p2p.PickLive(ctx, s.pool, peerId)
	}
	return s.pool.Get(ctx, peerId)
}

func (s *Source) ban(peerId string) {
	s.banMu.Lock()
	s.banned[peerId] = time.Now().Add(banTTL)
	s.banMu.Unlock()
}

func (s *Source) filterBanned(ids []string) []string {
	s.banMu.Lock()
	defer s.banMu.Unlock()
	now := time.Now()
	out := ids[:0:0]
	for _, id := range ids {
		if until, ok := s.banned[id]; ok {
			if now.Before(until) {
				continue
			}
			delete(s.banned, id) // lazy cleanup of an expired ban
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

package p2p

import (
	"slices"
	"sync"

	"github.com/anyproto/any-sync/app"
)

const peerStoreCName = "sdk.p2p.peerstore"

// Source is how a peer became known.
type Source uint8

const (
	// SourceLAN — the SpaceExchangeV2 handshake on the local network.
	SourceLAN Source = iota
	// SourceGlobal — a key-value record in a shared space.
	SourceGlobal
)

// String returns a stable lowercase token for logging / status.
func (s Source) String() string {
	if s == SourceGlobal {
		return "global"
	}
	return "lan"
}

// Observer is notified after a peer's space set changes (union over
// sources). before and after are the peer's space ids around the
// change; removed is true when the peer is gone from every source
// (after is then nil). Called outside the store's lock — observers may
// call back into the store.
type Observer func(peerId string, before, after []string, removed bool)

// SourceObserver is notified when a peer enters (present) or leaves a
// source. Called outside the store's lock.
type SourceObserver func(source Source, peerId string, present bool)

// sourceIndex is one bidirectional space↔peer index.
type sourceIndex struct {
	bySpace map[string][]string
	byPeer  map[string][]string
}

func newSourceIndex() *sourceIndex {
	return &sourceIndex{bySpace: map[string][]string{}, byPeer: map[string][]string{}}
}

// PeerStore tracks the peers that SHARE spaces with this device and
// which spaces, per source: LAN peers from the space exchange, global
// peers from key-value records. The per-space peer manager, pubsub and
// the files p2p source read the two sources separately — LAN peers are
// dialed inline, global peers are only used while already connected.
// In-memory; the LAN side is rediscovered from scratch on restart, the
// global side is rebuilt from the records of each loaded space.
type PeerStore struct {
	mu        sync.Mutex
	sources   [2]*sourceIndex
	observers []Observer
	srcObs    []SourceObserver
	status    *StatusBook
}

func NewPeerStore() *PeerStore {
	return &PeerStore{sources: [2]*sourceIndex{newSourceIndex(), newSourceIndex()}}
}

func (p *PeerStore) Init(_ *app.App) error { return nil }

func (p *PeerStore) Name() string { return peerStoreCName }

// SetStatus wires the liveness book that orders global peers and hides
// disabled ones. nil keeps insertion order and hides nobody.
func (p *PeerStore) SetStatus(b *StatusBook) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.status = b
}

// AddObserver registers a change callback. No removal — the observer
// set is fixed at wiring time and lives as long as the app.
func (p *PeerStore) AddObserver(o Observer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.observers = append(p.observers, o)
}

// AddSourceObserver registers a per-source presence callback.
func (p *PeerStore) AddSourceObserver(o SourceObserver) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.srcObs = append(p.srcObs, o)
}

// UpdateLocalPeer records the full LAN space set for a peer.
func (p *PeerStore) UpdateLocalPeer(peerId string, spaceIds []string) {
	p.update(SourceLAN, peerId, spaceIds)
}

// RemoveLocalPeer forgets a peer's LAN presence (dial failure,
// connection closed and gone). Unknown peers are a no-op.
func (p *PeerStore) RemoveLocalPeer(peerId string) { p.remove(SourceLAN, peerId) }

// UpdateGlobalPeer records the full global space set for a peer.
func (p *PeerStore) UpdateGlobalPeer(peerId string, spaceIds []string) {
	p.update(SourceGlobal, peerId, spaceIds)
}

// RemoveGlobalPeer forgets a peer's global presence.
func (p *PeerStore) RemoveGlobalPeer(peerId string) { p.remove(SourceGlobal, peerId) }

// update records the full space set of a peer in one source, diffing
// against the previous set. A no-change update fires no observers.
func (p *PeerStore) update(src Source, peerId string, spaceIds []string) {
	after := slices.Clone(spaceIds)
	slices.Sort(after)
	after = slices.Compact(after)
	if len(after) == 0 {
		p.remove(src, peerId)
		return
	}

	p.mu.Lock()
	idx := p.sources[src]
	before, known := idx.byPeer[peerId]
	if known && slices.Equal(before, after) {
		p.mu.Unlock()
		return
	}
	unionBefore := p.spaceIdsLocked(peerId)
	idx.byPeer[peerId] = after
	for _, spaceId := range diff(after, before) {
		idx.bySpace[spaceId] = append(idx.bySpace[spaceId], peerId)
	}
	for _, spaceId := range diff(before, after) {
		idx.dropPeerFromSpace(spaceId, peerId)
	}
	unionAfter := p.spaceIdsLocked(peerId)
	observers, srcObs := p.observers, p.srcObs
	p.mu.Unlock()

	if !known {
		for _, o := range srcObs {
			o(src, peerId, true)
		}
	}
	if !slices.Equal(unionBefore, unionAfter) {
		for _, o := range observers {
			o(peerId, unionBefore, unionAfter, false)
		}
	}
}

func (p *PeerStore) remove(src Source, peerId string) {
	p.mu.Lock()
	idx := p.sources[src]
	before, known := idx.byPeer[peerId]
	if !known {
		p.mu.Unlock()
		return
	}
	unionBefore := p.spaceIdsLocked(peerId)
	delete(idx.byPeer, peerId)
	for _, spaceId := range before {
		idx.dropPeerFromSpace(spaceId, peerId)
	}
	unionAfter := p.spaceIdsLocked(peerId)
	gone := len(unionAfter) == 0
	observers, srcObs := p.observers, p.srcObs
	p.mu.Unlock()

	for _, o := range srcObs {
		o(src, peerId, false)
	}
	if gone {
		for _, o := range observers {
			o(peerId, unionBefore, nil, true)
		}
	} else if !slices.Equal(unionBefore, unionAfter) {
		for _, o := range observers {
			o(peerId, unionBefore, unionAfter, false)
		}
	}
}

// LocalPeerIds returns the LAN peers known to have spaceId.
func (p *PeerStore) LocalPeerIds(spaceId string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.sources[SourceLAN].bySpace[spaceId])
}

// GlobalPeerIds returns the global peers known to have spaceId, most
// recently seen first, disabled tier excluded.
func (p *PeerStore) GlobalPeerIds(spaceId string) []string {
	p.mu.Lock()
	ids := slices.Clone(p.sources[SourceGlobal].bySpace[spaceId])
	status := p.status
	p.mu.Unlock()
	return p.rankGlobal(ids, status)
}

// AllLocalPeers returns every known LAN peer id, sorted.
func (p *PeerStore) AllLocalPeers() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return sortedKeys(p.sources[SourceLAN].byPeer)
}

// AllGlobalPeers returns every known global peer id, most recently
// seen first, disabled tier excluded.
func (p *PeerStore) AllGlobalPeers() []string {
	p.mu.Lock()
	ids := sortedKeys(p.sources[SourceGlobal].byPeer)
	status := p.status
	p.mu.Unlock()
	return p.rankGlobal(ids, status)
}

// HasGlobalPeer reports whether the peer is known through key-value
// records (any tier).
func (p *PeerStore) HasGlobalPeer(peerId string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.sources[SourceGlobal].byPeer[peerId]
	return ok
}

// GlobalSpaceIds returns the spaces a global peer is known to have.
func (p *PeerStore) GlobalSpaceIds(peerId string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.sources[SourceGlobal].byPeer[peerId])
}

// SpaceIds returns the spaces a peer is known to have, over every
// source.
func (p *PeerStore) SpaceIds(peerId string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.spaceIdsLocked(peerId)
}

// Sources returns the sources that know the peer.
func (p *PeerStore) Sources(peerId string) []Source {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []Source
	for src, idx := range p.sources {
		if _, ok := idx.byPeer[peerId]; ok {
			out = append(out, Source(src))
		}
	}
	return out
}

// spaceIdsLocked is the sorted union of a peer's space sets. Callers
// hold p.mu.
func (p *PeerStore) spaceIdsLocked(peerId string) []string {
	var out []string
	for _, idx := range p.sources {
		out = append(out, idx.byPeer[peerId]...)
	}
	if len(out) == 0 {
		return nil
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// rankGlobal orders global peers by LastSeen desc and drops the
// disabled tier. Without a status book the input order is kept.
func (p *PeerStore) rankGlobal(ids []string, status *StatusBook) []string {
	if status == nil || len(ids) == 0 {
		return ids
	}
	type ranked struct {
		id   string
		seen int64
	}
	out := make([]ranked, 0, len(ids))
	for _, id := range ids {
		if status.Tier(id) == TierDisabled {
			continue
		}
		out = append(out, ranked{id: id, seen: status.LastSeen(id).UnixNano()})
	}
	slices.SortStableFunc(out, func(a, b ranked) int {
		switch {
		case a.seen > b.seen:
			return -1
		case a.seen < b.seen:
			return 1
		}
		return 0
	})
	res := make([]string, 0, len(out))
	for _, r := range out {
		res = append(res, r.id)
	}
	return res
}

// dropPeerFromSpace removes peerId from a space's peer list, deleting
// the entry when it empties. Callers hold the store lock.
func (idx *sourceIndex) dropPeerFromSpace(spaceId, peerId string) {
	peers := slices.DeleteFunc(idx.bySpace[spaceId], func(s string) bool { return s == peerId })
	if len(peers) == 0 {
		delete(idx.bySpace, spaceId)
		return
	}
	idx.bySpace[spaceId] = peers
}

func sortedKeys(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for id := range m {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

// diff returns the elements of a that are not in b. Both inputs sorted.
func diff(a, b []string) []string {
	var out []string
	for _, v := range a {
		if _, found := slices.BinarySearch(b, v); !found {
			out = append(out, v)
		}
	}
	return out
}

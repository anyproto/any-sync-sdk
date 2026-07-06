package p2p

import (
	"slices"
	"sync"

	"github.com/anyproto/any-sync/app"
)

const peerStoreCName = "sdk.p2p.peerstore"

// Observer is notified after a local peer's space set changes. before
// and after are the peer's space ids around the change; removed is true
// when the peer itself was dropped (after is then nil). Called outside
// the store's lock — observers may call back into the store.
type Observer func(peerId string, before, after []string, removed bool)

// PeerStore tracks local-network peers and which spaces each of them
// SHARES with us, as learned from the SpaceExchangeV2 handshake.
// Bidirectional in-memory index: space→peers feeds the per-space peer manager and
// sync status; peer→spaces makes updates and removals cheap. Cleared
// on restart — the LAN is rediscovered from scratch.
type PeerStore struct {
	mu        sync.Mutex
	bySpace   map[string][]string
	byPeer    map[string][]string
	observers []Observer
}

func NewPeerStore() *PeerStore {
	return &PeerStore{
		bySpace: map[string][]string{},
		byPeer:  map[string][]string{},
	}
}

func (p *PeerStore) Init(_ *app.App) error { return nil }

func (p *PeerStore) Name() string { return peerStoreCName }

// AddObserver registers a change callback. No removal — the observer
// set is fixed at wiring time and lives as long as the app.
func (p *PeerStore) AddObserver(o Observer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.observers = append(p.observers, o)
}

// UpdateLocalPeer records the full space set for a peer, diffing
// against the previous set. A no-change update fires no observers.
func (p *PeerStore) UpdateLocalPeer(peerId string, spaceIds []string) {
	after := slices.Clone(spaceIds)
	slices.Sort(after)
	after = slices.Compact(after)

	p.mu.Lock()
	before, known := p.byPeer[peerId]
	if known && slices.Equal(before, after) {
		p.mu.Unlock()
		return
	}
	p.byPeer[peerId] = after
	for _, spaceId := range diff(after, before) {
		p.bySpace[spaceId] = append(p.bySpace[spaceId], peerId)
	}
	for _, spaceId := range diff(before, after) {
		p.dropPeerFromSpace(spaceId, peerId)
	}
	observers := p.observers
	p.mu.Unlock()

	for _, o := range observers {
		o(peerId, before, after, false)
	}
}

// RemoveLocalPeer forgets a peer entirely (dial failure, connection
// closed and gone). Unknown peers are a no-op.
func (p *PeerStore) RemoveLocalPeer(peerId string) {
	p.mu.Lock()
	before, known := p.byPeer[peerId]
	if !known {
		p.mu.Unlock()
		return
	}
	delete(p.byPeer, peerId)
	for _, spaceId := range before {
		p.dropPeerFromSpace(spaceId, peerId)
	}
	observers := p.observers
	p.mu.Unlock()

	for _, o := range observers {
		o(peerId, before, nil, true)
	}
}

// LocalPeerIds returns the peers known to have spaceId.
func (p *PeerStore) LocalPeerIds(spaceId string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.bySpace[spaceId])
}

// AllLocalPeers returns every known local peer id.
func (p *PeerStore) AllLocalPeers() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.byPeer))
	for id := range p.byPeer {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

// SpaceIds returns the spaces a peer is known to have.
func (p *PeerStore) SpaceIds(peerId string) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.byPeer[peerId])
}

// dropPeerFromSpace removes peerId from a space's peer list, deleting
// the entry when it empties. Callers hold p.mu.
func (p *PeerStore) dropPeerFromSpace(spaceId, peerId string) {
	peers := slices.DeleteFunc(p.bySpace[spaceId], func(s string) bool { return s == peerId })
	if len(peers) == 0 {
		delete(p.bySpace, spaceId)
		return
	}
	p.bySpace[spaceId] = peers
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

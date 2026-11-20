package p2p

import (
	"sync"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/net/peerservice"
	"github.com/anyproto/any-sync/net/transport"
)

const addrBookCName = "sdk.p2p.addrbook"

// AddrBook is the only writer of peer addresses into any-sync's peer
// service. A peer is registered with either its LAN addresses (from the
// space exchange) or its iroh ticket (from key-value records), never
// both: a LAN dial that fails must answer in one RTT, not fall through
// into a relay dial that can take the whole dial timeout. LAN wins
// while present; the ticket takes over once the LAN entry is cleared
// (mDNS lost, dial strikes). The one address the book does not own is
// the push node's (config.Push), registered by the SDK directly: it is
// neither a LAN nor a global peer and never enters the peer store.
type AddrBook struct {
	ps peerService

	mu      sync.Mutex
	lan     map[string][]string
	tickets map[string]string
}

// peerService is the slice of any-sync the book writes to.
type peerService interface {
	SetPeerAddrs(peerId string, addrs []string)
}

func NewAddrBook() *AddrBook {
	return &AddrBook{lan: map[string][]string{}, tickets: map[string]string{}}
}

func (b *AddrBook) Init(a *app.App) error {
	if b.ps == nil {
		b.ps = a.MustComponent(peerservice.CName).(peerservice.PeerService)
	}
	return nil
}

func (b *AddrBook) Name() string { return addrBookCName }

// SetLAN registers a peer's LAN addresses (already scheme-prefixed).
// Empty clears them.
func (b *AddrBook) SetLAN(peerId string, addrs []string) {
	b.mu.Lock()
	if len(addrs) == 0 {
		delete(b.lan, peerId)
	} else {
		b.lan[peerId] = append([]string(nil), addrs...)
	}
	b.applyLocked(peerId)
	b.mu.Unlock()
}

// ClearLAN forgets a peer's LAN addresses; its ticket, if any, takes
// over.
func (b *AddrBook) ClearLAN(peerId string) { b.SetLAN(peerId, nil) }

// SetTicket registers a peer's iroh ticket. Empty clears it.
func (b *AddrBook) SetTicket(peerId, ticket string) {
	b.mu.Lock()
	if ticket == "" {
		delete(b.tickets, peerId)
	} else {
		b.tickets[peerId] = ticket
	}
	b.applyLocked(peerId)
	b.mu.Unlock()
}

// ClearTicket forgets a peer's ticket.
func (b *AddrBook) ClearTicket(peerId string) { b.SetTicket(peerId, "") }

// Ticket returns a peer's registered ticket, empty when none.
func (b *AddrBook) Ticket(peerId string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tickets[peerId]
}

// HasLAN reports whether the peer currently has LAN addresses.
func (b *AddrBook) HasLAN(peerId string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.lan[peerId]
	return ok
}

// applyLocked pushes the peer's effective address list. Callers hold
// b.mu; the peer service takes its own lock and never calls back.
func (b *AddrBook) applyLocked(peerId string) {
	if b.ps == nil {
		return
	}
	if addrs, ok := b.lan[peerId]; ok {
		b.ps.SetPeerAddrs(peerId, addrs)
		return
	}
	if ticket, ok := b.tickets[peerId]; ok {
		b.ps.SetPeerAddrs(peerId, []string{transport.Iroh + "://" + ticket})
		return
	}
	b.ps.SetPeerAddrs(peerId, nil)
}

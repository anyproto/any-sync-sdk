package p2p

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// fakePeerService records the last address list set per peer.
type fakePeerService struct {
	addrs map[string][]string
	calls int
}

func (f *fakePeerService) SetPeerAddrs(peerId string, addrs []string) {
	if f.addrs == nil {
		f.addrs = map[string][]string{}
	}
	f.addrs[peerId] = addrs
	f.calls++
}

func newTestAddrBook() (*AddrBook, *fakePeerService) {
	ps := &fakePeerService{}
	b := NewAddrBook()
	b.ps = ps
	return b, ps
}

func TestAddrBookLANXorTicket(t *testing.T) {
	b, ps := newTestAddrBook()
	lan := []string{"yamux://10.0.0.2:4242", "quic://10.0.0.2:4242"}

	// Ticket alone → iroh address.
	b.SetTicket("p", "ticketA")
	require.Equal(t, []string{"iroh://ticketA"}, ps.addrs["p"])
	require.Equal(t, "ticketA", b.Ticket("p"))
	require.False(t, b.HasLAN("p"))

	// LAN wins while present; the ticket is kept, not registered.
	b.SetLAN("p", lan)
	require.Equal(t, lan, ps.addrs["p"])
	require.True(t, b.HasLAN("p"))
	require.Equal(t, "ticketA", b.Ticket("p"))
	b.SetTicket("p", "ticketB")
	require.Equal(t, lan, ps.addrs["p"], "a ticket update never mixes into a LAN list")

	// LAN gone → the ticket takes over.
	b.ClearLAN("p")
	require.Equal(t, []string{"iroh://ticketB"}, ps.addrs["p"])

	// Nothing left → the peer has no addresses.
	b.ClearTicket("p")
	require.Empty(t, ps.addrs["p"])
	require.Equal(t, "", b.Ticket("p"))
}

func TestAddrBookLANOnly(t *testing.T) {
	b, ps := newTestAddrBook()
	b.SetLAN("p", []string{"yamux://10.0.0.3:1"})
	require.Equal(t, []string{"yamux://10.0.0.3:1"}, ps.addrs["p"])
	b.ClearLAN("p")
	require.Empty(t, ps.addrs["p"])
	// Empty list is a clear.
	b.SetTicket("p", "tk")
	b.SetLAN("p", nil)
	require.Equal(t, []string{"iroh://tk"}, ps.addrs["p"])
}

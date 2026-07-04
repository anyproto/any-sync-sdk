package p2p

import (
	"context"
	"testing"

	"github.com/anyproto/any-sync/commonspace/clientspaceproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/stretchr/testify/require"
)

type fakePeerService struct {
	addrs map[string][]string
}

func (f *fakePeerService) SetPeerAddrs(peerId string, addrs []string) {
	if f.addrs == nil {
		f.addrs = map[string][]string{}
	}
	f.addrs[peerId] = addrs
}

func TestSpaceExchangeInbound(t *testing.T) {
	store := NewPeerStore()
	var kicked []string
	ex := NewExchange("self-peer", store, func() []string { return []string{"mine1", "mine2"} }, func(_ string, spaceIds []string) {
		kicked = spaceIds
	})
	ps := &fakePeerService{}
	ex.peerService = ps

	// The peer connected FROM an ephemeral source port (51000); its
	// real listen port is the advertised 4242.
	ctx := peer.CtxWithPeerId(context.Background(), "remote-peer")
	ctx = peer.CtxWithPeerAddr(ctx, "quic://192.168.1.5:51000")

	resp, err := ex.SpaceExchange(ctx, &clientspaceproto.SpaceExchangeRequest{
		SpaceIds: []string{"shared1"},
		LocalServer: &clientspaceproto.LocalServer{
			Ips:  []string{"192.168.1.5", "10.0.0.7"},
			Port: 4242,
		},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"mine1", "mine2"}, resp.SpaceIds)

	// The observed IP is paired with the ADVERTISED port (not the
	// ephemeral 51000), deduped against the advertised list; everything
	// carries the quic scheme.
	require.Equal(t, []string{"quic://192.168.1.5:4242", "quic://10.0.0.7:4242"}, ps.addrs["remote-peer"])
	require.ElementsMatch(t, []string{"remote-peer"}, store.LocalPeerIds("shared1"))
	require.Equal(t, []string{"shared1"}, kicked)
}

func TestSpaceExchangeInboundNoLocalServer(t *testing.T) {
	store := NewPeerStore()
	ex := NewExchange("self-peer", store, func() []string { return []string{"mine"} }, nil)
	ex.peerService = &fakePeerService{}

	// A request without LocalServer (plain probe) records nothing.
	resp, err := ex.SpaceExchange(context.Background(), &clientspaceproto.SpaceExchangeRequest{SpaceIds: []string{"s"}})
	require.NoError(t, err)
	require.Equal(t, []string{"mine"}, resp.SpaceIds)
	require.Empty(t, store.AllLocalPeers())
}

func TestSpaceExchangeRejectsBadAddresses(t *testing.T) {
	store := NewPeerStore()
	ex := NewExchange("self", store, func() []string { return nil }, nil)
	ps := &fakePeerService{}
	ex.peerService = ps
	ctx := peer.CtxWithPeerId(context.Background(), "p")

	// Out-of-range port: no addresses recorded at all.
	_, err := ex.SpaceExchange(ctx, &clientspaceproto.SpaceExchangeRequest{
		SpaceIds:    []string{"s"},
		LocalServer: &clientspaceproto.LocalServer{Ips: []string{"10.0.0.1"}, Port: 70000},
	})
	require.NoError(t, err)
	require.Empty(t, ps.addrs["p"])
	require.Empty(t, store.AllLocalPeers())

	// Garbage / hostname IPs are dropped; only literal IPs survive.
	_, err = ex.SpaceExchange(ctx, &clientspaceproto.SpaceExchangeRequest{
		SpaceIds:    []string{"s"},
		LocalServer: &clientspaceproto.LocalServer{Ips: []string{"evil.example.com", "not-an-ip", "10.0.0.2"}, Port: 4242},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"quic://10.0.0.2:4242"}, ps.addrs["p"])
}

func TestSpaceExchangeRefusesOwnPeerId(t *testing.T) {
	store := NewPeerStore()
	ex := NewExchange("self-peer", store, func() []string { return []string{"mine"} }, nil)
	ex.peerService = &fakePeerService{}

	// A remote presenting OUR peer id = two devices sharing a device
	// key. Refused loudly, nothing recorded.
	ctx := peer.CtxWithPeerId(context.Background(), "self-peer")
	_, err := ex.SpaceExchange(ctx, &clientspaceproto.SpaceExchangeRequest{
		SpaceIds:    []string{"s"},
		LocalServer: &clientspaceproto.LocalServer{Ips: []string{"10.0.0.9"}, Port: 1},
	})
	require.ErrorContains(t, err, "own peer id")
	require.Empty(t, store.AllLocalPeers())
}

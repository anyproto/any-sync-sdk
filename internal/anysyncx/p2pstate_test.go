package anysyncx

import (
	"testing"

	"github.com/stretchr/testify/require"

	sdkp2p "github.com/anyproto/any-sync-sdk/p2p"
	"github.com/anyproto/any-sync-sdk/space"
)

func TestP2PStateFor(t *testing.T) {
	connected := func(string) bool { return true }
	nobody := func(string) bool { return false }
	peers := []string{"lp1"}

	require.Equal(t, space.P2PStateNotPossible,
		p2pStateFor(false, sdkp2p.PossibilityPossible, peers, connected))
	require.Equal(t, space.P2PStateNotPossible,
		p2pStateFor(true, sdkp2p.PossibilityNoInterfaces, peers, connected))
	require.Equal(t, space.P2PStateRestricted,
		p2pStateFor(true, sdkp2p.PossibilityRestricted, peers, connected))
	require.Equal(t, space.P2PStateConnected,
		p2pStateFor(true, sdkp2p.PossibilityPossible, peers, connected))
	require.Equal(t, space.P2PStateNotConnected,
		p2pStateFor(true, sdkp2p.PossibilityPossible, peers, nobody))
	require.Equal(t, space.P2PStateNotConnected,
		p2pStateFor(true, sdkp2p.PossibilityPossible, nil, connected))
	// Unknown possibility (probe hasn't run yet) is not a blocker.
	require.Equal(t, space.P2PStateConnected,
		p2pStateFor(true, sdkp2p.PossibilityUnknown, peers, connected))
}

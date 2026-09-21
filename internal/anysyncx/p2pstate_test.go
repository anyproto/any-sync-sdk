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
	only := func(want string) func(string) bool {
		return func(id string) bool { return id == want }
	}
	lan := []string{"lp1"}
	global := []string{"gp1"}

	// LAN only — the pre-global verdicts.
	require.Equal(t, space.P2PStateNotPossible,
		p2pStateFor(false, false, sdkp2p.PossibilityPossible, lan, nil, connected))
	require.Equal(t, space.P2PStateNotPossible,
		p2pStateFor(true, false, sdkp2p.PossibilityNoInterfaces, lan, nil, connected))
	require.Equal(t, space.P2PStateRestricted,
		p2pStateFor(true, false, sdkp2p.PossibilityRestricted, lan, nil, connected))
	require.Equal(t, space.P2PStateConnected,
		p2pStateFor(true, false, sdkp2p.PossibilityPossible, lan, nil, connected))
	require.Equal(t, space.P2PStateNotConnected,
		p2pStateFor(true, false, sdkp2p.PossibilityPossible, lan, nil, nobody))
	require.Equal(t, space.P2PStateNotConnected,
		p2pStateFor(true, false, sdkp2p.PossibilityPossible, nil, nil, connected))
	// Unknown possibility (probe hasn't run yet) is not a blocker.
	require.Equal(t, space.P2PStateConnected,
		p2pStateFor(true, false, sdkp2p.PossibilityUnknown, lan, nil, connected))
	// Local discovery switched off: a LAN peer that is still live still
	// counts (known peers stay dialable); with nobody live it is
	// NotPossible alone and NotConnected once the global layer could
	// still find someone.
	require.Equal(t, space.P2PStateConnected,
		p2pStateFor(true, false, sdkp2p.PossibilityDisabled, lan, nil, connected))
	require.Equal(t, space.P2PStateNotPossible,
		p2pStateFor(true, false, sdkp2p.PossibilityDisabled, lan, nil, nobody))
	require.Equal(t, space.P2PStateConnected,
		p2pStateFor(true, true, sdkp2p.PossibilityDisabled, lan, global, only("lp1")))
	require.Equal(t, space.P2PStateNotConnected,
		p2pStateFor(true, true, sdkp2p.PossibilityDisabled, lan, global, nobody))

	// Global only: LAN verdicts don't apply, a connected global peer is
	// Connected, none is NotConnected.
	require.Equal(t, space.P2PStateConnected,
		p2pStateFor(false, true, sdkp2p.PossibilityNoInterfaces, lan, global, only("gp1")))
	require.Equal(t, space.P2PStateNotConnected,
		p2pStateFor(false, true, sdkp2p.PossibilityRestricted, lan, global, nobody))
	// A LAN peer does not count while the LAN layer is off.
	require.Equal(t, space.P2PStateNotConnected,
		p2pStateFor(false, true, sdkp2p.PossibilityPossible, lan, nil, only("lp1")))

	// Both on: either kind of live peer is Connected — a live global peer
	// wins even over a restricted LAN. With nobody live the LAN verdict
	// still tells why the LAN path is down.
	require.Equal(t, space.P2PStateConnected,
		p2pStateFor(true, true, sdkp2p.PossibilityPossible, lan, global, only("lp1")))
	require.Equal(t, space.P2PStateConnected,
		p2pStateFor(true, true, sdkp2p.PossibilityPossible, lan, global, only("gp1")))
	require.Equal(t, space.P2PStateConnected,
		p2pStateFor(true, true, sdkp2p.PossibilityRestricted, lan, global, only("gp1")))
	require.Equal(t, space.P2PStateRestricted,
		p2pStateFor(true, true, sdkp2p.PossibilityRestricted, lan, global, nobody))
	require.Equal(t, space.P2PStateNotPossible,
		p2pStateFor(true, true, sdkp2p.PossibilityNoInterfaces, lan, global, nobody))
	require.Equal(t, space.P2PStateNotConnected,
		p2pStateFor(true, true, sdkp2p.PossibilityPossible, lan, global, nobody))
}

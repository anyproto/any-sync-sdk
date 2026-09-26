package space

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dev builds a row with the app installed and, when seq > 0, a claim.
func dev(peerId string, app string, seq, at int64) Device {
	d := Device{PeerId: peerId, Apps: map[string]map[string]any{app: {}}}
	if seq > 0 {
		d.ActiveClaims = map[string]DeviceClaim{app: {Seq: seq, At: at}}
	}
	return d
}

// The election contract, tier by tier: highest seq wins; equal seq
// falls to highest at; equal (seq, at) falls to the largest peer id.
// Deterministic on converged data whatever the slice order.
func TestActiveDevice_Tiebreaks(t *testing.T) {
	cases := []struct {
		name    string
		devices []Device
		want    string
	}{
		{"highest seq wins", []Device{dev("a", "bao", 1, 99), dev("b", "bao", 2, 1)}, "b"},
		{"at breaks equal seq", []Device{dev("a", "bao", 2, 10), dev("b", "bao", 2, 20)}, "b"},
		{"peer id breaks equal seq+at", []Device{dev("a", "bao", 2, 10), dev("b", "bao", 2, 10)}, "b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ActiveDevice(tc.devices, "bao")
			require.True(t, ok)
			assert.Equal(t, tc.want, got)
			// Order independence — every reader must agree.
			rev := []Device{tc.devices[1], tc.devices[0]}
			got, ok = ActiveDevice(rev, "bao")
			require.True(t, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

// A claim on a device that no longer has the app installed is
// dangling and never wins; with no qualifying claim ok=false.
func TestActiveDevice_IgnoresDanglingClaims(t *testing.T) {
	uninstalled := Device{
		PeerId:       "a",
		ActiveClaims: map[string]DeviceClaim{"bao": {Seq: 9, At: 9}},
	}
	winner, ok := ActiveDevice([]Device{uninstalled, dev("b", "bao", 1, 1)}, "bao")
	require.True(t, ok)
	assert.Equal(t, "b", winner)

	_, ok = ActiveDevice([]Device{uninstalled, dev("c", "bao", 0, 0)}, "bao")
	assert.False(t, ok, "installed but unclaimed rows never win")

	_, ok = ActiveDevice(nil, "bao")
	assert.False(t, ok)
}

// A malformed synced claim decodes to the zero DeviceClaim; key
// presence alone must not win the election — real claims carry
// Seq >= 1 (ClaimActive mints from 1).
func TestActiveDevice_IgnoresInvalidClaims(t *testing.T) {
	bad := Device{
		PeerId:       "a",
		Apps:         map[string]map[string]any{"bao": {}},
		ActiveClaims: map[string]DeviceClaim{"bao": {}},
	}
	_, ok := ActiveDevice([]Device{bad, dev("b", "bao", 0, 0)}, "bao")
	assert.False(t, ok, "a zero claim never wins, even as sole claimant")

	winner, ok := ActiveDevice([]Device{bad, dev("c", "bao", 1, 1)}, "bao")
	require.True(t, ok)
	assert.Equal(t, "c", winner)
}

// Per-app independence: claims for one slug never leak into another's
// election.
func TestActiveDevice_PerApp(t *testing.T) {
	a := dev("a", "bao", 5, 5)
	b := dev("b", "other", 1, 1)
	winner, ok := ActiveDevice([]Device{a, b}, "other")
	require.True(t, ok)
	assert.Equal(t, "b", winner)
}

// remote builds a row whose claim hands app to target; the claimer
// has no app installed.
func remote(peerId, target, app string, seq, at int64) Device {
	return Device{PeerId: peerId, ActiveClaims: map[string]DeviceClaim{app: {Seq: seq, At: at, Target: target}}}
}

// A claim with a target elects the target, not the claimer — and the
// claimer needs no app installed.
func TestActiveDevice_RemoteTargetWins(t *testing.T) {
	devices := []Device{dev("a", "bao", 1, 1), dev("b", "bao", 0, 0), remote("phone", "b", "bao", 2, 2)}
	winner, ok := ActiveDevice(devices, "bao")
	require.True(t, ok)
	assert.Equal(t, "b", winner)
}

// A claim whose target is not a live row with the app falls through to
// the next-best claim.
func TestActiveDevice_UnqualifiedTargetFallsBack(t *testing.T) {
	cases := []struct {
		name    string
		devices []Device
	}{
		{"target unknown", []Device{dev("a", "bao", 1, 1), remote("c", "gone", "bao", 5, 5)}},
		{"target lacks app", []Device{dev("a", "bao", 1, 1), dev("b", "other", 0, 0), remote("c", "b", "bao", 5, 5)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			winner, ok := ActiveDevice(tc.devices, "bao")
			require.True(t, ok)
			assert.Equal(t, "a", winner)
		})
	}
}

// The tiebreak on equal (seq, at) is the claimer's peer id, not the
// target's: two claimers handing to different targets resolve the same
// way on every reader.
func TestActiveDevice_TiebreakOnClaimer(t *testing.T) {
	devices := []Device{
		dev("a", "bao", 0, 0), dev("z", "bao", 0, 0),
		remote("c1", "z", "bao", 3, 3), remote("c2", "a", "bao", 3, 3),
	}
	for _, order := range [][]Device{devices, {devices[3], devices[2], devices[1], devices[0]}} {
		winner, ok := ActiveDevice(order, "bao")
		require.True(t, ok)
		assert.Equal(t, "a", winner, "c2 > c1 wins; its target is a")
	}
}

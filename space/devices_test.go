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

// Per-app independence: claims for one slug never leak into another's
// election.
func TestActiveDevice_PerApp(t *testing.T) {
	a := dev("a", "bao", 5, 5)
	b := dev("b", "other", 1, 1)
	winner, ok := ActiveDevice([]Device{a, b}, "other")
	require.True(t, ok)
	assert.Equal(t, "b", winner)
}

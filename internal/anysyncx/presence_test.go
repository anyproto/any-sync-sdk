package anysyncx

import (
	"testing"

	"github.com/anyproto/any-sync/nodeconf"
	"github.com/stretchr/testify/assert"
)

// Only a refused handshake is an error for the rollup: a network that
// needs an update still accepts this client's version.
func TestNetworkRefusesClient(t *testing.T) {
	for st, want := range map[nodeconf.NetworkCompatibilityStatus]bool{
		nodeconf.NetworkCompatibilityStatusUnknown:      false,
		nodeconf.NetworkCompatibilityStatusOk:           false,
		nodeconf.NetworkCompatibilityStatusError:        false,
		nodeconf.NetworkCompatibilityStatusIncompatible: true,
		nodeconf.NetworkCompatibilityStatusNeedsUpdate:  false,
	} {
		assert.Equal(t, want, networkRefusesClient(st), "status %d", st)
	}
}

// Within the TTL a peer is picked once however many spaces ask.
func TestLivePicks_OnePickPerPeer(t *testing.T) {
	picks := map[string]int{}
	c := newLivePicks(func(id string) bool {
		picks[id]++
		return id == "up"
	})
	for i := 0; i < 5; i++ {
		assert.True(t, c.pickable("up"))
		assert.False(t, c.pickable("down"))
	}
	assert.Equal(t, map[string]int{"up": 1, "down": 1}, picks)
}

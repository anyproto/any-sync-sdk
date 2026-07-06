package p2p

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type observed struct {
	peerId  string
	before  []string
	after   []string
	removed bool
}

func newObservedStore() (*PeerStore, *[]observed) {
	s := NewPeerStore()
	var events []observed
	s.AddObserver(func(peerId string, before, after []string, removed bool) {
		events = append(events, observed{peerId, before, after, removed})
	})
	return s, &events
}

func TestPeerStoreAddUpdateRemove(t *testing.T) {
	s, events := newObservedStore()

	s.UpdateLocalPeer("p1", []string{"s1", "s2"})
	require.ElementsMatch(t, []string{"p1"}, s.LocalPeerIds("s1"))
	require.ElementsMatch(t, []string{"p1"}, s.LocalPeerIds("s2"))
	require.ElementsMatch(t, []string{"s1", "s2"}, s.SpaceIds("p1"))
	require.Len(t, *events, 1)
	require.False(t, (*events)[0].removed)

	// Second peer on an overlapping space.
	s.UpdateLocalPeer("p2", []string{"s2", "s3"})
	require.ElementsMatch(t, []string{"p1", "p2"}, s.LocalPeerIds("s2"))
	require.ElementsMatch(t, []string{"p1", "p2"}, s.AllLocalPeers())

	// Space set change: s1 dropped, s3 added.
	s.UpdateLocalPeer("p1", []string{"s2", "s3"})
	require.Empty(t, s.LocalPeerIds("s1"))
	require.ElementsMatch(t, []string{"p1", "p2"}, s.LocalPeerIds("s3"))
	last := (*events)[len(*events)-1]
	require.Equal(t, []string{"s1", "s2"}, last.before)
	require.Equal(t, []string{"s2", "s3"}, last.after)

	// Removal clears both directions and notifies with removed=true.
	s.RemoveLocalPeer("p1")
	require.ElementsMatch(t, []string{"p2"}, s.LocalPeerIds("s2"))
	require.ElementsMatch(t, []string{"p2"}, s.AllLocalPeers())
	require.Empty(t, s.SpaceIds("p1"))
	last = (*events)[len(*events)-1]
	require.True(t, last.removed)
	require.Nil(t, last.after)
}

func TestPeerStoreNoChangeNoNotify(t *testing.T) {
	s, events := newObservedStore()
	s.UpdateLocalPeer("p1", []string{"s2", "s1"})
	s.UpdateLocalPeer("p1", []string{"s1", "s2"}) // same set, different order
	require.Len(t, *events, 1)
}

func TestPeerStoreRemoveUnknownPeer(t *testing.T) {
	s, events := newObservedStore()
	s.RemoveLocalPeer("ghost")
	require.Empty(t, *events)
}

func TestPeerStoreObserverMayReenter(t *testing.T) {
	s := NewPeerStore()
	var reentered bool
	s.AddObserver(func(peerId string, _, _ []string, removed bool) {
		if !removed && !reentered {
			reentered = true
			// Must not deadlock: observers run outside the lock.
			s.RemoveLocalPeer(peerId)
		}
	})
	s.UpdateLocalPeer("p1", []string{"s1"})
	require.True(t, reentered)
	require.Empty(t, s.AllLocalPeers())
}

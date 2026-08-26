package p2p

import (
	"testing"
	"time"

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

func TestPeerStoreSourcesAreSeparate(t *testing.T) {
	s := NewPeerStore()
	var srcEvents []string
	s.AddSourceObserver(func(src Source, peerId string, present bool) {
		srcEvents = append(srcEvents, src.String()+":"+peerId+":"+map[bool]string{true: "in", false: "out"}[present])
	})
	var unionEvents []observed
	s.AddObserver(func(peerId string, before, after []string, removed bool) {
		unionEvents = append(unionEvents, observed{peerId, before, after, removed})
	})

	s.UpdateLocalPeer("p", []string{"s1"})
	s.UpdateGlobalPeer("p", []string{"s1", "s2"})
	s.UpdateGlobalPeer("g", []string{"s2"})

	require.Equal(t, []string{"p"}, s.LocalPeerIds("s1"))
	require.Empty(t, s.LocalPeerIds("s2"), "global membership never leaks into the LAN view")
	require.ElementsMatch(t, []string{"p", "g"}, s.GlobalPeerIds("s2"))
	require.Equal(t, []string{"p"}, s.GlobalPeerIds("s1"))
	require.Equal(t, []string{"s1", "s2"}, s.SpaceIds("p"))
	require.Equal(t, []Source{SourceLAN, SourceGlobal}, s.Sources("p"))
	require.True(t, s.HasGlobalPeer("g"))
	require.False(t, s.HasGlobalPeer("x"))
	require.Equal(t, []string{"lan:p:in", "global:p:in", "global:g:in"}, srcEvents)

	// Dropping the LAN source keeps the global one and the union.
	s.RemoveLocalPeer("p")
	require.Empty(t, s.LocalPeerIds("s1"))
	require.Equal(t, []string{"p"}, s.GlobalPeerIds("s1"))
	require.Equal(t, []string{"s1", "s2"}, s.SpaceIds("p"))
	require.Equal(t, "lan:p:out", srcEvents[len(srcEvents)-1])
	last := unionEvents[len(unionEvents)-1]
	require.False(t, last.removed, "still known globally")

	// Dropping the last source removes the peer.
	s.RemoveGlobalPeer("p")
	require.Empty(t, s.SpaceIds("p"))
	last = unionEvents[len(unionEvents)-1]
	require.True(t, last.removed)
	require.Nil(t, last.after)

	// An empty set keeps the peer known with no spaces; only remove
	// forgets it (a LAN peer sharing nothing yet stays reachable for
	// re-handshakes).
	s.UpdateGlobalPeer("g", nil)
	require.True(t, s.HasGlobalPeer("g"))
	require.Empty(t, s.GlobalSpaceIds("g"))
	s.UpdateLocalPeer("l", nil)
	require.True(t, s.HasLocalPeer("l"))
	require.Contains(t, s.AllLocalPeers(), "l")
	s.RemoveGlobalPeer("g")
	require.False(t, s.HasGlobalPeer("g"))
	require.Empty(t, s.AllGlobalPeers())
}

func TestPeerStoreGlobalOrderAndDisabled(t *testing.T) {
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	book := NewStatusBook("", testThresholds)
	book.now = func() time.Time { return now }
	s := NewPeerStore()
	s.SetStatus(book)

	for _, id := range []string{"old", "fresh", "mid", "dead"} {
		s.UpdateGlobalPeer(id, []string{"s"})
	}
	book.Seen("old", now.Add(-2*24*time.Hour))
	book.Seen("fresh", now)
	book.Seen("mid", now.Add(-time.Hour))
	book.Seen("dead", now.Add(-40*24*time.Hour))

	require.Equal(t, []string{"fresh", "mid", "old"}, s.GlobalPeerIds("s"))
	require.Equal(t, []string{"fresh", "mid", "old"}, s.AllGlobalPeers())
	require.True(t, s.HasGlobalPeer("dead"), "disabled peers stay known (inbound gate reads tiers)")

	// Reactivation puts a peer back in front.
	now = now.Add(time.Minute)
	book.Seen("dead", now)
	require.Equal(t, []string{"dead", "fresh", "mid", "old"}, s.GlobalPeerIds("s"))
}

func TestPeerStoreAccountSource(t *testing.T) {
	st := NewPeerStore()
	var presence []string
	st.AddSourceObserver(func(src Source, peerId string, present bool) {
		presence = append(presence, src.String()+":"+peerId+":"+map[bool]string{true: "in", false: "out"}[present])
	})

	st.UpdateGlobalPeer("g", []string{"s1"})
	st.UpdateAccountPeer("a")

	// an account peer counts for every space, listed after the space's
	// own record peers, and is a global peer for the inbound gate
	require.Equal(t, []string{"g", "a"}, st.GlobalPeerIds("s1"))
	require.Equal(t, []string{"a"}, st.GlobalPeerIds("s2"))
	require.Equal(t, []string{"g", "a"}, st.AllGlobalPeers())
	require.Equal(t, []string{"a"}, st.AccountPeerIds())
	require.True(t, st.HasGlobalPeer("a"))
	require.True(t, st.HasAccountPeer("a"))
	require.False(t, st.HasAccountPeer("g"))
	require.True(t, st.HasSpace("a", "anything"))
	require.True(t, st.HasSpace("g", "s1"))
	require.False(t, st.HasSpace("g", "s2"))
	require.Equal(t, []Source{SourceAccount}, st.Sources("a"))
	require.Nil(t, st.SpaceIds("a"), "account peers carry no space set")

	// a device known both ways is listed once
	st.UpdateAccountPeer("g")
	require.Equal(t, []string{"g", "a"}, st.GlobalPeerIds("s1"))

	st.RemoveAccountPeer("a")
	require.Equal(t, []string{"g"}, st.GlobalPeerIds("s1"))
	require.False(t, st.HasGlobalPeer("a"))
	require.Contains(t, presence, "account:a:in")
	require.Contains(t, presence, "account:a:out")
	require.Equal(t, "account", SourceAccount.String())
}

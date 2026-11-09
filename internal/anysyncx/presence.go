package anysyncx

import (
	"sync"
	"time"

	"github.com/anyproto/any-sync/nodeconf"
)

// networkRefusesClient reports whether the nodeconf compatibility
// status means the network turned this client's protocol version
// away at the handshake. NeedsUpdate is a newer network that still
// accepts this version and syncs; Error and Unknown are a check that
// did not run.
func networkRefusesClient(st nodeconf.NetworkCompatibilityStatus) bool {
	return st == nodeconf.NetworkCompatibilityStatusIncompatible
}

// livePickTTL is how long a peer's liveness answer is reused.
const livePickTTL = 500 * time.Millisecond

// livePicks answers "is this peer connected" from a short-lived cache
// per peer id. The sync-status presence poll asks about the same few
// node ids for every space once a second, and a pick of a peer still
// dialing waits out p2p.PickTimeout each time; the cache bounds a
// tick's cost to the distinct peers, not spaces × peers.
type livePicks struct {
	pick func(id string) bool
	mu   sync.Mutex
	seen map[string]livePick
}

type livePick struct {
	live bool
	at   time.Time
}

// newLivePicks caches pick, which must never dial.
func newLivePicks(pick func(id string) bool) *livePicks {
	return &livePicks{pick: pick, seen: map[string]livePick{}}
}

// pickable reports whether a live connection to id exists, as of at
// most livePickTTL ago.
func (c *livePicks) pickable(id string) bool {
	now := time.Now()
	c.mu.Lock()
	if e, ok := c.seen[id]; ok && now.Sub(e.at) < livePickTTL {
		c.mu.Unlock()
		return e.live
	}
	c.mu.Unlock()
	live := c.pick(id)
	c.mu.Lock()
	c.seen[id] = livePick{live: live, at: now}
	c.mu.Unlock()
	return live
}

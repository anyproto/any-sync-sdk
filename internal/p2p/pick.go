package p2p

import (
	"context"
	"time"

	"github.com/anyproto/any-sync/net/peer"
)

// PickTimeout bounds a pool.Pick on a global peer. The pool's Pick waits
// on an in-flight load for the same id (a LAN+global peer can have a LAN
// dial in flight), so an unbounded ctx would turn "never dials" into
// "waits for somebody else's dial".
const PickTimeout = 50 * time.Millisecond

// Picker is the pool slice PickLive needs.
type Picker interface {
	Pick(ctx context.Context, id string) (peer.Peer, error)
}

// PickLive returns the live pool connection to a peer, or an error
// within PickTimeout. It never dials.
func PickLive(ctx context.Context, p Picker, id string) (peer.Peer, error) {
	ctx, cancel := context.WithTimeout(ctx, PickTimeout)
	defer cancel()
	return p.Pick(ctx, id)
}

package anysyncx

import (
	"context"
	"errors"
	"sync"

	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/net/rpc/rpcerr"
)

// Per-peer request budget of the sync rounds.
//
// A responder caps the stream requests one peer holds open on it —
// any-sync's syncqueues.Limit: 20, stepping down to 5 as the node gets
// busier, counted across every space of the peer — and refuses the
// excess with ErrTooManyRequestsFromPeer. It also refuses a second
// request for a tree the same peer already has one open on
// (ErrDuplicateRequest). Neither says anything about the tree: the
// request passes once the peer has room.
//
// peerLimits keeps the probes and the fetches of all spaces' rounds
// under that cap, leaving room for the request queue any-sync runs
// beside the rounds (ten workers per peer). The width adapts: a
// refusal halves it, a run of answered requests widens it again.

const (
	// peerLimitMax is the widest a peer's budget gets.
	peerLimitMax = 8
	// peerLimitGrow is how many full windows of answered requests
	// widen the budget by one.
	peerLimitGrow = 4
)

// peerLimits holds one budget per peer, shared by every space's tree
// syncer.
type peerLimits struct {
	mu    sync.Mutex
	peers map[string]*peerLimit
}

func newPeerLimits() *peerLimits {
	return &peerLimits{peers: map[string]*peerLimit{}}
}

func (l *peerLimits) of(peerId string) *peerLimit {
	l.mu.Lock()
	defer l.mu.Unlock()
	pl := l.peers[peerId]
	if pl == nil {
		pl = &peerLimit{width: peerLimitMax, changed: make(chan struct{})}
		l.peers[peerId] = pl
	}
	return pl
}

type peerLimit struct {
	mu sync.Mutex
	// width is how many requests may be in flight; inUse how many are.
	width, inUse int
	// streak counts the answered requests since width last changed.
	streak int
	// epoch counts the times width was narrowed. Requests in flight
	// together are refused together; only the first refusal of such a
	// burst narrows the budget.
	epoch uint64
	// changed is closed, and replaced, whenever a slot may have opened.
	changed chan struct{}
}

// peerSlot is one acquired slot of a peer's budget.
type peerSlot struct{ epoch uint64 }

// acquire takes a slot, waiting for one while the budget is spent.
func (l *peerLimit) acquire(ctx context.Context) (peerSlot, error) {
	for {
		l.mu.Lock()
		if l.inUse < l.width {
			l.inUse++
			slot := peerSlot{epoch: l.epoch}
			l.mu.Unlock()
			return slot, nil
		}
		changed := l.changed
		l.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return peerSlot{}, ctx.Err()
		}
	}
}

// release returns a slot. tooMany reports that the peer refused the
// request as one over its cap.
func (l *peerLimit) release(slot peerSlot, tooMany bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inUse--
	switch {
	case tooMany && slot.epoch == l.epoch:
		l.width = max(1, l.width/2)
		l.streak = 0
		l.epoch++
	case tooMany:
		// Refused with the burst that already narrowed the budget.
	default:
		if l.streak++; l.width < peerLimitMax && l.streak >= l.width*peerLimitGrow {
			l.width++
			l.streak = 0
		}
	}
	close(l.changed)
	l.changed = make(chan struct{})
}

// peerTooMany reports a request the peer refused as one over its cap.
func peerTooMany(err error) bool {
	return isRPCErr(err, spacesyncproto.ErrTooManyRequestsFromPeer)
}

// peerBusy reports a request the peer turned away without looking at
// the tree: over its cap, or a second request for a tree it is already
// serving this peer. Both pass on a later attempt.
func peerBusy(err error) bool {
	return peerTooMany(err) || isRPCErr(err, spacesyncproto.ErrDuplicateRequest)
}

// isRPCErr matches a registered rpc error whether the chain holds it
// as a value or only as its wire code.
func isRPCErr(err, target error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, target) || errors.Is(rpcerr.Unwrap(err), target)
}

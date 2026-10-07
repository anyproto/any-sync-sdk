package anysyncx

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/stretchr/testify/require"
	"storj.io/drpc/drpcerr"
)

// A peer's budget lets peerLimitMax requests through at once and holds
// the next one until a slot is returned; a waiter gives up with its
// context.
func TestPeerLimitBoundsRequestsInFlight(t *testing.T) {
	l := newPeerLimits().of("peer1")
	slots := make([]peerSlot, peerLimitMax)
	for i := range slots {
		slot, err := l.acquire(context.Background())
		require.NoError(t, err)
		slots[i] = slot
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := l.acquire(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	got := make(chan error, 1)
	go func() {
		_, err := l.acquire(context.Background())
		got <- err
	}()
	select {
	case <-got:
		t.Fatal("acquired over the budget")
	case <-time.After(20 * time.Millisecond):
	}
	l.release(slots[0], false)
	require.NoError(t, <-got)
}

// A refusal over the peer's cap halves the budget, down to one; a run
// of answered requests widens it again, one at a time, up to the
// maximum.
func TestPeerLimitAdaptsToRefusals(t *testing.T) {
	l := newPeerLimits().of("peer1")
	cycle := func(tooMany bool) {
		slot, err := l.acquire(context.Background())
		require.NoError(t, err)
		l.release(slot, tooMany)
	}
	cycle(true)
	require.Equal(t, peerLimitMax/2, l.width)
	for i := 0; i < 10; i++ {
		cycle(true)
	}
	require.Equal(t, 1, l.width)

	for i := 0; i < peerLimitGrow; i++ {
		cycle(false)
	}
	require.Equal(t, 2, l.width)
	for i := 0; i < 1000; i++ {
		cycle(false)
	}
	require.Equal(t, peerLimitMax, l.width)
}

// Requests in flight together are refused together. Such a burst
// narrows the budget once, not once per refusal.
func TestPeerLimitNarrowsOncePerBurst(t *testing.T) {
	l := newPeerLimits().of("peer1")
	slots := make([]peerSlot, peerLimitMax)
	for i := range slots {
		slot, err := l.acquire(context.Background())
		require.NoError(t, err)
		slots[i] = slot
	}
	for _, slot := range slots {
		l.release(slot, true)
	}
	require.Equal(t, peerLimitMax/2, l.width)

	// A refusal of a request sent after that narrows it again.
	slot, err := l.acquire(context.Background())
	require.NoError(t, err)
	l.release(slot, true)
	require.Equal(t, peerLimitMax/4, l.width)
}

// Budgets are per peer, and one peer's is the same for every caller.
func TestPeerLimitsArePerPeer(t *testing.T) {
	ls := newPeerLimits()
	require.Same(t, ls.of("a"), ls.of("a"))
	require.NotSame(t, ls.of("a"), ls.of("b"))
}

// A refusal is recognised as a value in the chain and as the bare wire
// code a stream error carries.
func TestPeerBusyMatchesValueAndWireCode(t *testing.T) {
	require.True(t, peerTooMany(fmt.Errorf("fetch: %w", spacesyncproto.ErrTooManyRequestsFromPeer)))
	require.True(t, peerBusy(fmt.Errorf("fetch: %w", spacesyncproto.ErrDuplicateRequest)))
	require.False(t, peerTooMany(spacesyncproto.ErrDuplicateRequest))

	wire := drpcerr.WithCode(fmt.Errorf("stream closed"), drpcerr.Code(spacesyncproto.ErrTooManyRequestsFromPeer))
	require.True(t, peerTooMany(fmt.Errorf("fetch: %w", wire)))
	require.False(t, peerBusy(fmt.Errorf("boom")))
	require.False(t, peerBusy(nil))
}

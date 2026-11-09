package anysyncx

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/anyproto/any-sync/net/peer"
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

// pickFn is a Picker over a function.
type pickFn func(ctx context.Context, id string) (peer.Peer, error)

func (f pickFn) Pick(ctx context.Context, id string) (peer.Peer, error) { return f(ctx, id) }

// A live peer answers true; a peer that would make the pick wait, as
// one still being dialed does, answers false at once.
func TestPickNow(t *testing.T) {
	live := pickFn(func(context.Context, string) (peer.Peer, error) { return fakePeer{id: "up"}, nil })
	assert.True(t, pickNow(live, "up"))

	absent := pickFn(func(context.Context, string) (peer.Peer, error) { return nil, errors.New("not found") })
	assert.False(t, pickNow(absent, "gone"))

	dialing := pickFn(func(ctx context.Context, _ string) (peer.Peer, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
			return fakePeer{id: "dialing"}, nil
		}
	})
	start := time.Now()
	assert.False(t, pickNow(dialing, "dialing"))
	assert.Less(t, time.Since(start), 200*time.Millisecond, "a dial in flight is not waited on")
}

package anysyncx

import (
	"context"
	"slices"
	"testing"

	"github.com/anyproto/any-sync/commonspace/spacesyncproto"
	"github.com/anyproto/any-sync/commonspace/sync/objectsync/objectmessages"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/anyproto/any-sync/net/streampool"
	"github.com/stretchr/testify/require"
	"storj.io/drpc"
)

// taggingStreamPool records the tags added and removed through the
// message context; the rest of the pool is never reached.
type taggingStreamPool struct {
	streampool.StreamPool
	added, removed []string
}

func (p *taggingStreamPool) AddTagsCtx(_ context.Context, tags ...string) error {
	p.added = append(p.added, tags...)
	return nil
}

func (p *taggingStreamPool) RemoveTagsCtx(_ context.Context, tags ...string) error {
	p.removed = append(p.removed, tags...)
	return nil
}

type fakePeerKinds struct {
	local  []string
	global map[string][]string // spaceId → global peer ids
}

func (f *fakePeerKinds) HasLocalPeer(id string) bool { return slices.Contains(f.local, id) }
func (f *fakePeerKinds) HasGlobalPeer(id string) bool {
	for _, ids := range f.global {
		if slices.Contains(ids, id) {
			return true
		}
	}
	return false
}
func (f *fakePeerKinds) GlobalPeerIds(spaceId string) []string { return f.global[spaceId] }

func subscriptionMessage(t *testing.T, action spacesyncproto.SpaceSubscriptionAction, spaceIds ...string) drpc.Message {
	t.Helper()
	payload, err := (&spacesyncproto.SpaceSubscription{SpaceIds: spaceIds, Action: action}).MarshalVT()
	require.NoError(t, err)
	return &objectmessages.HeadUpdate{Bytes: payload}
}

// A subscription from a global peer earns the global-subscription tag
// for the spaces the records name it for; anyone else keeps only the
// plain space tags. Unsubscribe drops both.
func TestStreamHandlerTagsGlobalSubscriptions(t *testing.T) {
	sp := &taggingStreamPool{}
	h := &streamHandler{
		syncHandler: newSpaceSyncHandler(),
		streamPool:  sp,
		peers:       &fakePeerKinds{global: map[string][]string{"s1": {"g1"}, "s2": {"g2"}}},
	}
	ctx := peer.CtxWithPeerId(context.Background(), "g1")
	require.NoError(t, h.HandleMessage(ctx, "", subscriptionMessage(t, spacesyncproto.SpaceSubscriptionAction_Subscribe, "s1", "s2")))
	require.Equal(t, []string{"s1", "s2", globalSubTag("s1")}, sp.added, "s2 does not name g1")

	sp.added = nil
	stranger := peer.CtxWithPeerId(context.Background(), "x1")
	require.NoError(t, h.HandleMessage(stranger, "", subscriptionMessage(t, spacesyncproto.SpaceSubscriptionAction_Subscribe, "s1")))
	require.Equal(t, []string{"s1"}, sp.added, "unknown peer: plain tag only")

	require.NoError(t, h.HandleMessage(ctx, "", subscriptionMessage(t, spacesyncproto.SpaceSubscriptionAction_Unsubscribe, "s1")))
	require.Equal(t, []string{"s1", globalSubTag("s1")}, sp.removed)
}

// Without a peer classifier (global layer off) nothing changes.
func TestStreamHandlerNoPeerKinds(t *testing.T) {
	sp := &taggingStreamPool{}
	h := &streamHandler{syncHandler: newSpaceSyncHandler(), streamPool: sp}
	ctx := peer.CtxWithPeerId(context.Background(), "g1")
	require.NoError(t, h.HandleMessage(ctx, "", subscriptionMessage(t, spacesyncproto.SpaceSubscriptionAction_Subscribe, "s1")))
	require.Equal(t, []string{"s1"}, sp.added)
	require.False(t, h.isGlobalOnly("g1"))
}

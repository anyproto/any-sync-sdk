package anysyncx

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

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

func newSubsHandler(peers *fakePeerKinds) (*streamHandler, *taggingStreamPool, *globalSubs) {
	sp := &taggingStreamPool{}
	subs := newGlobalSubs(time.Minute)
	h := &streamHandler{syncHandler: newSpaceSyncHandler(), streamPool: sp, peers: peers, subs: subs}
	return h, sp, subs
}

// A subscription from a global peer is recorded for the spaces the
// records name it for; anyone else keeps only the plain space tags.
// Unsubscribe drops the record and the plain tag, nothing else.
func TestStreamHandlerRecordsGlobalSubscriptions(t *testing.T) {
	h, sp, subs := newSubsHandler(&fakePeerKinds{global: map[string][]string{"s1": {"g1"}, "s2": {"g2"}}})
	ctx := peer.CtxWithPeerId(context.Background(), "g1")
	require.NoError(t, h.HandleMessage(ctx, "", subscriptionMessage(t, spacesyncproto.SpaceSubscriptionAction_Subscribe, "s1", "s2")))
	require.Equal(t, []string{"s1", "s2"}, sp.added, "plain tags only")
	require.Equal(t, []string{"g1"}, subs.Subscribers("s1"))
	require.Empty(t, subs.Subscribers("s2"), "s2 does not name g1")

	sp.added = nil
	stranger := peer.CtxWithPeerId(context.Background(), "x1")
	require.NoError(t, h.HandleMessage(stranger, "", subscriptionMessage(t, spacesyncproto.SpaceSubscriptionAction_Subscribe, "s1")))
	require.Equal(t, []string{"s1"}, sp.added, "unknown peer: plain tag only")
	require.Equal(t, []string{"g1"}, subs.Subscribers("s1"), "unknown peer is not recorded")

	require.NoError(t, h.HandleMessage(ctx, "", subscriptionMessage(t, spacesyncproto.SpaceSubscriptionAction_Unsubscribe, "s1")))
	require.Equal(t, []string{"s1"}, sp.removed, "the withdrawal carries no internal tag")
	require.Empty(t, subs.Subscribers("s1"))
}

// Space ids are remote input: an id spelling an internal tag never
// reaches the tag index or the registry, and a message is capped.
func TestStreamHandlerRejectsForgedSpaceIds(t *testing.T) {
	h, sp, subs := newSubsHandler(&fakePeerKinds{global: map[string][]string{"s1": {"g1"}}})
	ctx := peer.CtxWithPeerId(context.Background(), "g1")
	forged := []string{"anysyncx/global-sub/s1", nodeStreamTag, "", "s1"}
	require.NoError(t, h.HandleMessage(ctx, "", subscriptionMessage(t, spacesyncproto.SpaceSubscriptionAction_Subscribe, forged...)))
	require.Equal(t, []string{"s1"}, sp.added)
	require.Equal(t, []string{"g1"}, subs.Subscribers("s1"))
	require.Empty(t, subs.Subscribers("anysyncx/global-sub/s1"))

	sp.added = nil
	many := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		many = append(many, fmt.Sprintf("space%d", i))
	}
	require.NoError(t, h.HandleMessage(ctx, "", subscriptionMessage(t, spacesyncproto.SpaceSubscriptionAction_Subscribe, many...)))
	require.Len(t, sp.added, maxSubscriptionSpaceIds)

	require.False(t, validSpaceId(string(make([]byte, maxSpaceIdLen+1))))
	require.True(t, validSpaceId("bafyreiabc.1w2wr7c0dqe6"))
}

// A peer reachable over the LAN is pushed to by the LAN path; its ask
// is not recorded even though the records name it.
func TestStreamHandlerLANPeerNotRegistered(t *testing.T) {
	h, sp, subs := newSubsHandler(&fakePeerKinds{local: []string{"g1"}, global: map[string][]string{"s1": {"g1"}}})
	ctx := peer.CtxWithPeerId(context.Background(), "g1")
	require.NoError(t, h.HandleMessage(ctx, "", subscriptionMessage(t, spacesyncproto.SpaceSubscriptionAction_Subscribe, "s1")))
	require.Equal(t, []string{"s1"}, sp.added)
	require.Empty(t, subs.Subscribers("s1"))
	require.False(t, h.isGlobalOnly("g1"))
}

// An ask that is not refreshed expires; a refresh restarts the clock.
func TestGlobalSubsExpiry(t *testing.T) {
	subs := newGlobalSubs(time.Minute)
	now := time.Unix(1000, 0)
	subs.now = func() time.Time { return now }
	subs.Add("g1", "s1")
	subs.Add("g2", "s1")
	require.ElementsMatch(t, []string{"g1", "g2"}, subs.Subscribers("s1"))

	now = now.Add(40 * time.Second)
	subs.Add("g1", "s1")
	now = now.Add(40 * time.Second)
	require.Equal(t, []string{"g1"}, subs.Subscribers("s1"), "g2's ask expired")

	now = now.Add(time.Minute + time.Second)
	require.Empty(t, subs.Subscribers("s1"))
	subs.Remove("g1", "s1")
	require.Empty(t, subs.subs, "empty spaces are dropped")
}

// Without a peer classifier (global layer off) nothing changes.
func TestStreamHandlerNoPeerKinds(t *testing.T) {
	sp := &taggingStreamPool{}
	h := &streamHandler{syncHandler: newSpaceSyncHandler(), streamPool: sp}
	ctx := peer.CtxWithPeerId(context.Background(), "g1")
	require.NoError(t, h.HandleMessage(ctx, "", subscriptionMessage(t, spacesyncproto.SpaceSubscriptionAction_Subscribe, "s1")))
	require.Equal(t, []string{"s1"}, sp.added)
	require.NoError(t, h.HandleMessage(ctx, "", subscriptionMessage(t, spacesyncproto.SpaceSubscriptionAction_Unsubscribe, "s1")))
	require.Equal(t, []string{"s1"}, sp.removed)
	require.False(t, h.isGlobalOnly("g1"))
}

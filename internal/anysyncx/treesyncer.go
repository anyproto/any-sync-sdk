package anysyncx

import (
	"context"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree"
	"github.com/anyproto/any-sync/commonspace/object/treesyncer"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"
	"github.com/anyproto/any-sync/commonspace/spacestate"
	"github.com/anyproto/any-sync/net/peer"
)

// treeSyncerAdapter is registered by anysyncx as the per-space
// commonspace.Deps.TreeSyncer. On SyncAll it builds (fetches) missing
// trees and pushes existing ones via SyncWithPeer. Trees are not closed
// here — they stay in ocache for the async exchange to land.
type treeSyncerAdapter struct {
	spaceId     string
	treeBuilder objecttreebuilder.TreeBuilder
}

func newTreeSyncer() *treeSyncerAdapter { return &treeSyncerAdapter{} }

func (t *treeSyncerAdapter) Init(a *app.App) error {
	t.spaceId = a.MustComponent(spacestate.CName).(*spacestate.SpaceState).SpaceId
	t.treeBuilder = a.MustComponent(objecttreebuilder.CName).(objecttreebuilder.TreeBuilder)
	return nil
}

func (t *treeSyncerAdapter) Name() string { return treesyncer.CName }

func (t *treeSyncerAdapter) Run(_ context.Context) error   { return nil }
func (t *treeSyncerAdapter) Close(_ context.Context) error { return nil }

func (t *treeSyncerAdapter) StartSync()                {}
func (t *treeSyncerAdapter) StopSync()                 {}
func (t *treeSyncerAdapter) ShouldSync(_ string) bool  { return true }

func (t *treeSyncerAdapter) SyncAll(ctx context.Context, p peer.Peer, existing, missing []string) error {
	peerCtx := peer.CtxWithPeerId(ctx, p.Id())
	for _, ids := range [][]string{missing, existing} {
		for _, id := range ids {
			tree, err := t.treeBuilder.BuildTree(peerCtx, id, objecttreebuilder.BuildTreeOpts{})
			if err != nil {
				continue
			}
			if st, ok := tree.(synctree.SyncTree); ok {
				_ = st.SyncWithPeer(ctx, p)
			}
			// Don't close — async exchange may still need it.
		}
	}
	return nil
}

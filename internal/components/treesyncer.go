package components

import (
	"context"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree"
	"github.com/anyproto/any-sync/commonspace/object/treesyncer"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"
	"github.com/anyproto/any-sync/commonspace/spacestate"
	"github.com/anyproto/any-sync/net/peer"
)

// TreeSyncerAdapter implements TreeSyncer for the SDK. On SyncAll it
// builds (fetches) missing trees and syncs existing ones via the
// TreeBuilder, which creates SyncTrees that pull data from remote peers.
type TreeSyncerAdapter struct {
	spaceId     string
	treeBuilder objecttreebuilder.TreeBuilder
}

func NewTreeSyncer() *TreeSyncerAdapter {
	return &TreeSyncerAdapter{}
}

func (t *TreeSyncerAdapter) Init(a *app.App) error {
	t.spaceId = a.MustComponent(spacestate.CName).(*spacestate.SpaceState).SpaceId
	t.treeBuilder = a.MustComponent(objecttreebuilder.CName).(objecttreebuilder.TreeBuilder)
	return nil
}

func (t *TreeSyncerAdapter) Name() string {
	return treesyncer.CName
}

func (t *TreeSyncerAdapter) Run(_ context.Context) error {
	return nil
}

func (t *TreeSyncerAdapter) Close(_ context.Context) error {
	return nil
}

func (t *TreeSyncerAdapter) StartSync() {}

func (t *TreeSyncerAdapter) StopSync() {}

func (t *TreeSyncerAdapter) ShouldSync(_ string) bool {
	return true
}

// SyncAll fetches missing trees and syncs existing ones with the peer.
// Trees are kept open so that the async SyncWithPeer exchange can complete.
// They are cleaned up when the space closes.
func (t *TreeSyncerAdapter) SyncAll(ctx context.Context, p peer.Peer, existing, missing []string) error {
	peerCtx := peer.CtxWithPeerId(ctx, p.Id())

	// Missing trees: remote has, local doesn't. Build fetches from remote and
	// stores locally. Then SyncWithPeer fetches any remaining data.
	for _, id := range missing {
		tree, err := t.treeBuilder.BuildTree(peerCtx, id, objecttreebuilder.BuildTreeOpts{})
		if err != nil {
			continue
		}
		if st, ok := tree.(synctree.SyncTree); ok {
			_ = st.SyncWithPeer(ctx, p)
		}
		// Don't close — tree stays open for async sync to complete.
	}

	// Existing trees: local has, remote doesn't. Open local tree and push
	// to the peer via SyncWithPeer.
	for _, id := range existing {
		tree, err := t.treeBuilder.BuildTree(peerCtx, id, objecttreebuilder.BuildTreeOpts{})
		if err != nil {
			continue
		}
		if st, ok := tree.(synctree.SyncTree); ok {
			_ = st.SyncWithPeer(ctx, p)
		}
		// Don't close — tree stays open for async sync to complete.
	}
	return nil
}

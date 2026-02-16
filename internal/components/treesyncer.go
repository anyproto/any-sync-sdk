package components

import (
	"context"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/treesyncer"
	"github.com/anyproto/any-sync/net/peer"
)

// TreeSyncerAdapter is a no-op TreeSyncer for the SDK. The actual sync
// is handled by the individual sync trees.
type TreeSyncerAdapter struct{}

func NewTreeSyncer() *TreeSyncerAdapter {
	return &TreeSyncerAdapter{}
}

func (t *TreeSyncerAdapter) Init(_ *app.App) error {
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

func (t *TreeSyncerAdapter) SyncAll(_ context.Context, _ peer.Peer, _, _ []string) error {
	return nil
}

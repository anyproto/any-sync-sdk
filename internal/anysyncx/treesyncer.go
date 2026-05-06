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
//
// SyncAll routes through SpaceRegistry.GetTree (NOT through the
// per-space TreeBuilder directly): the registry's per-space
// implementation is the only place that wires our SDK-side
// UpdateListener onto the tree, so that inbound changes fan out to
// the CRDT controller. A bare TreeBuilder.BuildTree call sets no
// listener, which silently breaks cold sync — the tree storage
// receives changes but our controller never sees them. See
// docs/sync-listener-wiring or the cold-sync e2e for the trail.
type treeSyncerAdapter struct {
	spaceId     string
	treeBuilder objecttreebuilder.TreeBuilder
	// registry is shared across all per-space treeSyncerAdapters; it
	// dispatches between tech-space and regular spaces and binds the
	// right listener for each.
	registry SpaceRegistry
}

func newTreeSyncer(registry SpaceRegistry) *treeSyncerAdapter {
	return &treeSyncerAdapter{registry: registry}
}

func (t *treeSyncerAdapter) Init(a *app.App) error {
	t.spaceId = a.MustComponent(spacestate.CName).(*spacestate.SpaceState).SpaceId
	t.treeBuilder = a.MustComponent(objecttreebuilder.CName).(objecttreebuilder.TreeBuilder)
	return nil
}

func (t *treeSyncerAdapter) Name() string { return treesyncer.CName }

func (t *treeSyncerAdapter) Run(_ context.Context) error   { return nil }
func (t *treeSyncerAdapter) Close(_ context.Context) error { return nil }

func (t *treeSyncerAdapter) StartSync()               {}
func (t *treeSyncerAdapter) StopSync()                {}
func (t *treeSyncerAdapter) ShouldSync(_ string) bool { return true }

// SyncAll resolves each tree id through the SpaceRegistry so its
// listener is bound, then asks the resulting SyncTree to ping the
// peer. For missing-locally trees the registry path also performs the
// remote fetch (BuildSyncTreeOrGetRemote) — same end state as the
// previous direct-treeBuilder call, with the listener now wired so
// the CRDT controller can replay the inbound changes.
//
// We deliberately do NOT fall back to TreeBuilder.BuildTree on
// registry errors: a missing registry would mean we have a bug in
// SDK boot ordering, and silently bypassing it would re-create the
// listener-loss bug this method exists to fix.
func (t *treeSyncerAdapter) SyncAll(ctx context.Context, p peer.Peer, existing, missing []string) error {
	if t.registry == nil {
		return ErrSpaceRegistryUnset
	}
	peerCtx := peer.CtxWithPeerId(ctx, p.Id())
	for _, ids := range [][]string{missing, existing} {
		for _, id := range ids {
			tree, err := t.registry.GetTree(peerCtx, t.spaceId, id)
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

package anysyncx

import (
	"context"
	"errors"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/object/treemanager"
)

// ErrSpaceRegistryUnset is returned when GetTree fires before the
// space registry has been wired up. Indicates init order bug, not a
// runtime miss.
var ErrSpaceRegistryUnset = errors.New("anysyncx: space registry not set")

// SpaceRegistry is the contract treeManager uses to resolve trees. Set
// once by the space layer after it builds its ocache. We keep this
// indirection so anysyncx doesn't depend on the higher-level space
// package — matches the pattern from anytype-heart's treemanager.
type SpaceRegistry interface {
	// GetTree returns the live ObjectTree for (spaceId, treeId), loading
	// the space and the object via ocache as needed. Errors propagate
	// from any-sync directly.
	GetTree(ctx context.Context, spaceId, treeId string) (objecttree.ObjectTree, error)

	// PutTree stores a remote-built tree payload — used by sync when a
	// node delivers a tree we don't have yet.
	PutTree(ctx context.Context, spaceId string, payload treestorage.TreeStorageCreatePayload) error

	// MarkTreeDeleted is the soft-delete hook fired when the settings
	// tree announces a deletion. Implementations typically clean up
	// cached state; the actual storage delete happens via DeleteTree.
	MarkTreeDeleted(ctx context.Context, spaceId, treeId string) error

	// DeleteTree removes a tree's local state.
	DeleteTree(ctx context.Context, spaceId, treeId string) error

	// ShouldPullTree decides whether a locally-missing tree announced by
	// a head update should be fetched (selective sync by tree type —
	// SYN-18). root is the tree's raw root change carried by the update;
	// heads are the sender's current heads. Implementations returning
	// false are expected to record the heads so the sync diff converges
	// without the tree's change bodies. Full-sync deployments always
	// return true.
	ShouldPullTree(ctx context.Context, spaceId, treeId string, root *treechangeproto.RawTreeChangeWithId, heads []string) bool
}

// treeManagerAdapter implements treemanager.TreeManager by routing
// every call through SpaceRegistry. Stays thin on purpose — the
// space-package implementation owns ocache wiring and lifecycle.
type treeManagerAdapter struct {
	registry SpaceRegistry
}

func newTreeManager() *treeManagerAdapter { return &treeManagerAdapter{} }

func (t *treeManagerAdapter) Init(_ *app.App) error         { return nil }
func (t *treeManagerAdapter) Name() string                  { return treemanager.CName }
func (t *treeManagerAdapter) Run(_ context.Context) error   { return nil }
func (t *treeManagerAdapter) Close(_ context.Context) error { return nil }

// SetRegistry wires in the SpaceRegistry. Must be called once before
// any inbound sync activity touches the tree manager.
func (t *treeManagerAdapter) SetRegistry(r SpaceRegistry) { t.registry = r }

func (t *treeManagerAdapter) GetTree(ctx context.Context, spaceId, treeId string) (objecttree.ObjectTree, error) {
	if t.registry == nil {
		return nil, ErrSpaceRegistryUnset
	}
	return t.registry.GetTree(ctx, spaceId, treeId)
}

func (t *treeManagerAdapter) ValidateAndPutTree(ctx context.Context, spaceId string, payload treestorage.TreeStorageCreatePayload) error {
	if t.registry == nil {
		return ErrSpaceRegistryUnset
	}
	return t.registry.PutTree(ctx, spaceId, payload)
}

func (t *treeManagerAdapter) MarkTreeDeleted(ctx context.Context, spaceId, treeId string) error {
	if t.registry == nil {
		return ErrSpaceRegistryUnset
	}
	return t.registry.MarkTreeDeleted(ctx, spaceId, treeId)
}

func (t *treeManagerAdapter) DeleteTree(ctx context.Context, spaceId, treeId string) error {
	if t.registry == nil {
		return ErrSpaceRegistryUnset
	}
	return t.registry.DeleteTree(ctx, spaceId, treeId)
}

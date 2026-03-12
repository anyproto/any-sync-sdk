package components

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/object/treemanager"
	"github.com/anyproto/any-sync/commonspace/objecttreebuilder"
)

var errTreeManagerNotSupported = errors.New("tree manager: operation not supported at top level")

type TreeManagerAdapter struct {
	mu       sync.RWMutex
	builders map[string]objecttreebuilder.TreeBuilder
	// trees caches open tree instances by "spaceId/treeId" so that incoming
	// sync requests (via objectManager.GetObject) return the same instance
	// the SDK user holds. Without this, sync updates go to a throwaway
	// instance and never reach the user's Object.
	trees map[string]objecttree.ObjectTree
}

func NewTreeManager() *TreeManagerAdapter {
	return &TreeManagerAdapter{
		builders: make(map[string]objecttreebuilder.TreeBuilder),
		trees:    make(map[string]objecttree.ObjectTree),
	}
}

func (t *TreeManagerAdapter) Init(_ *app.App) error {
	return nil
}

func (t *TreeManagerAdapter) Name() string {
	return treemanager.CName
}

func (t *TreeManagerAdapter) Run(_ context.Context) error {
	return nil
}

func (t *TreeManagerAdapter) Close(_ context.Context) error {
	return nil
}

// RegisterBuilder registers a per-space tree builder so incoming sync requests
// can resolve user trees. Called by SpaceImpl.ensure() after the commonspace is initialized.
func (t *TreeManagerAdapter) RegisterBuilder(spaceId string, builder objecttreebuilder.TreeBuilder) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.builders[spaceId] = builder
}

// UnregisterBuilder removes the builder for a space. Called by SpaceImpl.Close().
func (t *TreeManagerAdapter) UnregisterBuilder(spaceId string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.builders, spaceId)
}

func treeKey(spaceId, treeId string) string {
	return spaceId + "/" + treeId
}

// RegisterTree caches an open tree instance so GetTree returns it instead of
// creating a new one. Called by SpaceImpl when opening an object.
func (t *TreeManagerAdapter) RegisterTree(spaceId, treeId string, tree objecttree.ObjectTree) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.trees[treeKey(spaceId, treeId)] = tree
}

// UnregisterTree removes a cached tree instance. Called by SpaceImpl when
// closing an object or space.
func (t *TreeManagerAdapter) UnregisterTree(spaceId, treeId string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.trees, treeKey(spaceId, treeId))
}

// GetTree resolves a tree by first checking cached instances, then
// delegating to the registered per-space tree builder.
// This is called by the objectManager when incoming sync requests need to find user trees.
func (t *TreeManagerAdapter) GetTree(ctx context.Context, spaceId, treeId string) (objecttree.ObjectTree, error) {
	t.mu.RLock()
	if tree, ok := t.trees[treeKey(spaceId, treeId)]; ok {
		t.mu.RUnlock()
		fmt.Printf("[TREEMGR-DEBUG] GetTree: found cached treeId=%s\n", treeId)
		return tree, nil
	}
	builder, ok := t.builders[spaceId]
	t.mu.RUnlock()
	if !ok {
		return nil, errTreeManagerNotSupported
	}
	fmt.Printf("[TREEMGR-DEBUG] GetTree: building tree (background sync) treeId=%s\n", treeId)
	return builder.BuildTree(ctx, treeId, objecttreebuilder.BuildTreeOpts{})
}

func (t *TreeManagerAdapter) ValidateAndPutTree(_ context.Context, _ string, _ treestorage.TreeStorageCreatePayload) error {
	return nil
}

func (t *TreeManagerAdapter) MarkTreeDeleted(_ context.Context, _, _ string) error {
	return nil
}

func (t *TreeManagerAdapter) DeleteTree(_ context.Context, _, _ string) error {
	return nil
}

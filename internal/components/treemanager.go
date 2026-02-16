package components

import (
	"context"
	"errors"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/commonspace/object/treemanager"
)

var errTreeManagerNotSupported = errors.New("tree manager: operation not supported at top level")

type TreeManagerAdapter struct{}

func NewTreeManager() *TreeManagerAdapter {
	return &TreeManagerAdapter{}
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

func (t *TreeManagerAdapter) GetTree(_ context.Context, _, _ string) (objecttree.ObjectTree, error) {
	return nil, errTreeManagerNotSupported
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

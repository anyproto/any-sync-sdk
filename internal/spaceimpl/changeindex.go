package spaceimpl

import (
	"context"

	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/space"
)

// changeIndexAPI implements space.ChangeIndexAPI by delegating to the
// per-space spaceobjects.Store, which owns the _meta query and the live
// feed registry.
type changeIndexAPI struct {
	parent *spaceImpl
}

func newChangeIndexAPI(parent *spaceImpl) *changeIndexAPI { return &changeIndexAPI{parent: parent} }

func (c *changeIndexAPI) MaxApplySeq(ctx context.Context) (uint64, error) {
	return c.parent.store.MaxApplySeq(ctx)
}

func (c *changeIndexAPI) ChangedSince(ctx context.Context, since uint64, limit int) ([]space.ObjectChange, error) {
	rows, err := c.parent.store.ChangedObjects(ctx, since, limit)
	if err != nil {
		return nil, err
	}
	out := make([]space.ObjectChange, len(rows))
	for i, r := range rows {
		out[i] = space.ObjectChange{ObjectId: r.ObjectId, ApplySeq: r.ApplySeq}
	}
	return out, nil
}

func (c *changeIndexAPI) Subscribe(cb func(space.ObjectChange)) (cancel func()) {
	return c.parent.store.SubscribeChanges(func(ev spaceobjects.ObjectChange) {
		cb(space.ObjectChange{ObjectId: ev.ObjectId, ApplySeq: ev.ApplySeq})
	})
}

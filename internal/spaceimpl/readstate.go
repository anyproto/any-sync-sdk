package spaceimpl

import (
	"context"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/readstate"
	"github.com/anyproto/any-sync-sdk/space"
)

// readStateAPI implements space.ReadStateAPI over the per-space
// readstate engine (queries, subscription) and the SDK-level readsync
// service (marks — which also publish the frontier to the account's
// other devices).
type readStateAPI struct {
	parent *spaceImpl
}

func newReadStateAPI(parent *spaceImpl) *readStateAPI { return &readStateAPI{parent: parent} }

func (r *readStateAPI) engine() *readstate.Engine {
	return r.parent.store.ReadState()
}

func (r *readStateAPI) Subscribe(cb func(objectId string, stateSeq uint64)) (cancel func()) {
	eng := r.engine()
	if eng == nil {
		return func() {}
	}
	return eng.SubscribeState(cb)
}

func (r *readStateAPI) ChangedSince(ctx context.Context, since uint64, limit int) ([]space.ReadTransition, error) {
	eng := r.engine()
	if eng == nil {
		return nil, space.ErrReadTrackingDisabled
	}
	trs, err := eng.TransitionsSince(ctx, since, limit)
	if err != nil {
		return nil, err
	}
	out := make([]space.ReadTransition, len(trs))
	for i, tr := range trs {
		out[i] = space.ReadTransition{
			ObjectId:  tr.ObjectId,
			Dataset:   tr.Dataset,
			ChangeId:  tr.ChangeId,
			VersionId: crdt.VersionId(tr.VersionId),
			RecordIds: tr.RecordIds,
			Tags:      tr.Tags,
			Unread:    tr.Unread,
			StateSeq:  tr.StateSeq,
		}
	}
	return out, nil
}

func (r *readStateAPI) UnreadSnapshot(ctx context.Context, objectId string) ([]space.ReadTransition, uint64, error) {
	eng := r.engine()
	if eng == nil {
		return nil, 0, space.ErrReadTrackingDisabled
	}
	entries, stateSeq, err := eng.UnreadEntries(ctx, objectId)
	if err != nil {
		return nil, 0, err
	}
	out := make([]space.ReadTransition, len(entries))
	for i, en := range entries {
		out[i] = space.ReadTransition{
			ObjectId:  en.ObjectId,
			Dataset:   en.Dataset,
			ChangeId:  en.ChangeId,
			VersionId: crdt.VersionId(en.VersionId),
			AddSeq:    en.AddSeq,
			ApplySeq:  en.ApplySeq,
			RecordIds: en.RecordIds,
			Tags:      en.Tags,
			Unread:    true,
			StateSeq:  en.StateSeq,
		}
	}
	return out, stateSeq, nil
}

func (r *readStateAPI) UnreadCounts(ctx context.Context, objectId string) (map[string]int, error) {
	eng := r.engine()
	if eng == nil {
		return nil, space.ErrReadTrackingDisabled
	}
	return eng.Counts(ctx, objectId)
}

func (r *readStateAPI) MarkRead(ctx context.Context, objectId string, changeIds []string) error {
	rs := r.parent.parent.readSync()
	if rs == nil || r.engine() == nil {
		return space.ErrReadTrackingDisabled
	}
	_, err := rs.MarkRead(ctx, r.parent.id, objectId, changeIds)
	return err
}

func (r *readStateAPI) MarkReadUpTo(ctx context.Context, objectId string, upTo crdt.VersionId) error {
	rs := r.parent.parent.readSync()
	if rs == nil || r.engine() == nil {
		return space.ErrReadTrackingDisabled
	}
	_, err := rs.MarkReadUpTo(ctx, r.parent.id, objectId, string(upTo))
	return err
}

func (r *readStateAPI) Generation(ctx context.Context) (string, error) {
	return r.parent.store.Generation(ctx)
}

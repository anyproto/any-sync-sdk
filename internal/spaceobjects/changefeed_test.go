package spaceobjects

import (
	"context"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// feedStore builds a minimal Store wired only with what the change feed
// needs: a temp DB, a spaceId, and the registry. The query methods touch
// nothing else.
func feedStore(t *testing.T) (context.Context, *Store) {
	t.Helper()
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "feed.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return ctx, &Store{db: db, spaceId: "spaceA", changeSubs: newChangeRegistry()}
}

func TestChangeRegistry_DispatchAndCancel(t *testing.T) {
	_, s := feedStore(t)

	var got []ObjectChange
	cancel := s.SubscribeChanges(func(ev ObjectChange) { got = append(got, ev) })
	assert.True(t, s.changeSubs.hasSubscribers())

	s.changeSubs.dispatch(ObjectChange{ObjectId: "o1", AddSeq: 7})
	s.changeSubs.dispatch(ObjectChange{ObjectId: "o2", AddSeq: 8})
	require.Equal(t, []ObjectChange{{"o1", 7}, {"o2", 8}}, got)

	cancel()
	assert.False(t, s.changeSubs.hasSubscribers())
	s.changeSubs.dispatch(ObjectChange{ObjectId: "o3", AddSeq: 9})
	assert.Len(t, got, 2, "no delivery after cancel")

	cancel() // idempotent
}

func TestChangeRegistry_NilCallbackNoOp(t *testing.T) {
	_, s := feedStore(t)
	cancel := s.SubscribeChanges(nil)
	assert.False(t, s.changeSubs.hasSubscribers())
	cancel() // safe
}

func TestStoreChangedObjects_DelegatesToMeta(t *testing.T) {
	ctx, s := feedStore(t)
	coll, err := s.metaCollection(ctx)
	require.NoError(t, err)
	require.NoError(t, crdt.PersistMeta(ctx, coll, "o1", 5, nil, "spaceA"))
	require.NoError(t, crdt.PersistMeta(ctx, coll, "o2", 15, nil, "spaceA"))
	require.NoError(t, crdt.PersistMeta(ctx, coll, "other", 99, nil, "spaceB"))

	changed, err := s.ChangedObjects(ctx, 0, 0)
	require.NoError(t, err)
	require.Equal(t, []ObjectChange{{"o1", 5}, {"o2", 15}}, changed)

	max, err := s.MaxAddSeq(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(15), max)

	since, err := s.ChangedObjects(ctx, 5, 0)
	require.NoError(t, err)
	require.Equal(t, []ObjectChange{{"o2", 15}}, since)
}

package spaceobjects

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/fanout"
)

// purgeStore builds a Store wired with exactly what the purge path needs: a
// temp DB, the applySeq allocator (seeding from the _meta rows like the real
// store), the change-feed + row-event registries, and an ocache whose load
// func must never fire (purge never Gets).
func purgeStore(t *testing.T) (context.Context, *Store) {
	t.Helper()
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "purge.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	s := &Store{
		db:         db,
		spaceId:    "spaceA",
		changeSubs: fanout.New[ObjectChange](),
		rowEvents:  fanout.New[RowEvent](),
	}
	s.applySeqs = crdt.NewApplySeqAllocator(func(c context.Context) (uint64, error) {
		coll, err := s.applySeqMeta(c)
		if err != nil {
			return 0, err
		}
		return crdt.MaxObjectApplySeq(c, coll, s.spaceId)
	})
	s.cache = ocache.New(func(_ context.Context, id string) (ocache.Object, error) {
		return nil, fmt.Errorf("unexpected load %s", id)
	})
	t.Cleanup(func() { _ = s.cache.Close() })
	return ctx, s
}

// writeObjectRow materialises a shared `objects` row for id.
func writeObjectRow(t *testing.T, ctx context.Context, s *Store, id string) {
	t.Helper()
	coll, err := s.SharedObjects(ctx)
	require.NoError(t, err)
	_, err = coll.UpsertId(ctx, id, query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		v.Set("name", a.NewString("live"))
		return v, true, nil
	}))
	require.NoError(t, err)
}

func objectRowExists(t *testing.T, ctx context.Context, s *Store, id string) bool {
	t.Helper()
	coll, err := s.SharedObjects(ctx)
	require.NoError(t, err)
	_, err = coll.FindId(ctx, id)
	if errors.Is(err, anystore.ErrDocNotFound) {
		return false
	}
	require.NoError(t, err)
	return true
}

func TestPurgeObject_StampsDeletionAboveContentSeq(t *testing.T) {
	ctx, s := purgeStore(t)
	metaColl, err := s.metaCollection(ctx)
	require.NoError(t, err)

	// A live object whose last content change was applySeq 5.
	writeObjectRow(t, ctx, s, "o1")
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "o1", 5, 5, nil, "spaceA"))
	require.NoError(t, s.EnsureApplySeq(ctx)) // seed allocator to 5

	var live []ObjectChange
	cancel := s.SubscribeChanges(func(ev ObjectChange) { live = append(live, ev) })
	defer cancel()

	require.NoError(t, s.purgeObject(ctx, "o1"))

	// Row gone; deletion surfaces in the feed at a fresh seq > 5.
	assert.False(t, objectRowExists(t, ctx, s, "o1"))
	changed, err := s.ChangedObjects(ctx, 0, 0)
	require.NoError(t, err)
	require.Len(t, changed, 1)
	assert.Equal(t, "o1", changed[0].ObjectId)
	assert.True(t, changed[0].Deleted)
	assert.Greater(t, changed[0].ApplySeq, uint64(5))
	// Live dispatch mirrors the durable entry.
	require.Len(t, live, 1)
	assert.Equal(t, changed[0], live[0])
}

func TestPurgeObject_MetaExistsGate(t *testing.T) {
	ctx, s := purgeStore(t)
	metaColl, err := s.metaCollection(ctx)
	require.NoError(t, err)

	// Base-dataset-only object: a scoped _meta row, no `objects` row.
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "base", 2, 2, nil, "spaceA"))
	require.NoError(t, s.EnsureApplySeq(ctx))

	require.NoError(t, s.purgeObject(ctx, "base"))  // stamps via MetaExists
	require.NoError(t, s.purgeObject(ctx, "never")) // never materialised → nothing

	changed, err := s.ChangedObjects(ctx, 0, 0)
	require.NoError(t, err)
	require.Len(t, changed, 1, "only the materialised id announces a deletion")
	assert.Equal(t, "base", changed[0].ObjectId)
	assert.True(t, changed[0].Deleted)
}

func TestPurgeObject_DelStickyAcrossLateContentApply(t *testing.T) {
	ctx, s := purgeStore(t)
	metaColl, err := s.metaCollection(ctx)
	require.NoError(t, err)

	writeObjectRow(t, ctx, s, "o1")
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "o1", 5, 5, nil, "spaceA"))
	require.NoError(t, s.EnsureApplySeq(ctx))
	require.NoError(t, s.purgeObject(ctx, "o1"))

	// A late content apply (any-sync normally forbids it, but a change
	// already in the drainer can land) re-bumps `as`; it must NOT clear del.
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "o1", 6, 99, nil, "spaceA"))

	changed, err := s.ChangedObjects(ctx, 0, 0)
	require.NoError(t, err)
	require.Len(t, changed, 1)
	assert.Equal(t, uint64(99), changed[0].ApplySeq)
	assert.True(t, changed[0].Deleted, "del is sticky — the entry still evicts")
}

func TestPurgeObjects_Batch(t *testing.T) {
	ctx, s := purgeStore(t)
	metaColl, err := s.metaCollection(ctx)
	require.NoError(t, err)

	writeObjectRow(t, ctx, s, "live")
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "live", 5, 5, nil, "spaceA"))
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "base", 3, 3, nil, "spaceA")) // meta only
	require.NoError(t, s.EnsureApplySeq(ctx))

	require.NoError(t, s.PurgeObjects(ctx, []string{"live", "base", "never"}))

	assert.False(t, objectRowExists(t, ctx, s, "live"))
	changed, err := s.ChangedObjects(ctx, 0, 0)
	require.NoError(t, err)
	require.Len(t, changed, 2, "live + base announce; never does not")
	ids := map[string]bool{}
	for _, c := range changed {
		assert.True(t, c.Deleted)
		assert.Greater(t, c.ApplySeq, uint64(5))
		ids[c.ObjectId] = true
	}
	assert.Equal(t, map[string]bool{"live": true, "base": true}, ids)
}

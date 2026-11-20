package spaceobjects

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree/updatelistener"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/fanout"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/readstate"
	"github.com/anyproto/any-sync-sdk/internal/schema"
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
	s.gaps = NewGapSet()
	s.gaps.Set("o1", true)

	require.NoError(t, s.purgeObject(ctx, "o1"))
	assert.False(t, s.gaps.Has("o1"), "a purged object has nothing left to catch up")

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

func TestPurgeObject_MetaStatusGate(t *testing.T) {
	ctx, s := purgeStore(t)
	metaColl, err := s.metaCollection(ctx)
	require.NoError(t, err)

	// Base-dataset-only object: a scoped _meta row, no `objects` row.
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "base", 2, 2, nil, "spaceA"))
	require.NoError(t, s.EnsureApplySeq(ctx))

	require.NoError(t, s.purgeObject(ctx, "base"))  // stamps via MetaStatus
	require.NoError(t, s.purgeObject(ctx, "never")) // never materialised → nothing

	changed, err := s.ChangedObjects(ctx, 0, 0)
	require.NoError(t, err)
	require.Len(t, changed, 1, "only the materialised id announces a deletion")
	assert.Equal(t, "base", changed[0].ObjectId)
	assert.True(t, changed[0].Deleted)

	// A second purge of the deleted object stamps nothing.
	var announced []ObjectChange
	defer s.SubscribeChanges(func(c ObjectChange) { announced = append(announced, c) })()
	require.NoError(t, s.purgeObject(ctx, "base"))
	again, err := s.ChangedObjects(ctx, 0, 0)
	require.NoError(t, err)
	require.Len(t, again, 1)
	assert.Equal(t, changed[0].ApplySeq, again[0].ApplySeq, "the mark keeps its applySeq")
	assert.Empty(t, announced, "nothing is announced again")
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

// A reconcile pass over an object already purged and marked reclaims
// what the first purge's best-effort tail left behind, without stamping
// or announcing the deletion again.
func TestPurgeObjects_ReclaimsLeftoversOfAMarkedObject(t *testing.T) {
	ctx, s := purgeStore(t)
	metaColl, err := s.metaCollection(ctx)
	require.NoError(t, err)
	writeObjectRow(t, ctx, s, "o1")
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "o1", 5, 5, nil, "spaceA"))
	require.NoError(t, s.EnsureApplySeq(ctx))
	require.NoError(t, s.purgeObject(ctx, "o1"))
	before, err := s.ChangedObjects(ctx, 0, 0)
	require.NoError(t, err)
	require.Len(t, before, 1)

	// A leftover the first tail did not drop.
	_, err = s.db.Collection(ctx, "o1_blocks")
	require.NoError(t, err)

	require.NoError(t, s.PurgeObjects(ctx, []string{"o1"}))
	_, err = s.db.OpenCollection(ctx, "o1_blocks")
	assert.True(t, errors.Is(err, anystore.ErrCollectionNotFound), "the leftover is reclaimed")
	after, err := s.ChangedObjects(ctx, 0, 0)
	require.NoError(t, err)
	require.Len(t, after, 1)
	assert.Equal(t, before[0].ApplySeq, after[0].ApplySeq, "no second deletion mark")
}

// A repeated id announces the deletion its first occurrence stamped.
func TestPurgeObjects_RepeatedIdAnnouncesOnce(t *testing.T) {
	ctx, s := purgeStore(t)
	metaColl, err := s.metaCollection(ctx)
	require.NoError(t, err)
	writeObjectRow(t, ctx, s, "o1")
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "o1", 5, 5, nil, "spaceA"))
	require.NoError(t, s.EnsureApplySeq(ctx))

	var announced []ObjectChange
	defer s.SubscribeChanges(func(c ObjectChange) { announced = append(announced, c) })()
	require.NoError(t, s.PurgeObjects(ctx, []string{"o1", "o1"}))
	require.Len(t, announced, 1)
	assert.True(t, announced[0].Deleted)
	assert.Greater(t, announced[0].ApplySeq, uint64(5))
}

// trackRead records one unread change for objectId through a read-state
// engine on the store's DB, as the apply path would.
func trackRead(t *testing.T, ctx context.Context, s *Store, objectId, changeId string) *readstate.Engine {
	t.Helper()
	eng := readstate.New(s.db, s.spaceId, s.applySeqs.Next, nil, nil)
	require.NoError(t, eng.Open(ctx))
	require.NoError(t, eng.TrackChange(ctx, readstate.Track{
		ObjectId: objectId, ChangeId: changeId, VersionId: "v" + changeId,
		AddSeq: 1, ApplySeq: 1, RecordIds: []string{"r1"}, Tags: []string{"msg"}, Tracked: true,
	}))
	return eng
}

func unreadOf(t *testing.T, ctx context.Context, eng *readstate.Engine, objectId string) int {
	t.Helper()
	entries, _, err := eng.UnreadEntries(ctx, objectId)
	require.NoError(t, err)
	return len(entries)
}

func parkedOf(t *testing.T, ctx context.Context, s *Store, objectId string) int {
	t.Helper()
	n := 0
	require.NoError(t, s.IterDetached(ctx, func(row DetachedRow) bool {
		if row.ObjectId == objectId {
			n++
		}
		return true
	}))
	return n
}

// A purge clears the object's read state in the purge transaction and
// its parked changes in the tail, and runs the purge hook while the
// object's collections are still there. Other objects keep theirs.
func TestPurgeObject_ClearsReadStateParkedAndRunsHook(t *testing.T) {
	ctx, s := purgeStore(t)
	metaColl, err := s.metaCollection(ctx)
	require.NoError(t, err)
	writeObjectRow(t, ctx, s, "o1")
	writeObjectRow(t, ctx, s, "o2")
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "o1", 5, 5, nil, "spaceA"))
	require.NoError(t, s.EnsureApplySeq(ctx))
	eng := trackRead(t, ctx, s, "o1", "c1")
	trackRead(t, ctx, s, "o2", "c2")
	ran, err := eng.SeedFrontier(ctx, "o1", []string{"c1"})
	require.NoError(t, err)
	require.True(t, ran)
	// A never-materialized id (no row, no meta) with a parked change.
	for _, id := range []string{"o1", "o2", "ghost"} {
		require.NoError(t, s.Park(ctx, DetachedRow{ChangeId: "ch-" + id, SpaceId: "spaceA", ObjectId: id, Payload: []byte("x")}))
	}
	for _, id := range []string{"o1", "o2"} {
		_, err = s.db.Collection(ctx, id+"_blocks")
		require.NoError(t, err)
	}

	var hooked []string
	s.SetPurgeHook(func(ctx context.Context, objectId string) {
		if objectId != "ghost" {
			_, err := s.db.OpenCollection(ctx, objectId+"_blocks")
			assert.NoError(t, err, "the hook runs before the object's collections drop")
		}
		assert.False(t, objectRowExists(t, ctx, s, objectId), "the hook runs after the purge committed")
		hooked = append(hooked, objectId)
	})
	stateColl, err := s.db.OpenCollection(ctx, readstate.StateCollectionName)
	require.NoError(t, err)

	require.NoError(t, s.purgeObject(ctx, "o1"))

	assert.Equal(t, []string{"o1"}, hooked)
	assert.Zero(t, unreadOf(t, ctx, eng, "o1"))
	_, err = stateColl.FindId(ctx, "o1")
	assert.True(t, errors.Is(err, anystore.ErrDocNotFound), "the state row is gone")
	seeded, err := eng.Seeded(ctx, "o1")
	require.NoError(t, err)
	assert.False(t, seeded, "a re-materialized id seeds again")
	assert.Equal(t, 1, unreadOf(t, ctx, eng, "o2"))
	assert.Zero(t, parkedOf(t, ctx, s, "o1"))
	assert.Equal(t, 1, parkedOf(t, ctx, s, "o2"))
	_, err = s.db.OpenCollection(ctx, "o1_blocks")
	assert.True(t, errors.Is(err, anystore.ErrCollectionNotFound))

	// The batch path does the same per id; the hook and the unpark run
	// for a never-materialized id too.
	require.NoError(t, s.PurgeObjects(ctx, []string{"o2", "ghost"}))
	assert.Equal(t, []string{"o1", "o2", "ghost"}, hooked)
	assert.Zero(t, unreadOf(t, ctx, eng, "o2"))
	assert.Zero(t, parkedOf(t, ctx, s, "o2"))
	assert.Zero(t, parkedOf(t, ctx, s, "ghost"))
	changed, err := s.ChangedObjects(ctx, 0, 0)
	require.NoError(t, err)
	ids := map[string]bool{}
	for _, c := range changed {
		ids[c.ObjectId] = true
	}
	assert.Equal(t, map[string]bool{"o1": true, "o2": true}, ids, "only materialized ids announce a deletion")
}

// deletedStubTree is a tree another caller has already deleted.
type deletedStubTree struct{ closableStubTree }

func (s *deletedStubTree) Delete() error { return synctree.ErrSyncTreeDeleted }

// Objects.Delete and any-sync's deletion loop both call DeleteTree for
// one tree. The call that finds the tree deleted already still purges,
// so it never returns while the object's row is live.
func TestDeleteTree_PurgesATreeDeletedAlready(t *testing.T) {
	t.Run("load refused", func(t *testing.T) {
		ctx, s := purgeStore(t)
		writeObjectRow(t, ctx, s, "o1")
		_ = s.cache.Close()
		s.cache = ocache.New(func(context.Context, string) (ocache.Object, error) {
			return nil, fmt.Errorf("load: %w", spacestorage.ErrTreeStorageAlreadyDeleted)
		})
		require.NoError(t, s.DeleteTree(ctx, "o1"))
		assert.False(t, objectRowExists(t, ctx, s, "o1"))
	})
	t.Run("tree deleted under a cached object", func(t *testing.T) {
		ctx, s := purgeStore(t)
		writeObjectRow(t, ctx, s, "o1")
		ctrl, err := crdt.NewController(ctx, "o1", s.db, crdt.HandlerReg{Name: "blocks", Handler: &crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}})
		require.NoError(t, err)
		obj, err := object.New(object.Config{SpaceId: "spaceA", Controller: ctrl},
			func(updatelistener.UpdateListener) (objecttree.ObjectTree, error) {
				return &deletedStubTree{closableStubTree{stubTree{id: "o1"}}}, nil
			})
		require.NoError(t, err)
		_ = s.cache.Close()
		s.cache = ocache.New(func(context.Context, string) (ocache.Object, error) { return obj, nil })
		require.NoError(t, s.DeleteTree(ctx, "o1"))
		assert.False(t, objectRowExists(t, ctx, s, "o1"))
	})
}

// A caller arriving while a purge of the object runs returns only after
// that purge's reclaim finished, which removes the object's
// shared-dataset rows; its own purge is a repeat and stamps, announces
// and reclaims nothing.
func TestPurgeAndDrop_SecondCallerWaitsForTheRunningReclaim(t *testing.T) {
	ctx, s := purgeStore(t)
	metaColl, err := s.metaCollection(ctx)
	require.NoError(t, err)
	writeObjectRow(t, ctx, s, "o1")
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "o1", 5, 5, nil, "spaceA"))
	require.NoError(t, s.EnsureApplySeq(ctx))

	var (
		hooks   atomic.Int32
		entered = make(chan struct{})
		release = make(chan struct{})
	)
	s.SetPurgeHook(func(context.Context, string) {
		if hooks.Add(1) == 1 {
			close(entered)
			<-release
		}
	})
	first := make(chan error, 1)
	go func() { first <- s.MarkTreeDeleted(ctx, "o1") }()
	<-entered
	second := make(chan error, 1)
	go func() { second <- s.MarkTreeDeleted(ctx, "o1") }()
	select {
	case err := <-second:
		t.Fatalf("the second caller returned during the first one's reclaim: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	require.NoError(t, <-first)
	require.NoError(t, <-second)
	assert.Equal(t, int32(1), hooks.Load(), "reclaimed once")
	changed, err := s.ChangedObjects(ctx, 0, 0)
	require.NoError(t, err)
	require.Len(t, changed, 1)
}

// A purge repeating one that ran (Objects.Delete and the deletion loop
// both purge each object) removes, stamps and reclaims nothing; leftovers
// of a cut-short reclaim are the boot reconcile's (see
// TestPurgeObjects_ReclaimsLeftoversOfAMarkedObject).
func TestPurgeAndDrop_ARepeatSkipsTheReclaim(t *testing.T) {
	ctx, s := purgeStore(t)
	metaColl, err := s.metaCollection(ctx)
	require.NoError(t, err)
	writeObjectRow(t, ctx, s, "o1")
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "o1", 5, 5, nil, "spaceA"))
	require.NoError(t, s.EnsureApplySeq(ctx))
	var hooks atomic.Int32
	s.SetPurgeHook(func(context.Context, string) { hooks.Add(1) })

	require.NoError(t, s.MarkTreeDeleted(ctx, "o1"))
	require.NoError(t, s.MarkTreeDeleted(ctx, "o1"))
	assert.Equal(t, int32(1), hooks.Load(), "reclaimed once")
	changed, err := s.ChangedObjects(ctx, 0, 0)
	require.NoError(t, err)
	assert.Len(t, changed, 1, "announced once")
}

package spaceobjects

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree/updatelistener"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/subscribe"
	"github.com/anyproto/any-sync-sdk/internal/types"
)

// sharedDatasetStore boots a store whose catalog holds one shared
// records dataset and one per-object one on the same type.
func sharedDatasetStore(t *testing.T) (context.Context, *Store, string, string) {
	t.Helper()
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "shared.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	seedTypeObject(t, ctx, db, "spaceA", catTypeId)
	seedSharedDatasetDefs(t, ctx, db, catTypeId, "samples", "a")
	seedDatasetDefs(t, ctx, db, catTypeId, "notes", "b")

	store := NewStore(nil, db, nil, "spaceA", nil, nil, nil, nil)
	t.Cleanup(func() { _ = store.Close() })
	return ctx, store, types.CollectionName(catTypeId, "samples"), types.CollectionName(catTypeId, "notes")
}

// writeTitle upserts record recId with a title through the object's
// controller, as an applied change does.
func writeTitle(t *testing.T, ctx context.Context, ctrl *crdt.Controller, objectId, dataset, recId, ver, title string) {
	t.Helper()
	a := &anyenc.Arena{}
	fields := a.NewObject()
	fields.Set("title", a.NewString(title))
	require.NoError(t, ctrl.ApplyChange(ctx, crdt.Change{
		ObjectId: objectId, Dataset: dataset, ChangeId: "ch-" + objectId + "-" + recId + "-" + ver,
		VersionId: crdt.VersionId(ver), DataVersion: "dv", Timestamp: 1,
		Records: []crdt.RecordChange{{Id: recId, Upsert: true,
			Ops: []crdt.Op{{Type: crdt.OpSet, Payload: fields}}}},
	}))
}

func collectionIds(t *testing.T, ctx context.Context, coll anystore.Collection) []string {
	t.Helper()
	iter, err := coll.Find(nil).Sort(crdt.IdField).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var out []string
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		out = append(out, doc.Value().GetString(crdt.IdField))
	}
	return out
}

// A shared dataset registers keyed, every controller of the space
// writes one per-space collection, and discovery reports it.
func TestSharedDataset_RegistrationAndStorage(t *testing.T) {
	ctx, store, samples, notes := sharedDatasetStore(t)

	ds, ok := store.RuntimeDataset(samples)
	require.True(t, ok)
	assert.True(t, ds.Shared)
	assert.True(t, store.IsKeyedDataset(samples))
	assert.False(t, store.IsKeyedDataset(notes))
	assert.Equal(t, []string{samples}, store.keyedDatasets())

	regs, sharedNames, err := store.buildRegs()
	require.NoError(t, err)
	assert.Equal(t, []string{samples}, crdt.KeyedNames(regs))
	assert.NotContains(t, sharedNames, samples, "a keyed dataset is not a one-row-per-object collection")

	var listed bool
	for _, ns := range store.Schemas() {
		switch ns.Name {
		case samples:
			listed = true
			assert.True(t, ns.Shared)
		case notes:
			assert.False(t, ns.Shared)
		}
	}
	assert.True(t, listed)

	c1, err := store.newController(ctx, "obj1")
	require.NoError(t, err)
	c2, err := store.newController(ctx, "obj2")
	require.NoError(t, err)
	writeTitle(t, ctx, c1, "obj1", samples, "r1", "v1", "one")
	writeTitle(t, ctx, c2, "obj2", samples, "r1", "v1", "two")
	writeTitle(t, ctx, c1, "obj1", notes, "n1", "v2", "note")

	coll, err := store.keyedCollection(ctx, samples)
	require.NoError(t, err)
	assert.Equal(t, "spaceA_"+samples, coll.Name())
	assert.Equal(t, []string{"obj1/r1", "obj2/r1"}, collectionIds(t, ctx, coll))
	assert.Equal(t, "one", c1.Get(ctx, samples, "r1").GetString("title"))
	assert.Equal(t, "obj2", c2.Get(ctx, samples, "r1").GetString(crdt.ObjectIdField))

	// The per-object dataset keeps its own collection and plain ids.
	perObject, err := store.OpenObjectCollection(ctx, "obj1", notes)
	require.NoError(t, err)
	assert.Equal(t, []string{"n1"}, collectionIds(t, ctx, perObject))
}

// Purging an object deletes its rows from the shared dataset's
// collection and leaves every other object's.
func TestSharedDataset_PurgeDeletesTheObjectsRows(t *testing.T) {
	ctx, store, samples, _ := sharedDatasetStore(t)
	for _, objectId := range []string{"obj1", "obj10", "obj2"} {
		ctrl, err := store.newController(ctx, objectId)
		require.NoError(t, err)
		writeTitle(t, ctx, ctrl, objectId, samples, "r1", "v1", objectId)
		writeTitle(t, ctx, ctrl, objectId, samples, "r2", "v2", objectId)
	}
	coll, err := store.keyedCollection(ctx, samples)
	require.NoError(t, err)

	require.NoError(t, store.purgeObject(ctx, "obj1"))
	assert.Equal(t, []string{"obj10/r1", "obj10/r2", "obj2/r1", "obj2/r2"}, collectionIds(t, ctx, coll),
		"an object whose id extends the purged one keeps its rows")

	// The batch purge takes the same path, and a second purge is a no-op.
	require.NoError(t, store.PurgeObjects(ctx, []string{"obj2", "obj1"}))
	assert.Equal(t, []string{"obj10/r1", "obj10/r2"}, collectionIds(t, ctx, coll))
}

// A purge tells live queries which rows went: the reader across objects
// and the purged object's own, not another object's.
func TestSharedDataset_PurgeNotifiesSubscribers(t *testing.T) {
	ctx, store, samples, _ := sharedDatasetStore(t)
	for _, objectId := range []string{"obj1", "obj2"} {
		ctrl, err := store.newController(ctx, objectId)
		require.NoError(t, err)
		writeTitle(t, ctx, ctrl, objectId, samples, "r1", "v1", objectId)
		writeTitle(t, ctx, ctrl, objectId, samples, "r2", "v2", objectId)
	}
	coll, ok, err := store.KeyedCollection(ctx, samples)
	require.NoError(t, err)
	require.True(t, ok)

	all := subscribeKeyedRows(t, ctx, store, coll, subscribe.Scope{AllObjects: true, Dataset: samples}, nil)
	own := subscribeKeyedRows(t, ctx, store, coll, subscribe.Scope{ObjectId: "obj1", Dataset: samples}, crdt.KeyedRows("obj1"))
	other := subscribeKeyedRows(t, ctx, store, coll, subscribe.Scope{ObjectId: "obj2", Dataset: samples}, crdt.KeyedRows("obj2"))

	require.NoError(t, store.purgeObject(ctx, "obj1"))

	for _, sub := range []*subscribe.Sub{all, own} {
		waitCtx, cancel := context.WithTimeout(ctx, time.Second)
		ev, err := sub.Events().WaitOne(waitCtx)
		cancel()
		require.NoError(t, err)
		var removed []string
		for _, r := range ev.Removed {
			removed = append(removed, r.Id)
		}
		assert.ElementsMatch(t, []string{"obj1/r1", "obj1/r2"}, removed)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = other.Events().WaitOne(waitCtx)
	require.Error(t, err, "another object's reader hears nothing")
}

// KeyedCollection serves a shared dataset only.
func TestSharedDataset_KeyedCollection(t *testing.T) {
	ctx, store, samples, notes := sharedDatasetStore(t)
	coll, ok, err := store.KeyedCollection(ctx, samples)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "spaceA_"+samples, coll.Name())

	for _, dataset := range []string{notes, "unknown", "objects"} {
		coll, ok, err := store.KeyedCollection(ctx, dataset)
		require.NoError(t, err, dataset)
		assert.False(t, ok, dataset)
		assert.Nil(t, coll, dataset)
	}
}

// A re-index wipe clears the object's rows in the shared dataset's
// collection, keeps the collection, and keeps other objects' rows.
func TestSharedDataset_ReindexWipeClearsOnlyThisObject(t *testing.T) {
	ctx, store, samples, notes := sharedDatasetStore(t)
	c1, err := store.newController(ctx, "obj1")
	require.NoError(t, err)
	c2, err := store.newController(ctx, "obj2")
	require.NoError(t, err)
	writeTitle(t, ctx, c1, "obj1", samples, "r1", "v1", "one")
	writeTitle(t, ctx, c1, "obj1", notes, "n1", "v2", "note")
	writeTitle(t, ctx, c2, "obj2", samples, "r1", "v1", "two")

	require.NoError(t, store.wipeMaterialized(ctx, "obj1", c1))

	coll, err := store.keyedCollection(ctx, samples)
	require.NoError(t, err)
	assert.Equal(t, []string{"obj2/r1"}, collectionIds(t, ctx, coll))
	_, err = store.OpenObjectCollection(ctx, "obj1", notes)
	assert.ErrorIs(t, err, anystore.ErrCollectionNotFound, "the per-object collection is dropped")

	// The replay writes through the same handle.
	writeTitle(t, ctx, c1, "obj1", samples, "r1", "v1", "one")
	assert.Equal(t, []string{"obj1/r1", "obj2/r1"}, collectionIds(t, ctx, coll))
}

// subscribeKeyedRows registers a live query on the store's engine whose
// snapshot is the rows of coll matching rows (all of them for nil).
func subscribeKeyedRows(t *testing.T, ctx context.Context, store *Store, coll anystore.Collection, scope subscribe.Scope, rows query.Filter) *subscribe.Sub {
	t.Helper()
	sub, err := store.engine.Subscribe(subscribe.SubConfig{Scope: scope}, func(yield func(string, *anyenc.Value)) error {
		iter, err := coll.Find(rows).Iter(ctx)
		if err != nil {
			return err
		}
		defer iter.Close()
		for iter.Next() {
			doc, err := iter.Doc()
			if err != nil {
				return err
			}
			yield(doc.Value().GetString(crdt.IdField), doc.Value())
		}
		return iter.Err()
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })
	return sub
}

// removedIds reads sub's events until it has heard want removals.
func removedIds(t *testing.T, ctx context.Context, sub *subscribe.Sub, want int) []string {
	t.Helper()
	var out []string
	for len(out) < want {
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		ev, err := sub.Events().WaitOne(waitCtx)
		cancel()
		require.NoError(t, err, "heard %d of %d removals", len(out), want)
		for _, r := range ev.Removed {
			out = append(out, r.Id)
		}
	}
	return out
}

// assertQuiet checks that sub hears nothing for a short while.
func assertQuiet(t *testing.T, ctx context.Context, sub *subscribe.Sub) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err := sub.Events().WaitOne(waitCtx)
	require.Error(t, err, "the subscription hears nothing")
}

// A purge clears the rows of a shared dataset whose definition left the
// catalog after the store opened its collection. A store that never
// opened it leaves them, as documented.
func TestSharedDataset_PurgeCoversARemovedDefinition(t *testing.T) {
	ctx, store, samples, _ := sharedDatasetStore(t)
	for _, objectId := range []string{"obj1", "obj2"} {
		ctrl, err := store.newController(ctx, objectId)
		require.NoError(t, err)
		writeTitle(t, ctx, ctrl, objectId, samples, "r1", "v1", objectId)
	}
	coll, err := store.keyedCollection(ctx, samples)
	require.NoError(t, err)

	removeDef(t, ctx, store.db, catTypeId, catTypeId+"-head-samples", "z1")
	store.refreshType(ctx, catTypeId)
	require.False(t, store.IsKeyedDataset(samples), "the definition is gone")
	require.NotContains(t, store.keyedDatasets(), samples)

	require.NoError(t, store.purgeObject(ctx, "obj1"))
	assert.Equal(t, []string{"obj2/r1"}, collectionIds(t, ctx, coll))

	fresh := NewStore(nil, store.db, nil, "spaceA", nil, nil, nil, nil)
	t.Cleanup(func() { _ = fresh.Close() })
	require.NoError(t, fresh.purgeObject(ctx, "obj2"))
	assert.Equal(t, []string{"obj2/r1"}, collectionIds(t, ctx, coll))
}

// A purge deletes the object's rows in bounded chunks and tells live
// queries about every one of them; an object with no rows tells
// nothing.
func TestSharedDataset_PurgeInChunks(t *testing.T) {
	ctx, store, samples, _ := sharedDatasetStore(t)
	coll, err := store.keyedCollection(ctx, samples)
	require.NoError(t, err)

	// Rows straight into the collection: a purge reads the key range.
	n := keyedPurgeChunk + 1
	want := make([]string, 0, n)
	tx, err := store.db.WriteTx(ctx)
	require.NoError(t, err)
	a := &anyenc.Arena{}
	insert := func(objectId, id string) {
		a.Reset()
		doc := a.NewObject()
		doc.Set(crdt.IdField, a.NewString(id))
		doc.Set(crdt.ObjectIdField, a.NewString(objectId))
		require.NoError(t, coll.Insert(tx.Context(), doc))
	}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("obj1/r%05d", i)
		insert("obj1", id)
		want = append(want, id)
	}
	insert("obj2", "obj2/r1")
	require.NoError(t, tx.Commit())

	all := subscribeKeyedRows(t, ctx, store, coll, subscribe.Scope{AllObjects: true, Dataset: samples}, nil)
	other := subscribeKeyedRows(t, ctx, store, coll, subscribe.Scope{ObjectId: "obj2", Dataset: samples}, crdt.KeyedRows("obj2"))

	require.NoError(t, store.purgeObject(ctx, "obj1"))
	assert.Equal(t, []string{"obj2/r1"}, collectionIds(t, ctx, coll))
	got := removedIds(t, ctx, all, n)
	assert.ElementsMatch(t, want, got, "every purged row is told once")
	assertQuiet(t, ctx, other)

	// Nothing left of obj1: no write, nothing told.
	require.NoError(t, store.deleteKeyedRows(ctx, samples, "obj1"))
	assertQuiet(t, ctx, all)
}

// A re-index wipe tells live queries which of the object's rows went: a
// reader across objects holds them without the object loaded.
func TestSharedDataset_ReindexWipeNotifiesSubscribers(t *testing.T) {
	ctx, store, samples, _ := sharedDatasetStore(t)
	c1, err := store.newController(ctx, "obj1")
	require.NoError(t, err)
	c2, err := store.newController(ctx, "obj2")
	require.NoError(t, err)
	writeTitle(t, ctx, c1, "obj1", samples, "r1", "v1", "one")
	writeTitle(t, ctx, c1, "obj1", samples, "r2", "v2", "two")
	writeTitle(t, ctx, c2, "obj2", samples, "r1", "v1", "other")
	coll, err := store.keyedCollection(ctx, samples)
	require.NoError(t, err)

	all := subscribeKeyedRows(t, ctx, store, coll, subscribe.Scope{AllObjects: true, Dataset: samples}, nil)
	other := subscribeKeyedRows(t, ctx, store, coll, subscribe.Scope{ObjectId: "obj2", Dataset: samples}, crdt.KeyedRows("obj2"))

	require.NoError(t, store.wipeMaterialized(ctx, "obj1", c1))
	assert.ElementsMatch(t, []string{"obj1/r1", "obj1/r2"}, removedIds(t, ctx, all, 2))
	assertQuiet(t, ctx, other)
	assert.Equal(t, []string{"obj2/r1"}, collectionIds(t, ctx, coll))
}

// stubTree is the part of an object tree a device-local write touches.
type stubTree struct {
	objecttree.ObjectTree
	mu sync.Mutex
	id string
}

func (s *stubTree) Lock()                    { s.mu.Lock() }
func (s *stubTree) Unlock()                  { s.mu.Unlock() }
func (s *stubTree) Id() string               { return s.id }
func (s *stubTree) Root() *objecttree.Change { return nil }

// A re-index keeps the device-local values of the object's rows in a
// shared dataset, and leaves another object's rows as they were.
func TestSharedDataset_ReindexKeepsLocalValues(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "shared.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	seedTypeObject(t, ctx, db, "spaceA", catTypeId)
	seedSharedDatasetDefs(t, ctx, db, catTypeId, "samples", "a")
	seedFieldDef(t, ctx, db, catTypeId, "samples", "seen", "boolean", "local", "a9")
	store := NewStore(nil, db, nil, "spaceA", nil, nil, nil, nil)
	t.Cleanup(func() { _ = store.Close() })
	samples := types.CollectionName(catTypeId, "samples")

	setSeen := func(ctrl *crdt.Controller, objectId string) {
		a := &anyenc.Arena{}
		ch := crdt.Change{
			ObjectId: objectId, Dataset: samples, DataVersion: "dv", Timestamp: 1, Local: true,
			Records: []crdt.RecordChange{{Id: "r1", Ops: []crdt.Op{{Type: crdt.OpSet, Path: []string{"seen"}, Payload: a.NewTrue()}}}},
		}
		ch.VersionId = ctrl.NextLocalVersion(ctx, &ch)
		res, err := ctrl.ApplyChangeWithResult(ctx, ch)
		require.NoError(t, err)
		require.Empty(t, res.Rejections)
	}
	ctrls := map[string]*crdt.Controller{}
	for _, objectId := range []string{"obj1", "obj2"} {
		ctrl, err := store.newController(ctx, objectId)
		require.NoError(t, err)
		require.Equal(t, []string{"seen"}, ctrl.LocalFields(samples))
		writeTitle(t, ctx, ctrl, objectId, samples, "r1", "v1", objectId)
		setSeen(ctrl, objectId)
		ctrls[objectId] = ctrl
	}
	c1 := ctrls["obj1"]
	obj, err := object.New(object.Config{SpaceId: "spaceA", Controller: c1},
		func(updatelistener.UpdateListener) (objecttree.ObjectTree, error) { return &stubTree{id: "obj1"}, nil })
	require.NoError(t, err)
	coll, err := store.keyedCollection(ctx, samples)
	require.NoError(t, err)

	leaves, err := store.reindexPrepare(ctx, "obj1", c1, []string{samples})
	require.NoError(t, err)
	require.Len(t, leaves, 1)
	assert.Equal(t, []string{"obj2/r1"}, collectionIds(t, ctx, coll), "the wipe takes obj1's rows only")

	// The replay rebuilds the synced state, then the leaves come back.
	writeTitle(t, ctx, c1, "obj1", samples, "r1", "v1", "obj1")
	store.reindexFinish(ctx, obj, c1, leaves)

	assert.Equal(t, []string{"obj1/r1", "obj2/r1"}, collectionIds(t, ctx, coll), "no row beside the record's own")
	for _, objectId := range []string{"obj1", "obj2"} {
		row := ctrls[objectId].Get(ctx, samples, "r1")
		require.NotNil(t, row, objectId)
		assert.Equal(t, objectId, row.GetString("title"), objectId)
		assert.True(t, row.GetBool("seen"), "%s keeps its local value", objectId)
	}
}

// A shared dataset registers with a version of its own: an object whose
// rows a per-object registration of the same dataset materialized is
// stale, and its re-index moves them.
func TestSharedDataset_RegistrationVersion(t *testing.T) {
	ctx, store, samples, notes := sharedDatasetStore(t)
	regs := store.catalog.snapshot().regs
	assert.Equal(t, crdt.ComposeVersion(crdt.SchemaHandlerVersion, sharedLayoutVersion), regs[samples].Version)
	assert.Equal(t, crdt.SchemaHandlerVersion, regs[notes].Version)
	assert.NotEqual(t, regs[samples].Version, regs[notes].Version)

	meta, err := store.db.Collection(ctx, crdt.MetaCollectionName)
	require.NoError(t, err)
	require.NoError(t, crdt.PersistMeta(ctx, meta, "obj1", 1, 0,
		map[string]int{samples: crdt.SchemaHandlerVersion, notes: crdt.SchemaHandlerVersion}, "spaceA"))
	require.NoError(t, crdt.PersistMeta(ctx, meta, "obj2", 1, 0,
		map[string]int{samples: regs[samples].Version, notes: crdt.SchemaHandlerVersion}, "spaceA"))

	c1, err := store.newController(ctx, "obj1")
	require.NoError(t, err)
	assert.Equal(t, []string{samples}, c1.StaleDatasets(), "per-object rows of a shared dataset are stale")
	c2, err := store.newController(ctx, "obj2")
	require.NoError(t, err)
	assert.Empty(t, c2.StaleDatasets())
}

package spaceobjects

import (
	"context"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
)

// defsController opens the type object's definition datasets the way a
// seeded change applies to them.
func defsController(t *testing.T, ctx context.Context, db anystore.DB, typeId string) *crdt.Controller {
	t.Helper()
	ctrl, err := crdt.NewController(ctx, typeId, db,
		crdt.HandlerReg{Name: typetype.DatasetDefs, Handler: typetype.DatasetDefsHandler{}, Schema: schema.Dataset{Dynamic: true}},
		crdt.HandlerReg{Name: typetype.ShortIdsDataset, Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}},
	)
	require.NoError(t, err)
	return ctrl
}

// seedIndexDef declares an index over fields on the dataset seedDefs
// created for (typeId, dsKey); the record id is "index-<indexKey>".
func seedIndexDef(t *testing.T, ctx context.Context, db anystore.DB, typeId, dsKey, indexKey, ver string, fields ...string) {
	t.Helper()
	ctrl := defsController(t, ctx, db, typeId)
	a := &anyenc.Arena{}
	rec := a.NewObject()
	rec.Set(typetype.DefFieldDef, a.NewString(typetype.DefKindIndex))
	rec.Set(typetype.DefFieldDataset, a.NewString(typeId+"-head-"+dsKey))
	rec.Set(typetype.FieldKey, a.NewString(indexKey))
	arr := a.NewArray()
	for i, f := range fields {
		arr.SetArrayItem(i, a.NewString(f))
	}
	rec.Set(typetype.DefFieldIndexFields, arr)
	require.NoError(t, ctrl.ApplyChange(ctx, crdt.Change{
		ObjectId: typeId, Dataset: typetype.DatasetDefs, ChangeId: "cd-index-" + typeId + indexKey + ver,
		VersionId: crdt.VersionId(ver), DataVersion: typetype.DatasetDefsHandlerVersion,
		Records: []crdt.RecordChange{{Id: "index-" + indexKey, Upsert: true,
			Ops: []crdt.Op{{Type: crdt.OpSet, Payload: rec}}}},
	}))
	require.NoError(t, ctrl.CloseOwnedCollections())
}

func removeIndexDef(t *testing.T, ctx context.Context, db anystore.DB, typeId, indexKey, ver string) {
	t.Helper()
	ctrl := defsController(t, ctx, db, typeId)
	require.NoError(t, ctrl.ApplyChange(ctx, crdt.Change{
		ObjectId: typeId, Dataset: typetype.DatasetDefs, ChangeId: "cd-unindex-" + typeId + indexKey + ver,
		VersionId: crdt.VersionId(ver), DataVersion: typetype.DatasetDefsHandlerVersion,
		Records: []crdt.RecordChange{{Id: "index-" + indexKey, Ops: []crdt.Op{{Type: crdt.OpDelete}}}},
	}))
	require.NoError(t, ctrl.CloseOwnedCollections())
}

func declaredIndexNames(coll anystore.Collection) []string {
	var names []string
	for _, idx := range coll.GetIndexes() {
		if name := idx.Info().Name; len(name) > len(schema.IndexStorePrefix) && name[:len(schema.IndexStorePrefix)] == schema.IndexStorePrefix {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

// buildLog collects the index-build feed.
type buildLog struct {
	mu     sync.Mutex
	events []IndexBuild
}

func (l *buildLog) add(ev IndexBuild) {
	l.mu.Lock()
	l.events = append(l.events, ev)
	l.mu.Unlock()
}

func (l *buildLog) phases(dataset string) []IndexBuildPhase {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []IndexBuildPhase
	for _, ev := range l.events {
		if ev.Dataset == dataset {
			out = append(out, ev.Phase)
		}
	}
	return out
}

// A per-object dataset's declared indexes ride its registration: an
// object's collection has them from its open and loses a removed one at
// the next.
func TestIndexes_PerObjectDatasetFollowsItsRegistration(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "idx.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	seedTypeObject(t, ctx, db, "spaceA", catTypeId)
	seedDatasetDefs(t, ctx, db, catTypeId, "notes", "a")
	seedIndexDef(t, ctx, db, catTypeId, "notes", "by_title", "b1", "title")
	notes := types.CollectionName(catTypeId, "notes")

	store := NewStore(nil, db, nil, "spaceA", nil, nil, nil, nil)
	t.Cleanup(func() { _ = store.Close() })

	reg := store.catalog.snapshot().regs[notes]
	require.Len(t, reg.Indexes, 1)
	assert.Equal(t, "dx_title", reg.Indexes[0].Name)
	assert.Equal(t, schema.IndexStorePrefix, reg.PruneIndexPrefix)
	var listed bool
	for _, ns := range store.Schemas() {
		if ns.Name == notes {
			listed = true
			require.Len(t, ns.Indexes, 1)
			assert.Equal(t, schema.Index{Key: "by_title", Fields: []string{"title"}}, ns.Indexes[0])
		}
	}
	assert.True(t, listed)

	ctrl, err := store.newController(ctx, "obj1")
	require.NoError(t, err)
	writeTitle(t, ctx, ctrl, "obj1", notes, "n1", "v1", "one")
	coll, err := store.OpenObjectCollection(ctx, "obj1", notes)
	require.NoError(t, err)
	assert.Equal(t, []string{"dx_title"}, declaredIndexNames(coll))
	rev := ctrl.DatasetSchemaRev(notes)
	require.NoError(t, ctrl.CloseOwnedCollections())

	// The definition goes: the registration rotates and the next open
	// drops the index.
	removeIndexDef(t, ctx, db, catTypeId, "by_title", "b2")
	store.refreshType(ctx, catTypeId)
	assert.Empty(t, store.catalog.snapshot().regs[notes].Indexes)
	assert.NotEqual(t, rev, store.catalog.snapshot().regs[notes].SchemaRev)
	ctrl, err = store.newController(ctx, "obj1")
	require.NoError(t, err)
	writeTitle(t, ctx, ctrl, "obj1", notes, "n2", "v2", "two")
	coll, err = store.OpenObjectCollection(ctx, "obj1", notes)
	require.NoError(t, err)
	assert.Empty(t, declaredIndexNames(coll))
}

// A shared dataset's collection is indexed by the store: a definition
// that applies builds the index in the background and reports it, a
// removed one drops it, and a store opening over definitions builds
// what is missing.
func TestIndexes_SharedDatasetFollowsTheCatalog(t *testing.T) {
	ctx, store, samples, _ := sharedDatasetStore(t)
	for _, objectId := range []string{"obj1", "obj2"} {
		ctrl, err := store.newController(ctx, objectId)
		require.NoError(t, err)
		writeTitle(t, ctx, ctrl, objectId, samples, "r1", "v1", objectId)
	}
	coll, ok, err := store.KeyedCollection(ctx, samples)
	require.NoError(t, err)
	require.True(t, ok)
	require.Empty(t, declaredIndexNames(coll))

	log := &buildLog{}
	cancel := store.SubscribeIndexBuilds(log.add)
	defer cancel()

	seedIndexDef(t, ctx, store.db, catTypeId, "samples", "by_title", "c1", "title", schema.IndexPathObject)
	store.refreshType(ctx, catTypeId)
	const name = "dx_title,_objectId"
	require.Eventually(t, func() bool { return store.KeyedIndexReady(ctx, samples, name) }, 5*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool { return len(log.phases(samples)) == 2 }, time.Second, 5*time.Millisecond)
	assert.Equal(t, []IndexBuildPhase{IndexBuildStarted, IndexBuildDone}, log.phases(samples))
	assert.Equal(t, []string{name}, declaredIndexNames(coll))
	// The registration is not what indexes a shared collection.
	assert.Empty(t, store.catalog.snapshot().regs[samples].Indexes)

	// Rows written after the build are read through it.
	ctrl, err := store.newController(ctx, "obj3")
	require.NoError(t, err)
	writeTitle(t, ctx, ctrl, "obj3", samples, "r1", "v1", "obj3")
	n, err := coll.Find(`{"title":"obj3"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	// Another pass with nothing missing builds and reports nothing.
	store.refreshType(ctx, catTypeId)
	time.Sleep(50 * time.Millisecond)
	assert.Len(t, log.phases(samples), 2)

	removeIndexDef(t, ctx, store.db, catTypeId, "by_title", "c2")
	store.refreshType(ctx, catTypeId)
	require.Eventually(t, func() bool { return !store.KeyedIndexReady(ctx, samples, name) }, 5*time.Second, 5*time.Millisecond)
	assert.Len(t, log.phases(samples), 2, "a drop is not a build")

	// A store that opens over a definition builds the missing index.
	seedIndexDef(t, ctx, store.db, catTypeId, "samples", "by_title2", "c3", "title")
	reopened := NewStore(nil, store.db, nil, "spaceA", nil, nil, nil, nil)
	t.Cleanup(func() { _ = reopened.Close() })
	require.Eventually(t, func() bool { return reopened.KeyedIndexReady(ctx, samples, "dx_title") }, 5*time.Second, 5*time.Millisecond)

	// Close waits for the worker and is safe to repeat.
	require.NoError(t, reopened.Close())
	reopened.kickIndexSync()
	assert.False(t, store.KeyedIndexReady(ctx, "unknown", name))
}

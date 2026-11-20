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
	removeDef(t, ctx, db, typeId, "index-"+indexKey, ver)
}

// removeDef tombstones one definition record of typeId.
func removeDef(t *testing.T, ctx context.Context, db anystore.DB, typeId, recId, ver string) {
	t.Helper()
	ctrl := defsController(t, ctx, db, typeId)
	require.NoError(t, ctrl.ApplyChange(ctx, crdt.Change{
		ObjectId: typeId, Dataset: typetype.DatasetDefs, ChangeId: "cd-remove-" + typeId + recId + ver,
		VersionId: crdt.VersionId(ver), DataVersion: typetype.DatasetDefsHandlerVersion,
		Records: []crdt.RecordChange{{Id: recId, Ops: []crdt.Op{{Type: crdt.OpDelete}}}},
	}))
	require.NoError(t, ctrl.CloseOwnedCollections())
}

// seedFieldDef declares field key of kind (and scope, when set) on the
// dataset seedDefs created for (typeId, dsKey); the record id is
// "field-<key>".
func seedFieldDef(t *testing.T, ctx context.Context, db anystore.DB, typeId, dsKey, key, kind, scope, ver string) {
	t.Helper()
	ctrl := defsController(t, ctx, db, typeId)
	a := &anyenc.Arena{}
	rec := a.NewObject()
	rec.Set(typetype.DefFieldDef, a.NewString(typetype.DefKindField))
	rec.Set(typetype.DefFieldDataset, a.NewString(typeId+"-head-"+dsKey))
	rec.Set(typetype.FieldKey, a.NewString(key))
	rec.Set(typetype.FieldKind, a.NewString(kind))
	if scope != "" {
		rec.Set(typetype.FieldScope, a.NewString(scope))
	}
	require.NoError(t, ctrl.ApplyChange(ctx, crdt.Change{
		ObjectId: typeId, Dataset: typetype.DatasetDefs, ChangeId: "cd-field-" + typeId + key + ver,
		VersionId: crdt.VersionId(ver), DataVersion: typetype.DatasetDefsHandlerVersion,
		Records: []crdt.RecordChange{{Id: "field-" + key, Upsert: true,
			Ops: []crdt.Op{{Type: crdt.OpSet, Payload: rec}}}},
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

// indexLen counts the entries of the collection's index named name.
func indexLen(t *testing.T, ctx context.Context, coll anystore.Collection, name string) int {
	t.Helper()
	for _, idx := range coll.GetIndexes() {
		if idx.Info().Name == name {
			n, err := idx.Len(ctx)
			require.NoError(t, err)
			return n
		}
	}
	t.Fatalf("index %q not found", name)
	return 0
}

// A shared dataset's collection is indexed by the store: a definition
// that applies builds the index in the background and reports it, rows
// written later enter it, and a removed definition drops it.
func TestIndexes_SharedDatasetFollowsTheCatalog(t *testing.T) {
	ctx, store, samples, _ := sharedDatasetStore(t)
	store.StartIndexSync()
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

	// The build indexed the rows already there; a row written after it
	// enters the index.
	assert.Equal(t, 2, indexLen(t, ctx, coll, name))
	ctrl, err := store.newController(ctx, "obj3")
	require.NoError(t, err)
	writeTitle(t, ctx, ctrl, "obj3", samples, "r1", "v1", "obj3")
	assert.Equal(t, 3, indexLen(t, ctx, coll, name))

	// A pass with nothing missing builds and reports nothing.
	require.NoError(t, store.syncKeyedIndexes(ctx, samples))
	assert.Len(t, log.phases(samples), 2)

	removeIndexDef(t, ctx, store.db, catTypeId, "by_title", "c2")
	store.refreshType(ctx, catTypeId)
	require.Eventually(t, func() bool { return !store.KeyedIndexReady(ctx, samples, name) }, 5*time.Second, 5*time.Millisecond)
	assert.Len(t, log.phases(samples), 2, "a drop is not a build")
	assert.False(t, store.KeyedIndexReady(ctx, "unknown", name), "an unknown dataset holds no index")
}

// A store that opens over a definition builds the missing index once
// its worker is started; Close waits for a build in flight and leaves
// no worker a later kick could reach.
func TestIndexes_StartAndClose(t *testing.T) {
	ctx, store, samples, _ := sharedDatasetStore(t)
	ctrl, err := store.newController(ctx, "obj1")
	require.NoError(t, err)
	writeTitle(t, ctx, ctrl, "obj1", samples, "r1", "v1", "one")
	seedIndexDef(t, ctx, store.db, catTypeId, "samples", "by_title", "c1", "title")

	reopened := NewStore(nil, store.db, nil, "spaceA", nil, nil, nil, nil)
	t.Cleanup(func() { _ = reopened.Close() })
	// Not started: a refresh kicks nothing, and nothing builds.
	reopened.refreshType(ctx, catTypeId)
	reopened.kickIndexSync()
	reopened.indexMu.Lock()
	assert.Nil(t, reopened.indexKick, "no worker before StartIndexSync")
	reopened.indexMu.Unlock()
	assert.False(t, reopened.KeyedIndexReady(ctx, samples, "dx_title"))

	// A build blocks in its Started report until released.
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	cancel := reopened.SubscribeIndexBuilds(func(ev IndexBuild) {
		if ev.Dataset == samples && ev.Phase == IndexBuildStarted {
			once.Do(func() { close(started) })
			<-release
		}
	})
	defer cancel()
	reopened.StartIndexSync()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the started worker builds the missing index")
	}

	closed := make(chan error, 1)
	go func() { closed <- reopened.Close() }()
	select {
	case <-closed:
		t.Fatal("Close returned while a build is in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close returns once the build ends")
	}

	// The worker is gone, and a kick after Close starts none.
	reopened.indexMu.Lock()
	done := reopened.indexDone
	reopened.indexMu.Unlock()
	select {
	case <-done:
	default:
		t.Fatal("the worker exited before Close returned")
	}
	reopened.kickIndexSync()
	reopened.StartIndexSync()
	reopened.indexMu.Lock()
	assert.Empty(t, reopened.indexKick, "a kick after Close is not queued")
	assert.Equal(t, done, reopened.indexDone, "no second worker starts")
	reopened.indexMu.Unlock()
	assert.NotPanics(t, func() { _ = reopened.Close() }, "Close is safe to repeat")
}

// While a type object replays, the worker leaves its datasets alone: a
// half-replayed catalog that misses an index the collection holds does
// not drop it. The end of the replay asks for the pass that reconciles.
func TestIndexes_HeldTypeIsSkipped(t *testing.T) {
	ctx, store, samples, _ := sharedDatasetStore(t)
	ctrl, err := store.newController(ctx, "obj1")
	require.NoError(t, err)
	writeTitle(t, ctx, ctrl, "obj1", samples, "r1", "v1", "one")
	seedIndexDef(t, ctx, store.db, catTypeId, "samples", "by_title", "c1", "title")
	store.refreshType(ctx, catTypeId)
	require.NoError(t, store.syncKeyedIndexes(ctx, samples))
	require.True(t, store.KeyedIndexReady(ctx, samples, "dx_title"))

	store.holdIndexSync(catTypeId)
	store.StartIndexSync()
	removeIndexDef(t, ctx, store.db, catTypeId, "by_title", "c2")
	store.refreshType(ctx, catTypeId)
	require.NoError(t, store.syncKeyedIndexes(ctx, samples))
	assert.True(t, store.KeyedIndexReady(ctx, samples, "dx_title"), "a held type's index stays")

	store.releaseIndexSync(catTypeId)
	require.Eventually(t, func() bool { return !store.KeyedIndexReady(ctx, samples, "dx_title") },
		5*time.Second, 5*time.Millisecond, "the release reconciles the collection")

	// A release without a hold is a no-op.
	store.releaseIndexSync(catTypeId)
	assert.False(t, store.indexSyncHeld(catTypeId))
}

// A subscriber that joins while a build is in flight hears Started at
// once and then the build's end; one that joins after the end hears
// nothing of it.
func TestIndexes_SubscribeDuringABuild(t *testing.T) {
	_, store, samples, _ := sharedDatasetStore(t)
	assert.NotPanics(t, func() { store.SubscribeIndexBuilds(nil)() })

	store.reportBuild(IndexBuild{Dataset: samples, Phase: IndexBuildStarted})
	during := &buildLog{}
	cancel := store.SubscribeIndexBuilds(during.add)
	defer cancel()
	assert.Equal(t, []IndexBuildPhase{IndexBuildStarted}, during.phases(samples))
	store.reportBuild(IndexBuild{Dataset: samples, Phase: IndexBuildDone})
	assert.Equal(t, []IndexBuildPhase{IndexBuildStarted, IndexBuildDone}, during.phases(samples))

	after := &buildLog{}
	cancelAfter := store.SubscribeIndexBuilds(after.add)
	defer cancelAfter()
	assert.Empty(t, after.phases(samples), "nothing is in flight after Done")

	// A failed build ends it too.
	store.reportBuild(IndexBuild{Dataset: samples, Phase: IndexBuildStarted})
	store.reportBuild(IndexBuild{Dataset: samples, Phase: IndexBuildFailed})
	late := &buildLog{}
	cancelLate := store.SubscribeIndexBuilds(late.add)
	defer cancelLate()
	assert.Empty(t, late.phases(samples), "nothing is in flight after Failed")
	assert.Equal(t, []IndexBuildPhase{IndexBuildStarted, IndexBuildFailed}, after.phases(samples))
}

// An index whose field is removed turns invalid: discovery stops
// listing it and the store drops it from the shared collection.
func TestIndexes_InvalidIndexIsDropped(t *testing.T) {
	ctx, store, samples, _ := sharedDatasetStore(t)
	store.StartIndexSync()
	seedFieldDef(t, ctx, store.db, catTypeId, "samples", "score", "number", "", "d1")
	seedIndexDef(t, ctx, store.db, catTypeId, "samples", "by_score", "d2", "score")
	store.refreshType(ctx, catTypeId)
	require.Eventually(t, func() bool { return store.KeyedIndexReady(ctx, samples, "dx_score") }, 5*time.Second, 5*time.Millisecond)
	listed := func() []schema.Index {
		for _, ns := range store.Schemas() {
			if ns.Name == samples {
				return ns.Indexes
			}
		}
		t.Fatalf("%s not listed", samples)
		return nil
	}
	assert.Equal(t, []schema.Index{{Key: "by_score", Fields: []string{"score"}}}, listed())

	removeDef(t, ctx, store.db, catTypeId, "field-score", "d3")
	store.refreshType(ctx, catTypeId)
	ds, ok := store.RuntimeDataset(samples)
	require.True(t, ok, "the dataset stays valid")
	require.Len(t, ds.Indexes, 1)
	assert.True(t, ds.Indexes[0].Invalid)
	assert.Empty(t, listed(), "an invalid index is not listed")
	require.Eventually(t, func() bool { return !store.KeyedIndexReady(ctx, samples, "dx_score") }, 5*time.Second, 5*time.Millisecond)
}

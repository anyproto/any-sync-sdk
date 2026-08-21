package spaceobjects

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/fanout"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
)

const reindexNotes = "notes"

// reindexStore builds a Store around a real DB with the two dataset
// shapes the capture path has to tell apart: the shared per-space
// `objects` collection (one row per object, per-PROPERTY scopes resolved
// through the type registry) and a per-object dataset with a declared
// local field.
func reindexStore(t *testing.T) (context.Context, *Store, *crdt.Controller) {
	t.Helper()
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "reindex.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	s := &Store{
		db:         db,
		spaceId:    "spaceA",
		changeSubs: fanout.New[ObjectChange](),
		rowEvents:  fanout.New[RowEvent](),
		reg: types.NewLiveRegistry(db, map[string]map[string]types.PropInfo{
			"typeA": {
				"pSynced": {Id: "pSynced", Name: "Title", Kind: schema.KindString, Scope: schema.ScopeSynced},
				"pLocal":  {Id: "pLocal", Name: "Device state", Kind: schema.KindNumber, Scope: schema.ScopeLocal},
			},
		}),
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

	shared, err := s.SharedObjects(ctx)
	require.NoError(t, err)
	ctrl, err := crdt.NewControllerWithShared(ctx, "obj1", db,
		crdt.SharedCollections{properties.Dataset: shared},
		crdt.HandlerReg{
			Name: properties.Dataset, Handler: crdt.DefaultHandler{},
			Schema: schema.Dataset{Dynamic: true}, DynamicScopeByKey: true,
		},
		crdt.HandlerReg{
			Name: reindexNotes, Handler: crdt.DefaultHandler{},
			Schema: schema.Dataset{Fields: []schema.Field{
				{Id: "text", Schema: schema.Leaf(schema.KindString), Scope: schema.ScopeSynced},
				{Id: "unread", Schema: schema.Leaf(schema.KindBoolean), Scope: schema.ScopeLocal},
			}},
		},
	)
	require.NoError(t, err)
	ctrl.SetSpaceId("spaceA")
	return ctx, s, ctrl
}

func upsert(t *testing.T, ctx context.Context, coll anystore.Collection, id string, set func(a *anyenc.Arena, v *anyenc.Value)) {
	t.Helper()
	_, err := coll.UpsertId(ctx, id, query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		set(a, v)
		return v, true, nil
	}))
	require.NoError(t, err)
}

func TestReindex_CaptureLocalLeaves(t *testing.T) {
	ctx, s, ctrl := reindexStore(t)

	shared, err := s.SharedObjects(ctx)
	require.NoError(t, err)
	upsert(t, ctx, shared, "obj1", func(a *anyenc.Arena, v *anyenc.Value) {
		typed := a.NewObject()
		typed.Set("pSynced", a.NewString("from the DAG"))
		typed.Set("pLocal", a.NewNumberInt(7))
		v.Set("typeA", typed)
	})
	// A second object's row must not be captured — the shared collection
	// holds every object in the space.
	upsert(t, ctx, shared, "obj2", func(a *anyenc.Arena, v *anyenc.Value) {
		typed := a.NewObject()
		typed.Set("pLocal", a.NewNumberInt(99))
		v.Set("typeA", typed)
	})

	notes, err := s.db.Collection(ctx, "obj1_"+reindexNotes)
	require.NoError(t, err)
	upsert(t, ctx, notes, "rec1", func(a *anyenc.Arena, v *anyenc.Value) {
		v.Set("text", a.NewString("hello"))
		v.Set("unread", a.NewTrue())
	})
	upsert(t, ctx, notes, "rec2", func(a *anyenc.Arena, v *anyenc.Value) {
		v.Set("text", a.NewString("read already"))
	})

	leaves := s.captureLocalLeaves(ctx, "obj1", ctrl)
	require.Len(t, leaves, 2, "one per-property local value, one record flag")

	i := slices.IndexFunc(leaves, func(l localLeaf) bool { return l.dataset == properties.Dataset })
	require.GreaterOrEqual(t, i, 0, "the objects row's local property must be captured")
	assert.Equal(t, "obj1", leaves[i].recordId)
	assert.Equal(t, []string{"typeA", "pLocal"}, leaves[i].path)
	got, err := anyenc.Parse(leaves[i].value)
	require.NoError(t, err)
	assert.Equal(t, 7, got.GetInt())

	j := slices.IndexFunc(leaves, func(l localLeaf) bool { return l.dataset == reindexNotes })
	require.GreaterOrEqual(t, j, 0)
	assert.Equal(t, "rec1", leaves[j].recordId, "only the record carrying the flag")
	assert.Equal(t, []string{"unread"}, leaves[j].path)
}

func TestReindex_WipeClearsOnlyThisObject(t *testing.T) {
	ctx, s, ctrl := reindexStore(t)

	shared, err := s.SharedObjects(ctx)
	require.NoError(t, err)
	upsert(t, ctx, shared, "obj1", func(a *anyenc.Arena, v *anyenc.Value) { v.Set("name", a.NewString("rebuilt")) })
	upsert(t, ctx, shared, "obj2", func(a *anyenc.Arena, v *anyenc.Value) { v.Set("name", a.NewString("untouched")) })
	notes, err := s.db.Collection(ctx, "obj1_"+reindexNotes)
	require.NoError(t, err)
	upsert(t, ctx, notes, "rec1", func(a *anyenc.Arena, v *anyenc.Value) { v.Set("text", a.NewString("hello")) })

	s.wipeMaterialized(ctx, "obj1", ctrl)

	_, err = shared.FindId(ctx, "obj1")
	assert.ErrorIs(t, err, anystore.ErrDocNotFound, "the rebuilt object's row is gone")
	_, err = shared.FindId(ctx, "obj2")
	assert.NoError(t, err, "other objects in the space are untouched")

	names, err := s.db.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.NotContains(t, names, "obj1_"+reindexNotes, "per-object collections are dropped whole")

	// Deletion semantics must NOT apply: a `del` stamp is sticky and would
	// evict the object from every consumer index permanently.
	metaColl, err := s.metaCollection(ctx)
	require.NoError(t, err)
	if doc, ferr := metaColl.FindId(ctx, "obj1"); ferr == nil {
		_, deleted := crdt.MetaOwnership(doc.Value())
		assert.False(t, deleted, "a rebuild must not stamp the object deleted")
	}
}

func TestReindex_WipeIsIdempotent(t *testing.T) {
	ctx, s, ctrl := reindexStore(t)
	s.wipeMaterialized(ctx, "obj1", ctrl)
	s.wipeMaterialized(ctx, "obj1", ctrl)
}

// TestReindex_SweepLoadsStaleObjectsOnly: the sweep picks its work list
// from the _meta rows and drives each stale object through the normal
// load path (loading IS the rebuild). Objects already on the current
// version, purged objects and other spaces' rows stay untouched.
func TestReindex_SweepLoadsStaleObjectsOnly(t *testing.T) {
	ctx, s, _ := reindexStore(t)

	metaColl, err := s.metaCollection(ctx)
	require.NoError(t, err)
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "stale-1", 1, 1, map[string]int{reindexNotes: 1}, "spaceA"))
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "stale-2", 1, 1, map[string]int{reindexNotes: 1}, "spaceA"))
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "current", 1, 1, map[string]int{reindexNotes: 2}, "spaceA"))
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "elsewhere", 1, 1, map[string]int{reindexNotes: 1}, "spaceB"))

	// The store's registered version for the dataset — buildRegs is
	// raw-mode here, so the sweep compares against exactly this.
	s.customHandlers = []crdt.HandlerReg{{
		Name: reindexNotes, Version: 2, Handler: crdt.DefaultHandler{},
		Schema: schema.Dataset{Dynamic: true},
	}}

	var mu sync.Mutex
	var loaded []string
	s.cache.Close()
	s.cache = ocache.New(func(_ context.Context, id string) (ocache.Object, error) {
		mu.Lock()
		loaded = append(loaded, id)
		mu.Unlock()
		return nil, fmt.Errorf("load stub %s", id)
	})
	t.Cleanup(func() { _ = s.cache.Close() })

	defer swapSweepPacing(time.Millisecond, time.Millisecond)()
	s.StartReindexSweep()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(loaded) >= 2
	}, 5*time.Second, 10*time.Millisecond, "the sweep must load both stale objects")

	mu.Lock()
	got := slices.Clone(loaded)
	mu.Unlock()
	slices.Sort(got)
	assert.Equal(t, []string{"stale-1", "stale-2"}, got,
		"only this space's live objects whose handler version moved")
}

// swapSweepPacing shortens the sweep's timers for the duration of a test
// and returns the restore func.
func swapSweepPacing(start, pace time.Duration) func() {
	oldStart, oldPace := reindexSweepStartDelay, reindexSweepPace
	reindexSweepStartDelay, reindexSweepPace = start, pace
	return func() { reindexSweepStartDelay, reindexSweepPace = oldStart, oldPace }
}

// A wipe that cannot even enumerate the object's collections must fail
// the rebuild, not press on: the replay would land on top of rows the
// old handler wrote, and stamping the current versions over that would
// disarm the trigger for good.
func TestReindex_WipeFailureAbortsRebuild(t *testing.T) {
	ctx, s, ctrl := reindexStore(t)
	require.NoError(t, s.db.Close())

	err := s.wipeMaterialized(ctx, "obj1", ctrl)
	require.Error(t, err, "a failed wipe must surface")

	_, err = s.reindexPrepare(ctx, "obj1", ctrl, []string{reindexNotes})
	require.Error(t, err, "prepare propagates it, so loadObject never stamps versions")
}

// Close must not return while a rebuild is still writing: an offload
// drops the space's collections immediately after, and a sweep that
// outlived Close would re-create them behind it.
func TestReindex_CloseWaitsForSweep(t *testing.T) {
	ctx, s, _ := reindexStore(t)

	metaColl, err := s.metaCollection(ctx)
	require.NoError(t, err)
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "stale-1", 1, 1, map[string]int{reindexNotes: 1}, "spaceA"))
	s.customHandlers = []crdt.HandlerReg{{
		Name: reindexNotes, Version: 2, Handler: crdt.DefaultHandler{},
		Schema: schema.Dataset{Dynamic: true},
	}}

	inLoad := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	s.cache.Close()
	s.cache = ocache.New(func(_ context.Context, id string) (ocache.Object, error) {
		once.Do(func() { close(inLoad) })
		<-release
		return nil, fmt.Errorf("load stub %s", id)
	})
	t.Cleanup(func() { _ = s.cache.Close() })

	defer swapSweepPacing(time.Millisecond, time.Millisecond)()
	s.StartReindexSweep()

	select {
	case <-inLoad:
	case <-time.After(5 * time.Second):
		t.Fatal("sweep never reached a load")
	}

	stopped := make(chan struct{})
	go func() {
		s.stopSweep()
		close(stopped)
	}()
	select {
	case <-stopped:
		t.Fatal("stopSweep returned while a rebuild was still in flight")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("stopSweep did not return after the rebuild finished")
	}

	// Documented "safe to call multiple times" — and two shutdown paths
	// (offload, SDK teardown) can reach the same store.
	s.stopSweep()
}

// A store whose sweep never started still closes cleanly.
func TestReindex_StopSweepWithoutStart(t *testing.T) {
	_, s, _ := reindexStore(t)
	s.stopSweep()
	s.stopSweep()
}

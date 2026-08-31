package crdt

import (
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/schema"
)

func TestStaleDatasets(t *testing.T) {
	tests := []struct {
		name       string
		stored     map[string]int
		registered map[string]int
		want       []string
	}{
		{
			name:       "nothing stored — a fresh object is never stale",
			registered: map[string]int{"blocks": 2},
		},
		{
			name:       "same version",
			stored:     map[string]int{"blocks": 2},
			registered: map[string]int{"blocks": 2},
		},
		{
			name:       "bumped version",
			stored:     map[string]int{"blocks": 1},
			registered: map[string]int{"blocks": 2},
			want:       []string{"blocks"},
		},
		{
			name:       "rolled back version still counts as a mismatch",
			stored:     map[string]int{"blocks": 3},
			registered: map[string]int{"blocks": 2},
			want:       []string{"blocks"},
		},
		{
			name:       "unregistered dataset is ignored — its handler isn't loaded",
			stored:     map[string]int{"gone": 1},
			registered: map[string]int{"blocks": 2},
		},
		{
			name:       "registered but never written is not stale",
			stored:     map[string]int{"blocks": 2},
			registered: map[string]int{"blocks": 2, "chat": 5},
		},
		{
			name:       "sorted for deterministic logs",
			stored:     map[string]int{"chat": 1, "blocks": 1},
			registered: map[string]int{"chat": 2, "blocks": 2},
			want:       []string{"blocks", "chat"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, staleDatasets(tc.stored, tc.registered))
		})
	}
}

// reindexDB opens a DB carrying one object's _meta row stamped with the
// given handler version.
func reindexDB(t *testing.T, storedVersion int) anystore.DB {
	t.Helper()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "reindex.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	coll, err := db.Collection(ctx, MetaCollectionName)
	require.NoError(t, err)
	require.NoError(t, PersistMeta(ctx, coll, "obj1", 55, 55, map[string]int{"blocks": storedVersion}, "spaceA"))
	return db
}

func reindexCtrl(t *testing.T, db anystore.DB, version int) *Controller {
	t.Helper()
	ctrl, err := NewController(ctx, "obj1", db, HandlerReg{
		Name: "blocks", Version: version, Handler: DefaultHandler{}, Schema: dynSchema,
	})
	require.NoError(t, err)
	ctrl.SetSpaceId("spaceA")
	return ctrl
}

func TestReindex_ControllerReportsStaleAtConstruction(t *testing.T) {
	db := reindexDB(t, 1)

	assert.Equal(t, []string{"blocks"}, reindexCtrl(t, db, 2).StaleDatasets(),
		"a bumped handler version must surface at construction")
	assert.Empty(t, reindexCtrl(t, db, 1).StaleDatasets(),
		"an unchanged version must not trigger a rebuild")
}

func TestReindex_ResetRewindsAndKeepsTriggerArmed(t *testing.T) {
	db := reindexDB(t, 1)
	ctrl := reindexCtrl(t, db, 2)
	require.Equal(t, uint64(55), ctrl.MaxAddSeq())

	require.NoError(t, ctrl.ResetForReindex(ctx, nil))
	assert.Zero(t, ctrl.MaxAddSeq(), "the replay must start from the beginning of the tree")
	assert.Empty(t, ctrl.StaleDatasets(), "the verdict is consumed once acted on")

	// The rewind is durable BEFORE the wipe, so a crash mid-wipe replays.
	coll, err := db.Collection(ctx, MetaCollectionName)
	require.NoError(t, err)
	seq, _, hv, err := LoadMeta(ctx, coll, "obj1")
	require.NoError(t, err)
	assert.Zero(t, seq)
	assert.Equal(t, map[string]int{"blocks": 1}, hv,
		"stored versions stay stale until a replay re-applies something")

	// A fresh controller on the same DB — the crash-restart case — still
	// sees the object as stale and rebuilds again.
	assert.Equal(t, []string{"blocks"}, reindexCtrl(t, db, 2).StaleDatasets())
}

func TestReindex_LeavesRideTheMetaRow(t *testing.T) {
	db := reindexDB(t, 1)
	ctrl := reindexCtrl(t, db, 2)
	a := &anyenc.Arena{}
	leaves := []LocalLeaf{
		{Dataset: "blocks", RecordId: "r1", Path: []string{"pinned"}, Value: a.NewTrue().MarshalTo(nil)},
		{Dataset: "objects", RecordId: "obj1", Path: []string{"typeA", "pLocal"}, Value: a.NewNumberInt(7).MarshalTo(nil)},
		{Dataset: "blocks", RecordId: "bad", Path: []string{"x"}, Value: []byte{0xff}},
	}
	require.NoError(t, ctrl.ResetForReindex(ctx, leaves))
	assert.True(t, ctrl.ReindexPending())

	// The crash-restart case: a fresh controller finds the rebuild in
	// flight and the readable leaves intact; the unreadable one is gone.
	next := reindexCtrl(t, db, 2)
	require.True(t, next.ReindexPending())
	assert.Equal(t, leaves[:2], next.ReindexLocalLeaves())
	assert.Equal(t, []string{"blocks"}, next.StaleDatasets(), "versions stay stale until a replay stamps them")

	// The replay's own stamp does not end the rebuild — only the finish does.
	coll, err := db.Collection(ctx, MetaCollectionName)
	require.NoError(t, err)
	require.NoError(t, next.PersistMeta(ctx, coll))
	assert.True(t, reindexCtrl(t, db, 2).ReindexPending())
	ids, err := StaleObjects(ctx, coll, "spaceA", map[string]int{"blocks": 2})
	require.NoError(t, err)
	assert.Equal(t, []string{"obj1"}, ids, "the sweep still visits an in-flight rebuild")

	require.NoError(t, next.PersistVersions(ctx))
	assert.False(t, next.ReindexPending())
	last := reindexCtrl(t, db, 2)
	assert.False(t, last.ReindexPending())
	assert.Nil(t, last.ReindexLocalLeaves())
	assert.Empty(t, last.StaleDatasets())
}

func TestReindex_PersistVersionsDisarmsTrigger(t *testing.T) {
	db := reindexDB(t, 1)
	ctrl := reindexCtrl(t, db, 2)
	require.NoError(t, ctrl.ResetForReindex(ctx, nil))
	require.NoError(t, ctrl.PersistVersions(ctx))

	assert.Empty(t, reindexCtrl(t, db, 2).StaleDatasets(),
		"an object whose replay re-applied nothing must not rebuild on every load")
}

func TestReindex_LocalFields(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "fields.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctrl, err := NewController(ctx, "obj1", db, HandlerReg{
		Name: "notes", Handler: DefaultHandler{}, Schema: schema.Dataset{Fields: []schema.Field{
			{Id: "text", Schema: schema.Leaf(schema.KindString), Scope: schema.ScopeSynced},
			{Id: "unread", Schema: schema.Leaf(schema.KindBoolean), Scope: schema.ScopeLocal},
			{Id: "pinned", Schema: schema.Leaf(schema.KindBoolean), Scope: schema.ScopeLocal},
			{Id: "createdAt", Schema: schema.Leaf(schema.KindNumber), Scope: schema.ScopeDerived},
		}},
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"pinned", "unread"}, ctrl.LocalFields("notes"),
		"only local-scope leaves need capturing — the DAG reproduces the rest")
	assert.Nil(t, ctrl.LocalFields("unknown"))
	assert.Equal(t, []string{"notes"}, ctrl.RegisteredDatasets())
}

func TestReindex_StaleObjectsScan(t *testing.T) {
	db, coll := openMetaColl(t)
	_ = db

	require.NoError(t, PersistMeta(ctx, coll, "fresh", 1, 1, map[string]int{"blocks": 2}, "spaceA"))
	require.NoError(t, PersistMeta(ctx, coll, "stale", 1, 1, map[string]int{"blocks": 1}, "spaceA"))
	require.NoError(t, PersistMeta(ctx, coll, "otherSpace", 1, 1, map[string]int{"blocks": 1}, "spaceB"))
	require.NoError(t, PersistMeta(ctx, coll, "unwritten", 1, 1, nil, "spaceA"))
	require.NoError(t, PersistMeta(ctx, coll, "purged", 1, 1, map[string]int{"blocks": 1}, "spaceA"))
	require.NoError(t, PersistDeletionMark(ctx, coll, "purged", "spaceA", 9))

	ids, err := StaleObjects(ctx, coll, "spaceA", map[string]int{"blocks": 2})
	require.NoError(t, err)
	assert.Equal(t, []string{"stale"}, ids,
		"only this space's live objects with a version mismatch")
}

// stampSecs reads a derived timestamp leaf as unix seconds, asserting it
// really is a datetime instant — a stamp that regressed to a plain
// number would otherwise read as a silent zero.
func stampSecs(t *testing.T, rec *anyenc.Value, field string) int64 {
	t.Helper()
	leaf := rec.Get(field)
	require.NotNil(t, leaf, "stamp %q present", field)
	ms, err := leaf.DateTimeMillis()
	require.NoError(t, err, "stamp %q is a datetime", field)
	return ms / 1000
}

// The composed version has to be injective in both halves: a sum lets a
// consumer's bump cancel an SDK bump, so neither logic change rebuilds.
func TestComposeVersion(t *testing.T) {
	seen := map[int][2]int{}
	for sdk := 1; sdk <= 4; sdk++ {
		for consumer := 0; consumer <= 4; consumer++ {
			got := ComposeVersion(sdk, consumer)
			if prev, dup := seen[got]; dup {
				t.Fatalf("ComposeVersion(%d,%d) collides with (%d,%d) at %d",
					sdk, consumer, prev[0], prev[1], got)
			}
			seen[got] = [2]int{sdk, consumer}
		}
	}
	// A plain unversioned handler must not land on the value a composed
	// pair produces.
	assert.NotEqual(t, NormalizedVersion(0), ComposeVersion(1, 0))
}

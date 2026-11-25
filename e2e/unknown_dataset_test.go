package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_UnknownDataset: a read, on an object the device holds, of a
// dataset the space does not have fails every terminal with
// ErrDatasetUnknown, as a write does. A dataset the space has reads as
// empty on an object with no records of it, and a live window on it
// fills when a record lands. An object the device does not hold
// answers as before, whatever the dataset.
func TestE2E_UnknownDataset(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lab := openSharedLab(t, ctx, "UnknownDataset")
	sp := lab.sp

	holder := lab.newObject(t, ctx)
	lab.put(t, ctx, holder, lab.plain, "p1", map[string]any{"label": "plain"})
	bare := lab.newObject(t, ctx)
	otherType, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Other"})
	require.NoError(t, err)
	foreign, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: otherType})
	require.NoError(t, err)

	const pipeline = `[{"$count": "n"}]`
	refused := func(t *testing.T, objectId, dataset string) {
		t.Helper()
		query := func() space.Query { return sp.Query(objectId, dataset) }
		_, err := query().All(ctx)
		assert.ErrorIs(t, err, space.ErrDatasetUnknown, "All")
		_, err = query().Count(ctx)
		assert.ErrorIs(t, err, space.ErrDatasetUnknown, "Count")
		_, err = query().One(ctx)
		assert.ErrorIs(t, err, space.ErrDatasetUnknown, "One")
		it, err := query().Iter(ctx)
		assert.ErrorIs(t, err, space.ErrDatasetUnknown, "Iter")
		if it != nil {
			_ = it.Close()
		}
		_, err = query().Snapshot(ctx, space.QueryOpts{IncludeTotal: true})
		assert.ErrorIs(t, err, space.ErrDatasetUnknown, "Snapshot")
		for _, q := range []space.Query{query(), query().Sort("id").Limit(5)} {
			res, err := q.Subscribe(ctx, space.QueryOpts{})
			assert.ErrorIs(t, err, space.ErrDatasetUnknown, "Subscribe")
			if res != nil && res.Sub != nil {
				_ = res.Sub.Close()
			}
		}

		agg := func() space.Agg { return sp.Aggregate(objectId, dataset, pipeline) }
		ait, err := agg().Iter(ctx)
		assert.ErrorIs(t, err, space.ErrDatasetUnknown, "agg Iter")
		if ait != nil {
			_ = ait.Close()
		}
		_, err = agg().All(ctx)
		assert.ErrorIs(t, err, space.ErrDatasetUnknown, "agg All")
		_, err = agg().Count(ctx)
		assert.ErrorIs(t, err, space.ErrDatasetUnknown, "agg Count")
		_, err = agg().Explain(ctx)
		assert.ErrorIs(t, err, space.ErrDatasetUnknown, "agg Explain")
	}

	for _, tc := range []struct{ name, objectId, dataset string }{
		{"Typo", holder, lab.plain + "x"},
		{"UndeclaredKey", holder, lab.typeId + "_nope"},
		{"BareKey", holder, "plain"},
		{"Empty", holder, ""},
		{"StorageName", holder, holder + "_" + lab.plain},
		{"ObjectWithoutRecords", bare, "no-such-dataset"},
	} {
		t.Run(tc.name, func(t *testing.T) { refused(t, tc.objectId, tc.dataset) })
	}

	// The object comes first: one the device does not hold is not
	// found, and a live window on it opens empty.
	t.Run("NoSuchObject", func(t *testing.T) {
		_, err := sp.Query("no-such-object", "no-such-dataset").All(ctx)
		assert.ErrorIs(t, err, space.ErrObjectNotFound, "All")
		_, err = sp.Query("no-such-object", "no-such-dataset").Snapshot(ctx, space.QueryOpts{})
		assert.ErrorIs(t, err, space.ErrObjectNotFound, "Snapshot")
		_, err = sp.Aggregate("no-such-object", "no-such-dataset", pipeline).All(ctx)
		assert.ErrorIs(t, err, space.ErrObjectNotFound, "agg All")
		res, err := sp.Query("no-such-object", "no-such-dataset").Subscribe(ctx, space.QueryOpts{})
		require.NoError(t, err, "Subscribe")
		assert.Empty(t, res.Initial)
		_ = res.Sub.Close()
	})

	t.Run("Write", func(t *testing.T) {
		_, err := sp.Modify(ctx, space.ModifyBatch{
			ObjectId: holder, Dataset: "no-such-dataset",
			Records: []space.RecordModify{{Id: "r1", Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"label": "x"}}}}},
		})
		assert.ErrorIs(t, err, space.ErrDatasetUnknown, "Modify")
		_, err = sp.Delete(ctx, space.DeleteBatch{ObjectId: holder, Dataset: "no-such-dataset", RecordIds: []string{"r1"}})
		assert.ErrorIs(t, err, space.ErrDatasetUnknown, "Delete")
	})

	// A dataset the space has is never unknown: not on an object of the
	// declaring type with no records, not on an object of another type.
	empty := func(t *testing.T, objectId, dataset string) {
		t.Helper()
		rows, err := sp.Query(objectId, dataset).All(ctx)
		require.NoError(t, err, "All")
		assert.Empty(t, rows, "All")
		n, err := sp.Query(objectId, dataset).Count(ctx)
		require.NoError(t, err, "Count")
		assert.Zero(t, n, "Count")
		_, err = sp.Query(objectId, dataset).One(ctx)
		assert.ErrorIs(t, err, space.ErrNotFound, "One")
		res, err := sp.Query(objectId, dataset).Snapshot(ctx, space.QueryOpts{IncludeTotal: true})
		require.NoError(t, err, "Snapshot")
		assert.Empty(t, res.Initial, "Snapshot")
		assert.Zero(t, res.Total, "Snapshot total")
		_, err = sp.Aggregate(objectId, dataset, pipeline).All(ctx)
		require.NoError(t, err, "agg All")
	}
	for _, tc := range []struct{ name, objectId, dataset string }{
		{"NoRecords", bare, lab.plain},
		{"NoRecordsShared", bare, lab.samples},
		{"OtherType", foreign, lab.plain},
		{"OtherTypeShared", foreign, lab.samples},
		{"ModuleNotDeclared", bare, "notes_body"},
	} {
		t.Run("Empty/"+tc.name, func(t *testing.T) { empty(t, tc.objectId, tc.dataset) })
	}

	t.Run("LiveWindowFills", func(t *testing.T) {
		res, err := sp.Query(bare, lab.plain).Subscribe(ctx, space.QueryOpts{})
		require.NoError(t, err)
		defer func() { _ = res.Sub.Close() }()
		assert.Empty(t, res.Initial)

		lab.put(t, ctx, bare, lab.plain, "p2", map[string]any{"label": "late"})
		log := newSubLog()
		log.receiveUntil(t, res.Sub, 30*time.Second, func() bool { return log.added["p2"] != nil })
		assert.Equal(t, "late", log.added["p2"].GetString("label"))
	})

	// A dataset declared after a refused subscribe is read from then on.
	t.Run("DeclaredLater", func(t *testing.T) {
		late := lab.typeId + "_late"
		_, err := sp.Query(bare, late).Subscribe(ctx, space.QueryOpts{})
		require.ErrorIs(t, err, space.ErrDatasetUnknown)

		_, err = sp.Types().AddDataset(ctx, lab.typeId, lab.partId, space.DatasetDraft{
			Key: "late", IdRule: space.IdUser, Fields: []space.DatasetFieldDraft{
				{Key: "label", Kind: space.PropertyKindString, MutableBy: space.MutableByAnyone},
			},
		})
		require.NoError(t, err)
		res, err := sp.Query(bare, late).Subscribe(ctx, space.QueryOpts{})
		require.NoError(t, err)
		defer func() { _ = res.Sub.Close() }()
		assert.Empty(t, res.Initial)

		lab.put(t, ctx, bare, late, "l1", map[string]any{"label": "late"})
		log := newSubLog()
		log.receiveUntil(t, res.Sub, 30*time.Second, func() bool { return log.added["l1"] != nil })
	})

	// The refused reads changed nothing.
	assert.Len(t, lab.rows(t, ctx, holder, lab.plain), 1)
}

// TestE2E_UnknownDataset_RemovedDefinition: after a dataset's
// definition is removed, an object still loaded keeps reading it, and
// it is unknown to an object loaded after the removal.
func TestE2E_UnknownDataset_RemovedDefinition(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Modules: []handler.Module{notesModule()},
	}
	provider := newFixedSeedProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sdk1, err := anysyncsdk.Open(ctx, cfg, provider)
	require.NoError(t, err)
	sp1, err := sdk1.Spaces().Create(ctx, space.CreateRequest{Name: "UnknownRemovedDef"})
	if err != nil {
		_ = sdk1.Close()
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatal(err)
	}
	lab := &sharedLab{sdk: sdk1, sp: sp1}
	lab.declare(t, ctx)
	holder := lab.newObject(t, ctx)
	lab.put(t, ctx, holder, lab.plain, "p1", map[string]any{"label": "plain"})
	require.NoError(t, sp1.Types().RemoveDataset(ctx, lab.typeId, datasetDefs(t, ctx, sp1, lab.typeId)["plain"].Id))
	assert.Len(t, lab.rows(t, ctx, holder, lab.plain), 1, "the loaded object still reads its record")
	spaceId := sp1.Id()
	require.NoError(t, sdk1.Close())

	sdk2, err := anysyncsdk.Open(ctx, cfg, provider)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk2.Close() })
	sp2, err := sdk2.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)

	_, err = sp2.Query(holder, lab.plain).All(ctx)
	assert.ErrorIs(t, err, space.ErrDatasetUnknown, "All")
	_, err = sp2.Query(holder, lab.plain).Snapshot(ctx, space.QueryOpts{})
	assert.ErrorIs(t, err, space.ErrDatasetUnknown, "Snapshot")
	res, err := sp2.Query(holder, lab.plain).Subscribe(ctx, space.QueryOpts{})
	assert.ErrorIs(t, err, space.ErrDatasetUnknown, "Subscribe")
	if res != nil && res.Sub != nil {
		_ = res.Sub.Close()
	}
	_, err = sp2.Aggregate(holder, lab.plain, `[{"$count": "n"}]`).All(ctx)
	assert.ErrorIs(t, err, space.ErrDatasetUnknown, "agg All")
}

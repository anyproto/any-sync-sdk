package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/space"
)

// sharedLabPart declares the datasets the shared-dataset tests write:
// two shared records datasets — `samples` with caller ids (a mutable
// label and score, a write-once kind, a device-local seen flag) and
// `events` with derived ids — and the per-object `plain` next to them.
func sharedLabPart() space.PartDraft {
	mutable := func(key string, kind space.PropertyKind) space.DatasetFieldDraft {
		return space.DatasetFieldDraft{Key: key, Kind: kind, MutableBy: space.MutableByAnyone}
	}
	return space.PartDraft{
		Key: "lab", Name: "Lab",
		Datasets: []space.DatasetDraft{
			{Key: "samples", Shared: true, IdRule: space.IdUser, Fields: []space.DatasetFieldDraft{
				mutable("label", space.PropertyKindString),
				mutable("score", space.PropertyKindNumber),
				{Key: "kind", Kind: space.PropertyKindString},
				{Key: "seen", Kind: space.PropertyKindBoolean, Scope: space.ScopeLocal, MutableBy: space.MutableByAnyone},
			}},
			{Key: "events", Shared: true, Fields: []space.DatasetFieldDraft{
				mutable("label", space.PropertyKindString),
			}},
			{Key: "plain", IdRule: space.IdUser, Fields: []space.DatasetFieldDraft{
				mutable("label", space.PropertyKindString),
			}},
		},
	}
}

// sharedLab is one space with a type declaring sharedLabPart.
type sharedLab struct {
	sdk    *anysyncsdk.SDK
	sp     space.Space
	typeId string
	partId string
	// Collection names of the part's datasets.
	samples, events, plain string
}

// openSharedLab opens an SDK on a fresh data dir, creates a space and
// declares the lab type in it. Skips when no network is reachable.
func openSharedLab(t *testing.T, ctx context.Context, name string) *sharedLab {
	t.Helper()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

	sdk, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Modules: []handler.Module{notesModule()},
	}, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: name})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatal(err)
	}
	lab := &sharedLab{sdk: sdk, sp: sp}
	lab.declare(t, ctx)
	return lab
}

// declare creates the lab type and its part.
func (l *sharedLab) declare(t *testing.T, ctx context.Context) {
	t.Helper()
	typeId, err := l.sp.Types().Create(ctx, space.TypeCreateParams{Name: "Experiment"})
	require.NoError(t, err)
	partId, err := l.sp.Types().AddPart(ctx, typeId, sharedLabPart())
	require.NoError(t, err)
	l.typeId, l.partId = typeId, partId
	l.samples, l.events, l.plain = typeId+"_samples", typeId+"_events", typeId+"_plain"
}

// newObject creates an object of the lab type.
func (l *sharedLab) newObject(t *testing.T, ctx context.Context) string {
	t.Helper()
	id, err := l.sp.Objects().Create(ctx, space.CreateObjectOpts{Type: l.typeId})
	require.NoError(t, err)
	return id
}

// put creates one record with a multi-field $set. The write must land
// whole.
func (l *sharedLab) put(t *testing.T, ctx context.Context, objectId, dataset, recordId string, fields map[string]any) space.ModifyResult {
	t.Helper()
	res, err := l.sp.Modify(ctx, space.ModifyBatch{
		ObjectId: objectId, Dataset: dataset,
		Records: []space.RecordModify{{Id: recordId, Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: fields}}}},
	})
	require.NoError(t, err, "create %s on %s", recordId, objectId)
	require.Empty(t, res.Rejections, "create %s on %s", recordId, objectId)
	return res
}

// set updates one field of an existing record. The write must land.
func (l *sharedLab) set(t *testing.T, ctx context.Context, objectId, dataset, recordId, path string, value any) space.ModifyResult {
	t.Helper()
	res, err := l.sp.Modify(ctx, space.ModifyBatch{
		ObjectId: objectId, Dataset: dataset,
		Records: []space.RecordModify{{Id: recordId, Ops: []space.Op{{Type: space.OpSet, Path: path, Value: value}}}},
	})
	require.NoError(t, err, "set %s.%s on %s", recordId, path, objectId)
	require.Empty(t, res.Rejections, "set %s.%s on %s", recordId, path, objectId)
	return res
}

// rows reads the live records objectId holds in dataset, by id.
func (l *sharedLab) rows(t *testing.T, ctx context.Context, objectId, dataset string) map[string]*anyenc.Value {
	t.Helper()
	all, err := l.sp.Query(objectId, dataset).All(ctx)
	require.NoError(t, err)
	out := make(map[string]*anyenc.Value, len(all))
	for _, row := range all {
		out[row.GetString("id")] = row
	}
	require.Len(t, out, len(all), "every row of %s has its own id", objectId)
	return out
}

// storedRows counts the rows naming objectId in the per-space
// collection of a shared dataset, `<spaceId>_<dataset>`, read straight
// from sdk.db: the rows Space.Query cannot reach once the object is
// gone.
func (l *sharedLab) storedRows(t *testing.T, ctx context.Context, dataset, objectId string) int {
	t.Helper()
	coll, err := l.sdk.Store().OpenCollection(ctx, l.sp.Id()+"_"+dataset)
	require.NoError(t, err, "a shared dataset lives in one collection per space")
	n, err := coll.Find(query.Key{Path: []string{"_objectId"}, Filter: query.NewComp(query.CompOpEq, objectId)}).Count(ctx)
	require.NoError(t, err)
	return n
}

// assertOwnedRows checks that rows are exactly objectId's records
// recordIds: each id is `<objectId>/<recordId>` and each row names
// objectId in `_objectId`.
func assertOwnedRows(t *testing.T, rows map[string]*anyenc.Value, objectId string, recordIds ...string) {
	t.Helper()
	want := make([]string, 0, len(recordIds))
	for _, r := range recordIds {
		want = append(want, objectId+"/"+r)
	}
	got := make([]string, 0, len(rows))
	for id, row := range rows {
		got = append(got, id)
		assert.Equal(t, objectId, row.GetString("_objectId"), "row %s names its object", id)
	}
	assert.ElementsMatch(t, want, got, "records of %s", objectId)
}

// eventIds lists every record id a subscription event carries.
func eventIds(ev space.SubscriptionEvent) []string {
	var out []string
	for _, r := range ev.Added {
		out = append(out, r.Id)
	}
	for _, r := range ev.Updated {
		out = append(out, r.Id)
	}
	for _, r := range ev.Removed {
		out = append(out, r.Id)
	}
	return out
}

// TestE2E_SharedDatasets_Declaration: a records dataset declared
// Shared reads back shared through the type surface and discovery,
// under the usual `<typeId>_<key>` collection name; a per-object
// dataset on the same part does not; the flag is pinned; a module
// dataset cannot be declared shared.
func TestE2E_SharedDatasets_Declaration(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lab := openSharedLab(t, ctx, "SharedDecl")
	sp := lab.sp

	defs, err := sp.Types().Datasets(ctx, lab.typeId)
	require.NoError(t, err)
	byKey := map[string]space.DatasetDef{}
	for _, d := range defs {
		byKey[d.Key] = d
	}
	require.Len(t, byKey, 3)

	samples := byKey["samples"]
	assert.False(t, samples.Invalid, samples.InvalidReason)
	assert.True(t, samples.Shared, "samples is declared shared")
	assert.Equal(t, lab.samples, samples.Collection, "a shared dataset keeps its `<typeId>_<key>` collection name")
	assert.Equal(t, space.RecordsModule, samples.Module)
	assert.Equal(t, space.IdUser, samples.IdRule)
	assert.Equal(t, lab.partId, samples.PartId)
	assert.Len(t, samples.Fields, 4)

	events := byKey["events"]
	assert.False(t, events.Invalid, events.InvalidReason)
	assert.True(t, events.Shared, "events is declared shared")
	assert.Equal(t, lab.events, events.Collection)
	assert.Equal(t, space.IdAuto, events.IdRule)

	plain := byKey["plain"]
	assert.False(t, plain.Shared, "a records dataset is per-object unless declared shared")
	assert.Equal(t, lab.plain, plain.Collection)

	parts, err := sp.Types().Parts(ctx, lab.typeId)
	require.NoError(t, err)
	require.Len(t, parts, 1)
	require.Len(t, parts[0].Datasets, 3)
	for _, d := range parts[0].Datasets {
		assert.Equal(t, d.Key != "plain", d.Shared, "Parts reports the flag of %s", d.Key)
	}

	// Discovery lists the shared datasets under their collection names,
	// owned by the type.
	dss := sp.Datasets()
	ds := findDataset(t, dss, lab.samples)
	assert.True(t, ds.Shared, "discovery reports samples shared")
	assert.Equal(t, []string{lab.typeId}, ds.Owners)
	assert.Equal(t, space.RecordsModule, ds.Module)
	assert.True(t, findDataset(t, dss, lab.events).Shared, "discovery reports events shared")
	assert.False(t, findDataset(t, dss, lab.plain).Shared, "discovery reports plain per-object")
	for _, d := range dss {
		assert.NotEqual(t, sp.Id()+"_"+lab.samples, d.Name, "the per-space storage name is not a dataset name")
	}

	// The flag is pinned either way.
	require.ErrorIs(t, sp.Types().PatchDataset(ctx, lab.typeId, samples.Id, space.DatasetDefPatch{
		Set: map[string]any{"perSpace": false},
	}), space.ErrPinnedField)
	require.ErrorIs(t, sp.Types().PatchDataset(ctx, lab.typeId, plain.Id, space.DatasetDefPatch{
		Set: map[string]any{"perSpace": true},
	}), space.ErrPinnedField)

	// A module dataset cannot be shared, as a namespaced instance or as
	// the module's canonical collection; the refusal writes nothing.
	_, err = sp.Types().AddPart(ctx, lab.typeId, space.PartDraft{
		Key: "body", Datasets: []space.DatasetDraft{{Key: "summary", Module: "notes", Shared: true}},
	})
	require.ErrorContains(t, err, "only a records dataset is shared")
	_, err = sp.Types().AddDataset(ctx, lab.typeId, lab.partId, space.DatasetDraft{Module: "notes", Shared: true})
	require.ErrorContains(t, err, "only a records dataset is shared")
	defs, err = sp.Types().Datasets(ctx, lab.typeId)
	require.NoError(t, err)
	assert.Len(t, defs, 3, "a refused declaration writes nothing")
	parts, err = sp.Types().Parts(ctx, lab.typeId)
	require.NoError(t, err)
	assert.Len(t, parts, 1, "a refused part writes nothing")

	// The same module dataset without the flag declares.
	_, err = sp.Types().AddDataset(ctx, lab.typeId, lab.partId, space.DatasetDraft{Key: "summary", Module: "notes"})
	require.NoError(t, err)
	defs, err = sp.Types().Datasets(ctx, lab.typeId)
	require.NoError(t, err)
	require.Len(t, defs, 4)
	for _, d := range defs {
		if d.Key == "summary" {
			assert.False(t, d.Shared)
			assert.Equal(t, "notes", d.Module)
		}
	}
}

// TestE2E_SharedDatasets_ReadsAndWrites covers one device writing a
// shared dataset on several objects: reads see only the object's own
// records under `<objectId>/<recordId>`; writes address a record by
// its plain or its read-back id and refuse another object's; deletes,
// upserts and aggregations stay inside the object; an object whose
// type does not declare the dataset cannot write it. Each subtest
// creates its own objects.
func TestE2E_SharedDatasets_ReadsAndWrites(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	lab := openSharedLab(t, ctx, "SharedRW")
	sp := lab.sp

	t.Run("SameRecordIdsOnTwoObjects", func(t *testing.T) {
		a, b := lab.newObject(t, ctx), lab.newObject(t, ctx)
		res, err := sp.Modify(ctx, space.ModifyBatch{
			ObjectId: a, Dataset: lab.samples,
			Records: []space.RecordModify{
				{Id: "r1", Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"label": "a-one", "score": 1, "kind": "x"}}}},
				{Id: "r2", Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"label": "a-two", "score": 2, "kind": "x"}}}},
			},
		})
		require.NoError(t, err)
		require.Empty(t, res.Rejections)
		assert.Equal(t, []string{a + "/r1", a + "/r2"}, res.RecordIds, "RecordIds are the ids records read back under")

		res = lab.put(t, ctx, b, lab.samples, "r1", map[string]any{"label": "b-one", "score": 10, "kind": "y"})
		assert.Equal(t, []string{b + "/r1"}, res.RecordIds)
		lab.put(t, ctx, b, lab.samples, "r2", map[string]any{"label": "b-two", "score": 20, "kind": "y"})
		lab.put(t, ctx, b, lab.samples, "r3", map[string]any{"label": "b-three", "score": 30, "kind": "y"})

		// Each object reads back its own records with its own values.
		rowsA := lab.rows(t, ctx, a, lab.samples)
		assertOwnedRows(t, rowsA, a, "r1", "r2")
		if row := rowsA[a+"/r1"]; assert.NotNil(t, row) {
			assert.Equal(t, "a-one", row.GetString("label"))
			assert.Equal(t, float64(1), row.GetFloat64("score"))
		}
		if row := rowsA[a+"/r2"]; assert.NotNil(t, row) {
			assert.Equal(t, "a-two", row.GetString("label"))
		}
		rowsB := lab.rows(t, ctx, b, lab.samples)
		assertOwnedRows(t, rowsB, b, "r1", "r2", "r3")
		if row := rowsB[b+"/r1"]; assert.NotNil(t, row) {
			assert.Equal(t, "b-one", row.GetString("label"))
			assert.Equal(t, float64(10), row.GetFloat64("score"))
		}

		n, err := sp.Query(a, lab.samples).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, n, "Count sees A's records only")
		n, err = sp.Query(b, lab.samples).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 3, n, "Count sees B's records only")
		snap, err := sp.Query(a, lab.samples).Snapshot(ctx, space.QueryOpts{IncludeTotal: true})
		require.NoError(t, err)
		assert.Equal(t, 2, snap.Total, "Snapshot total counts A's records only")
		assert.ElementsMatch(t, []string{a + "/r1", a + "/r2"}, idsOfInitial(snap.Initial))

		// A filter matching B's scores, and B's ids, finds nothing on A.
		rows, err := sp.Query(a, lab.samples).Filter(map[string]any{"score": map[string]any{"$gte": 2}}).All(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{a + "/r2"}, idsOfInitial(rows), "a filter matches the object's own rows only")
		_, err = sp.Query(a, lab.samples).Filter(map[string]any{"id": b + "/r1"}).One(ctx)
		assert.ErrorIs(t, err, space.ErrNotFound, "another object's record is not readable through this object")
		n, err = sp.Query(a, lab.samples).Filter(map[string]any{"_objectId": b}).Count(ctx)
		require.NoError(t, err)
		assert.Zero(t, n, "no row of B is visible through A")
		row, err := sp.Query(a, lab.samples).Filter(map[string]any{"id": a + "/r1"}).One(ctx)
		require.NoError(t, err)
		assert.Equal(t, "a-one", row.GetString("label"))

		// Sort + Limit picks the top of the object's own rows; B's
		// higher scores never enter.
		rows, err = sp.Query(a, lab.samples).Sort("-score").Limit(1).All(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{a + "/r2"}, idsOfInitial(rows))
		rows, err = sp.Query(b, lab.samples).Sort("score").Limit(2).All(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{b + "/r1", b + "/r2"}, idsOfInitial(rows))
		rows, err = sp.Query(a, lab.samples).Sort("-score").Offset(1).All(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{a + "/r1"}, idsOfInitial(rows), "Offset pages within the object's rows")

		// An object of the type that wrote nothing reads nothing.
		empty := lab.newObject(t, ctx)
		n, err = sp.Query(empty, lab.samples).Count(ctx)
		require.NoError(t, err)
		assert.Zero(t, n, "a fresh object holds no records")
		rows, err = sp.Query(empty, lab.samples).All(ctx)
		require.NoError(t, err)
		assert.Empty(t, rows)
	})

	t.Run("AddressingOnWrite", func(t *testing.T) {
		a, b := lab.newObject(t, ctx), lab.newObject(t, ctx)
		lab.put(t, ctx, a, lab.samples, "r1", map[string]any{"label": "a", "score": 1, "kind": "x"})
		lab.put(t, ctx, b, lab.samples, "r1", map[string]any{"label": "b", "score": 1, "kind": "x"})

		// The plain id and the read-back id address the same record.
		res := lab.set(t, ctx, a, lab.samples, "r1", "score", 2)
		assert.Equal(t, []string{a + "/r1"}, res.RecordIds)
		res = lab.set(t, ctx, a, lab.samples, a+"/r1", "label", "a-prefixed")
		assert.Equal(t, []string{a + "/r1"}, res.RecordIds)
		rowsA := lab.rows(t, ctx, a, lab.samples)
		assertOwnedRows(t, rowsA, a, "r1")
		if row := rowsA[a+"/r1"]; assert.NotNil(t, row) {
			assert.Equal(t, "a-prefixed", row.GetString("label"))
			assert.Equal(t, float64(2), row.GetFloat64("score"))
		}

		// A record is created by its read-back id too, under that id.
		res = lab.put(t, ctx, a, lab.samples, a+"/r2", map[string]any{"label": "a2", "score": 5, "kind": "x"})
		assert.Equal(t, []string{a + "/r2"}, res.RecordIds)
		assertOwnedRows(t, lab.rows(t, ctx, a, lab.samples), a, "r1", "r2")

		// Another object's id, or any other id holding "/", fails the
		// call: Modify, Delete and ModifyMany alike, and a batch mixing
		// a valid id with a foreign one writes nothing.
		for _, rec := range []space.RecordModify{
			{Id: b + "/r1", Ops: []space.Op{{Type: space.OpSet, Path: "label", Value: "hijack"}}},
			{Id: b + "/r1", Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: "label", Value: "hijack"}}},
			{Id: b + "/new", Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"label": "hijack", "kind": "x"}}}},
			{Id: "x/y", Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"label": "slash", "kind": "x"}}}},
			{Id: a + "/x/y", Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"label": "slash", "kind": "x"}}}},
		} {
			_, err := sp.Modify(ctx, space.ModifyBatch{ObjectId: a, Dataset: lab.samples, Records: []space.RecordModify{rec}})
			assert.ErrorIs(t, err, space.ErrRecordIdOfAnotherObject, "Modify %q (upsert %t)", rec.Id, rec.Upsert)
		}
		_, err := sp.Modify(ctx, space.ModifyBatch{ObjectId: a, Dataset: lab.samples, Records: []space.RecordModify{
			{Id: "r1", Ops: []space.Op{{Type: space.OpSet, Path: "score", Value: 99}}},
			{Id: b + "/r1", Ops: []space.Op{{Type: space.OpSet, Path: "score", Value: 99}}},
		}})
		assert.ErrorIs(t, err, space.ErrRecordIdOfAnotherObject, "a batch naming a foreign id")
		_, err = sp.Delete(ctx, space.DeleteBatch{ObjectId: a, Dataset: lab.samples, RecordIds: []string{b + "/r1"}})
		assert.ErrorIs(t, err, space.ErrRecordIdOfAnotherObject, "Delete of a foreign id")
		_, err = sp.ModifyMany(ctx, []space.ModifyBatch{{ObjectId: a, Dataset: lab.samples, Records: []space.RecordModify{
			{Id: b + "/r1", Ops: []space.Op{{Type: space.OpSet, Path: "label", Value: "hijack"}}},
		}}})
		assert.ErrorIs(t, err, space.ErrRecordIdOfAnotherObject, "ModifyMany of a foreign id")

		// None of the refused writes landed anywhere.
		rowsA = lab.rows(t, ctx, a, lab.samples)
		assertOwnedRows(t, rowsA, a, "r1", "r2")
		if row := rowsA[a+"/r1"]; assert.NotNil(t, row) {
			assert.Equal(t, float64(2), row.GetFloat64("score"), "the refused mixed batch wrote nothing")
		}
		rowsB := lab.rows(t, ctx, b, lab.samples)
		assertOwnedRows(t, rowsB, b, "r1")
		if row := rowsB[b+"/r1"]; assert.NotNil(t, row) {
			assert.Equal(t, "b", row.GetString("label"), "B's record with the same id is untouched")
			assert.Equal(t, float64(1), row.GetFloat64("score"))
		}

		// An empty-id create on a derived-id shared dataset returns the
		// read-back id, and that id writes back.
		res, err = sp.Modify(ctx, space.ModifyBatch{
			ObjectId: a, Dataset: lab.events,
			Records: []space.RecordModify{{Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: "label", Value: "auto"}}}},
		})
		require.NoError(t, err)
		require.Empty(t, res.Rejections)
		require.Len(t, res.RecordIds, 1)
		autoId := res.RecordIds[0]
		assert.True(t, strings.HasPrefix(autoId, a+"/"), "derived id %q carries the object's prefix", autoId)
		assert.NotEqual(t, a+"/", autoId)
		row, err := sp.Query(a, lab.events).Filter(map[string]any{"id": autoId}).One(ctx)
		require.NoError(t, err, "the derived id reads back")
		assert.Equal(t, "auto", row.GetString("label"))
		assert.Equal(t, a, row.GetString("_objectId"))
		res = lab.set(t, ctx, a, lab.events, autoId, "label", "auto-2")
		assert.Equal(t, []string{autoId}, res.RecordIds)
		res = lab.set(t, ctx, a, lab.events, strings.TrimPrefix(autoId, a+"/"), "label", "auto-3")
		assert.Equal(t, []string{autoId}, res.RecordIds)
		rowsEv := lab.rows(t, ctx, a, lab.events)
		require.Len(t, rowsEv, 1, "the write-backs updated the derived record, created nothing")
		if row := rowsEv[autoId]; assert.NotNil(t, row) {
			assert.Equal(t, "auto-3", row.GetString("label"))
		}
		n, err := sp.Query(b, lab.events).Count(ctx)
		require.NoError(t, err)
		assert.Zero(t, n, "B holds no events")
	})

	t.Run("Delete", func(t *testing.T) {
		a, b := lab.newObject(t, ctx), lab.newObject(t, ctx)
		lab.put(t, ctx, a, lab.samples, "r1", map[string]any{"label": "a1", "score": 1, "kind": "x"})
		lab.put(t, ctx, a, lab.samples, "r2", map[string]any{"label": "a2", "score": 2, "kind": "x"})
		lab.put(t, ctx, b, lab.samples, "r1", map[string]any{"label": "b1", "score": 1, "kind": "x"})

		// Delete by the read-back id.
		dres, err := sp.Delete(ctx, space.DeleteBatch{ObjectId: a, Dataset: lab.samples, RecordIds: []string{a + "/r1"}})
		require.NoError(t, err)
		assert.Empty(t, dres.Rejections)
		assert.Equal(t, []string{a + "/r1"}, dres.RecordIds)
		assertOwnedRows(t, lab.rows(t, ctx, a, lab.samples), a, "r2")
		n, err := sp.Query(a, lab.samples).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n)

		// The tombstone surfaces with IncludeDeleted under the same id.
		tomb, err := sp.Query(a, lab.samples).
			Filter(map[string]any{"id": a + "/r1"}).
			Projection(space.ProjectionOpts{IncludeDeleted: true}).
			One(ctx)
		require.NoError(t, err)
		assert.NotNil(t, tomb.Get("_deletedAt"), "the record is a tombstone")
		assert.Equal(t, a+"/r1", tomb.GetString("id"))
		assert.Equal(t, a, tomb.GetString("_objectId"), "the tombstone names its object")
		n, err = sp.Query(a, lab.samples).Projection(space.ProjectionOpts{IncludeDeleted: true}).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, n, "IncludeDeleted counts A's live row and tombstone only")

		// B's record with the same id is live and unchanged.
		rowsB := lab.rows(t, ctx, b, lab.samples)
		assertOwnedRows(t, rowsB, b, "r1")
		if row := rowsB[b+"/r1"]; assert.NotNil(t, row) {
			assert.Nil(t, row.Get("_deletedAt"))
			assert.Equal(t, "b1", row.GetString("label"))
		}

		// Delete by the plain id.
		dres, err = sp.Delete(ctx, space.DeleteBatch{ObjectId: a, Dataset: lab.samples, RecordIds: []string{"r2"}})
		require.NoError(t, err)
		assert.Empty(t, dres.Rejections)
		assert.Equal(t, []string{a + "/r2"}, dres.RecordIds)
		assert.Empty(t, lab.rows(t, ctx, a, lab.samples))
		n, err = sp.Query(a, lab.samples).Projection(space.ProjectionOpts{IncludeDeleted: true}).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, n)

		// A write landing on the tombstone is rejected under the
		// read-back id.
		mres, err := sp.Modify(ctx, space.ModifyBatch{ObjectId: a, Dataset: lab.samples, Records: []space.RecordModify{
			{Id: "r1", Ops: []space.Op{{Type: space.OpSet, Path: "label", Value: "zombie"}}},
		}})
		require.NoError(t, err)
		require.Len(t, mres.Rejections, 1)
		assert.Equal(t, a+"/r1", mres.Rejections[0].RecordId, "a rejection names the read-back id")
		assert.Equal(t, 0, mres.Rejections[0].RecordIndex)
		assert.ErrorIs(t, mres.Rejections[0].ReasonErr, space.ErrRecordDeleted)

		// B's record still takes writes.
		lab.set(t, ctx, b, lab.samples, "r1", "label", "b1-edited")
		if row := lab.rows(t, ctx, b, lab.samples)[b+"/r1"]; assert.NotNil(t, row) {
			assert.Equal(t, "b1-edited", row.GetString("label"))
		}

		// Deleting a record the object never held seeds its tombstone
		// under the object's id, and nothing on B.
		dres, err = sp.Delete(ctx, space.DeleteBatch{ObjectId: a, Dataset: lab.samples, RecordIds: []string{"ghost"}})
		require.NoError(t, err)
		assert.Equal(t, []string{a + "/ghost"}, dres.RecordIds)
		ghost, err := sp.Query(a, lab.samples).
			Filter(map[string]any{"id": a + "/ghost"}).
			Projection(space.ProjectionOpts{IncludeDeleted: true}).
			One(ctx)
		require.NoError(t, err)
		assert.NotNil(t, ghost.Get("_deletedAt"))
		assert.Equal(t, a, ghost.GetString("_objectId"))
		n, err = sp.Query(b, lab.samples).Projection(space.ProjectionOpts{IncludeDeleted: true}).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n, "B holds its one live record and no tombstone")
	})

	t.Run("ModifyMany", func(t *testing.T) {
		a := lab.newObject(t, ctx)
		lab.put(t, ctx, a, lab.samples, "r1", map[string]any{"label": "a1", "score": 1, "kind": "x"})
		results, err := sp.ModifyMany(ctx, []space.ModifyBatch{
			{ObjectId: a, Dataset: lab.samples, Records: []space.RecordModify{
				{Id: a + "/r1", Ops: []space.Op{{Type: space.OpSet, Path: "label", Value: "many"}}},
				{Id: "r2", Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"label": "new", "kind": "x"}}}},
			}},
			{ObjectId: a, Dataset: lab.plain, Records: []space.RecordModify{
				{Id: "p1", Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: "label", Value: "plain"}}},
			}},
		})
		require.NoError(t, err)
		require.Len(t, results, 2)
		assert.Equal(t, []string{a + "/r1", a + "/r2"}, results[0].RecordIds, "shared dataset ids carry the object's prefix")
		assert.Equal(t, []string{"p1"}, results[1].RecordIds, "per-object dataset ids stay plain")
		rowsA := lab.rows(t, ctx, a, lab.samples)
		assertOwnedRows(t, rowsA, a, "r1", "r2")
		if row := rowsA[a+"/r1"]; assert.NotNil(t, row) {
			assert.Equal(t, "many", row.GetString("label"))
		}
		plainRows := lab.rows(t, ctx, a, lab.plain)
		require.Contains(t, plainRows, "p1", "a per-object record reads back under its plain id")
		assert.Nil(t, plainRows["p1"].Get("_objectId"), "a per-object row carries no _objectId")
	})

	t.Run("ReservedObjectIdField", func(t *testing.T) {
		a, b := lab.newObject(t, ctx), lab.newObject(t, ctx)
		lab.put(t, ctx, a, lab.samples, "r1", map[string]any{"label": "a1", "score": 1, "kind": "x"})

		// `_objectId` is not writable: an attempt fails the call as a
		// validation error, and the row keeps naming its object.
		for _, rec := range []space.RecordModify{
			{Id: "r1", Ops: []space.Op{{Type: space.OpSet, Path: "_objectId", Value: b}}},
			{Id: "r1", Ops: []space.Op{{Type: space.OpUnset, Path: "_objectId"}}},
			{Id: "r2", Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"_objectId": b, "label": "a2", "kind": "x"}}}},
		} {
			_, err := sp.Modify(ctx, space.ModifyBatch{ObjectId: a, Dataset: lab.samples, Records: []space.RecordModify{rec}})
			assert.ErrorIs(t, err, crdt.ErrValidation, "a write of _objectId (%s on %s) is refused", rec.Ops[0].Type, rec.Id)
		}
		ures, err := sp.Upsert(ctx, space.UpsertBatch{ObjectId: a, Dataset: lab.samples, Records: []space.UpsertRecord{
			{Id: "r3", Fields: map[string]any{"_objectId": b, "label": "a3", "kind": "x"}},
		}})
		require.NoError(t, err)
		require.Len(t, ures.Rejections, 1, "Upsert refuses the reserved field")
		assert.Equal(t, "r3", ures.Rejections[0].Id)

		assertOwnedRows(t, lab.rows(t, ctx, a, lab.samples), a, "r1")
		assert.Empty(t, lab.rows(t, ctx, b, lab.samples), "nothing moved to B")
	})

	t.Run("LocalScope", func(t *testing.T) {
		a, b := lab.newObject(t, ctx), lab.newObject(t, ctx)
		lab.put(t, ctx, a, lab.samples, "r1", map[string]any{"label": "a1", "score": 1, "kind": "x"})
		lab.put(t, ctx, a, lab.samples, "r2", map[string]any{"label": "a2", "score": 2, "kind": "x"})
		lab.put(t, ctx, b, lab.samples, "r1", map[string]any{"label": "b1", "score": 1, "kind": "x"})
		local := func(objectId, recordId string) (space.ModifyResult, error) {
			return sp.Modify(ctx, space.ModifyBatch{
				ObjectId: objectId, Dataset: lab.samples, Scope: space.ScopeLocal,
				Records: []space.RecordModify{{Id: recordId, Ops: []space.Op{{Type: space.OpSet, Path: "seen", Value: true}}}},
			})
		}

		// A device-local field is written by either id form, on the
		// object's own record only.
		res, err := local(a, "r1")
		require.NoError(t, err)
		require.Empty(t, res.Rejections)
		assert.Equal(t, []string{a + "/r1"}, res.RecordIds)
		res, err = local(a, a+"/r2")
		require.NoError(t, err)
		require.Empty(t, res.Rejections)
		assert.Equal(t, []string{a + "/r2"}, res.RecordIds)
		_, err = local(a, b+"/r1")
		assert.ErrorIs(t, err, space.ErrRecordIdOfAnotherObject)
		// A local write never creates: an absent record is rejected
		// under the read-back id.
		res, err = local(a, "absent")
		require.NoError(t, err)
		if assert.Len(t, res.Rejections, 1) {
			assert.Equal(t, a+"/absent", res.Rejections[0].RecordId)
		}

		rowsA := lab.rows(t, ctx, a, lab.samples)
		assertOwnedRows(t, rowsA, a, "r1", "r2")
		for id, row := range rowsA {
			assert.True(t, row.GetBool("seen"), "%s carries the local flag", id)
		}
		rowsB := lab.rows(t, ctx, b, lab.samples)
		if row := rowsB[b+"/r1"]; assert.NotNil(t, row) {
			assert.Nil(t, row.Get("seen"), "B's record with the same id carries no flag")
		}
	})

	t.Run("Upsert", func(t *testing.T) {
		a, b := lab.newObject(t, ctx), lab.newObject(t, ctx)
		batch := func(objectId string, recs ...space.UpsertRecord) space.UpsertBatch {
			return space.UpsertBatch{ObjectId: objectId, Dataset: lab.samples, Records: recs}
		}
		seed := []space.UpsertRecord{
			{Id: "u1", Fields: map[string]any{"label": "one", "score": 1, "kind": "x"}},
			{Id: "u2", Fields: map[string]any{"label": "two", "score": 2, "kind": "x"}},
			{Id: "u3", Fields: map[string]any{"label": "three", "score": 3, "kind": "y"}},
		}

		res, err := sp.Upsert(ctx, batch(a, seed...))
		require.NoError(t, err)
		assert.Equal(t, 3, res.Created)
		assert.Empty(t, res.Rejections)
		require.Len(t, res.Pages, 1)
		assert.ElementsMatch(t, []string{a + "/u1", a + "/u2", a + "/u3"}, res.Pages[0].RecordIds)
		assertOwnedRows(t, lab.rows(t, ctx, a, lab.samples), a, "u1", "u2", "u3")

		// The same batch again is a no-op.
		res, err = sp.Upsert(ctx, batch(a, seed...))
		require.NoError(t, err)
		assert.Equal(t, 3, res.Skipped, "re-running the batch skips every record")
		assert.Zero(t, res.Created+res.Updated)
		assert.Empty(t, res.Pages)
		assert.Empty(t, res.Rejections)

		// One mutable field changed: one update.
		changed := append([]space.UpsertRecord(nil), seed...)
		changed[1] = space.UpsertRecord{Id: "u2", Fields: map[string]any{"label": "two", "score": 22, "kind": "x"}}
		res, err = sp.Upsert(ctx, batch(a, changed...))
		require.NoError(t, err)
		assert.Equal(t, 1, res.Updated)
		assert.Equal(t, 2, res.Skipped)
		assert.Zero(t, res.Created)
		require.Len(t, res.Pages, 1)
		assert.Equal(t, []string{a + "/u2"}, res.Pages[0].RecordIds)
		rowsA := lab.rows(t, ctx, a, lab.samples)
		assertOwnedRows(t, rowsA, a, "u1", "u2", "u3")
		if row := rowsA[a+"/u2"]; assert.NotNil(t, row) {
			assert.Equal(t, float64(22), row.GetFloat64("score"))
		}

		// A write-once change is rejected under the plain id at its
		// batch index.
		res, err = sp.Upsert(ctx, batch(a, seed[0],
			space.UpsertRecord{Id: "u3", Fields: map[string]any{"label": "three", "score": 3, "kind": "z"}}))
		require.NoError(t, err)
		require.Len(t, res.Rejections, 1)
		assert.Equal(t, "u3", res.Rejections[0].Id, "a rejection reports the plain id")
		assert.Equal(t, 1, res.Rejections[0].Index)
		assert.ErrorIs(t, res.Rejections[0].Err, space.ErrImmutableFieldChanged)
		assert.Equal(t, 1, res.Skipped)

		// The same plain ids on B are B's own records.
		res, err = sp.Upsert(ctx, batch(b, seed...))
		require.NoError(t, err)
		assert.Equal(t, 3, res.Created, "B's records are new although A holds the same ids")
		assert.Empty(t, res.Rejections)
		require.Len(t, res.Pages, 1)
		assert.ElementsMatch(t, []string{b + "/u1", b + "/u2", b + "/u3"}, res.Pages[0].RecordIds)
		rowsB := lab.rows(t, ctx, b, lab.samples)
		assertOwnedRows(t, rowsB, b, "u1", "u2", "u3")
		if row := rowsB[b+"/u2"]; assert.NotNil(t, row) {
			assert.Equal(t, float64(2), row.GetFloat64("score"))
		}
		if row := lab.rows(t, ctx, a, lab.samples)[a+"/u2"]; assert.NotNil(t, row) {
			assert.Equal(t, float64(22), row.GetFloat64("score"), "A's record is untouched by B's upsert")
		}

		// A tombstone on A rejects A's upsert under the plain id; B's
		// record with that id is unaffected.
		_, err = sp.Delete(ctx, space.DeleteBatch{ObjectId: a, Dataset: lab.samples, RecordIds: []string{"u1"}})
		require.NoError(t, err)
		res, err = sp.Upsert(ctx, batch(a, seed[0]))
		require.NoError(t, err)
		require.Len(t, res.Rejections, 1)
		assert.Equal(t, "u1", res.Rejections[0].Id)
		assert.Equal(t, 0, res.Rejections[0].Index)
		assert.ErrorIs(t, res.Rejections[0].Err, space.ErrRecordDeleted)
		res, err = sp.Upsert(ctx, batch(b, seed[0]))
		require.NoError(t, err)
		assert.Equal(t, 1, res.Skipped, "B's u1 is live and unchanged")
		assert.Empty(t, res.Rejections)
	})

	// An Upsert naming another object's record, any other id holding
	// "/", or the object's bare prefix, is a per-record rejection as
	// ErrRecordIdOfAnotherObject, and writes nothing.
	t.Run("UpsertForeignIdRefused", func(t *testing.T) {
		a, b := lab.newObject(t, ctx), lab.newObject(t, ctx)
		fields := map[string]any{"label": "one", "score": 1, "kind": "x"}
		for _, obj := range []string{a, b} {
			res, err := sp.Upsert(ctx, space.UpsertBatch{ObjectId: obj, Dataset: lab.samples,
				Records: []space.UpsertRecord{{Id: "u1", Fields: fields}}})
			require.NoError(t, err)
			require.Equal(t, 1, res.Created)
		}
		for _, id := range []string{b + "/u1", "x/y", a + "/x/y", a + "/"} {
			res, err := sp.Upsert(ctx, space.UpsertBatch{ObjectId: a, Dataset: lab.samples, Records: []space.UpsertRecord{
				{Id: id, Fields: map[string]any{"label": "hijack", "score": 1, "kind": "x"}},
			}})
			require.NoError(t, err, "Upsert of %q on A", id)
			if assert.Len(t, res.Rejections, 1, "Upsert of %q on A", id) {
				assert.Equal(t, 0, res.Rejections[0].Index)
				assert.Equal(t, id, res.Rejections[0].Id, "the rejection names the id as written")
				assert.ErrorIs(t, res.Rejections[0].Err, space.ErrRecordIdOfAnotherObject, "Upsert of %q on A", id)
			}
			assert.Zero(t, res.Created+res.Updated+res.Skipped, "Upsert of %q wrote nothing", id)
			assert.Empty(t, res.Pages, "Upsert of %q wrote no change", id)
		}
		assertOwnedRows(t, lab.rows(t, ctx, a, lab.samples), a, "u1")
		rowsB := lab.rows(t, ctx, b, lab.samples)
		assertOwnedRows(t, rowsB, b, "u1")
		if row := rowsB[b+"/u1"]; assert.NotNil(t, row) {
			assert.Equal(t, "one", row.GetString("label"), "B's record is untouched")
		}
	})

	// With an id pattern admitting "/", the read-back id of an existing
	// record reaches the write path, and addresses that record: an
	// Upsert keyed by it finds the record unchanged and skips it, on
	// every run, writing no change.
	t.Run("UpsertReadBackIdRerun", func(t *testing.T) {
		_, err := sp.Types().AddDataset(ctx, lab.typeId, lab.partId, space.DatasetDraft{
			Key: "paths", Shared: true, IdRule: space.IdUser, IdPattern: `[A-Za-z0-9._:/-]+`,
			Fields: []space.DatasetFieldDraft{
				{Key: "label", Kind: space.PropertyKindString, MutableBy: space.MutableByAnyone},
			},
		})
		require.NoError(t, err)
		paths := lab.typeId + "_paths"
		a := lab.newObject(t, ctx)
		fields := map[string]any{"label": "one"}
		res, err := sp.Upsert(ctx, space.UpsertBatch{ObjectId: a, Dataset: paths,
			Records: []space.UpsertRecord{{Id: "p1", Fields: fields}}})
		require.NoError(t, err)
		require.Equal(t, 1, res.Created)

		for run := 1; run <= 2; run++ {
			res, err = sp.Upsert(ctx, space.UpsertBatch{ObjectId: a, Dataset: paths,
				Records: []space.UpsertRecord{{Id: a + "/p1", Fields: fields}}})
			require.NoError(t, err, "run %d", run)
			assert.Empty(t, res.Rejections, "run %d", run)
			assert.Equal(t, 1, res.Skipped, "run %d: %s/p1 is the unchanged record p1", run, a)
			assert.Zero(t, res.Created+res.Updated, "run %d: nothing is created or updated", run)
			assert.Empty(t, res.Pages, "run %d: an unchanged record writes no change", run)
		}
		assertOwnedRows(t, lab.rows(t, ctx, a, paths), a, "p1")
		list, err := sp.History().ListChanges(ctx, a, space.HistoryFilter{Dataset: paths}, 0, "")
		require.NoError(t, err)
		assert.Len(t, list.Changes, 1, "the record's history holds its create only")
	})

	// The plain id and the object's stored id name one record: a page
	// holding both rejects the second as a duplicate, under the plain
	// id, and writes the first.
	t.Run("UpsertBothIdForms", func(t *testing.T) {
		a := lab.newObject(t, ctx)
		res, err := sp.Upsert(ctx, space.UpsertBatch{ObjectId: a, Dataset: lab.samples, Records: []space.UpsertRecord{
			{Id: "u1", Fields: map[string]any{"label": "plain", "score": 1, "kind": "x"}},
			{Id: a + "/u1", Fields: map[string]any{"label": "stored", "score": 2, "kind": "x"}},
		}})
		require.NoError(t, err)
		assert.Equal(t, 1, res.Created)
		if assert.Len(t, res.Rejections, 1) {
			assert.Equal(t, 1, res.Rejections[0].Index)
			assert.Equal(t, "u1", res.Rejections[0].Id)
			assert.ErrorContains(t, res.Rejections[0].Err, "duplicate id")
		}
		require.Len(t, res.Pages, 1)
		assert.Equal(t, []string{a + "/u1"}, res.Pages[0].RecordIds)
		rows := lab.rows(t, ctx, a, lab.samples)
		assertOwnedRows(t, rows, a, "u1")
		if row := rows[a+"/u1"]; assert.NotNil(t, row) {
			assert.Equal(t, "plain", row.GetString("label"))
		}
	})

	// The object's bare prefix names no record: a write naming it fails
	// rather than creating a record under a derived id.
	t.Run("BarePrefixRefused", func(t *testing.T) {
		a := lab.newObject(t, ctx)
		lab.put(t, ctx, a, lab.events, "", map[string]any{"label": "auto"})
		bare := a + "/"
		for _, tc := range []struct {
			dataset string
			rec     space.RecordModify
		}{
			{lab.samples, space.RecordModify{Id: bare, Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"label": "bare", "kind": "x"}}}}},
			{lab.samples, space.RecordModify{Id: bare, Ops: []space.Op{{Type: space.OpSet, Path: "label", Value: "bare"}}}},
			{lab.events, space.RecordModify{Id: bare, Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: "label", Value: "bare"}}}},
		} {
			_, err := sp.Modify(ctx, space.ModifyBatch{ObjectId: a, Dataset: tc.dataset, Records: []space.RecordModify{tc.rec}})
			assert.ErrorIs(t, err, space.ErrRecordIdOfAnotherObject, "Modify of the bare prefix on %s (upsert %t)", tc.dataset, tc.rec.Upsert)
		}
		_, err := sp.Delete(ctx, space.DeleteBatch{ObjectId: a, Dataset: lab.samples, RecordIds: []string{bare}})
		assert.ErrorIs(t, err, space.ErrRecordIdOfAnotherObject, "Delete of the bare prefix")
		res, err := sp.Upsert(ctx, space.UpsertBatch{ObjectId: a, Dataset: lab.samples, Records: []space.UpsertRecord{
			{Id: bare, Fields: map[string]any{"label": "bare", "kind": "x"}},
		}})
		require.NoError(t, err)
		if assert.Len(t, res.Rejections, 1) {
			assert.ErrorIs(t, res.Rejections[0].Err, space.ErrRecordIdOfAnotherObject)
		}
		assert.Zero(t, res.Created)

		assert.Empty(t, lab.rows(t, ctx, a, lab.samples), "nothing was written to samples")
		assert.Len(t, lab.rows(t, ctx, a, lab.events), 1, "events holds its one derived record")
		n, err := sp.Query(a, lab.samples).Projection(space.ProjectionOpts{IncludeDeleted: true}).Count(ctx)
		require.NoError(t, err)
		assert.Zero(t, n, "no tombstone either")
	})

	t.Run("Aggregate", func(t *testing.T) {
		a, b := lab.newObject(t, ctx), lab.newObject(t, ctx)
		for i, kind := range []string{"x", "x", "y"} {
			lab.put(t, ctx, a, lab.samples, fmt.Sprintf("s%d", i+1), map[string]any{"label": "a", "score": i, "kind": kind})
		}
		for i, kind := range []string{"x", "y", "y", "z"} {
			lab.put(t, ctx, b, lab.samples, fmt.Sprintf("s%d", i+1), map[string]any{"label": "b", "score": i, "kind": kind})
		}
		groupByKind := `[
			{"$group": {"_id": "$kind", "n": {"$count": {}}}},
			{"$sort": {"id": 1}}
		]`
		groups := func(objectId string) map[string]int {
			t.Helper()
			rows, err := sp.Aggregate(objectId, lab.samples, groupByKind).All(ctx)
			require.NoError(t, err)
			out := map[string]int{}
			for _, r := range rows {
				out[r.GetString("id")] = r.GetInt("n")
			}
			return out
		}
		assert.Equal(t, map[string]int{"x": 2, "y": 1}, groups(a), "A's groups count A's records only")
		assert.Equal(t, map[string]int{"x": 1, "y": 2, "z": 1}, groups(b), "B's groups count B's records only")

		rows, err := sp.Aggregate(a, lab.samples, `[{"$count": "n"}]`).All(ctx)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, 3, rows[0].GetInt("n"))

		rows, err = sp.Aggregate(a, lab.samples, `[{"$group": {"_id": "$_objectId", "n": {"$count": {}}}}]`).All(ctx)
		require.NoError(t, err)
		require.Len(t, rows, 1, "one owning object in A's pipeline")
		assert.Equal(t, a, rows[0].GetString("id"))
		assert.Equal(t, 3, rows[0].GetInt("n"))

		n, err := sp.Aggregate(a, lab.samples, `[{"$match": {"_objectId": "`+b+`"}}]`).Count(ctx)
		require.NoError(t, err)
		assert.Zero(t, n, "B's rows never enter A's pipeline")

		// A tombstone leaves the object's aggregation.
		_, err = sp.Delete(ctx, space.DeleteBatch{ObjectId: a, Dataset: lab.samples, RecordIds: []string{"s3"}})
		require.NoError(t, err)
		assert.Equal(t, map[string]int{"x": 2}, groups(a))
		assert.Equal(t, map[string]int{"x": 1, "y": 2, "z": 1}, groups(b))
	})

	t.Run("WriteGate", func(t *testing.T) {
		holder := lab.newObject(t, ctx)
		lab.put(t, ctx, holder, lab.samples, "g1", map[string]any{"label": "held", "score": 1, "kind": "x"})

		stray, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: markerTypeId(t, ctx, sp, "Stray")})
		require.NoError(t, err)
		_, err = sp.Modify(ctx, space.ModifyBatch{ObjectId: stray, Dataset: lab.samples, Records: []space.RecordModify{
			{Id: "g1", Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"label": "stray", "kind": "x"}}}},
		}})
		require.ErrorIs(t, err, space.ErrDatasetNotDeclared)
		_, err = sp.Upsert(ctx, space.UpsertBatch{ObjectId: stray, Dataset: lab.samples, Records: []space.UpsertRecord{
			{Id: "g1", Fields: map[string]any{"label": "stray", "kind": "x"}},
		}})
		require.ErrorIs(t, err, space.ErrDatasetNotDeclared)
		_, err = sp.Modify(ctx, space.ModifyBatch{ObjectId: stray, Dataset: lab.events, Records: []space.RecordModify{
			{Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: "label", Value: "stray"}}},
		}})
		require.ErrorIs(t, err, space.ErrDatasetNotDeclared)

		// The stray object reads none of the holder's rows.
		rows, err := sp.Query(stray, lab.samples).All(ctx)
		require.NoError(t, err)
		assert.Empty(t, rows)
		assertOwnedRows(t, lab.rows(t, ctx, holder, lab.samples), holder, "g1")
	})

	// Runs last: it changes the samples schema the subtests above use.
	t.Run("SchemaEvolution", func(t *testing.T) {
		a, b := lab.newObject(t, ctx), lab.newObject(t, ctx)
		lab.put(t, ctx, a, lab.samples, "r1", map[string]any{"label": "a1", "score": 1, "kind": "x"})
		lab.put(t, ctx, b, lab.samples, "r1", map[string]any{"label": "b1", "score": 1, "kind": "x"})
		// Load both objects' controllers before the schema changes.
		assertOwnedRows(t, lab.rows(t, ctx, a, lab.samples), a, "r1")
		assertOwnedRows(t, lab.rows(t, ctx, b, lab.samples), b, "r1")

		defs, err := sp.Types().Datasets(ctx, lab.typeId)
		require.NoError(t, err)
		var defId string
		for _, d := range defs {
			if d.Key == "samples" {
				defId = d.Id
			}
		}
		require.NotEmpty(t, defId)
		_, err = sp.Types().AddDatasetField(ctx, lab.typeId, defId, space.DatasetFieldDraft{
			Key: "note", Kind: space.PropertyKindString, MutableBy: space.MutableByAnyone,
		})
		require.NoError(t, err)

		defs, err = sp.Types().Datasets(ctx, lab.typeId)
		require.NoError(t, err)
		for _, d := range defs {
			if d.Key == "samples" {
				assert.True(t, d.Shared, "a field added to a shared dataset keeps it shared")
				assert.Len(t, d.Fields, 5)
			}
		}
		assert.True(t, findDataset(t, sp.Datasets(), lab.samples).Shared)

		// The new field writes on the object's own record; both objects'
		// records survive the re-registration.
		res := lab.set(t, ctx, a, lab.samples, "r1", "note", "n")
		assert.Equal(t, []string{a + "/r1"}, res.RecordIds)
		lab.put(t, ctx, b, lab.samples, "r2", map[string]any{"label": "b2", "kind": "x", "note": "fresh"})
		rowsA := lab.rows(t, ctx, a, lab.samples)
		assertOwnedRows(t, rowsA, a, "r1")
		if row := rowsA[a+"/r1"]; assert.NotNil(t, row) {
			assert.Equal(t, "n", row.GetString("note"))
			assert.Equal(t, "a1", row.GetString("label"))
		}
		rowsB := lab.rows(t, ctx, b, lab.samples)
		assertOwnedRows(t, rowsB, b, "r1", "r2")
		if row := rowsB[b+"/r1"]; assert.NotNil(t, row) {
			assert.Nil(t, row.Get("note"))
		}
		ures, err := sp.Upsert(ctx, space.UpsertBatch{ObjectId: b, Dataset: lab.samples, Records: []space.UpsertRecord{
			{Id: "r1", Fields: map[string]any{"label": "b1", "score": 1, "kind": "x"}},
		}})
		require.NoError(t, err)
		assert.Equal(t, 1, ures.Skipped, "Upsert finds B's record after the re-registration")
	})
}

// TestE2E_SharedDatasets_Restart: a shared dataset's records, tombstones
// and history survive an SDK restart on the same data dir, still split
// per object.
func TestE2E_SharedDatasets_Restart(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

	dataDir := t.TempDir()
	provider := newFixedSeedProvider(t)
	cfg := config.Config{
		Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// First boot: declare, write on two objects, delete one record.
	sdk1, err := anysyncsdk.Open(ctx, cfg, provider)
	require.NoError(t, err)
	sp1, err := sdk1.Spaces().Create(ctx, space.CreateRequest{Name: "SharedRestart"})
	if err != nil {
		_ = sdk1.Close()
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatal(err)
	}
	lab1 := &sharedLab{sdk: sdk1, sp: sp1}
	lab1.declare(t, ctx)
	a, b := lab1.newObject(t, ctx), lab1.newObject(t, ctx)
	v1 := lab1.put(t, ctx, a, lab1.samples, "r1", map[string]any{"label": "a1", "score": 1, "kind": "x"}).ChangeId
	lab1.put(t, ctx, a, lab1.samples, "r2", map[string]any{"label": "a2", "score": 2, "kind": "x"})
	lab1.put(t, ctx, b, lab1.samples, "r1", map[string]any{"label": "b1", "score": 1, "kind": "x"})
	_, err = sp1.Delete(ctx, space.DeleteBatch{ObjectId: a, Dataset: lab1.samples, RecordIds: []string{"r2"}})
	require.NoError(t, err)
	v2 := lab1.set(t, ctx, a, lab1.samples, "r1", "label", "a1-edited").ChangeId
	spaceId := sp1.Id()
	require.NoError(t, sdk1.Close())

	// Second boot on the same data dir.
	sdk2, err := anysyncsdk.Open(ctx, cfg, provider)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk2.Close() })
	sp2, err := sdk2.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)
	lab := &sharedLab{sdk: sdk2, sp: sp2, typeId: lab1.typeId, partId: lab1.partId,
		samples: lab1.samples, events: lab1.events, plain: lab1.plain}

	defs, err := sp2.Types().Datasets(ctx, lab.typeId)
	require.NoError(t, err)
	for _, d := range defs {
		assert.Equal(t, d.Key != "plain", d.Shared, "after restart %s reports its flag", d.Key)
	}

	rowsA := lab.rows(t, ctx, a, lab.samples)
	assertOwnedRows(t, rowsA, a, "r1")
	if row := rowsA[a+"/r1"]; assert.NotNil(t, row) {
		assert.Equal(t, "a1-edited", row.GetString("label"))
	}
	assertOwnedRows(t, lab.rows(t, ctx, b, lab.samples), b, "r1")
	tomb, err := sp2.Query(a, lab.samples).
		Filter(map[string]any{"id": a + "/r2"}).
		Projection(space.ProjectionOpts{IncludeDeleted: true}).
		One(ctx)
	require.NoError(t, err, "the tombstone survives the restart")
	assert.NotNil(t, tomb.Get("_deletedAt"))

	sub, err := sp2.Query(a, lab.samples).Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Sub.Close() })
	assert.Equal(t, []string{a + "/r1"}, idsOfInitial(sub.Initial))

	// Upsert finds the restored records; a new write lands beside them.
	ures, err := sp2.Upsert(ctx, space.UpsertBatch{ObjectId: b, Dataset: lab.samples, Records: []space.UpsertRecord{
		{Id: "r1", Fields: map[string]any{"label": "b1", "score": 1, "kind": "x"}},
	}})
	require.NoError(t, err)
	assert.Equal(t, 1, ures.Skipped)
	res := lab.put(t, ctx, a, lab.samples, "r3", map[string]any{"label": "a3", "score": 3, "kind": "x"})
	assert.Equal(t, []string{a + "/r3"}, res.RecordIds)
	assertOwnedRows(t, lab.rows(t, ctx, a, lab.samples), a, "r1", "r3")
	assert.Equal(t, 3, lab.storedRows(t, ctx, lab.samples, a), "r1, the r2 tombstone and r3")
	assert.Equal(t, 1, lab.storedRows(t, ctx, lab.samples, b))

	// History reads the same ids after the restart.
	list, err := sp2.History().ListChanges(ctx, a, space.HistoryFilter{Dataset: lab.samples, RecordId: "r1"}, 0, "")
	require.NoError(t, err)
	versions := make([]space.Version, 0, len(list.Changes))
	for _, c := range list.Changes {
		versions = append(versions, c.Version)
		if assert.Len(t, c.Touched, 1) {
			assert.Equal(t, a+"/r1", c.Touched[0].RecordId)
		}
	}
	assert.Equal(t, []space.Version{v2, v1}, versions)
	rec, err := sp2.History().RecordAt(ctx, a, lab.samples, a+"/r1", v1)
	require.NoError(t, err)
	if assert.NotNil(t, rec) {
		assert.Equal(t, "a1", rec.GetString("label"))
	}
}

// TestE2E_SharedDatasets_Subscribe: a live query on one object's
// shared dataset starts from that object's records and receives only
// that object's writes, each under `<objectId>/<recordId>`. Isolation
// is checked by ordering: after a write on the other object, the next
// event a subscription receives is its own object's.
func TestE2E_SharedDatasets_Subscribe(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lab := openSharedLab(t, ctx, "SharedSub")
	sp := lab.sp

	a, b := lab.newObject(t, ctx), lab.newObject(t, ctx)
	lab.put(t, ctx, a, lab.samples, "r1", map[string]any{"label": "a1", "score": 1, "kind": "x"})
	lab.put(t, ctx, b, lab.samples, "r1", map[string]any{"label": "b1", "score": 1, "kind": "x"})
	lab.put(t, ctx, b, lab.samples, "r9", map[string]any{"label": "b9", "score": 9, "kind": "x"})

	subA, err := sp.Query(a, lab.samples).Subscribe(ctx, space.QueryOpts{IncludeTotal: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = subA.Sub.Close() })
	assert.Equal(t, []string{a + "/r1"}, idsOfInitial(subA.Initial), "A's snapshot holds A's records only")
	assert.Equal(t, 1, subA.Total)
	for _, row := range subA.Initial {
		assert.Equal(t, a, row.GetString("_objectId"))
	}

	subB, err := sp.Query(b, lab.samples).Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = subB.Sub.Close() })
	assert.ElementsMatch(t, []string{b + "/r1", b + "/r9"}, idsOfInitial(subB.Initial))

	// Writes on B — a create, and an update of the record id A holds
	// too — reach B's subscription under B's ids.
	lab.put(t, ctx, b, lab.samples, "r2", map[string]any{"label": "b2", "score": 2, "kind": "x"})
	ev := receiveOne(t, subB.Sub, 2*time.Second)
	rec, where := subRecordFor(ev, b+"/r2")
	require.NotNil(t, rec, "B's create reaches B's subscription; event ids %v", eventIds(ev))
	assert.Equal(t, "added", where)
	require.NotNil(t, rec.Doc)
	assert.Equal(t, b, rec.Doc.GetString("_objectId"))
	lab.set(t, ctx, b, lab.samples, "r1", "label", "b1-edited")
	ev = receiveOne(t, subB.Sub, 2*time.Second)
	rec, where = subRecordFor(ev, b+"/r1")
	require.NotNil(t, rec, "B's update reaches B's subscription; event ids %v", eventIds(ev))
	assert.Equal(t, "updated", where)
	assert.Equal(t, "b1-edited", rec.Doc.GetString("label"))

	// A's subscription saw neither: the next event it receives is A's
	// own create.
	lab.put(t, ctx, a, lab.samples, "r2", map[string]any{"label": "a2", "score": 2, "kind": "x"})
	ev = receiveOne(t, subA.Sub, 2*time.Second)
	for _, id := range eventIds(ev) {
		assert.True(t, strings.HasPrefix(id, a+"/"), "A's subscription received %q", id)
	}
	rec, where = subRecordFor(ev, a+"/r2")
	require.NotNil(t, rec, "A's create reaches A's subscription; event ids %v", eventIds(ev))
	assert.Equal(t, "added", where)
	require.NotNil(t, rec.Doc)
	assert.Equal(t, a, rec.Doc.GetString("_objectId"))
	assert.Equal(t, a+"/r2", rec.Doc.GetString("id"))
	assert.Equal(t, "a2", rec.Doc.GetString("label"))

	// An update by the read-back id, then a delete by the plain id.
	lab.set(t, ctx, a, lab.samples, a+"/r1", "label", "a1-edited")
	ev = receiveOne(t, subA.Sub, 2*time.Second)
	rec, where = subRecordFor(ev, a+"/r1")
	require.NotNil(t, rec, "A's update reaches A's subscription; event ids %v", eventIds(ev))
	assert.Equal(t, "updated", where)
	assert.Equal(t, "a1-edited", rec.Doc.GetString("label"))
	_, err = sp.Delete(ctx, space.DeleteBatch{ObjectId: a, Dataset: lab.samples, RecordIds: []string{"r1"}})
	require.NoError(t, err)
	ev = receiveOne(t, subA.Sub, 2*time.Second)
	assert.Contains(t, ev.Removed, space.RemovedRecord{Id: a + "/r1", Reason: space.RemoveDeleted},
		"A's delete leaves A's window under the read-back id; event ids %v", eventIds(ev))

	// B's subscription saw none of A's writes: its next event is B's own.
	lab.set(t, ctx, b, lab.samples, "r9", "label", "b9-edited")
	ev = receiveOne(t, subB.Sub, 2*time.Second)
	for _, id := range eventIds(ev) {
		assert.True(t, strings.HasPrefix(id, b+"/"), "B's subscription received %q", id)
	}
	rec, _ = subRecordFor(ev, b+"/r9")
	require.NotNil(t, rec, "B's update reaches B's subscription; event ids %v", eventIds(ev))
	assert.Equal(t, "b9-edited", rec.Doc.GetString("label"))
}

// TestE2E_SharedDatasets_CreateEventCarriesObjectStamp: a create in a
// shared dataset reaches a live query with its `_objectId` among the
// record's ops, next to its fields, so a client rebuilds the row from
// the ops alone.
func TestE2E_SharedDatasets_CreateEventCarriesObjectStamp(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lab := openSharedLab(t, ctx, "SharedCreateStamp")
	sp := lab.sp
	a := lab.newObject(t, ctx)

	spaceSub, err := sp.QueryDataset(lab.samples).Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = spaceSub.Sub.Close() })
	objectSub, err := sp.Query(a, lab.samples).Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = objectSub.Sub.Close() })

	lab.put(t, ctx, a, lab.samples, "r1", map[string]any{"label": "a1", "score": 1, "kind": "x"})
	for name, sub := range map[string]space.QuerySubscription{"space": spaceSub.Sub, "object": objectSub.Sub} {
		ev := receiveOne(t, sub, 2*time.Second)
		rec, where := subRecordFor(ev, a+"/r1")
		require.NotNil(t, rec, "%s: event ids %v", name, eventIds(ev))
		assert.Equal(t, "added", where, name)
		stamps := 0
		for _, op := range rec.Ops {
			if len(op.Path) == 1 && op.Path[0] == "_objectId" {
				stamps++
				assert.Equal(t, crdt.OpSet, op.Type, name)
				if assert.NotNil(t, op.Payload, name) {
					assert.Equal(t, a, string(op.Payload.GetStringBytes()), name)
				}
			}
		}
		assert.Equal(t, 1, stamps, "%s: the create carries one _objectId op", name)
	}

	// An update does not carry it again.
	lab.set(t, ctx, a, lab.samples, "r1", "label", "a1-edited")
	ev := receiveOne(t, spaceSub.Sub, 2*time.Second)
	rec, where := subRecordFor(ev, a+"/r1")
	require.NotNil(t, rec, "event ids %v", eventIds(ev))
	assert.Equal(t, "updated", where)
	for _, op := range rec.Ops {
		assert.NotEqual(t, []string{"_objectId"}, op.Path, "an update carries no _objectId op")
	}
}

// TestE2E_SharedDatasets_RemovedDefinition: after a shared dataset's
// definition is removed, an object still resident reads the per-space
// collection through its controller, and every per-object read stays
// inside that object's rows; the space-wide read is refused; deleting
// an object still takes its rows out of the collection.
func TestE2E_SharedDatasets_RemovedDefinition(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lab := openSharedLab(t, ctx, "SharedRemovedDef")
	sp := lab.sp

	a, b := lab.newObject(t, ctx), lab.newObject(t, ctx)
	lab.putRows(t, ctx,
		sample(a, "r1", "x", 1), sample(a, "r2", "y", 2),
		sample(b, "r1", "x", 10), sample(b, "r2", "y", 20), sample(b, "r3", "x", 30))
	own := map[string][]string{a: {a + "/r1", a + "/r2"}, b: {b + "/r1", b + "/r2", b + "/r3"}}
	others := map[string]string{a: b, b: a}
	// Both objects load, and stay resident through the test.
	assertOwnedRows(t, lab.rows(t, ctx, a, lab.samples), a, "r1", "r2")
	assertOwnedRows(t, lab.rows(t, ctx, b, lab.samples), b, "r1", "r2", "r3")

	require.NoError(t, sp.Types().RemoveDataset(ctx, lab.typeId, datasetDefs(t, ctx, sp, lab.typeId)["samples"].Id))
	_, ok := datasetDefs(t, ctx, sp, lab.typeId)["samples"]
	require.False(t, ok, "the definition is gone")

	_, err := sp.QueryDataset(lab.samples).All(ctx)
	assert.ErrorIs(t, err, space.ErrDatasetNotShared)
	_, err = sp.AggregateDataset(lab.samples, `[{"$count": "n"}]`).All(ctx)
	assert.ErrorIs(t, err, space.ErrDatasetNotShared)

	for _, obj := range []string{a, b} {
		want := own[obj]
		rows, err := sp.Query(obj, lab.samples).All(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, want, idsOfInitial(rows), "All reads %s's rows only", obj)
		for _, row := range rows {
			assert.Equal(t, obj, row.GetString("_objectId"))
		}
		n, err := sp.Query(obj, lab.samples).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, len(want), n, "Count of %s", obj)
		n, err = sp.Query(obj, lab.samples).Filter(map[string]any{"_objectId": others[obj]}).Count(ctx)
		require.NoError(t, err)
		assert.Zero(t, n, "no row of the other object is visible through %s", obj)
		_, err = sp.Query(obj, lab.samples).Filter(map[string]any{"id": others[obj] + "/r1"}).One(ctx)
		assert.ErrorIs(t, err, space.ErrNotFound)

		snap, err := sp.Query(obj, lab.samples).Snapshot(ctx, space.QueryOpts{IncludeTotal: true})
		require.NoError(t, err)
		assert.Equal(t, len(want), snap.Total, "Snapshot total of %s", obj)
		assert.ElementsMatch(t, want, idsOfInitial(snap.Initial))

		sub, err := sp.Query(obj, lab.samples).Subscribe(ctx, space.QueryOpts{IncludeTotal: true})
		require.NoError(t, err)
		assert.ElementsMatch(t, want, idsOfInitial(sub.Initial), "Subscribe starts from %s's rows", obj)
		assert.Equal(t, len(want), sub.Total)
		_ = sub.Sub.Close()

		agg, err := sp.Aggregate(obj, lab.samples, `[{"$group": {"_id": "$_objectId", "n": {"$count": {}}}}]`).All(ctx)
		require.NoError(t, err)
		if assert.Len(t, agg, 1, "one owning object in %s's pipeline", obj) {
			assert.Equal(t, obj, agg[0].GetString("id"))
			assert.Equal(t, len(want), agg[0].GetInt("n"))
		}
		n, err = sp.Aggregate(obj, lab.samples, `[{"$match": {"_objectId": "`+others[obj]+`"}}]`).Count(ctx)
		require.NoError(t, err)
		assert.Zero(t, n, "the other object's rows never enter %s's pipeline", obj)
	}

	// The collection stays open: deleting an object purges its rows.
	require.Equal(t, 2, lab.storedRows(t, ctx, lab.samples, a))
	require.NoError(t, sp.Objects().Delete(ctx, a))
	assert.Zero(t, lab.storedRows(t, ctx, lab.samples, a), "A's rows are purged")
	assert.Equal(t, 3, lab.storedRows(t, ctx, lab.samples, b), "B's rows stay")
}

// TestE2E_SharedDatasets_ObjectDelete: deleting an object removes its
// rows from the per-space collection of every shared dataset and
// leaves the other objects' rows, read back unchanged.
func TestE2E_SharedDatasets_ObjectDelete(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	lab := openSharedLab(t, ctx, "SharedDelete")
	sp := lab.sp

	a, b := lab.newObject(t, ctx), lab.newObject(t, ctx)
	for _, obj := range []string{a, b} {
		lab.put(t, ctx, obj, lab.samples, "r1", map[string]any{"label": obj[len(obj)-4:] + "-1", "score": 1, "kind": "x"})
		lab.put(t, ctx, obj, lab.samples, "r2", map[string]any{"label": obj[len(obj)-4:] + "-2", "score": 2, "kind": "y"})
		res, err := sp.Modify(ctx, space.ModifyBatch{ObjectId: obj, Dataset: lab.events, Records: []space.RecordModify{
			{Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: "label", Value: "event"}}},
		}})
		require.NoError(t, err)
		require.Empty(t, res.Rejections)
	}
	_, err := sp.Delete(ctx, space.DeleteBatch{ObjectId: a, Dataset: lab.samples, RecordIds: []string{"r2"}})
	require.NoError(t, err, "A also holds a tombstone")

	bSamples := lab.rows(t, ctx, b, lab.samples)
	assertOwnedRows(t, bSamples, b, "r1", "r2")
	bEvents := lab.rows(t, ctx, b, lab.events)
	require.Len(t, bEvents, 1)

	// Both objects' rows share one collection per dataset.
	assert.Equal(t, 2, lab.storedRows(t, ctx, lab.samples, a), "A's live row and tombstone")
	assert.Equal(t, 2, lab.storedRows(t, ctx, lab.samples, b))
	assert.Equal(t, 1, lab.storedRows(t, ctx, lab.events, a))
	assert.Equal(t, 1, lab.storedRows(t, ctx, lab.events, b))

	require.NoError(t, sp.Objects().Delete(ctx, a))

	// B's records read back unchanged.
	afterSamples := lab.rows(t, ctx, b, lab.samples)
	assertOwnedRows(t, afterSamples, b, "r1", "r2")
	for id, before := range bSamples {
		if after := afterSamples[id]; assert.NotNil(t, after, id) {
			assert.Equal(t, before.GetString("label"), after.GetString("label"), id)
			assert.Equal(t, before.GetFloat64("score"), after.GetFloat64("score"), id)
			assert.Equal(t, before.GetString("kind"), after.GetString("kind"), id)
		}
	}
	afterEvents := lab.rows(t, ctx, b, lab.events)
	require.Len(t, afterEvents, 1)
	for id := range bEvents {
		assert.Contains(t, afterEvents, id)
	}

	// A's rows, tombstone included, are gone from the per-space
	// collections; B's stay.
	assert.Zero(t, lab.storedRows(t, ctx, lab.samples, a), "A's samples rows are purged")
	assert.Zero(t, lab.storedRows(t, ctx, lab.events, a), "A's events rows are purged")
	assert.Equal(t, 2, lab.storedRows(t, ctx, lab.samples, b))
	assert.Equal(t, 1, lab.storedRows(t, ctx, lab.events, b))

	// Nothing of A reads back, deleted rows included: the deleted object
	// has no tree to open, and the space-wide read holds none of its
	// rows.
	_, err = sp.Query(a, lab.samples).Projection(space.ProjectionOpts{IncludeDeleted: true}).All(ctx)
	assert.ErrorIs(t, err, space.ErrObjectNotFound)
	for id, row := range lab.spaceRows(t, ctx, lab.samples) {
		assert.Equal(t, b, row.GetString("_objectId"), "row %s", id)
	}

	// B keeps taking writes.
	lab.set(t, ctx, b, lab.samples, "r1", "label", "after")
	if row := lab.rows(t, ctx, b, lab.samples)[b+"/r1"]; assert.NotNil(t, row) {
		assert.Equal(t, "after", row.GetString("label"))
	}
}

// TestE2E_SharedDatasets_History: version history of a shared dataset
// names records `<objectId>/<recordId>`. ListChanges filtered by
// record and RecordAt accept the plain or the read-back id and see the
// object's own changes only; Diff and ViewAt name records the same way.
func TestE2E_SharedDatasets_History(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	lab := openSharedLab(t, ctx, "SharedHistory")
	sp := lab.sp

	a, b := lab.newObject(t, ctx), lab.newObject(t, ctx)
	v1 := lab.put(t, ctx, a, lab.samples, "r1", map[string]any{"label": "first", "score": 1, "kind": "x"}).ChangeId
	b1 := lab.put(t, ctx, b, lab.samples, "r1", map[string]any{"label": "other", "score": 9, "kind": "y"}).ChangeId
	vSibling := lab.put(t, ctx, a, lab.samples, "r2", map[string]any{"label": "sibling", "score": 5, "kind": "x"}).ChangeId
	v2 := lab.set(t, ctx, a, lab.samples, "r1", "label", "second").ChangeId
	b2 := lab.set(t, ctx, b, lab.samples, "r1", "label", "other-2").ChangeId
	v3 := lab.set(t, ctx, a, lab.samples, a+"/r1", "score", 3).ChangeId
	for _, v := range []space.Version{v1, b1, vSibling, v2, b2, v3} {
		require.NotEmpty(t, v)
	}
	hist := sp.History()

	versionsOf := func(list space.ChangeList) []space.Version {
		out := make([]space.Version, 0, len(list.Changes))
		for _, c := range list.Changes {
			out = append(out, c.Version)
		}
		return out
	}

	t.Run("ListChanges", func(t *testing.T) {
		for _, recId := range []string{a + "/r1", "r1"} {
			list, err := hist.ListChanges(ctx, a, space.HistoryFilter{Dataset: lab.samples, RecordId: recId}, 0, "")
			require.NoError(t, err)
			assert.Equal(t, []space.Version{v3, v2, v1}, versionsOf(list), "A's changes to r1, filtered by %q", recId)
			for _, c := range list.Changes {
				assert.Equal(t, lab.samples, c.Dataset)
				if assert.Len(t, c.Touched, 1, "change %s", c.Version) {
					assert.Equal(t, a+"/r1", c.Touched[0].RecordId, "Touched names the read-back id")
				}
			}
		}

		list, err := hist.ListChanges(ctx, a, space.HistoryFilter{Dataset: lab.samples}, 0, "")
		require.NoError(t, err)
		assert.Equal(t, []space.Version{v3, v2, vSibling, v1}, versionsOf(list), "A's changes to the dataset")

		for _, recId := range []string{b + "/r1", "r1"} {
			list, err = hist.ListChanges(ctx, b, space.HistoryFilter{Dataset: lab.samples, RecordId: recId}, 0, "")
			require.NoError(t, err)
			assert.Equal(t, []space.Version{b2, b1}, versionsOf(list), "B's changes to r1, filtered by %q", recId)
			for _, c := range list.Changes {
				if assert.Len(t, c.Touched, 1) {
					assert.Equal(t, b+"/r1", c.Touched[0].RecordId)
				}
			}
		}

		list, err = hist.ListChanges(ctx, a, space.HistoryFilter{Dataset: lab.samples, RecordId: b + "/r1"}, 0, "")
		require.NoError(t, err)
		assert.Empty(t, list.Changes, "B's record has no history on A")
	})

	t.Run("RecordAt", func(t *testing.T) {
		for _, recId := range []string{"r1", a + "/r1"} {
			for _, tc := range []struct {
				version space.Version
				label   string
				score   float64
			}{
				{v1, "first", 1},
				{v2, "second", 1},
				{v3, "second", 3},
			} {
				rec, err := hist.RecordAt(ctx, a, lab.samples, recId, tc.version)
				require.NoError(t, err)
				if !assert.NotNil(t, rec, "RecordAt(%q, %s)", recId, tc.version) {
					continue
				}
				assert.Equal(t, a+"/r1", rec.GetString("id"), "RecordAt(%q, %s) id", recId, tc.version)
				assert.Equal(t, tc.label, rec.GetString("label"), "RecordAt(%q, %s) label", recId, tc.version)
				assert.Equal(t, tc.score, rec.GetFloat64("score"), "RecordAt(%q, %s) score", recId, tc.version)
			}
		}
		rec, err := hist.RecordAt(ctx, a, lab.samples, "r2", v1)
		require.NoError(t, err)
		assert.Nil(t, rec, "r2 does not exist on A at v1")

		for _, recId := range []string{"r1", b + "/r1"} {
			rec, err = hist.RecordAt(ctx, b, lab.samples, recId, b1)
			require.NoError(t, err)
			if assert.NotNil(t, rec, "RecordAt on B (%q)", recId) {
				assert.Equal(t, b+"/r1", rec.GetString("id"))
				assert.Equal(t, "other", rec.GetString("label"))
			}
		}
	})

	t.Run("Diff", func(t *testing.T) {
		for _, recId := range []string{"r1", a + "/r1"} {
			diff, err := hist.Diff(ctx, a, v1, v3, space.DiffFilter{Dataset: lab.samples, RecordIds: []string{recId}})
			require.NoError(t, err)
			require.Len(t, diff.Datasets, 1)
			require.Len(t, diff.Datasets[0].Records, 1, "record-scoped diff by %q", recId)
			rd := diff.Datasets[0].Records[0]
			assert.Equal(t, a+"/r1", rd.Id)
			assert.Equal(t, space.DiffChanged, rd.Kind)
			paths := map[string]bool{}
			for _, fd := range rd.Fields {
				paths[strings.Join(fd.Path, ".")] = true
			}
			assert.Equal(t, map[string]bool{"label": true, "score": true}, paths)
		}

		diff, err := hist.Diff(ctx, a, v1, v3, space.DiffFilter{Dataset: lab.samples})
		require.NoError(t, err)
		require.Len(t, diff.Datasets, 1)
		kinds := map[string]space.DiffKind{}
		for _, rd := range diff.Datasets[0].Records {
			kinds[rd.Id] = rd.Kind
		}
		assert.Equal(t, map[string]space.DiffKind{a + "/r1": space.DiffChanged, a + "/r2": space.DiffAdded}, kinds)
	})

	t.Run("ViewAt", func(t *testing.T) {
		view, err := hist.ViewAt(ctx, a, v2)
		require.NoError(t, err)
		defer view.Close()
		recs, err := view.Records(ctx, lab.samples)
		require.NoError(t, err)
		ids := idsOfInitial(recs)
		assert.ElementsMatch(t, []string{a + "/r1", a + "/r2"}, ids, "the view holds A's records only")
		for _, recId := range []string{"r1", a + "/r1"} {
			rec, err := view.Record(ctx, lab.samples, recId)
			require.NoError(t, err)
			if assert.NotNil(t, rec, "view.Record(%q)", recId) {
				assert.Equal(t, "second", rec.GetString("label"))
				assert.Equal(t, float64(1), rec.GetFloat64("score"))
			}
		}
	})

	// A derived id (empty-id create) is listed and reconstructed under
	// the id the write returned, or its plain form.
	t.Run("DerivedIds", func(t *testing.T) {
		res, err := sp.Modify(ctx, space.ModifyBatch{ObjectId: a, Dataset: lab.events, Records: []space.RecordModify{
			{Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: "label", Value: "e1"}}},
		}})
		require.NoError(t, err)
		require.Len(t, res.RecordIds, 1)
		eventId := res.RecordIds[0]
		created := res.ChangeId
		edited := lab.set(t, ctx, a, lab.events, eventId, "label", "e2").ChangeId
		lab.put(t, ctx, b, lab.events, "", map[string]any{"label": "b-event"})

		for _, recId := range []string{eventId, strings.TrimPrefix(eventId, a+"/")} {
			list, err := hist.ListChanges(ctx, a, space.HistoryFilter{Dataset: lab.events, RecordId: recId}, 0, "")
			require.NoError(t, err)
			assert.Equal(t, []space.Version{edited, created}, versionsOf(list), "changes of the derived record, filtered by %q", recId)
			for _, c := range list.Changes {
				if assert.Len(t, c.Touched, 1) {
					assert.Equal(t, eventId, c.Touched[0].RecordId)
				}
			}
			rec, err := hist.RecordAt(ctx, a, lab.events, recId, created)
			require.NoError(t, err)
			if assert.NotNil(t, rec, "RecordAt(%q)", recId) {
				assert.Equal(t, eventId, rec.GetString("id"))
				assert.Equal(t, "e1", rec.GetString("label"))
			}
		}
	})
}

// TestE2E_SharedDatasets_SecondDevice: a second device of the same
// account converges on the shared declaration and on each object's
// records under the same `<objectId>/<recordId>` ids, and its writes
// address the same records.
func TestE2E_SharedDatasets_SecondDevice(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("cold-sync e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	provider := newFixedSeedProvider(t)
	open := func() *anysyncsdk.SDK {
		sdk, oerr := anysyncsdk.Open(ctx, config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}, provider)
		require.NoError(t, oerr)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}

	sdkA := open()
	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "SharedSync"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatal(err)
	}
	labA := &sharedLab{sdk: sdkA, sp: spA}
	labA.declare(t, ctx)
	objA, objB := labA.newObject(t, ctx), labA.newObject(t, ctx)
	vA1 := labA.put(t, ctx, objA, labA.samples, "r1", map[string]any{"label": "a1", "score": 1, "kind": "x"}).ChangeId
	labA.put(t, ctx, objA, labA.samples, "r2", map[string]any{"label": "a2", "score": 2, "kind": "x"})
	vB1 := labA.put(t, ctx, objB, labA.samples, "r1", map[string]any{"label": "b1", "score": 10, "kind": "y"}).ChangeId
	res, err := spA.Modify(ctx, space.ModifyBatch{ObjectId: objA, Dataset: labA.events, Records: []space.RecordModify{
		{Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: "label", Value: "event"}}},
	}})
	require.NoError(t, err)
	require.Len(t, res.RecordIds, 1)
	eventId := res.RecordIds[0]

	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = spA.SyncHeads(ctx)

	sdkB := open()
	var spB space.Space
	require.True(t, waitFor(ctx, 90*time.Second, 250*time.Millisecond, func() bool {
		_ = sdkB.Spaces().SyncSpaceList(ctx)
		got, gerr := sdkB.Spaces().Get(ctx, spA.Id())
		if gerr != nil {
			return false
		}
		spB = got
		return true
	}), "device B must see the space")
	labB := &sharedLab{sdk: sdkB, sp: spB, typeId: labA.typeId, partId: labA.partId,
		samples: labA.samples, events: labA.events, plain: labA.plain}

	// The declaration converges shared.
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		defs, derr := spB.Types().Datasets(ctx, labA.typeId)
		return derr == nil && len(defs) == 3
	}), "device B must converge on the dataset definitions")
	defs, err := spB.Types().Datasets(ctx, labA.typeId)
	require.NoError(t, err)
	for _, d := range defs {
		assert.Equal(t, d.Key != "plain", d.Shared, "device B reports the flag of %s", d.Key)
		assert.Equal(t, labA.typeId+"_"+d.Key, d.Collection)
	}
	assert.True(t, findDataset(t, spB.Datasets(), labA.samples).Shared, "device B's discovery reports samples shared")

	// The records converge per object.
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		ra, aerr := spB.Query(objA, labA.samples).All(ctx)
		rb, berr := spB.Query(objB, labA.samples).All(ctx)
		re, eerr := spB.Query(objA, labA.events).All(ctx)
		return aerr == nil && berr == nil && eerr == nil && len(ra) == 2 && len(rb) == 1 && len(re) == 1
	}), "device B must converge on the records")

	rowsA := labB.rows(t, ctx, objA, labA.samples)
	assertOwnedRows(t, rowsA, objA, "r1", "r2")
	if row := rowsA[objA+"/r1"]; assert.NotNil(t, row) {
		assert.Equal(t, "a1", row.GetString("label"))
		assert.Equal(t, float64(1), row.GetFloat64("score"))
	}
	rowsB := labB.rows(t, ctx, objB, labA.samples)
	assertOwnedRows(t, rowsB, objB, "r1")
	if row := rowsB[objB+"/r1"]; assert.NotNil(t, row) {
		assert.Equal(t, "b1", row.GetString("label"))
		assert.Equal(t, float64(10), row.GetFloat64("score"))
	}
	rowsEv := labB.rows(t, ctx, objA, labA.events)
	assert.Contains(t, rowsEv, eventId, "the derived id reads back the same on device B")
	n, err := spB.Query(objB, labA.events).Count(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)

	// History of the synced objects on device B names the same ids.
	for _, tc := range []struct {
		objectId string
		version  space.Version
		label    string
	}{
		{objA, vA1, "a1"},
		{objB, vB1, "b1"},
	} {
		for _, recId := range []string{"r1", tc.objectId + "/r1"} {
			list, lerr := spB.History().ListChanges(ctx, tc.objectId, space.HistoryFilter{Dataset: labA.samples, RecordId: recId}, 0, "")
			require.NoError(t, lerr)
			if assert.Len(t, list.Changes, 1, "device B history of %s filtered by %q", tc.objectId, recId) {
				assert.Equal(t, tc.version, list.Changes[0].Version)
				if assert.Len(t, list.Changes[0].Touched, 1) {
					assert.Equal(t, tc.objectId+"/r1", list.Changes[0].Touched[0].RecordId)
				}
			}
			rec, rerr := spB.History().RecordAt(ctx, tc.objectId, labA.samples, recId, tc.version)
			require.NoError(t, rerr)
			if assert.NotNil(t, rec, "device B RecordAt(%s, %q)", tc.objectId, recId) {
				assert.Equal(t, tc.objectId+"/r1", rec.GetString("id"))
				assert.Equal(t, tc.label, rec.GetString("label"))
			}
		}
	}

	// A live query on the first device receives device B's writes on
	// object A under the read-back id, and none of object B's.
	subA, err := spA.Query(objA, labA.samples).Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = subA.Sub.Close() })

	// Device B addresses the same records: an upsert by the plain id
	// updates A's record, a write by the read-back id updates B's.
	ures, err := spB.Upsert(ctx, space.UpsertBatch{ObjectId: objA, Dataset: labA.samples, Records: []space.UpsertRecord{
		{Id: "r1", Fields: map[string]any{"label": "a1-from-B", "score": 1, "kind": "x"}},
	}})
	require.NoError(t, err)
	assert.Equal(t, 1, ures.Updated)
	assert.Zero(t, ures.Created, "the plain id finds the synced record")
	assert.Empty(t, ures.Rejections)
	mres := labB.set(t, ctx, objB, labA.samples, objB+"/r1", "label", "b1-from-B")
	assert.Equal(t, []string{objB + "/r1"}, mres.RecordIds)
	if row := labB.rows(t, ctx, objA, labA.samples)[objA+"/r1"]; assert.NotNil(t, row) {
		assert.Equal(t, "a1-from-B", row.GetString("label"))
	}
	rowsB = labB.rows(t, ctx, objB, labA.samples)
	assertOwnedRows(t, rowsB, objB, "r1")
	if row := rowsB[objB+"/r1"]; assert.NotNil(t, row) {
		assert.Equal(t, "b1-from-B", row.GetString("label"))
	}

	// The writes travel back to the first device.
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		_ = spA.SyncHeads(ctx)
		ra, aerr := spA.Query(objA, labA.samples).Filter(map[string]any{"id": objA + "/r1"}).One(ctx)
		rb, berr := spA.Query(objB, labA.samples).Filter(map[string]any{"id": objB + "/r1"}).One(ctx)
		return aerr == nil && berr == nil &&
			ra.GetString("label") == "a1-from-B" && rb.GetString("label") == "b1-from-B"
	}), "device A must converge on device B's writes")
	assertOwnedRows(t, labA.rows(t, ctx, objA, labA.samples), objA, "r1", "r2")
	assertOwnedRows(t, labA.rows(t, ctx, objB, labA.samples), objB, "r1")
	var remote *space.SubRecord
	for remote == nil {
		ev := receiveOne(t, subA.Sub, 10*time.Second)
		for _, id := range eventIds(ev) {
			assert.True(t, strings.HasPrefix(id, objA+"/"), "A's subscription received %q", id)
		}
		remote, _ = subRecordFor(ev, objA+"/r1")
	}
	require.NotNil(t, remote.Doc)
	assert.Equal(t, "a1-from-B", remote.Doc.GetString("label"), "the remote write arrives under the read-back id")
	assert.Equal(t, objA, remote.Doc.GetString("_objectId"))

	// Deleting object A on the first device purges its rows on the
	// second; object B's stay.
	require.NoError(t, spA.Objects().Delete(ctx, objA))
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spA.SyncHeads(ctx)
		_ = spB.SyncHeads(ctx)
		return labB.storedRows(t, ctx, labA.samples, objA) == 0 && labB.storedRows(t, ctx, labA.events, objA) == 0
	}), "device B must purge object A's shared rows")
	rowsB = labB.rows(t, ctx, objB, labA.samples)
	assertOwnedRows(t, rowsB, objB, "r1")
	if row := rowsB[objB+"/r1"]; assert.NotNil(t, row) {
		assert.Equal(t, "b1-from-B", row.GetString("label"))
	}
	assert.Equal(t, 1, labB.storedRows(t, ctx, labA.samples, objB))

	// A shared dataset declared after device B has object B resident
	// reaches it with the same ids.
	_, err = spA.Types().AddDataset(ctx, labA.typeId, labA.partId, space.DatasetDraft{
		Key: "late", Shared: true, IdRule: space.IdUser,
		Fields: []space.DatasetFieldDraft{{Key: "label", Kind: space.PropertyKindString, MutableBy: space.MutableByAnyone}},
	})
	require.NoError(t, err)
	late := labA.typeId + "_late"
	labA.put(t, ctx, objB, late, "l1", map[string]any{"label": "late"})
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spA.SyncHeads(ctx)
		_ = spB.SyncHeads(ctx)
		rows, qerr := spB.Query(objB, late).All(ctx)
		return qerr == nil && len(rows) == 1
	}), "device B must converge on the late dataset's record")
	rowsLate := labB.rows(t, ctx, objB, late)
	assertOwnedRows(t, rowsLate, objB, "l1")
	if row := rowsLate[objB+"/l1"]; assert.NotNil(t, row) {
		assert.Equal(t, "late", row.GetString("label"))
	}
	assert.True(t, findDataset(t, spB.Datasets(), late).Shared)
	assert.Equal(t, 1, labB.storedRows(t, ctx, late, objB), "the late dataset's row lives in its per-space collection")
}

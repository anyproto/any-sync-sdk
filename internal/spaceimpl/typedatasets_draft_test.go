package spaceimpl

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
	"github.com/anyproto/any-sync-sdk/space"
)

// A time stamp's value is handler-produced. A declaration naming another
// kind is rejected rather than normalized: the draft is signed into the
// type object and read back by discovery, so accepting one the apply
// path then overrides would publish a lie.
func TestDraftFieldDecl_TimeStampRequiresDatetime(t *testing.T) {
	for _, stamp := range []space.Stamp{space.StampCreateTime, space.StampModifyTime} {
		_, err := draftFieldDecl(&space.DatasetFieldDraft{
			Key: "createdAt", Stamp: stamp, Kind: space.PropertyKindNumber,
		})
		assert.Error(t, err, "stamp %v with kind number", stamp)

		f, err := draftFieldDecl(&space.DatasetFieldDraft{Key: "createdAt", Stamp: stamp})
		require.NoError(t, err, "stamp %v with kind omitted", stamp)
		require.NotNil(t, f.Schema)
		assert.Equal(t, schema.KindDatetime, f.Schema.Kind)
	}
	// A Shape saying the same thing another way is refused too.
	_, err := draftFieldDecl(&space.DatasetFieldDraft{
		Key: "createdAt", Stamp: space.StampCreateTime, Shape: schema.Leaf(schema.KindNumber),
	})
	assert.Error(t, err, "stamped field declared through Shape")
}

// The descriptive slice rides the declaration untouched: description
// and the opaque descriptor bag land on the schema field the record
// encoder and discovery read from.
func TestDraftFieldDecl_CarriesDescriptorSlice(t *testing.T) {
	f, err := draftFieldDecl(&space.DatasetFieldDraft{
		Key: "stage", Kind: space.PropertyKindArray, Description: "Pipeline stage",
		Shape:   &schema.Schema{Kind: schema.KindArray, Items: schema.Leaf(schema.KindString)},
		XFormat: map[string]any{"type": "choice", "options": map[string]any{"lead": map[string]any{"name": "Lead"}}},
	})
	require.NoError(t, err)
	assert.Equal(t, "Pipeline stage", f.Description)
	assert.Equal(t, "choice", f.XFormat["type"])
	require.NotNil(t, f.Schema.Items)
	assert.Equal(t, schema.KindString, f.Schema.Items.Kind)
}

// A declaration follows its key: with none it takes its module's
// canonical name. The head record a part writes carries the canonical
// marker exactly when the key is that name — the compile resolves the
// collection from the marker.
func TestPartRecords_CanonicalKeyAndMarker(t *testing.T) {
	modules := types.NewModules(types.ModuleInfo{Name: "editor", Canonical: "editor_blocks"})
	draft := space.PartDraft{Key: "body", Datasets: []space.DatasetDraft{
		{Module: "editor"},
		{Module: "editor", Key: "summary"},
		{Key: "segments"},
	}}
	wantColl := []string{"editor_blocks", "type_summary", "type_segments"}
	for i := range draft.Datasets {
		coll, err := normalizeDatasetDraft(modules, "type", &draft.Datasets[i])
		require.NoError(t, err)
		assert.Equal(t, wantColl[i], coll)
	}

	_, recs, err := partRecords(&anyenc.Arena{}, modules, &draft)
	require.NoError(t, err)
	marked := map[string]bool{}
	for _, rec := range recs {
		head := rec.Ops[0].Payload
		if head.GetString(typetype.DefFieldDef) != typetype.DefKindDataset {
			continue
		}
		marked[head.GetString(typetype.FieldKey)] = head.GetBool(typetype.DefFieldCanonical)
	}
	assert.Equal(t, map[string]bool{"editor_blocks": true, "summary": false, "segments": false}, marked)

	spelled := space.DatasetDraft{Module: "editor", Key: "editor_blocks"}
	_, err = normalizeDatasetDraft(modules, "type", &spelled)
	require.NoError(t, err)
	_, recs, err = datasetDefRecords(&anyenc.Arena{}, modules, "part", &spelled)
	require.NoError(t, err)
	assert.True(t, recs[0].Ops[0].Payload.GetBool(typetype.DefFieldCanonical), "the canonical name spelled out is the canonical dataset")

	_, err = normalizeDatasetDraft(modules, "type", &space.DatasetDraft{})
	require.Error(t, err, "records has no canonical collection and needs a key")
}

// A shared records dataset stores its marker on the head record; a
// module dataset cannot be shared.
func TestPartRecords_SharedMarker(t *testing.T) {
	modules := types.NewModules(types.ModuleInfo{Name: "editor", Canonical: "editor_blocks"})
	shared := space.DatasetDraft{Key: "samples", Shared: true}
	coll, err := normalizeDatasetDraft(modules, "type", &shared)
	require.NoError(t, err)
	assert.Equal(t, "type_samples", coll)
	_, recs, err := datasetDefRecords(&anyenc.Arena{}, modules, "part", &shared)
	require.NoError(t, err)
	assert.True(t, recs[0].Ops[0].Payload.GetBool(typetype.DefFieldPerSpace))

	plain := space.DatasetDraft{Key: "segments"}
	_, err = normalizeDatasetDraft(modules, "type", &plain)
	require.NoError(t, err)
	_, recs, err = datasetDefRecords(&anyenc.Arena{}, modules, "part", &plain)
	require.NoError(t, err)
	assert.Nil(t, recs[0].Ops[0].Payload.Get(typetype.DefFieldPerSpace))

	_, err = normalizeDatasetDraft(modules, "type", &space.DatasetDraft{Module: "editor", Shared: true})
	require.ErrorIs(t, err, schema.ErrDecl)
}

// A draft's indexes ride the dataset's change as index records under
// its head, and a draft naming one no collection would build is
// refused before anything is written.
func TestDatasetDefRecords_Indexes(t *testing.T) {
	modules := types.NewModules(types.ModuleInfo{Name: "editor", Canonical: "editor_blocks"})
	fields := []space.DatasetFieldDraft{
		{Key: "ts", Kind: space.PropertyKindDatetime},
		{Key: "value", Kind: space.PropertyKindNumber},
		{Key: "tags", Kind: space.PropertyKindArray},
	}
	draft := space.DatasetDraft{Key: "samples", Shared: true, Fields: fields, Indexes: []space.IndexDraft{
		{Key: "by_ts", Fields: []string{"ts", space.IndexPathObject}},
		{Key: "top", Fields: []string{"-value"}, Sparse: true},
	}}
	_, err := normalizeDatasetDraft(modules, "type", &draft)
	require.NoError(t, err)
	headId, recs, err := datasetDefRecords(&anyenc.Arena{}, modules, "part", &draft)
	require.NoError(t, err)

	type indexRec struct {
		fields []string
		sparse bool
	}
	got := map[string]indexRec{}
	for _, rec := range recs {
		payload := rec.Ops[0].Payload
		if payload.GetString(typetype.DefFieldDef) != typetype.DefKindIndex {
			continue
		}
		assert.Equal(t, headId, payload.GetString(typetype.DefFieldDataset))
		assert.True(t, rec.Upsert)
		var paths []string
		for _, f := range payload.GetArray(typetype.DefFieldIndexFields) {
			paths = append(paths, string(f.GetStringBytes()))
		}
		got[payload.GetString(typetype.FieldKey)] = indexRec{fields: paths, sparse: payload.GetBool(typetype.DefFieldIndexSparse)}
	}
	assert.Equal(t, map[string]indexRec{
		"by_ts": {fields: []string{"ts", "_objectId"}},
		"top":   {fields: []string{"-value"}, sparse: true},
	}, got)

	refused := []struct {
		name  string
		draft space.DatasetDraft
	}{
		{"undeclared field", space.DatasetDraft{Key: "s", Fields: fields, Indexes: []space.IndexDraft{{Key: "x", Fields: []string{"missing"}}}}},
		{"array field", space.DatasetDraft{Key: "s", Fields: fields, Indexes: []space.IndexDraft{{Key: "x", Fields: []string{"tags"}}}}},
		{"object path on a per-object dataset", space.DatasetDraft{Key: "s", Fields: fields, Indexes: []space.IndexDraft{{Key: "x", Fields: []string{space.IndexPathObject}}}}},
		{"one key twice", space.DatasetDraft{Key: "s", Fields: fields, Indexes: []space.IndexDraft{
			{Key: "x", Fields: []string{"ts"}}, {Key: "x", Fields: []string{"value"}},
		}}},
		{"no fields", space.DatasetDraft{Key: "s", Fields: fields, Indexes: []space.IndexDraft{{Key: "x"}}}},
	}
	for _, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := datasetDefRecords(&anyenc.Arena{}, modules, "part", &tc.draft)
			require.ErrorIs(t, err, schema.ErrDecl)
		})
	}

	_, _, err = datasetDefRecords(&anyenc.Arena{}, modules, "part", &space.DatasetDraft{
		Module: "editor", Key: "summary", Indexes: []space.IndexDraft{{Key: "x", Fields: []string{"ts"}}},
	})
	require.ErrorIs(t, err, space.ErrModuleOwned)
}

// An added index is checked against the indexes the dataset holds: its
// key is free and the dataset stays within its limit.
func TestValidateIndexDrafts_AgainstExisting(t *testing.T) {
	ds := schema.Dataset{Fields: []schema.Field{
		{Id: "a", Schema: schema.Leaf(schema.KindString)},
		{Id: "b", Schema: schema.Leaf(schema.KindString)},
	}}
	existing := []space.IndexDef{{Key: "by_a", Fields: []string{"a"}}}
	require.NoError(t, validateIndexDrafts(ds, []space.IndexDraft{{Key: "by_b", Fields: []string{"b"}}}, existing, false))
	require.ErrorIs(t, validateIndexDrafts(ds, []space.IndexDraft{{Key: "by_a", Fields: []string{"b"}}}, existing, false), schema.ErrDecl)

	// Invalid definitions hold no slot of the limit, and keep their key.
	full := []space.IndexDef{{Key: "broken", Fields: []string{"gone"}, Invalid: true}}
	for i := 0; i < schema.MaxDatasetIndexes-1; i++ {
		full = append(full, space.IndexDef{Key: "k" + string(rune('a'+i)), Fields: []string{"a"}})
	}
	require.NoError(t, validateIndexDrafts(ds, []space.IndexDraft{{Key: "last", Fields: []string{"b"}}}, full, false))
	require.ErrorIs(t, validateIndexDrafts(ds, []space.IndexDraft{
		{Key: "last", Fields: []string{"b"}}, {Key: "over", Fields: []string{"a", "b"}},
	}, full, false), schema.ErrDecl)
	require.ErrorIs(t, validateIndexDrafts(ds, []space.IndexDraft{{Key: "broken", Fields: []string{"b"}}}, full, false), schema.ErrDecl)
}

package anysyncsdk

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/space"
)

// Objects().Create with From, offline: a registered type with two
// properties and a bespoke-handler dataset (explicit record ids), so
// the copy of values, membership, records and the template stamp is
// observable without a network.

const (
	cfType    = "cf-doc"
	cfDataset = "cf_blocks"
	cfNotes   = "cf_notes"
	cfStatus  = "status"
	cfSecret  = "secret"
	cfMarker  = "cf-pin"
)

func cfConfig(t *testing.T, dataDir string) config.Config {
	t.Helper()
	cfg := wmConfig(t, dataDir)
	cfg.Types = append(cfg.Types, handler.Type{
		Id:   cfType,
		Name: "Doc",
		Properties: []handler.PropertyDecl{
			{Id: cfStatus, Name: "Status", Kind: handler.PropertyKindString},
			{Id: cfSecret, Name: "Secret", Kind: handler.PropertyKindString, Scope: handler.ScopeLocal},
		},
		Datasets: []handler.Dataset{
			{
				Name:        cfDataset,
				DataVersion: "cf-v1",
				Handler:     handler.DefaultHandler{},
			},
			// The generic schema handler: derived ids, a creator stamp,
			// a required field, undeclared fields refused.
			{
				Name:        cfNotes,
				DataVersion: "cf-v1",
				Schema: handler.Schema{Fields: []handler.Field{
					{Id: "text", Schema: handler.Leaf(handler.PropertyKindString), Required: true},
					{Id: "by", Stamp: handler.StampCreator},
				}},
			},
		},
	})
	// A consumer marker, the bin's shape: never carried into a copy.
	cfg.Collections = append(cfg.Collections, handler.Collection{
		Id: cfMarker, Name: "Pin", Hidden: true,
		Properties: []handler.PropertyDecl{{Id: "pos", Name: "Pos", Kind: handler.PropertyKindString}},
	})
	return cfg
}

func cfOpen(t *testing.T) (context.Context, space.Space) {
	t.Helper()
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	sdk, err := Open(ctx, cfConfig(t, dataDir), wmProvider(t, dataDir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "CreateFrom"})
	require.NoError(t, err)
	return ctx, sp
}

// cfRecords reads the dataset's live records keyed by id, system fields
// dropped.
func cfRecords(t *testing.T, ctx context.Context, sp space.Space, objectId string) map[string]map[string]any {
	return cfRecordsOf(t, ctx, sp, objectId, cfDataset)
}

func cfRecordsOf(t *testing.T, ctx context.Context, sp space.Space, objectId, dataset string) map[string]map[string]any {
	t.Helper()
	docs, err := sp.Query(objectId, dataset).All(ctx)
	require.NoError(t, err)
	out := make(map[string]map[string]any, len(docs))
	for _, d := range docs {
		obj, err := d.Object()
		require.NoError(t, err)
		fields := map[string]any{}
		obj.Visit(func(k []byte, v *anyenc.Value) {
			key := string(k)
			if key == "id" || key[0] == '_' {
				return
			}
			switch v.Type() {
			case anyenc.TypeString:
				fields[key] = string(v.GetStringBytes())
			case anyenc.TypeNumber:
				fields[key] = v.GetFloat64()
			default:
				fields[key] = v.String()
			}
		})
		out[string(d.GetStringBytes("id"))] = fields
	}
	return out
}

func cfSeed(t *testing.T, ctx context.Context, sp space.Space, collections []string, extra map[string]map[string]any) string {
	t.Helper()
	initial := map[string]map[string]any{
		"any":  {"name": "Weekly review", "description": "d", "tags": []string{"a"}},
		cfType: {cfStatus: "todo"},
	}
	for owner, kv := range extra {
		if initial[owner] == nil {
			initial[owner] = map[string]any{}
		}
		for k, v := range kv {
			initial[owner][k] = v
		}
	}
	id, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: cfType, Collections: collections, InitialProperties: initial})
	require.NoError(t, err)
	_, err = sp.Modify(ctx, space.ModifyBatch{ObjectId: id, Dataset: cfDataset, Records: []space.RecordModify{
		{Id: "b1", Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"text": "Heading", "nav": map[string]any{"parentId": "", "pos": "a0"}}}}},
		{Id: "b2", Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"text": "Child", "nav": map[string]any{"parentId": "b1", "pos": "a0"}}}}},
		{Id: "b3", Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{}}}},
	}})
	require.NoError(t, err)
	_, err = sp.Modify(ctx, space.ModifyBatch{ObjectId: id, Dataset: cfNotes, Records: []space.RecordModify{
		{Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"text": "note one"}}}},
		{Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"text": "note two"}}}},
	}})
	require.NoError(t, err)
	// A local-scope value on the source never travels.
	_, err = sp.Properties().Set(ctx, id, cfType, map[string]any{cfSecret: "hidden"})
	require.NoError(t, err)
	return id
}

func TestCreateFrom_Template(t *testing.T) {
	ctx, sp := cfOpen(t)

	// The marker is registered, hidden, static, with its two columns.
	cols, err := sp.Collections().List(ctx)
	require.NoError(t, err)
	var tpl *space.CollectionInfo
	for i := range cols {
		if cols[i].Id == space.TemplateCollectionId {
			tpl = &cols[i]
		}
	}
	require.NotNil(t, tpl, "template collection listed")
	assert.True(t, tpl.BuiltIn)
	assert.True(t, tpl.Hidden)
	props, err := sp.Collections().Properties(ctx, space.TemplateCollectionId)
	require.NoError(t, err)
	kinds := map[string]space.PropertyKind{}
	for _, p := range props {
		kinds[p.Id] = p.Kind
	}
	assert.Equal(t, map[string]space.PropertyKind{"title": space.PropertyKindString, "version": space.PropertyKindNumber}, kinds)

	src := cfSeed(t, ctx, sp, []string{space.TemplateCollectionId}, map[string]map[string]any{
		space.TemplateCollectionId: {"title": "Weekly", "version": 2},
	})

	id, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		From:              src,
		InitialProperties: map[string]map[string]any{"any": {"name": "Weekly review — 13 Oct"}},
	})
	require.NoError(t, err)
	require.NotEqual(t, src, id)

	row, err := sp.Objects().Get(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, cfType, string(row.GetStringBytes("any", "type")))
	assert.Empty(t, row.GetArray("any", "collections"), "the marker is not carried over")
	assert.Equal(t, "Weekly review — 13 Oct", string(row.GetStringBytes("any", "name")), "the request wins")
	assert.Equal(t, "d", string(row.GetStringBytes("any", "description")))
	require.Len(t, row.GetArray("any", "tags"), 1)
	assert.Equal(t, "todo", string(row.GetStringBytes(cfType, cfStatus)))
	assert.Nil(t, row.Get(cfType, cfSecret), "a local-scope value stays behind")
	assert.Nil(t, row.Get(space.TemplateCollectionId), "no template namespace on the copy")
	assert.Equal(t, src, string(row.GetStringBytes("any", "template", "id")))
	assert.Equal(t, float64(2), row.GetFloat64("any", "template", "version"))

	got := cfRecords(t, ctx, sp, id)
	want := cfRecords(t, ctx, sp, src)
	assert.Equal(t, want, got, "records copied with their ids and fields")
	require.Contains(t, got, "b2")
	assert.Contains(t, got, "b3", "a record with nothing but its id is recreated")

	// A schema dataset: fresh derived ids, the stamp re-derived.
	notes := cfRecordsOf(t, ctx, sp, id, cfNotes)
	srcNotes := cfRecordsOf(t, ctx, sp, src, cfNotes)
	require.Len(t, notes, 2)
	texts := map[string]bool{}
	for rid, fields := range notes {
		_, same := srcNotes[rid]
		assert.False(t, same, "a derived id is not reused")
		texts[fields["text"].(string)] = true
		assert.NotEmpty(t, fields["by"], "the creator stamp is re-derived")
	}
	assert.Equal(t, map[string]bool{"note one": true, "note two": true}, texts)

	// A later edit of the template leaves the copy alone.
	_, err = sp.Modify(ctx, space.ModifyBatch{ObjectId: src, Dataset: cfDataset, Records: []space.RecordModify{
		{Id: "b1", Ops: []space.Op{{Type: space.OpSet, Path: "text", Value: "Changed"}}},
	}})
	require.NoError(t, err)
	_, err = sp.Properties().Set(ctx, src, space.TemplateCollectionId, map[string]any{"version": 3})
	require.NoError(t, err)
	after := cfRecords(t, ctx, sp, id)
	assert.Equal(t, "Heading", after["b1"]["text"])
	row, err = sp.Objects().Get(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, float64(2), row.GetFloat64("any", "template", "version"))

	// Type given and equal passes; another type is the typed sentinel.
	_, err = sp.Objects().Create(ctx, space.CreateObjectOpts{From: src, Type: cfType})
	require.NoError(t, err)
	_, err = sp.Objects().Create(ctx, space.CreateObjectOpts{From: src, Type: "wm-blocks-type"})
	assert.ErrorIs(t, err, space.ErrFromTypeMismatch)
}

func TestCreateFrom_DuplicateAndSaveAsTemplate(t *testing.T) {
	ctx, sp := cfOpen(t)
	src := cfSeed(t, ctx, sp, nil, nil)

	// A plain duplicate: same values and records, no stamp.
	dup, err := sp.Objects().Create(ctx, space.CreateObjectOpts{From: src})
	require.NoError(t, err)
	row, err := sp.Objects().Get(ctx, dup)
	require.NoError(t, err)
	assert.Equal(t, "Weekly review", string(row.GetStringBytes("any", "name")))
	assert.Nil(t, row.Get("any", "template"), "a non-template source stamps nothing")
	assert.Equal(t, cfRecords(t, ctx, sp, src), cfRecords(t, ctx, sp, dup))

	// Save as template: the copy is filed under the marker with its
	// own columns, and is itself a template a later copy drops.
	tpl, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		From:        src,
		Collections: []string{space.TemplateCollectionId},
		InitialProperties: map[string]map[string]any{
			space.TemplateCollectionId: {"title": "Weekly", "version": 1},
		},
	})
	require.NoError(t, err)
	row, err = sp.Objects().Get(ctx, tpl)
	require.NoError(t, err)
	require.Len(t, row.GetArray("any", "collections"), 1)
	assert.Equal(t, space.TemplateCollectionId, string(row.GetArray("any", "collections")[0].GetStringBytes()))
	assert.Equal(t, "Weekly", string(row.GetStringBytes(space.TemplateCollectionId, "title")))
	assert.Nil(t, row.Get("any", "template"))

	made, err := sp.Objects().Create(ctx, space.CreateObjectOpts{From: tpl})
	require.NoError(t, err)
	row, err = sp.Objects().Get(ctx, made)
	require.NoError(t, err)
	assert.Empty(t, row.GetArray("any", "collections"))
	assert.Equal(t, tpl, string(row.GetStringBytes("any", "template", "id")))
	assert.Equal(t, float64(1), row.GetFloat64("any", "template", "version"))

	// A duplicate of a stamped object keeps its lineage.
	again, err := sp.Objects().Create(ctx, space.CreateObjectOpts{From: made})
	require.NoError(t, err)
	row, err = sp.Objects().Get(ctx, again)
	require.NoError(t, err)
	assert.Equal(t, tpl, string(row.GetStringBytes("any", "template", "id")))
}

// A consumer's registered collection is a marker of the source — like
// the bin or the sidebar — and never travels, values included; the
// request may still file the copy under one.
func TestCreateFrom_MarkersStayBehind(t *testing.T) {
	ctx, sp := cfOpen(t)
	src := cfSeed(t, ctx, sp, []string{cfMarker}, map[string]map[string]any{cfMarker: {"pos": "a0"}})

	dup, err := sp.Objects().Create(ctx, space.CreateObjectOpts{From: src})
	require.NoError(t, err)
	row, err := sp.Objects().Get(ctx, dup)
	require.NoError(t, err)
	assert.Empty(t, row.GetArray("any", "collections"))
	assert.Nil(t, row.Get(cfMarker))

	// Refiled under the marker by the request: the marker's values come
	// from the request, never from the source.
	pinned, err := sp.Objects().Create(ctx, space.CreateObjectOpts{From: src, Collections: []string{cfMarker}})
	require.NoError(t, err)
	row, err = sp.Objects().Get(ctx, pinned)
	require.NoError(t, err)
	require.Len(t, row.GetArray("any", "collections"), 1)
	assert.Nil(t, row.Get(cfMarker, "pos"), "the source's marker value did not travel")
	placed, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		From: src, Collections: []string{cfMarker},
		InitialProperties: map[string]map[string]any{cfMarker: {"pos": "b0"}},
	})
	require.NoError(t, err)
	row, err = sp.Objects().Get(ctx, placed)
	require.NoError(t, err)
	assert.Equal(t, "b0", string(row.GetStringBytes(cfMarker, "pos")))
}

// Membership has one source: a `type` or `collections` value under
// InitialProperties["any"] is ignored, so the pre-flight and the apply
// never disagree on what the row carries.
func TestCreate_MembershipFromOptsOnly(t *testing.T) {
	ctx, sp := cfOpen(t)
	id, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		Type:        cfType,
		Collections: []string{cfMarker},
		InitialProperties: map[string]map[string]any{
			"any": {"name": "n", "type": "wm-blocks-type", "collections": []string{"nowhere"}},
		},
	})
	require.NoError(t, err)
	row, err := sp.Objects().Get(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, cfType, string(row.GetStringBytes("any", "type")))
	require.Len(t, row.GetArray("any", "collections"), 1)
	assert.Equal(t, cfMarker, string(row.GetArray("any", "collections")[0].GetStringBytes()))
	assert.Equal(t, "n", string(row.GetStringBytes("any", "name")))
}

func TestCreateFrom_Refusals(t *testing.T) {
	ctx, sp := cfOpen(t)

	_, err := sp.Objects().Create(ctx, space.CreateObjectOpts{From: "bafyunknown"})
	assert.ErrorIs(t, err, space.ErrObjectNotFound)

	// A definition's own row (a user type) is never copied.
	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "T", XKey: "t"})
	require.NoError(t, err)
	_, err = sp.Objects().Create(ctx, space.CreateObjectOpts{From: typeId})
	assert.ErrorIs(t, err, space.ErrMetaType)

	// A deleted source.
	src := cfSeed(t, ctx, sp, nil, nil)
	require.NoError(t, sp.Objects().Delete(ctx, src))
	_, err = sp.Objects().Create(ctx, space.CreateObjectOpts{From: src})
	assert.ErrorIs(t, err, space.ErrObjectDeleted)
}

// A consumer may not register the SDK's own collection id, as a
// collection or as a type.
func TestCreateFrom_TemplateIdReserved(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := wmConfig(t, dataDir)
	cfg.Collections = []handler.Collection{{Id: space.TemplateCollectionId, Name: "Mine"}}
	_, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.Error(t, err)
	cfg = wmConfig(t, dataDir)
	cfg.Types = append(cfg.Types, handler.Type{Id: space.TemplateCollectionId, Name: "Mine"})
	_, err = Open(ctx, cfg, wmProvider(t, dataDir))
	require.Error(t, err)
	cfg = wmConfig(t, dataDir)
	cfg.Modules = []handler.Module{{
		Name: space.TemplateCollectionId, Canonical: "template_body", DataVersion: "t-v1",
		New: func(handler.ModuleInstance) handler.Dataset {
			return handler.Dataset{Schema: handler.Schema{Dynamic: true}}
		},
	}}
	_, err = Open(ctx, cfg, wmProvider(t, dataDir))
	require.Error(t, err)
}

// A NoCopy module's records stay behind: the copy carries the type's
// other datasets and starts the module's collection empty.
func TestCreateFrom_NoCopyModule(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cfg := cfConfig(t, dataDir)
	cfg.Modules = append(cfg.Modules, handler.Module{
		Name: "transcript", Canonical: "transcript_log", CanonicalOnly: true, NoCopy: true, DataVersion: "tr-v1",
		New: func(handler.ModuleInstance) handler.Dataset {
			return handler.Dataset{Schema: handler.Schema{Dynamic: true}}
		},
	})
	cfg.Types = append(cfg.Types, handler.Type{
		Id: "tr-doc", Name: "Thread",
		Datasets: []handler.Dataset{{Name: "tr_meta", DataVersion: "tr-v1", Handler: handler.DefaultHandler{}}},
		Parts: []handler.Part{
			{Key: "meta", Datasets: []handler.PartDataset{{Name: "tr_meta"}}},
			{Key: "log", Datasets: []handler.PartDataset{{Module: "transcript"}}},
		},
	})
	sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "NoCopy"})
	require.NoError(t, err)

	src, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: "tr-doc"})
	require.NoError(t, err)
	_, err = sp.Modify(ctx, space.ModifyBatch{ObjectId: src, Dataset: "tr_meta", Records: []space.RecordModify{
		{Id: "m1", Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"text": "kept"}}}},
	}})
	require.NoError(t, err)
	_, err = sp.Modify(ctx, space.ModifyBatch{ObjectId: src, Dataset: "transcript_log", Records: []space.RecordModify{
		{Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"text": "left behind"}}}},
	}})
	require.NoError(t, err)

	dup, err := sp.Objects().Create(ctx, space.CreateObjectOpts{From: src})
	require.NoError(t, err)
	assert.Equal(t, cfRecordsOf(t, ctx, sp, src, "tr_meta"), cfRecordsOf(t, ctx, sp, dup, "tr_meta"))
	assert.Len(t, cfRecordsOf(t, ctx, sp, src, "transcript_log"), 1)
	assert.Empty(t, cfRecordsOf(t, ctx, sp, dup, "transcript_log"))
	// Empty, not absent: the copy's own transcript takes writes.
	_, err = sp.Modify(ctx, space.ModifyBatch{ObjectId: dup, Dataset: "transcript_log", Records: []space.RecordModify{
		{Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"text": "fresh"}}}},
	}})
	require.NoError(t, err)
	assert.Len(t, cfRecordsOf(t, ctx, sp, dup, "transcript_log"), 1)
	assert.Len(t, cfRecordsOf(t, ctx, sp, src, "transcript_log"), 1)

	// The runtime-declared path: a user type's part names the module.
	roomType, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Room"})
	require.NoError(t, err)
	_, err = sp.Types().AddPart(ctx, roomType, space.PartDraft{Key: "log", Datasets: []space.DatasetDraft{{Module: "transcript"}}})
	require.NoError(t, err)
	room, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		Type: roomType, InitialProperties: map[string]map[string]any{"any": {"name": "Room"}},
	})
	require.NoError(t, err)
	_, err = sp.Modify(ctx, space.ModifyBatch{ObjectId: room, Dataset: "transcript_log", Records: []space.RecordModify{
		{Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"text": "left behind"}}}},
	}})
	require.NoError(t, err)
	roomDup, err := sp.Objects().Create(ctx, space.CreateObjectOpts{From: room})
	require.NoError(t, err)
	row, err := sp.Objects().Get(ctx, roomDup)
	require.NoError(t, err)
	assert.Equal(t, "Room", string(row.GetStringBytes("any", "name")))
	assert.Len(t, cfRecordsOf(t, ctx, sp, room, "transcript_log"), 1)
	assert.Empty(t, cfRecordsOf(t, ctx, sp, roomDup, "transcript_log"))
	_, err = sp.Modify(ctx, space.ModifyBatch{ObjectId: roomDup, Dataset: "transcript_log", Records: []space.RecordModify{
		{Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: map[string]any{"text": "fresh"}}}},
	}})
	require.NoError(t, err)
	assert.Len(t, cfRecordsOf(t, ctx, sp, roomDup, "transcript_log"), 1)
}

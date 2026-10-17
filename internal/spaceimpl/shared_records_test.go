package spaceimpl

import (
	"context"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/types"
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
	"github.com/anyproto/any-sync-sdk/space"
)

const sharedTypeId = "type-lab"

// seedRecordsDataset declares, on the type object typeId, a part holding
// one records dataset keyed dsKey (shared when asked) with a string
// field `title`, through a real defs controller. verPrefix keeps the
// creation versions distinct across datasets.
func seedRecordsDataset(t *testing.T, ctx context.Context, db anystore.DB, typeId, dsKey string, shared bool, verPrefix string) {
	t.Helper()
	ctrl, err := crdt.NewController(ctx, typeId, db,
		crdt.HandlerReg{Name: typetype.DatasetDefs, Handler: typetype.DatasetDefsHandler{}, Schema: schema.Dataset{Dynamic: true}},
		crdt.HandlerReg{Name: typetype.ShortIdsDataset, Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}},
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, ctrl.CloseOwnedCollections()) }()
	a := &anyenc.Arena{}
	apply := func(ver, recId string, payload *anyenc.Value) {
		require.NoError(t, ctrl.ApplyChange(ctx, crdt.Change{
			ObjectId: typeId, Dataset: typetype.DatasetDefs, ChangeId: "cd-" + recId,
			VersionId: crdt.VersionId(verPrefix + ver), DataVersion: typetype.DatasetDefsHandlerVersion,
			Records: []crdt.RecordChange{{Id: recId, Upsert: true, Ops: []crdt.Op{{Type: crdt.OpSet, Payload: payload}}}},
		}))
	}
	partId, headId := typeId+"-part-"+dsKey, typeId+"-head-"+dsKey
	part := a.NewObject()
	part.Set(typetype.DefFieldDef, a.NewString(typetype.DefKindPart))
	part.Set(typetype.FieldKey, a.NewString(dsKey))
	apply("0", partId, part)
	head := a.NewObject()
	head.Set(typetype.DefFieldDef, a.NewString(typetype.DefKindDataset))
	head.Set(typetype.FieldKey, a.NewString(dsKey))
	head.Set(typetype.DefFieldModule, a.NewString(types.RecordsModule))
	head.Set(typetype.DefFieldPart, a.NewString(partId))
	head.Set(typetype.DefFieldIdRule, a.NewString("user"))
	if shared {
		head.Set(typetype.DefFieldPerSpace, a.NewTrue())
	}
	apply("1", headId, head)
	field := a.NewObject()
	field.Set(typetype.DefFieldDef, a.NewString(typetype.DefKindField))
	field.Set(typetype.DefFieldDataset, a.NewString(headId))
	field.Set(typetype.FieldKey, a.NewString("title"))
	field.Set(typetype.FieldKind, a.NewString("string"))
	apply("2", typeId+"-field-"+dsKey, field)
}

// sharedRecordsSpace is a spaceImpl over a store whose catalog holds a
// shared records dataset and a per-object one on one type.
func sharedRecordsSpace(t *testing.T) (s *spaceImpl, samples, plain string) {
	t.Helper()
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "shared.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	objects, err := db.Collection(ctx, "spaceA_"+spaceobjects.SpaceObjectsCollection)
	require.NoError(t, err)
	a := &anyenc.Arena{}
	row := a.NewObject()
	row.Set("id", a.NewString(sharedTypeId))
	anyObj := a.NewObject()
	anyObj.Set(anytype.FieldType, a.NewString(typetype.MetaTypeMarker))
	row.Set(anytype.TypeId, anyObj)
	require.NoError(t, objects.UpsertOne(ctx, row))
	seedRecordsDataset(t, ctx, db, sharedTypeId, "samples", true, "a")
	seedRecordsDataset(t, ctx, db, sharedTypeId, "plain", false, "b")

	store := spaceobjects.NewStore(nil, db, nil, "spaceA", nil, nil, nil, nil)
	t.Cleanup(func() { _ = store.Close() })
	samples, plain = types.CollectionName(sharedTypeId, "samples"), types.CollectionName(sharedTypeId, "plain")
	require.True(t, store.IsKeyedDataset(samples))
	require.False(t, store.IsKeyedDataset(plain))
	return &spaceImpl{id: "spaceA", store: store}, samples, plain
}

// A write to a shared dataset carries the plain record id: the object's
// own stored id loses its prefix; another object's id, any other id
// holding "/", and the bare prefix are refused. A per-object dataset
// keeps every id as written.
func TestChangeRecordIds(t *testing.T) {
	s, samples, plain := sharedRecordsSpace(t)
	for _, tc := range []struct {
		name, id, want string
		refused        bool
	}{
		{name: "own stored id", id: "obj1/r1", want: "r1"},
		{name: "plain id", id: "r1", want: "r1"},
		{name: "empty id", id: "", want: ""},
		{name: "another object's id", id: "obj2/r1", refused: true},
		{name: "bare prefix", id: "obj1/", refused: true},
		{name: "slash in the remainder", id: "obj1/a/b", refused: true},
		{name: "slash", id: "a/b", refused: true},
		{name: "prefix of a longer object id", id: "obj10/r1", refused: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recs := []crdt.RecordChange{{Id: tc.id}}
			err := s.changeRecordIds("obj1", samples, recs)
			if tc.refused {
				require.ErrorIs(t, err, space.ErrRecordIdOfAnotherObject)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, recs[0].Id)

			recs = []crdt.RecordChange{{Id: tc.id}}
			require.NoError(t, s.changeRecordIds("obj1", plain, recs))
			assert.Equal(t, tc.id, recs[0].Id, "a per-object dataset keeps the id")
		})
	}

	// A batch fails on its first refused record and names its position.
	recs := []crdt.RecordChange{{Id: "r1"}, {Id: "obj1/r2"}, {Id: "obj1/"}}
	err := s.changeRecordIds("obj1", samples, recs)
	require.ErrorIs(t, err, space.ErrRecordIdOfAnotherObject)
	assert.Contains(t, err.Error(), "record 2")
	recs = []crdt.RecordChange{{Id: "obj2/r1"}, {Id: "obj1/"}}
	require.NoError(t, s.changeRecordIds("obj1", plain, recs))
	assert.Equal(t, []crdt.RecordChange{{Id: "obj2/r1"}, {Id: "obj1/"}}, recs)
}

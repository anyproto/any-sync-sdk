package spaceobjects

import (
	"context"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
)

// A create written as one multi-field op (membership, name and every
// value together) is rebuilt after one of its properties was removed:
// the replay through the current schema sheds the removed key and keeps
// the type, the collections, the name and the other values, instead of
// dropping the op and leaving a row with nothing but its stamps.
func TestReindex_RebuildShedsRemovedPropertyFromSingleOpCreate(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "reindex.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	s := &Store{db: db, spaceId: "spaceA"}
	shared, err := s.SharedObjects(ctx)
	require.NoError(t, err)

	const typeT, collC = "typeT", "collC"
	reg := &types.StubRegistry{}
	reg.Set("any", "type", schema.KindString)
	reg.Set("any", "collections", schema.KindArray)
	reg.Set("any", "name", schema.KindString)
	reg.Set(typeT, "title", schema.KindString)
	reg.Set(collC, "state", schema.KindString)
	reg.Set(collC, "gone", schema.KindNumber)

	open := func(version int) *crdt.Controller {
		ctrl, err := crdt.NewControllerWithShared(ctx, "obj1", db,
			crdt.SharedCollections{properties.Dataset: shared},
			crdt.HandlerReg{
				Name: properties.Dataset, Handler: properties.New(reg),
				Schema: schema.Dataset{Dynamic: true}, DynamicScopeByKey: true, Version: version,
			},
		)
		require.NoError(t, err)
		ctrl.SetSpaceId("spaceA")
		return ctrl
	}

	a := &anyenc.Arena{}
	payload := a.NewObject()
	payload.Set("any.type", a.NewString(typeT))
	colls := a.NewArray()
	colls.SetArrayItem(0, a.NewString(collC))
	payload.Set("any.collections", colls)
	payload.Set("any.name", a.NewString("Issue"))
	payload.Set(typeT+".title", a.NewString("t"))
	payload.Set(collC+".state", a.NewString("todo"))
	payload.Set(collC+".gone", a.NewNumberFloat64(3))
	create := crdt.Change{
		SpaceId: "spaceA", ObjectId: "obj1", Dataset: properties.Dataset, ChangeId: "c1", VersionId: "v1",
		Timestamp: 100, Creator: "acct-a", DataVersion: properties.HandlerVersion,
		Records: []crdt.RecordChange{{Id: "obj1", Upsert: true, Ops: []crdt.Op{
			{Type: crdt.OpSet, Payload: payload},
		}}},
	}

	previous := open(properties.LocalVersion - 1)
	require.NoError(t, previous.ApplyChange(ctx, create))
	row := previous.Get(ctx, properties.Dataset, "obj1")
	require.NotNil(t, row)
	assert.Equal(t, float64(3), row.GetFloat64(collC, "gone"), "applied in order, the row holds every value")

	// The property is removed; a rebuild under the bumped handler
	// version replays the create against the narrowed schema.
	delete(reg.Kinds[collC], "gone")
	current := open(properties.LocalVersion)
	require.Equal(t, []string{properties.Dataset}, current.StaleDatasets())
	require.NoError(t, s.wipeMaterialized(ctx, "obj1", current))
	require.NoError(t, current.ResetForReindex(ctx, nil))
	require.NoError(t, current.ApplyChange(ctx, create))

	row = current.Get(ctx, properties.Dataset, "obj1")
	require.NotNil(t, row)
	assert.Equal(t, typeT, row.GetString("any", "type"), "type kept")
	require.NotNil(t, row.Get("any", "collections"), "collections kept")
	assert.Equal(t, "Issue", row.GetString("any", "name"), "name kept")
	assert.Equal(t, "t", row.GetString(typeT, "title"), "type value kept")
	assert.Equal(t, "todo", row.GetString(collC, "state"), "declared collection value kept")
	assert.Nil(t, row.Get(collC, "gone"), "the removed property is shed")
}

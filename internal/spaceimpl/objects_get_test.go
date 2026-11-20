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
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	"github.com/anyproto/any-sync-sdk/space"
)

func TestLiveObjectRow(t *testing.T) {
	a := &anyenc.Arena{}
	assert.False(t, liveObjectRow(nil))
	live := a.NewObject()
	live.Set("id", a.NewString("o"))
	assert.True(t, liveObjectRow(live))
	tomb := a.NewObject()
	tomb.Set("id", a.NewString("o"))
	tomb.Set(crdt.DeletedAtField, a.NewNumberInt(1))
	assert.False(t, liveObjectRow(tomb))
}

// The found path is a pure any-store read: the row comes back cloned
// with its membership fields. (The absent / deleted verdicts need a
// loaded any-sync space and are covered end to end.)
func TestObjectsGet_ReturnsLiveRow(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "objs.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	coll, err := db.Collection(ctx, "spaceA_"+spaceobjects.SpaceObjectsCollection)
	require.NoError(t, err)
	a := &anyenc.Arena{}
	row := a.NewObject()
	row.Set("id", a.NewString("obj-1"))
	anyObj := a.NewObject()
	anyObj.Set(anytype.FieldType, a.NewString("type-x"))
	colls := a.NewArray()
	colls.SetArrayItem(0, a.NewString("coll-y"))
	anyObj.Set(anytype.FieldCollections, colls)
	row.Set(anytype.TypeId, anyObj)
	require.NoError(t, coll.UpsertOne(ctx, row))

	store := spaceobjects.NewStore(nil, db, nil, "spaceA", nil, nil, nil, nil)
	t.Cleanup(func() { _ = store.Close() })
	s := &spaceImpl{id: "spaceA", store: store}
	got, err := newObjectService(s).Get(ctx, "obj-1")
	require.NoError(t, err)
	assert.Equal(t, "obj-1", got.GetString("id"))
	assert.Equal(t, "type-x", got.GetString(anytype.TypeId, anytype.FieldType))
	assert.Equal(t, "coll-y", got.GetString(anytype.TypeId, anytype.FieldCollections, "0"))

	_, err = newObjectService(s).Get(ctx, "")
	require.Error(t, err)
}

// Create refuses a typeless request before it touches the store, so a
// refused Create leaves no tree behind: a spaceImpl with no store
// would panic on the first store access.
func TestObjectsCreate_TypeRequiredBeforeStore(t *testing.T) {
	s := &spaceImpl{id: "spaceA"}
	id, err := newObjectService(s).Create(context.Background(), space.CreateObjectOpts{})
	require.ErrorIs(t, err, space.ErrTypeRequired)
	require.Empty(t, id)
}

package techspace_test

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
)

// The row carries the account's use of the space from its create, and
// a later write replaces it: the handler pins nothing.
func TestSpaceIndexRecord_AnySpaceType(t *testing.T) {
	ctx := context.Background()
	arena := &anyenc.Arena{}
	with := techspace.SpaceIndexRecord{Id: "s1", Type: "any.space", AnySpaceType: "bao"}.EncodeCreate(arena)
	assert.Equal(t, "bao", with.GetString(techspace.FieldAnySpaceType))
	without := techspace.SpaceIndexRecord{Id: "s2", Type: "any.space"}.EncodeCreate(arena)
	assert.Nil(t, without.Get(techspace.FieldAnySpaceType), "an empty value is omitted")

	ctrl := newSpaceIndexController(t)
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v1", "s1", true, crdt.Op{Type: crdt.OpSet, Payload: with})))
	rec := techspace.DecodeSpaceIndexRecord(ctrl.Get(ctx, techspace.SpaceIndexDataset, "s1"))
	assert.Equal(t, "bao", rec.AnySpaceType)

	res, err := ctrl.ApplyChangeWithResult(ctx, makeChange("v2", "s1", false, crdt.Op{
		Type:    crdt.OpSet,
		Path:    []string{techspace.FieldAnySpaceType},
		Payload: arena.NewString("notes"),
	}))
	require.NoError(t, err)
	assert.Empty(t, res.Rejections)
	rec = techspace.DecodeSpaceIndexRecord(ctrl.Get(ctx, techspace.SpaceIndexDataset, "s1"))
	assert.Equal(t, "notes", rec.AnySpaceType)
}

// A schema that does not declare the field drops it from a create and
// keeps the rest of the row, and drops a write of the field alone.
func TestSpaceIndexHandler_AnySpaceTypeShedByOlderSchema(t *testing.T) {
	ctx := context.Background()
	older := techspace.SpaceIndexSchema()
	older.Fields = slices.DeleteFunc(older.Fields, func(f schema.Field) bool { return f.Id == techspace.FieldAnySpaceType })

	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctrl, err := crdt.NewController(ctx, testObjectId, db,
		crdt.HandlerReg{Name: techspace.SpaceIndexDataset, Handler: techspace.SpaceIndexHandler{}, Schema: older},
	)
	require.NoError(t, err)

	arena := &anyenc.Arena{}
	create := techspace.SpaceIndexRecord{Id: "s1", Type: "any.space", Name: "Bao", AnySpaceType: "bao"}.EncodeCreate(arena)
	res, err := ctrl.ApplyChangeWithResult(ctx, makeChange("v1", "s1", true, crdt.Op{Type: crdt.OpSet, Payload: create}))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1, "only the undeclared key is shed")

	rec := techspace.DecodeSpaceIndexRecord(ctrl.Get(ctx, techspace.SpaceIndexDataset, "s1"))
	assert.Equal(t, "Bao", rec.Name)
	assert.Equal(t, "any.space", rec.Type)
	assert.Empty(t, rec.AnySpaceType)

	res, err = ctrl.ApplyChangeWithResult(ctx, makeChange("v2", "s1", false, crdt.Op{
		Type:    crdt.OpSet,
		Path:    []string{techspace.FieldAnySpaceType},
		Payload: arena.NewString("bao"),
	}))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	rec = techspace.DecodeSpaceIndexRecord(ctrl.Get(ctx, techspace.SpaceIndexDataset, "s1"))
	assert.Empty(t, rec.AnySpaceType)
	assert.Equal(t, "Bao", rec.Name)
}

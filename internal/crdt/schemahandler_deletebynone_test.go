package crdt

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// shNoDeleteDecl is the multi-writer shape: caller ids, every field
// mutable by anyone, no stamps, no record deletes.
func shNoDeleteDecl() schema.Dataset {
	return schema.Dataset{
		Fields: []schema.Field{
			{Id: "title", Schema: schema.Leaf(schema.KindString), MutableBy: schema.MutableByAnyone},
			{Id: "note", Schema: schema.Leaf(schema.KindString), MutableBy: schema.MutableByAnyone},
		},
		DeleteBy: schema.DeleteByNone,
		IdRule:   schema.IdUser,
	}
}

func TestSchemaHandler_DeleteByNone(t *testing.T) {
	st := newSchemaHandlerController(t, shNoDeleteDecl())
	a := &anyenc.Arena{}
	res, err := st.ApplyChangeWithResult(ctx, shChange("v1", shAuthorA, 100,
		shCreateRecord(a, "row-1", map[string]string{"title": "t"})))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)

	// Nobody deletes: not another member, not the writer who created it.
	for _, tc := range []struct{ version, creator string }{{"v2", shAuthorB}, {"v3", shAuthorA}} {
		res, err = st.ApplyChangeWithResult(ctx, shChange(tc.version, tc.creator, 200, RecordChange{
			Id: "row-1", Ops: []Op{{Type: OpDelete}},
		}))
		require.NoError(t, err)
		require.Len(t, res.Rejections, 1)
		assert.Equal(t, -1, res.Rejections[0].OpIndex)
		assert.ErrorIs(t, res.Rejections[0].Err, ErrValidation)
		assert.ErrorIs(t, res.Rejections[0].Err, ErrDatasetUndeletable)
		rec := st.Get(ctx, shTestDS, "row-1")
		require.NotNil(t, rec)
		assert.Nil(t, rec.Get(DeletedAtField))
		assert.Equal(t, "t", string(rec.GetStringBytes("title")))
	}

	// A delete racing ahead of its create leaves no tombstone, so the
	// create that follows lands whole.
	res, err = st.ApplyChangeWithResult(ctx, shChange("v4", shAuthorB, 200, RecordChange{
		Id: "row-9", Ops: []Op{{Type: OpDelete}},
	}))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.ErrorIs(t, res.Rejections[0].Err, ErrDatasetUndeletable)
	assert.Nil(t, st.Get(ctx, shTestDS, "row-9"))
	res, err = st.ApplyChangeWithResult(ctx, shChange("v5", shAuthorA, 300,
		shCreateRecord(a, "row-9", map[string]string{"title": "late"})))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)
	rec := st.Get(ctx, shTestDS, "row-9")
	require.NotNil(t, rec)
	assert.Equal(t, "late", string(rec.GetStringBytes("title")))

	// The record stays writable after the refused deletes.
	res, err = st.ApplyChangeWithResult(ctx, shChange("v6", shAuthorB, 400, RecordChange{
		Id: "row-1", Ops: []Op{{Type: OpSet, Path: []string{"note"}, Payload: a.NewString("n")}},
	}))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)
	assert.Equal(t, "n", string(st.Get(ctx, shTestDS, "row-1").GetStringBytes("note")))
}

// The local pre-flight refuses the delete before a change exists, with
// the sentinel a caller matches, whatever else the change carries.
func TestSchemaHandler_DeleteByNonePreValidate(t *testing.T) {
	h, err := NewSchemaHandler(shNoDeleteDecl())
	require.NoError(t, err)
	a := &anyenc.Arena{}
	ch := shChange("v1", shAuthorA, 100,
		shCreateRecord(a, "row-1", map[string]string{"title": "t"}),
		RecordChange{Id: "row-2", Ops: []Op{{Type: OpDelete}}})
	err = h.PreValidateMulti(&ch, func(string) *anyenc.Value { return nil })
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrValidation)
	assert.ErrorIs(t, err, ErrDatasetUndeletable)

	// The author-gated policy still leaves deletes to apply time.
	h, err = NewSchemaHandler(shDecl())
	require.NoError(t, err)
	del := shChange("v2", shAuthorA, 100, RecordChange{Id: "row-2", Ops: []Op{{Type: OpDelete}}})
	require.NoError(t, h.PreValidateMulti(&del, func(string) *anyenc.Value { return nil }))
}

package properties_test

import (
	"context"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
)

// A multi-field op on the apply path sheds the keys that fail and lands
// the rest; it drops only when every key fails. On a create the handler
// sheds in BeforeCreate; on a modify the controller re-probes the
// rejected op per key. The pre-flight keeps refusing the whole write on
// the first failing key.

const (
	shedUserT = "user-t" // the object's type
	shedColl  = "coll-c" // a collection one of whose properties is gone
)

func shedRegistry() *types.StubRegistry {
	r := defaultRegistry()
	r.Set(typeAny, anytype.FieldType, schema.KindString)
	r.Set(typeAny, anytype.FieldCollections, schema.KindArray)
	r.Set(shedUserT, "title", schema.KindString)
	r.Set(shedColl, "state", schema.KindString)
	return r
}

// shedCreatePayload is the single-op create shape: membership, an
// `any` value, a type value and two collection values, one of which is
// no longer declared.
func shedCreatePayload(a *anyenc.Arena) *anyenc.Value {
	p := a.NewObject()
	p.Set(typeAny+"."+anytype.FieldType, a.NewString(shedUserT))
	colls := a.NewArray()
	colls.SetArrayItem(0, a.NewString(shedColl))
	p.Set(typeAny+"."+anytype.FieldCollections, colls)
	p.Set(typeAny+"."+propName, a.NewString("named"))
	p.Set(shedUserT+".title", a.NewString("t"))
	p.Set(shedColl+".state", a.NewString("todo"))
	p.Set(shedColl+".removed", a.NewNumberFloat64(3))
	return p
}

func TestSystemPropertiesHandler_CreateMultiFieldShedsFailingKeys(t *testing.T) {
	ctrl := newPropsController(t, shedRegistry())
	a := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Payload: shedCreatePayload(a)},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, shedUserT, rec.GetString(typeAny, anytype.FieldType), "membership kept")
	colls := rec.Get(typeAny, anytype.FieldCollections)
	require.NotNil(t, colls, "collections kept")
	assert.Equal(t, anyenc.TypeArray, colls.Type())
	assert.Equal(t, "named", rec.GetString(typeAny, propName))
	assert.Equal(t, "t", rec.GetString(shedUserT, "title"))
	assert.Equal(t, "todo", rec.GetString(shedColl, "state"))
	assert.Nil(t, rec.Get(shedColl, "removed"), "the undeclared key is shed")
}

func TestSystemPropertiesHandler_ModifyMultiFieldShedsFailingKeys(t *testing.T) {
	ctrl := newPropsController(t, shedRegistry())
	a := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: a.NewString("ok")},
	)))

	p := a.NewObject()
	p.Set(shedColl+".state", a.NewString("done"))
	p.Set(shedColl+".removed", a.NewNumberFloat64(4))
	p.Set(typeAny+"."+propRating, a.NewString("not-a-number"))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", testObjectId, false,
		crdt.Op{Type: crdt.OpSet, Payload: p},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "ok", rec.GetString(typeAny, propName))
	assert.Equal(t, "done", rec.GetString(shedColl, "state"), "the declared key landed")
	assert.Nil(t, rec.Get(shedColl, "removed"), "unknown property shed")
	assert.Nil(t, rec.Get(typeAny, propRating), "kind mismatch shed")
}

func TestSystemPropertiesHandler_CreateMultiFieldDropsWhenEveryKeyFails(t *testing.T) {
	ctrl := newPropsController(t, shedRegistry())
	a := &anyenc.Arena{}

	// Nothing to shed around: the op drops, the row carries no value.
	p := a.NewObject()
	p.Set(shedColl+".removed", a.NewNumberFloat64(3))
	p.Set(typeAny+"."+propRating, a.NewString("not-a-number"))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Payload: p},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	if rec != nil {
		assert.Nil(t, rec.Get(shedColl), "no collection value")
		assert.Nil(t, rec.Get(typeAny, propRating), "no kind-mismatched value")
	}
}

func TestPreValidate_MultiFieldRefusesTheWholeWrite(t *testing.T) {
	h := properties.New(shedRegistry())
	a := &anyenc.Arena{}
	p := a.NewObject()
	p.Set(typeAny+"."+propName, a.NewString("ok"))
	p.Set(typeAny+".ghost", a.NewString("x"))
	err := h.PreValidate(multiFieldChange(p), nil)
	require.ErrorIs(t, err, crdt.ErrValidation)
	var ve *properties.ValidationError
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, properties.ReasonUnknownProperty, ve.Reason)
	obj, _ := p.Object()
	assert.Equal(t, 2, obj.Len(), "the pre-flight never sheds")
}

func TestSystemPropertiesHandler_MultiFieldDropsWhenEveryKeyFails(t *testing.T) {
	ctrl := newPropsController(t, shedRegistry())
	a := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: a.NewString("ok")},
	)))

	p := a.NewObject()
	p.Set(shedColl+".removed", a.NewNumberFloat64(4))
	p.Set(typeAny+"."+propRating, a.NewString("not-a-number"))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", testObjectId, false,
		crdt.Op{Type: crdt.OpSet, Payload: p},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "ok", rec.GetString(typeAny, propName))
	assert.Nil(t, rec.Get(shedColl, "removed"))
	assert.Nil(t, rec.Get(typeAny, propRating))
	assert.Equal(t, crdt.VersionId("v1"), crdt.VersionId(rec.GetString("_ver", "id")), "the record's creation version is untouched")
}

package properties_test

import (
	"context"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
)

// T4b — kindCheck's op-type-aware branches: $addToSet/$pull demand an
// array property, $inc demands a number. The wrong-kind op drops per-op;
// a same-change op on the right-kind property still lands.
func TestSystemPropertiesHandler_AddToSetPullRequireArray(t *testing.T) {
	ctx := context.Background()
	ctrl := newPropsController(t, defaultRegistry())
	a := &anyenc.Arena{}

	// Create with a string name and an (empty) array tags field.
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange(
		"v1", testObjectId, "_base", true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: a.NewString("ok")},
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propTags}, Payload: a.NewArray()},
	)))

	// $addToSet on the string field drops; on the array field lands.
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange(
		"v2", testObjectId, "_base", false,
		crdt.Op{Type: crdt.OpAddToSet, Path: []string{typeAny, propName}, Payload: a.NewString("x")},
		crdt.Op{Type: crdt.OpAddToSet, Path: []string{typeAny, propTags}, Payload: a.NewString("t1")},
	)))

	rec := ctrl.Get(ctx, properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "ok", rec.GetString("_base", typeAny, propName), "$addToSet on a string field dropped")
	tags := rec.GetArray("_base", typeAny, propTags)
	require.Len(t, tags, 1)
	assert.Equal(t, "t1", string(tags[0].GetStringBytes()), "$addToSet on the array field landed")

	// $pull on the string field also drops (still not an array).
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange(
		"v3", testObjectId, "_base", false,
		crdt.Op{Type: crdt.OpPull, Path: []string{typeAny, propName}, Payload: a.NewString("x")},
	)))
	rec = ctrl.Get(ctx, properties.Dataset, testObjectId)
	assert.Equal(t, "ok", rec.GetString("_base", typeAny, propName), "$pull on a string field dropped")
}

func TestSystemPropertiesHandler_IncRequiresNumber(t *testing.T) {
	ctx := context.Background()
	ctrl := newPropsController(t, defaultRegistry())
	a := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(ctx, makeChange(
		"v1", testObjectId, "_base", true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: a.NewString("ok")},
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propRating}, Payload: a.NewNumberFloat64(1)},
	)))

	// $inc on the string field drops; on the number field applies (1 -> 6).
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange(
		"v2", testObjectId, "_base", false,
		crdt.Op{Type: crdt.OpInc, Path: []string{typeAny, propName}, Payload: a.NewNumberFloat64(5)},
		crdt.Op{Type: crdt.OpInc, Path: []string{typeAny, propRating}, Payload: a.NewNumberFloat64(5)},
	)))

	rec := ctrl.Get(ctx, properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "ok", rec.GetString("_base", typeAny, propName), "$inc on a string field dropped")
	assert.Equal(t, float64(6), rec.GetFloat64("_base", typeAny, propRating), "$inc on the number field applied")
}

// T4c — multi-field $set (empty Path, dotted-key object payload) is
// all-or-nothing per op: one bad key drops the WHOLE op (v1 doesn't
// filter individual keys), and an all-good op lands every key.
func TestSystemPropertiesHandler_MultiFieldAllOrNothing(t *testing.T) {
	ctx := context.Background()
	ctrl := newPropsController(t, defaultRegistry())
	a := &anyenc.Arena{}

	// One bad key (string into number) poisons the whole multi-field op.
	bad := a.NewObject()
	bad.Set(typeAny+"."+propName, a.NewString("ok"))
	bad.Set(typeAny+"."+propRating, a.NewString("not-a-number"))
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange(
		"v1", testObjectId, "_base", true,
		crdt.Op{Type: crdt.OpSet, Payload: bad},
	)))
	rec := ctrl.Get(ctx, properties.Dataset, testObjectId)
	if rec != nil {
		assert.Nil(t, rec.Get("_base", typeAny, propName), "good key dropped with the bad one (whole-op drop)")
		assert.Nil(t, rec.Get("_base", typeAny, propRating), "bad key dropped")
	}

	// All-good multi-field op lands every key.
	good := a.NewObject()
	good.Set(typeAny+"."+propName, a.NewString("hi"))
	good.Set(typeAny+"."+propRating, a.NewNumberFloat64(3))
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange(
		"v2", testObjectId, "_base", true,
		crdt.Op{Type: crdt.OpSet, Payload: good},
	)))
	rec = ctrl.Get(ctx, properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "hi", rec.GetString("_base", typeAny, propName))
	assert.Equal(t, float64(3), rec.GetFloat64("_base", typeAny, propRating))

	// A key that isn't a dotted typeId.propId pair (here "any", no dot)
	// is an invalid path → whole op drops, prior values untouched.
	nodot := a.NewObject()
	nodot.Set(typeAny, a.NewString("x"))
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange(
		"v3", testObjectId, "_base", false,
		crdt.Op{Type: crdt.OpSet, Payload: nodot},
	)))
	rec = ctrl.Get(ctx, properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "hi", rec.GetString("_base", typeAny, propName), "malformed-key op dropped; prior values intact")
}

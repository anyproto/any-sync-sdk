package properties_test

import (
	"context"
	"errors"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
)

const propBag = "bag"

func nestedRegistry() *types.StubRegistry {
	r := defaultRegistry()
	r.Set(typeAny, propBag, schema.KindObject)
	return r
}

// Nested writes land under an object-kind property, per leaf, and are
// dropped under any other kind — on apply and on the local preflight.
func TestSystemPropertiesHandler_NestedUnderObject(t *testing.T) {
	ctrl := newPropsController(t, nestedRegistry())
	ctx := context.Background()
	a := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v1", testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propBag, "index"}, Payload: a.NewString("none")},
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propBag, "rank"}, Payload: a.NewNumberFloat64(3)},
		// below a string property: dropped
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName, "sub"}, Payload: a.NewString("x")},
	)))
	rec := ctrl.Get(ctx, properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "none", rec.GetString(typeAny, propBag, "index"))
	assert.Equal(t, 3.0, rec.GetFloat64(typeAny, propBag, "rank"))
	assert.Nil(t, rec.Get(typeAny, propName))

	// A later per-key write touches only its key; the multi-field form
	// addresses leaves the same way.
	multi := a.NewObject()
	multi.Set(typeAny+"."+propBag+".index", a.NewString("basic"))
	multi.Set(typeAny+"."+propName+".sub", a.NewString("x"))
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v2", testObjectId, false,
		crdt.Op{Type: crdt.OpSet, Payload: multi},
	)))
	rec = ctrl.Get(ctx, properties.Dataset, testObjectId)
	assert.Equal(t, "basic", rec.GetString(typeAny, propBag, "index"))
	assert.Equal(t, 3.0, rec.GetFloat64(typeAny, propBag, "rank"), "sibling key untouched")
	assert.Nil(t, rec.Get(typeAny, propName), "multi-field nested write under a string property dropped")
}

func TestPreValidate_NestedPaths(t *testing.T) {
	h := properties.New(nestedRegistry())
	a := &anyenc.Arena{}
	require.NoError(t, h.PreValidate(singlePathChange(crdt.OpSet, []string{typeAny, propBag, "k"}, a.NewString("v")), nil))

	err := h.PreValidate(singlePathChange(crdt.OpSet, []string{typeAny, propName, "k"}, a.NewString("v")), nil)
	require.ErrorIs(t, err, crdt.ErrValidation)
	var ve *properties.ValidationError
	require.True(t, errors.As(err, &ve))
	assert.Equal(t, properties.ReasonKindMismatch, ve.Reason)

	err = h.PreValidate(singlePathChange(crdt.OpInc, []string{typeAny, propBag, "k"}, a.NewNumberFloat64(1)), nil)
	require.ErrorIs(t, err, crdt.ErrValidation)
	require.True(t, errors.As(err, &ve))
	assert.Equal(t, properties.ReasonInvalidPath, ve.Reason, "container ops address the property, never a leaf")
}

package properties_test

import (
	"errors"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
)

// metaTypeRegistry knows the meta-type namespace, mirroring what
// spaceobjects.buildStaticSchema seeds in production.
func metaTypeRegistry() *types.StubRegistry {
	r := preflightRegistry()
	r.Set(typetype.TypeId, typetype.FieldXKeyProp, schema.KindString)
	return r
}

// TestPreValidate_MetaTypeXKeyNeedsMarker is the invariant that moving
// `xkey` out of `any` buys: the value is writable only on a row that
// declares itself a type object. Under `any` every object could carry
// one, since `any` is implemented by everything.
//
// It also pins the marker→namespace grant: the row carries `__type__`
// in any.types, never the `type` namespace itself.
func TestPreValidate_MetaTypeXKeyNeedsMarker(t *testing.T) {
	h := properties.New(metaTypeRegistry())
	a := &anyenc.Arena{}

	err := h.PreValidate(singlePathChange(
		crdt.OpSet, []string{typetype.TypeId, typetype.FieldXKeyProp}, a.NewString("movie")), nil)
	require.ErrorIs(t, err, crdt.ErrValidation)
	var ve *properties.ValidationError
	require.True(t, errors.As(err, &ve))
	assert.Equal(t, properties.ReasonTypeNotImplemented, ve.Reason)
	assert.Equal(t, typetype.TypeId, ve.TypeId)

	// Carrying an unrelated type is not enough either.
	err = h.PreValidate(singlePathChange(
		crdt.OpSet, []string{typetype.TypeId, typetype.FieldXKeyProp}, a.NewString("movie")),
		beforeWithTypes(a, propUserT))
	require.ErrorIs(t, err, crdt.ErrValidation)

	// Nor is listing the meta-type id itself in any.types — nothing
	// stops a client attaching it, and the marker is the only grant.
	err = h.PreValidate(singlePathChange(
		crdt.OpSet, []string{typetype.TypeId, typetype.FieldXKeyProp}, a.NewString("movie")),
		beforeWithTypes(a, typetype.TypeId))
	require.ErrorIs(t, err, crdt.ErrValidation)
	require.True(t, errors.As(err, &ve))
	assert.Equal(t, properties.ReasonTypeNotImplemented, ve.Reason)

	// A row already marked as a type object accepts it.
	require.NoError(t, h.PreValidate(singlePathChange(
		crdt.OpSet, []string{typetype.TypeId, typetype.FieldXKeyProp}, a.NewString("movie")),
		beforeWithTypes(a, typetype.MetaTypeMarker)))
}

// TestPreValidate_MetaTypeCreateShape pins the exact multi-field $set
// typesAPI.Create emits: the marker and the xkey ride one change, so
// the pre-flight sees the namespace as implemented.
func TestPreValidate_MetaTypeCreateShape(t *testing.T) {
	h := properties.New(metaTypeRegistry())
	a := &anyenc.Arena{}

	payload := a.NewObject()
	marker := a.NewArray()
	marker.SetArrayItem(0, a.NewString(typetype.MetaTypeMarker))
	payload.Set("any.types", marker)
	payload.Set(typeAny+"."+propName, a.NewString("Movie"))
	payload.Set(typetype.TypeId+"."+typetype.FieldXKeyProp, a.NewString("movie"))

	require.NoError(t, h.PreValidate(multiFieldChange(payload), nil))
}

// TestPreValidate_MetaTypeXKeyKind keeps the declared kind enforced —
// the fence is a namespace, not an escape hatch from validation.
func TestPreValidate_MetaTypeXKeyKind(t *testing.T) {
	h := properties.New(metaTypeRegistry())
	a := &anyenc.Arena{}

	err := h.PreValidate(singlePathChange(
		crdt.OpSet, []string{typetype.TypeId, typetype.FieldXKeyProp}, a.NewNumberFloat64(7)),
		beforeWithTypes(a, typetype.MetaTypeMarker))
	require.ErrorIs(t, err, crdt.ErrValidation)
	var ve *properties.ValidationError
	require.True(t, errors.As(err, &ve))
	assert.Equal(t, properties.ReasonKindMismatch, ve.Reason)
}

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
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	collectiontype "github.com/anyproto/any-sync-sdk/internal/types/collection"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
)

// metaTypeRegistry knows both meta namespaces, mirroring what
// spaceobjects.buildStaticSchema seeds in production.
func metaTypeRegistry() *types.StubRegistry {
	r := preflightRegistry()
	r.Set(typetype.TypeId, typetype.FieldXKeyProp, schema.KindString)
	r.Set(collectiontype.TypeId, typetype.FieldXKeyProp, schema.KindString)
	return r
}

// TestPreValidate_MetaTypeXKeyNeedsMarker is the invariant that moving
// `xkey` out of `any` buys: the value is writable only on a row that
// declares itself a type object. Under `any` every object could carry
// one, since `any` is implemented by everything.
//
// It also pins the marker→namespace grant: the row carries `__type__`
// in any.type, never the `type` namespace itself.
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
		beforeWithMembers(a, propUserT))
	require.ErrorIs(t, err, crdt.ErrValidation)

	// Nor is naming the meta-type id itself as any.type — nothing stops
	// a client writing it, and the marker is the only grant.
	err = h.PreValidate(singlePathChange(
		crdt.OpSet, []string{typetype.TypeId, typetype.FieldXKeyProp}, a.NewString("movie")),
		beforeWithMembers(a, typetype.TypeId))
	require.ErrorIs(t, err, crdt.ErrValidation)
	require.True(t, errors.As(err, &ve))
	assert.Equal(t, properties.ReasonTypeNotImplemented, ve.Reason)

	// Nor does the collection marker reach the type namespace.
	err = h.PreValidate(singlePathChange(
		crdt.OpSet, []string{typetype.TypeId, typetype.FieldXKeyProp}, a.NewString("movie")),
		beforeWithMembers(a, collectiontype.MetaMarker))
	require.ErrorIs(t, err, crdt.ErrValidation)
	require.True(t, errors.As(err, &ve))
	assert.Equal(t, properties.ReasonTypeNotImplemented, ve.Reason)

	// A row already marked as a type object accepts it.
	require.NoError(t, h.PreValidate(singlePathChange(
		crdt.OpSet, []string{typetype.TypeId, typetype.FieldXKeyProp}, a.NewString("movie")),
		beforeWithMembers(a, typetype.MetaTypeMarker)))
}

// TestPreValidate_MetaCollectionXKeyNeedsMarker is the same fence one
// marker over: `collection.*` is granted by `__collection__` alone, and
// a collection row cannot reach `type.*`.
func TestPreValidate_MetaCollectionXKeyNeedsMarker(t *testing.T) {
	h := properties.New(metaTypeRegistry())
	a := &anyenc.Arena{}
	write := func(before *anyenc.Value) error {
		return h.PreValidate(singlePathChange(
			crdt.OpSet, []string{collectiontype.TypeId, typetype.FieldXKeyProp}, a.NewString("shelf")), before)
	}

	var ve *properties.ValidationError
	err := write(nil)
	require.True(t, errors.As(err, &ve))
	assert.Equal(t, properties.ReasonTypeNotImplemented, ve.Reason)
	assert.Equal(t, collectiontype.TypeId, ve.TypeId)

	// Naming the meta-collection id as any.type grants nothing.
	require.ErrorIs(t, write(beforeWithMembers(a, collectiontype.TypeId)), crdt.ErrValidation)
	// Neither does the type marker.
	require.ErrorIs(t, write(beforeWithMembers(a, typetype.MetaTypeMarker)), crdt.ErrValidation)

	// The collection marker grants it.
	require.NoError(t, write(beforeWithMembers(a, collectiontype.MetaMarker)))
}

// TestPreValidate_MetaMarkerRevoked pins that the grant follows the
// slot, not history: the change that overwrites the marker in any.type
// takes the meta namespace with it.
func TestPreValidate_MetaMarkerRevoked(t *testing.T) {
	h := properties.New(metaTypeRegistry())
	a := &anyenc.Arena{}
	var ve *properties.ValidationError

	// One change that retypes a collection row to a user type and keeps
	// writing `collection.xkey`: the namespace is gone by then.
	payload := a.NewObject()
	payload.Set(typeAny+"."+anytype.FieldType, a.NewString(propUserT))
	payload.Set(collectiontype.TypeId+"."+typetype.FieldXKeyProp, a.NewString("shelf"))
	err := h.PreValidate(multiFieldChange(payload), beforeWithMembers(a, collectiontype.MetaMarker))
	require.True(t, errors.As(err, &ve))
	assert.Equal(t, properties.ReasonTypeNotImplemented, ve.Reason)
	assert.Equal(t, collectiontype.TypeId, ve.TypeId)

	// Same for the type marker.
	payload = a.NewObject()
	payload.Set(typeAny+"."+anytype.FieldType, a.NewString(propUserT))
	payload.Set(typetype.TypeId+"."+typetype.FieldXKeyProp, a.NewString("movie"))
	err = h.PreValidate(multiFieldChange(payload), beforeWithMembers(a, typetype.MetaTypeMarker))
	require.True(t, errors.As(err, &ve))
	assert.Equal(t, properties.ReasonTypeNotImplemented, ve.Reason)
	assert.Equal(t, typetype.TypeId, ve.TypeId)
}

// TestPreValidate_MetaTypeCreateShape pins the exact multi-field $set
// typesAPI.Create emits: the marker and the xkey ride one change, so
// the pre-flight sees the namespace as implemented. The collection form
// is the same shape with the other marker.
func TestPreValidate_MetaTypeCreateShape(t *testing.T) {
	h := properties.New(metaTypeRegistry())
	a := &anyenc.Arena{}

	payload := a.NewObject()
	payload.Set(typeAny+"."+anytype.FieldType, a.NewString(typetype.MetaTypeMarker))
	payload.Set(typeAny+"."+propName, a.NewString("Movie"))
	payload.Set(typetype.TypeId+"."+typetype.FieldXKeyProp, a.NewString("movie"))
	require.NoError(t, h.PreValidate(multiFieldChange(payload), nil))

	payload = a.NewObject()
	payload.Set(typeAny+"."+anytype.FieldType, a.NewString(collectiontype.MetaMarker))
	payload.Set(typeAny+"."+propName, a.NewString("Shelf"))
	payload.Set(collectiontype.TypeId+"."+typetype.FieldXKeyProp, a.NewString("shelf"))
	require.NoError(t, h.PreValidate(multiFieldChange(payload), nil))
}

// TestPreValidate_MetaTypeXKeyKind keeps the declared kind enforced —
// the fence is a namespace, not an escape hatch from validation.
func TestPreValidate_MetaTypeXKeyKind(t *testing.T) {
	h := properties.New(metaTypeRegistry())
	a := &anyenc.Arena{}

	err := h.PreValidate(singlePathChange(
		crdt.OpSet, []string{typetype.TypeId, typetype.FieldXKeyProp}, a.NewNumberFloat64(7)),
		beforeWithMembers(a, typetype.MetaTypeMarker))
	require.ErrorIs(t, err, crdt.ErrValidation)
	var ve *properties.ValidationError
	require.True(t, errors.As(err, &ve))
	assert.Equal(t, properties.ReasonKindMismatch, ve.Reason)
}

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
	collectiontype "github.com/anyproto/any-sync-sdk/internal/types/collection"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
)

// Membership fixture: one type, two collections, one of each the object
// does not belong to.
const (
	movieT    = "movieT"
	shelfC    = "shelfC"
	starredC  = "starredC"
	otherC    = "otherC"
	propTitle = "p-title"
)

// membersRegistry declares a string property on every definition in the
// fixture, so a rejection is always about membership, never the schema.
func membersRegistry() *types.StubRegistry {
	r := preflightRegistry()
	for _, id := range []string{movieT, shelfC, starredC, otherC} {
		r.Set(id, propTitle, schema.KindString)
	}
	return r
}

// TestSystemPropertiesHandler_InboundIgnoresMembership pins the
// load-bearing asymmetry behind the orphan-resurrection decision
// (docs/06-data-structure.md §16): the inbound apply path does NOT
// consult any.type / any.collections. A value written under a
// definition the object does not have lands as orphan on every peer,
// which is what makes concurrent detach-vs-write convergent.
//
// Compare TestPreValidate_TypeNotImplemented: the LOCAL path rejects
// the identical write. Inbound must accept it.
func TestSystemPropertiesHandler_InboundIgnoresMembership(t *testing.T) {
	ctx := context.Background()

	reg := defaultRegistry()                  // any.* props
	reg.Set("userT", "p1", schema.KindString) // userT is KNOWN to the registry…
	ctrl := newPropsController(t, reg)
	arena := &anyenc.Arena{}

	// …but the object never takes userT (no any.type write). An inbound
	// $set on userT.p1 must still apply — membership is a local-write
	// concern only.
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange(
		"v1", testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{"userT", "p1"}, Payload: arena.NewString("orphan")},
	)))

	rec := ctrl.Get(ctx, properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "orphan", rec.GetString("userT", "p1"),
		"inbound write to an unimplemented type must land as orphan, not drop")

	// The orphan value is real data the registry still validates by kind:
	// a kind-mismatched inbound write to the same unimplemented type still
	// drops per-op, proving the accept path is kind-checked, not blanket.
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange(
		"v2", testObjectId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{"userT", "p1"}, Payload: arena.NewNumberFloat64(5)},
	)))
	rec = ctrl.Get(ctx, properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "orphan", rec.GetString("userT", "p1"),
		"kind-mismatched inbound write drops; prior orphan string survives")
}

// TestPreValidate_TypeAndCollectionsUnion pins the local-write grant
// set: the universal `any`, the one type in any.type, and every id in
// any.collections — nothing else.
func TestPreValidate_TypeAndCollectionsUnion(t *testing.T) {
	h := properties.New(membersRegistry())
	a := &anyenc.Arena{}
	before := beforeWithMembers(a, movieT, shelfC, starredC)

	for _, owner := range []string{typeAny, movieT, shelfC, starredC} {
		propId := propTitle
		if owner == typeAny {
			propId = propName
		}
		require.NoError(t, h.PreValidate(
			singlePathChange(crdt.OpSet, []string{owner, propId}, a.NewString("v")), before), owner)
	}

	// A collection the object is not in stays out of reach, and the
	// rejection names the whole union.
	err := h.PreValidate(
		singlePathChange(crdt.OpSet, []string{otherC, propTitle}, a.NewString("v")), before)
	var ve *properties.ValidationError
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, properties.ReasonTypeNotImplemented, ve.Reason)
	assert.Equal(t, otherC, ve.TypeId)
	assert.ElementsMatch(t, []string{typeAny, movieT, shelfC, starredC}, ve.Members)
}

// TestPreValidate_RetypeDropsOldNamespace pins the single slot: the
// type is a register, so setting a new one in the same change revokes
// the old one's namespace. Collections are unaffected.
func TestPreValidate_RetypeDropsOldNamespace(t *testing.T) {
	h := properties.New(membersRegistry())
	a := &anyenc.Arena{}
	before := beforeWithMembers(a, movieT, shelfC)

	payload := a.NewObject()
	payload.Set(typeAny+"."+anytype.FieldType, a.NewString(propUserT))
	payload.Set(movieT+"."+propTitle, a.NewString("v"))
	err := h.PreValidate(multiFieldChange(payload), before)
	var ve *properties.ValidationError
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, properties.ReasonTypeNotImplemented, ve.Reason)
	assert.Equal(t, movieT, ve.TypeId)

	// The new type and the untouched collection are both writable.
	payload = a.NewObject()
	payload.Set(typeAny+"."+anytype.FieldType, a.NewString(propUserT))
	payload.Set(propUserT+".p1", a.NewString("v"))
	payload.Set(shelfC+"."+propTitle, a.NewString("v"))
	require.NoError(t, h.PreValidate(multiFieldChange(payload), before))
}

// TestPreValidate_DefinitionImplementsItself pins the implicit self
// grant that replaced self-typed bundle roots: a row whose any.type is
// a marker may write values under its OWN id, without naming itself
// anywhere in a membership field.
func TestPreValidate_DefinitionImplementsItself(t *testing.T) {
	a := &anyenc.Arena{}
	selfWrite := func(ownerId, propId string) *crdt.Change {
		ch := singlePathChange(crdt.OpSet, []string{ownerId, propId}, a.NewString("v"))
		ch.ObjectId, ch.Records[0].Id = ownerId, ""
		return ch
	}

	reg := membersRegistry()
	reg.Set(typetype.TypeId, typetype.FieldXKeyProp, schema.KindString)
	reg.Set(collectiontype.TypeId, typetype.FieldXKeyProp, schema.KindString)
	h := properties.New(reg)

	// The type object movieT holds movieT.<prop> under the `__type__`
	// marker alone.
	require.NoError(t, h.PreValidate(selfWrite(movieT, propTitle),
		beforeWithMembers(a, typetype.MetaTypeMarker)))
	// Same for a collection object under `__collection__`.
	require.NoError(t, h.PreValidate(selfWrite(shelfC, propTitle),
		beforeWithMembers(a, collectiontype.MetaMarker)))

	// The grant is the row's own id and no other definition's: movieT's
	// type object cannot reach shelfC's namespace.
	cross := singlePathChange(crdt.OpSet, []string{shelfC, propTitle}, a.NewString("v"))
	cross.ObjectId, cross.Records[0].Id = movieT, ""
	err := h.PreValidate(cross, beforeWithMembers(a, typetype.MetaTypeMarker))
	var ve *properties.ValidationError
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, properties.ReasonTypeNotImplemented, ve.Reason)
	assert.Equal(t, shelfC, ve.TypeId)

	// And a plain object never gets it: naming movieT as its own id
	// without a marker grants nothing.
	err = h.PreValidate(selfWrite(movieT, propTitle), beforeWithMembers(a, propUserT))
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, properties.ReasonTypeNotImplemented, ve.Reason)
}

// classifier stubs the definition lookup the store provides in
// production: movieT is a type, shelfC a collection, everything else
// unresolvable here.
func classifier(_ context.Context, id string) (properties.OwnerKind, error) {
	switch id {
	case movieT:
		return properties.OwnerType, nil
	case shelfC:
		return properties.OwnerCollection, nil
	}
	return properties.OwnerUnknown, nil
}

// TestPreValidate_WrongSlot pins the slot rule: a known collection is
// refused in any.type, a known type (and either marker) in
// any.collections.
func TestPreValidate_WrongSlot(t *testing.T) {
	h := properties.New(membersRegistry())
	h.Classify = classifier
	a := &anyenc.Arena{}

	err := h.PreValidate(singlePathChange(
		crdt.OpSet, []string{typeAny, anytype.FieldType}, a.NewString(shelfC)), nil)
	require.ErrorIs(t, err, properties.ErrWrongSlot)
	var ve *properties.ValidationError
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, properties.ReasonWrongSlot, ve.Reason)
	assert.Equal(t, shelfC, ve.TypeId)
	assert.Equal(t, anytype.FieldType, ve.Slot)
	assert.Equal(t, properties.OwnerCollection, ve.Kind)

	err = h.PreValidate(singlePathChange(
		crdt.OpAddToSet, []string{typeAny, anytype.FieldCollections}, a.NewString(movieT)), nil)
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, properties.ReasonWrongSlot, ve.Reason)
	assert.Equal(t, movieT, ve.TypeId)
	assert.Equal(t, anytype.FieldCollections, ve.Slot)
	assert.Equal(t, properties.OwnerType, ve.Kind)

	// The markers are the type slot's own vocabulary and never belong in
	// the collections list.
	for _, marker := range []string{typetype.MetaTypeMarker, collectiontype.MetaMarker} {
		require.ErrorIs(t, h.PreValidate(singlePathChange(
			crdt.OpAddToSet, []string{typeAny, anytype.FieldCollections}, a.NewString(marker)), nil),
			properties.ErrWrongSlot, marker)
	}

	// The multi-field create shape is checked the same way.
	payload := a.NewObject()
	payload.Set(typeAny+"."+anytype.FieldType, a.NewString(shelfC))
	require.ErrorIs(t, h.PreValidate(multiFieldChange(payload), nil), properties.ErrWrongSlot)
}

// TestPreValidate_WrongSlotUnknownIdsPass keeps the rule tolerant: an
// id that resolves to no definition here may not have synced yet, so
// either slot accepts it. The markers stay valid in any.type.
func TestPreValidate_WrongSlotUnknownIdsPass(t *testing.T) {
	h := properties.New(membersRegistry())
	h.Classify = classifier
	a := &anyenc.Arena{}

	require.NoError(t, h.PreValidate(singlePathChange(
		crdt.OpSet, []string{typeAny, anytype.FieldType}, a.NewString("not-synced-yet")), nil))
	require.NoError(t, h.PreValidate(singlePathChange(
		crdt.OpAddToSet, []string{typeAny, anytype.FieldCollections}, a.NewString("not-synced-yet")), nil))
	for _, marker := range []string{typetype.MetaTypeMarker, collectiontype.MetaMarker} {
		require.NoError(t, h.PreValidate(singlePathChange(
			crdt.OpSet, []string{typeAny, anytype.FieldType}, a.NewString(marker)), nil), marker)
	}

	// Without a classifier nothing is checked at all.
	bare := properties.New(membersRegistry())
	require.NoError(t, bare.PreValidate(singlePathChange(
		crdt.OpSet, []string{typeAny, anytype.FieldType}, a.NewString(shelfC)), nil))
}

// TestPreValidate_TypeRequired pins the one-type rule on the local
// write path: every object has exactly one type, so a change that
// empties `any.type` is refused whatever shape it takes.
func TestPreValidate_TypeRequired(t *testing.T) {
	h := properties.New(membersRegistry())
	h.Classify = classifier
	a := &anyenc.Arena{}
	before := beforeWithMembers(a, movieT)

	refused := func(label string, ch *crdt.Change) {
		t.Helper()
		err := h.PreValidate(ch, before)
		require.ErrorIs(t, err, properties.ErrTypeRequired, label)
		var ve *properties.ValidationError
		require.ErrorAs(t, err, &ve, label)
		assert.Equal(t, properties.ReasonTypeRequired, ve.Reason, label)
		assert.Equal(t, testObjectId, ve.ObjectId, label)
	}

	refused("$unset any.type", singlePathChange(
		crdt.OpUnset, []string{typeAny, anytype.FieldType}, nil))
	refused(`$set any.type = ""`, singlePathChange(
		crdt.OpSet, []string{typeAny, anytype.FieldType}, a.NewString("")))

	// The multi-field create shape is checked the same way.
	payload := a.NewObject()
	payload.Set(typeAny+"."+anytype.FieldType, a.NewString(""))
	refused(`multi-field any.type = ""`, multiFieldChange(payload))

	// Collections carry no such rule: an object files itself nowhere.
	require.NoError(t, h.PreValidate(singlePathChange(
		crdt.OpPull, []string{typeAny, anytype.FieldCollections}, a.NewString(shelfC)), before))
}

// TestSystemPropertiesHandler_InboundToleratesClearedType is the other
// half of that rule: only the LOCAL path refuses. A peer's change that
// clears `any.type` applies as written — dropping the op would leave
// the row diverged from every other device.
func TestSystemPropertiesHandler_InboundToleratesClearedType(t *testing.T) {
	ctx := context.Background()
	ctrl := newPropsController(t, membersRegistry())
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v1", testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, anytype.FieldType}, Payload: arena.NewString(movieT)},
	)))
	rec := ctrl.Get(ctx, properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	require.Equal(t, movieT, rec.GetString(typeAny, anytype.FieldType))

	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v2", testObjectId, false,
		crdt.Op{Type: crdt.OpUnset, Path: []string{typeAny, anytype.FieldType}},
	)))
	rec = ctrl.Get(ctx, properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Nil(t, rec.Get(typeAny, anytype.FieldType), "inbound $unset clears the slot")

	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v3", testObjectId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, anytype.FieldType}, Payload: arena.NewString("")},
	)))
	rec = ctrl.Get(ctx, properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	slot := rec.Get(typeAny, anytype.FieldType)
	require.NotNil(t, slot, `inbound $set to "" lands`)
	assert.Empty(t, string(slot.GetStringBytes()))
}

package properties_test

import (
	"context"
	"errors"
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

const (
	testObjectId = "obj-1"
	testDataVer  = "systemPropertyHandler-v1" // matches properties.HandlerVersion
	typeAny      = "any"
	propName     = "p-name"     // string
	propRating   = "p-rating"   // number
	propTags     = "p-tags"     // array
)

func newPropsController(t *testing.T, reg types.Registry) *crdt.Controller {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := anystore.Open(context.Background(), dbPath, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctrl, err := crdt.NewController(context.Background(), testObjectId, db,
		properties.New(reg),
	)
	require.NoError(t, err)
	return ctrl
}

func makeChange(versionId crdt.VersionId, recId, variant string, upsert bool, ops ...crdt.Op) crdt.Change {
	return crdt.Change{
		ObjectId:    testObjectId,
		Dataset:     properties.Dataset,
		ChangeId:    "ch-" + string(versionId),
		VersionId:   versionId,
		DataVersion: testDataVer,
		Records: []crdt.RecordChange{
			{Id: recId, Upsert: upsert, Variant: variant, Ops: ops},
		},
	}
}

func defaultRegistry() *types.StubRegistry {
	r := &types.StubRegistry{}
	r.Set(typeAny, propName, schema.KindString)
	r.Set(typeAny, propRating, schema.KindNumber)
	r.Set(typeAny, propTags, schema.KindArray)
	return r
}

// ----------------------------------------------------------------------------
// Variant routing — values land inside the named subdocument
// ----------------------------------------------------------------------------

func TestSystemPropertiesHandler_BaseVariantRouting(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, "_base", true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("Hello")},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)

	// Top-level _ver.id is the creation marker.
	assert.Equal(t, "v1", rec.GetString("_ver", "id"))

	// Value lands inside _base, not at root.
	assert.Equal(t, "Hello", rec.GetString("_base", typeAny, propName))
	assert.Nil(t, rec.Get(typeAny, propName), "nothing at root for variant writes")

	// _ver for the field lives inside the _base subdoc — read via the
	// version helper so collapse/expansion is handled.
	assert.Equal(t, crdt.VersionId("v1"),
		crdt.GetRecordVersion(rec.Get("_base"), typeAny, propName))
}

func TestSystemPropertiesHandler_VariantsAreIsolated(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	// Write the same field in three variants in three changes.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, "_base", true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("base name")},
	)))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", testObjectId, "_account", false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("account name")},
	)))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v3", testObjectId, "_device", false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("device name")},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "base name", rec.GetString("_base", typeAny, propName))
	assert.Equal(t, "account name", rec.GetString("_account", typeAny, propName))
	assert.Equal(t, "device name", rec.GetString("_device", typeAny, propName))

	// Each variant carries its own _ver — no cross-variant overlap.
	assert.Equal(t, crdt.VersionId("v1"),
		crdt.GetRecordVersion(rec.Get("_base"), typeAny, propName))
	assert.Equal(t, crdt.VersionId("v2"),
		crdt.GetRecordVersion(rec.Get("_account"), typeAny, propName))
	assert.Equal(t, crdt.VersionId("v3"),
		crdt.GetRecordVersion(rec.Get("_device"), typeAny, propName))
}

// ----------------------------------------------------------------------------
// Kind validation
// ----------------------------------------------------------------------------

func TestSystemPropertiesHandler_KindMismatchDrops(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	// Create with a valid name.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, "_base", true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("ok")},
	)))

	// Modify: bundle a kind-mismatched op (number into a string field)
	// with a valid op for `rating`. The bad op drops; the good one lands.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", testObjectId, "_base", false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewNumberFloat64(42)},
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propRating}, Payload: arena.NewNumberFloat64(7.5)},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "ok", rec.GetString("_base", typeAny, propName), "string name survives kind-mismatch attempt")
	assert.Equal(t, 7.5, rec.GetFloat64("_base", typeAny, propRating), "number rating landed")
}

func TestSystemPropertiesHandler_UnknownPropertyDrops(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, "_base", true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("ok")},
	)))

	// `unknown-prop` is not in the registry. Bundle with a valid op
	// to verify per-op drop on modify (not whole-record).
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", testObjectId, "_base", false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, "unknown-prop"}, Payload: arena.NewString("nope")},
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propRating}, Payload: arena.NewNumberFloat64(3)},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Nil(t, rec.Get("_base", typeAny, "unknown-prop"), "unknown property dropped")
	assert.Equal(t, float64(3), rec.GetFloat64("_base", typeAny, propRating))
}

func TestSystemPropertiesHandler_CreatePerOpDrop(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	// Per-op drop on create (inbound read-tolerance): the kind-mismatched
	// op (string into a number field) is dropped, the valid op lands, and
	// the record is created.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, "_base", true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("ok")},
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propRating}, Payload: arena.NewString("not-a-number")},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "ok", rec.GetString("_base", typeAny, propName), "valid op landed")
	assert.Nil(t, rec.Get("_base", typeAny, propRating), "kind-mismatched op dropped")
}

func TestSystemPropertiesHandler_CreateSkippedWhenAllOpsDrop(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	// Every op invalid → all drop. The record still materializes with
	// only the auto-stamped fields (author/createdAt/spaceId are absent
	// here since the change carries no envelope), so assert no property
	// value landed under the type namespace.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, "_base", true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propRating}, Payload: arena.NewString("not-a-number")},
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, "unknown-prop"}, Payload: arena.NewString("x")},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	if rec != nil {
		assert.Nil(t, rec.Get("_base", typeAny, propRating), "mismatched op dropped")
		assert.Nil(t, rec.Get("_base", typeAny, "unknown-prop"), "unknown-prop op dropped")
	}
}

// ----------------------------------------------------------------------------
// $unset has no kind to check — should always pass given a known property
// ----------------------------------------------------------------------------

func TestSystemPropertiesHandler_UnsetPasses(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, "_base", true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("hi")},
	)))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", testObjectId, "_base", false,
		crdt.Op{Type: crdt.OpUnset, Path: []string{typeAny, propName}},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Nil(t, rec.Get("_base", typeAny, propName), "unset cleared the field")
}

// ----------------------------------------------------------------------------
// Nil Registry → passthrough (bring-up mode before type system is wired)
// ----------------------------------------------------------------------------

func TestSystemPropertiesHandler_NilRegistryPasses(t *testing.T) {
	ctrl := newPropsController(t, nil)
	arena := &anyenc.Arena{}

	// No Registry → no kind check. Even gibberish (typeId, propId) lands.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, "_base", true,
		crdt.Op{Type: crdt.OpSet, Path: []string{"not-a-type", "not-a-prop"}, Payload: arena.NewString("anything")},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "anything", rec.GetString("_base", "not-a-type", "not-a-prop"))
}

// ----------------------------------------------------------------------------
// PreValidate — strict local write-time validation (LocalPreValidator)
// ----------------------------------------------------------------------------

const propUserT = "userT" // a non-universal user type for membership tests

func preflightRegistry() *types.StubRegistry {
	r := defaultRegistry() // any: p-name(string), p-rating(number), p-tags(array)
	r.Set(typeAny, "types", schema.KindArray) // any.types is a real built-in prop
	r.Set(propUserT, "p1", schema.KindString)
	return r
}

// beforeWithTypes builds a record pre-state carrying any.types.
func beforeWithTypes(arena *anyenc.Arena, typeIds ...string) *anyenc.Value {
	rec := arena.NewObject()
	anyNs := arena.NewObject()
	arr := arena.NewArray()
	for i, t := range typeIds {
		arr.SetArrayItem(i, arena.NewString(t))
	}
	anyNs.Set("types", arr)
	rec.Set("any", anyNs)
	return rec
}

// singlePathChange wraps one single-path op into a change.
func singlePathChange(opType crdt.OpType, path []string, payload *anyenc.Value) *crdt.Change {
	return &crdt.Change{
		ObjectId: testObjectId, Dataset: properties.Dataset, DataVersion: testDataVer,
		Records: []crdt.RecordChange{{Id: testObjectId, Upsert: true,
			Ops: []crdt.Op{{Type: opType, Path: path, Payload: payload}}}},
	}
}

// multiFieldChange wraps one multi-field $set op (dotted keys) into a change.
func multiFieldChange(payload *anyenc.Value) *crdt.Change {
	return &crdt.Change{
		ObjectId: testObjectId, Dataset: properties.Dataset, DataVersion: testDataVer,
		Records: []crdt.RecordChange{{Id: testObjectId, Upsert: true,
			Ops: []crdt.Op{{Type: crdt.OpSet, Payload: payload}}}},
	}
}

func TestPreValidate_GoodWrites(t *testing.T) {
	h := properties.New(preflightRegistry())
	a := &anyenc.Arena{}

	// Universal `any` namespace — always implemented.
	require.NoError(t, h.PreValidate(
		singlePathChange(crdt.OpSet, []string{typeAny, propName}, a.NewString("x")), nil))

	// User type present in any.types, valid prop + kind.
	require.NoError(t, h.PreValidate(
		singlePathChange(crdt.OpSet, []string{propUserT, "p1"}, a.NewString("v")),
		beforeWithTypes(a, propUserT)))
}

func TestPreValidate_UnknownProperty(t *testing.T) {
	h := properties.New(preflightRegistry())
	a := &anyenc.Arena{}
	err := h.PreValidate(
		singlePathChange(crdt.OpSet, []string{typeAny, "ghost"}, a.NewString("x")), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, crdt.ErrValidation)
	var ve *properties.ValidationError
	require.True(t, errors.As(err, &ve))
	assert.Equal(t, properties.ReasonUnknownProperty, ve.Reason)
}

func TestPreValidate_KindMismatch(t *testing.T) {
	h := properties.New(preflightRegistry())
	a := &anyenc.Arena{}
	err := h.PreValidate(
		singlePathChange(crdt.OpSet, []string{typeAny, propRating}, a.NewString("not-a-number")), nil)
	require.ErrorIs(t, err, crdt.ErrValidation)
	var ve *properties.ValidationError
	require.True(t, errors.As(err, &ve))
	assert.Equal(t, properties.ReasonKindMismatch, ve.Reason)
	assert.Equal(t, schema.KindNumber, ve.Expected)
	assert.Equal(t, schema.KindString, ve.Got)
}

func TestPreValidate_TypeNotImplemented(t *testing.T) {
	h := properties.New(preflightRegistry())
	a := &anyenc.Arena{}
	// userT is known to the registry but NOT in the object's any.types.
	err := h.PreValidate(
		singlePathChange(crdt.OpSet, []string{propUserT, "p1"}, a.NewString("v")), nil)
	require.ErrorIs(t, err, crdt.ErrValidation)
	var ve *properties.ValidationError
	require.True(t, errors.As(err, &ve))
	assert.Equal(t, properties.ReasonTypeNotImplemented, ve.Reason)
}

func TestPreValidate_TypeUnknown(t *testing.T) {
	h := properties.New(preflightRegistry())
	a := &anyenc.Arena{}
	// ghostT is in any.types (member) but not in the registry.
	err := h.PreValidate(
		singlePathChange(crdt.OpSet, []string{"ghostT", "x"}, a.NewString("v")),
		beforeWithTypes(a, "ghostT"))
	require.ErrorIs(t, err, crdt.ErrValidation)
	var ve *properties.ValidationError
	require.True(t, errors.As(err, &ve))
	assert.Equal(t, properties.ReasonTypeUnknown, ve.Reason)
}

func TestPreValidate_AttachAndWriteInSameChange(t *testing.T) {
	h := properties.New(preflightRegistry())
	a := &anyenc.Arena{}
	// Multi-field $set that both adds userT to any.types and writes its prop.
	payload := a.NewObject()
	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewString(propUserT))
	payload.Set("any.types", arr)
	payload.Set(propUserT+".p1", a.NewString("v"))
	require.NoError(t, h.PreValidate(multiFieldChange(payload), nil))
}

func TestPreValidate_InvalidPath(t *testing.T) {
	h := properties.New(preflightRegistry())
	a := &anyenc.Arena{}
	err := h.PreValidate(
		singlePathChange(crdt.OpSet, []string{typeAny}, a.NewString("x")), nil)
	require.ErrorIs(t, err, crdt.ErrValidation)
	var ve *properties.ValidationError
	require.True(t, errors.As(err, &ve))
	assert.Equal(t, properties.ReasonInvalidPath, ve.Reason)
}

func TestPreValidate_UnsetAlwaysValid(t *testing.T) {
	h := properties.New(preflightRegistry())
	// $unset of an unknown property under an unimplemented type is a no-op.
	require.NoError(t, h.PreValidate(
		singlePathChange(crdt.OpUnset, []string{"whatever", "ghost"}, nil), nil))
}

func TestPreValidate_NilRegistryPasses(t *testing.T) {
	h := properties.New(nil)
	a := &anyenc.Arena{}
	require.NoError(t, h.PreValidate(
		singlePathChange(crdt.OpSet, []string{"x", "y"}, a.NewString("z")), nil))
}

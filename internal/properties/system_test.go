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
	propName     = "p-name"   // string
	propRating   = "p-rating" // number
	propTags     = "p-tags"   // array
)

func newPropsController(t *testing.T, reg types.Registry) *crdt.Controller {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := anystore.Open(context.Background(), dbPath, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctrl, err := crdt.NewController(context.Background(), testObjectId, db,
		crdt.HandlerReg{Name: properties.Dataset, Handler: properties.New(reg), Schema: schema.Dataset{Dynamic: true}},
	)
	require.NoError(t, err)
	return ctrl
}

func makeChange(versionId crdt.VersionId, recId string, upsert bool, ops ...crdt.Op) crdt.Change {
	return crdt.Change{
		ObjectId:    testObjectId,
		Dataset:     properties.Dataset,
		ChangeId:    "ch-" + string(versionId),
		VersionId:   versionId,
		DataVersion: testDataVer,
		Records: []crdt.RecordChange{
			{Id: recId, Upsert: upsert, Ops: ops},
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
// Basic apply — values land at their normal root paths
// ----------------------------------------------------------------------------

func TestSystemPropertiesHandler_RootApply(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("Hello")},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)

	// Top-level _ver.id is the creation marker.
	assert.Equal(t, "v1", rec.GetString("_ver", "id"))

	assert.Equal(t, "Hello", rec.GetString(typeAny, propName))
	assert.Equal(t, crdt.VersionId("v1"),
		crdt.GetRecordVersion(rec, typeAny, propName))
}

// ----------------------------------------------------------------------------
// Scope enforcement — the DAG route only writes synced-scope props
// ----------------------------------------------------------------------------

const (
	propRead = "p-read" // account-scoped boolean
	propPin  = "p-pin"  // local-scoped boolean
)

func scopedRegistry() *types.StubRegistry {
	r := defaultRegistry()
	r.SetScoped(typeAny, propRead, schema.KindBoolean, schema.ScopeAccount)
	r.SetScoped(typeAny, propPin, schema.KindBoolean, schema.ScopeLocal)
	return r
}

// An inbound DAG change addressing an account- or local-scoped propId
// is dropped per-op (the convergent guard keeping version domains
// path-disjoint); sibling synced ops in the same change still land.
func TestSystemPropertiesHandler_ScopeMismatchDrops(t *testing.T) {
	ctrl := newPropsController(t, scopedRegistry())
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("ok")},
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propRead}, Payload: arena.NewTrue()},
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propPin}, Payload: arena.NewTrue()},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "ok", rec.GetString(typeAny, propName), "synced op landed")
	assert.Nil(t, rec.Get(typeAny, propRead), "account-scoped op dropped on the DAG route")
	assert.Nil(t, rec.Get(typeAny, propPin), "local-scoped op dropped on the DAG route")
}

// Derived built-ins are likewise unwritable by input ops.
func TestSystemPropertiesHandler_DerivedScopeDrops(t *testing.T) {
	r := defaultRegistry()
	r.SetScoped(typeAny, "createdAt", schema.KindNumber, schema.ScopeDerived)
	ctrl := newPropsController(t, r)
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, "createdAt"}, Payload: arena.NewNumberFloat64(1)},
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("ok")},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Nil(t, rec.Get(typeAny, "createdAt"), "derived-scoped op dropped")
	assert.Equal(t, "ok", rec.GetString(typeAny, propName))
}

// ----------------------------------------------------------------------------
// Kind validation
// ----------------------------------------------------------------------------

func TestSystemPropertiesHandler_KindMismatchDrops(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	// Create with a valid name.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("ok")},
	)))

	// Modify: bundle a kind-mismatched op (number into a string field)
	// with a valid op for `rating`. The bad op drops; the good one lands.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", testObjectId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewNumberFloat64(42)},
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propRating}, Payload: arena.NewNumberFloat64(7.5)},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "ok", rec.GetString(typeAny, propName), "string name survives kind-mismatch attempt")
	assert.Equal(t, 7.5, rec.GetFloat64(typeAny, propRating), "number rating landed")
}

func TestSystemPropertiesHandler_UnknownPropertyDrops(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("ok")},
	)))

	// `unknown-prop` is not in the registry. Bundle with a valid op
	// to verify per-op drop on modify (not whole-record).
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", testObjectId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, "unknown-prop"}, Payload: arena.NewString("nope")},
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propRating}, Payload: arena.NewNumberFloat64(3)},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Nil(t, rec.Get(typeAny, "unknown-prop"), "unknown property dropped")
	assert.Equal(t, float64(3), rec.GetFloat64(typeAny, propRating))
}

func TestSystemPropertiesHandler_CreatePerOpDrop(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	// Per-op drop on create (inbound read-tolerance): the kind-mismatched
	// op (string into a number field) is dropped, the valid op lands, and
	// the record is created.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("ok")},
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propRating}, Payload: arena.NewString("not-a-number")},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "ok", rec.GetString(typeAny, propName), "valid op landed")
	assert.Nil(t, rec.Get(typeAny, propRating), "kind-mismatched op dropped")
}

func TestSystemPropertiesHandler_CreateSkippedWhenAllOpsDrop(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	// Every op invalid → all drop. The record still materializes with
	// only the auto-stamped fields (author/createdAt/spaceId are absent
	// here since the change carries no envelope), so assert no property
	// value landed under the type namespace.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propRating}, Payload: arena.NewString("not-a-number")},
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, "unknown-prop"}, Payload: arena.NewString("x")},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	if rec != nil {
		assert.Nil(t, rec.Get(typeAny, propRating), "mismatched op dropped")
		assert.Nil(t, rec.Get(typeAny, "unknown-prop"), "unknown-prop op dropped")
	}
}

// ----------------------------------------------------------------------------
// $unset has no kind to check — should always pass given a known property
// ----------------------------------------------------------------------------

func TestSystemPropertiesHandler_UnsetPasses(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("hi")},
	)))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", testObjectId, false,
		crdt.Op{Type: crdt.OpUnset, Path: []string{typeAny, propName}},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Nil(t, rec.Get(typeAny, propName), "unset cleared the field")
}

// ----------------------------------------------------------------------------
// Nil Registry → passthrough (bring-up mode before type system is wired)
// ----------------------------------------------------------------------------

func TestSystemPropertiesHandler_NilRegistryPasses(t *testing.T) {
	ctrl := newPropsController(t, nil)
	arena := &anyenc.Arena{}

	// No Registry → no kind check. Even gibberish (typeId, propId) lands.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{"not-a-type", "not-a-prop"}, Payload: arena.NewString("anything")},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, "anything", rec.GetString("not-a-type", "not-a-prop"))
}

// ----------------------------------------------------------------------------
// PreValidate — strict local write-time validation (LocalPreValidator)
// ----------------------------------------------------------------------------

const propUserT = "userT" // a non-universal user type for membership tests

func preflightRegistry() *types.StubRegistry {
	r := defaultRegistry()                    // any: p-name(string), p-rating(number), p-tags(array)
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

func TestPreValidate_ScopeMismatch(t *testing.T) {
	r := preflightRegistry()
	r.SetScoped(typeAny, propRead, schema.KindBoolean, schema.ScopeAccount)
	h := properties.New(r)
	a := &anyenc.Arena{}

	err := h.PreValidate(
		singlePathChange(crdt.OpSet, []string{typeAny, propRead}, a.NewTrue()), nil)
	require.ErrorIs(t, err, crdt.ErrValidation)
	require.ErrorIs(t, err, properties.ErrScopeMismatch)
	var ve *properties.ValidationError
	require.True(t, errors.As(err, &ve))
	assert.Equal(t, properties.ReasonScopeMismatch, ve.Reason)
	assert.Equal(t, schema.ScopeAccount, ve.DeclaredScope)
	assert.Equal(t, schema.ScopeSynced, ve.WriteRoute)
}

// ----------------------------------------------------------------------------
// modifiedAt — derived per-change stamp (create seeds it, modify bumps it)
// ----------------------------------------------------------------------------

// makeChangeAt is makeChange plus the per-change envelope Timestamp the
// modifiedAt stamp derives from.
func makeChangeAt(versionId crdt.VersionId, ts int64, recId string, upsert bool, ops ...crdt.Op) crdt.Change {
	ch := makeChange(versionId, recId, upsert, ops...)
	ch.Timestamp = ts
	return ch
}

func TestSystemPropertiesHandler_ModifiedAtSeededOnCreate(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChangeAt(
		"v1", 100, testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("hi")},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, float64(100), rec.GetFloat64("modifiedAt"), "create seeds modifiedAt from the change Timestamp")
}

func TestSystemPropertiesHandler_ModifiedAtBumpsOnModify(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChangeAt(
		"v1", 100, testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("hi")},
	)))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChangeAt(
		"v2", 200, testObjectId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propRating}, Payload: arena.NewNumberFloat64(5)},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, float64(200), rec.GetFloat64("modifiedAt"), "valid modify bumps modifiedAt")
}

// ----------------------------------------------------------------------------
// createdAt — root header when present, first-touch fallback for derived trees
// ----------------------------------------------------------------------------

func TestSystemPropertiesHandler_CreatedAtFromRootHeader(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	ch := makeChangeAt("v1", 100, testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("hi")},
	)
	ch.ObjectCreatedAt = 50
	require.NoError(t, ctrl.ApplyChange(context.Background(), ch))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, float64(50), rec.GetFloat64("createdAt"), "root header wins over the change envelope")
}

// A derived tree's root is deterministic and timestamp-less, so
// ObjectCreatedAt is 0 for every change — createdAt falls back to the
// first-touch change's envelope Timestamp instead of staying unstamped.
func TestSystemPropertiesHandler_CreatedAtDerivedFallback(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChangeAt(
		"v1", 100, testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("hi")},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, float64(100), rec.GetFloat64("createdAt"), "derived root: createdAt = first-touch change time")
}

// Out-of-order delivery: an older change (lower VersionId) arriving after
// a newer one must NOT regress modifiedAt — the stamp is LWW-gated on the
// change's VersionId like any other field write.
func TestSystemPropertiesHandler_ModifiedAtOutOfOrderConverges(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChangeAt(
		"v1", 100, testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("hi")},
	)))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChangeAt(
		"v3", 300, testObjectId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propRating}, Payload: arena.NewNumberFloat64(5)},
	)))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChangeAt(
		"v2", 200, testObjectId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("later-but-older")},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, float64(300), rec.GetFloat64("modifiedAt"), "older change must not regress modifiedAt")
}

// A change whose every op fails validation stamps nothing: the stamp
// runs after the per-op validation gate.
func TestSystemPropertiesHandler_ModifiedAtNotBumpedWhenAllOpsRejected(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChangeAt(
		"v1", 100, testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("hi")},
	)))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChangeAt(
		"v2", 200, testObjectId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propRating}, Payload: arena.NewString("not-a-number")},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Equal(t, float64(100), rec.GetFloat64("modifiedAt"), "fully-rejected change must not bump modifiedAt")
}

// A multi-op RecordChange stamps modifiedAt exactly once (DeriveOnce),
// so the wire projection carries one derived op, not one per user op.
func TestSystemPropertiesHandler_ModifiedAtSingleStampPerRecord(t *testing.T) {
	ctrl := newPropsController(t, defaultRegistry())
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChangeAt(
		"v1", 100, testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("hi")},
	)))

	res, err := ctrl.ApplyChangeWithResult(context.Background(), makeChangeAt(
		"v2", 200, testObjectId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("bye")},
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propRating}, Payload: arena.NewNumberFloat64(5)},
	))
	require.NoError(t, err)

	stamps := 0
	for _, recOps := range res.DerivedOps {
		for _, op := range recOps {
			if len(op.Path) == 1 && op.Path[0] == "modifiedAt" {
				stamps++
			}
		}
	}
	assert.Equal(t, 1, stamps, "multi-op record must carry exactly one modifiedAt derived op")
}

// Client writes addressing any.modifiedAt are dropped by the scope gate,
// same as the other derived built-ins.
func TestSystemPropertiesHandler_ModifiedAtClientWriteDropped(t *testing.T) {
	r := defaultRegistry()
	r.SetScoped(typeAny, "modifiedAt", schema.KindNumber, schema.ScopeDerived)
	ctrl := newPropsController(t, r)
	arena := &anyenc.Arena{}

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChangeAt(
		"v1", 100, testObjectId, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, propName}, Payload: arena.NewString("hi")},
		crdt.Op{Type: crdt.OpSet, Path: []string{typeAny, "modifiedAt"}, Payload: arena.NewNumberFloat64(9999)},
	)))

	rec := ctrl.Get(context.Background(), properties.Dataset, testObjectId)
	require.NotNil(t, rec)
	assert.Nil(t, rec.Get(typeAny, "modifiedAt"), "derived-scoped client op dropped")
	assert.Equal(t, float64(100), rec.GetFloat64("modifiedAt"), "handler stamp still lands at row root")
}

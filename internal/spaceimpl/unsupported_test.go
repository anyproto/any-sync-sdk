package spaceimpl

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

var errorType = reflect.TypeOf((*error)(nil)).Elem()

// callZero invokes method m on v with zero arguments (a background
// context for context.Context parameters) and returns its results.
func callZero(v reflect.Value, m reflect.Method) []reflect.Value {
	args := make([]reflect.Value, 0, m.Type.NumIn()-1)
	for i := 1; i < m.Type.NumIn(); i++ {
		in := m.Type.In(i)
		if in == reflect.TypeOf((*context.Context)(nil)).Elem() {
			args = append(args, reflect.ValueOf(context.Background()))
			continue
		}
		args = append(args, reflect.Zero(in))
	}
	return v.Method(m.Index).Call(args)
}

// Every method of every stub refuses with space.ErrUnsupported; the
// few non-error-shaped methods return a usable zero (a no-op cancel
// func, a Query whose terminals refuse) instead of nil.
func TestUnsupportedStubs_RefuseEverything(t *testing.T) {
	stubs := []any{
		unsupportedProperties{}, unsupportedACL{}, unsupportedMembers{},
		unsupportedFiles{}, unsupportedHistory{}, unsupportedPayloads{},
		unsupportedPubSub{}, unsupportedReadState{}, unsupportedChanges{},
		unsupportedQuery{err: errUnsupported("q")},
	}
	for _, stub := range stubs {
		v := reflect.ValueOf(stub)
		typ := v.Type()
		for i := 0; i < typ.NumMethod(); i++ {
			m := typ.Method(i)
			name := typ.Name() + "." + m.Name
			out := callZero(v, m)
			require.NotEmpty(t, out, name)
			last := out[len(out)-1]
			if last.Type().Implements(errorType) {
				err, _ := last.Interface().(error)
				assert.True(t, errors.Is(err, space.ErrUnsupported), "%s: %v", name, err)
				continue
			}
			// Chain methods return the query itself; Subscribe-style
			// methods return a cancel func; Query() returns a query.
			switch last.Kind() {
			case reflect.Func:
				require.False(t, last.IsNil(), "%s: nil cancel func", name)
				last.Call(nil)
			case reflect.Interface, reflect.Struct:
				require.False(t, last.Kind() == reflect.Interface && last.IsNil(), "%s: nil result", name)
			default:
				t.Errorf("%s: unexpected non-error result %s", name, last.Type())
			}
		}
	}
}

// The overriding wrappers refuse exactly the lifecycle methods; the
// promoted reads stay reachable through the embedded pointer.
func TestTechWrappers_RefuseLifecycle(t *testing.T) {
	ctx := context.Background()
	o := techObjects{}
	_, err := o.Create(ctx, space.CreateObjectOpts{})
	assert.ErrorIs(t, err, space.ErrUnsupported)
	_, err = o.Derive(ctx, space.DeriveObjectOpts{})
	assert.ErrorIs(t, err, space.ErrUnsupported)
	// Delete is conditional now — refused for non-bundle-roots, allowed
	// for created bundle roots (uninstall). Needs a store to decide;
	// covered by the tech bundles e2e.

	ty := techTypes{}
	_, err = ty.Create(ctx, space.TypeCreateParams{})
	assert.ErrorIs(t, err, space.ErrUnsupported)
	assert.ErrorIs(t, ty.Delete(ctx, "t"), space.ErrUnsupported)
	_, err = ty.AddProperty(ctx, "t", space.PropertyDraft{})
	assert.ErrorIs(t, err, space.ErrUnsupported)
	assert.ErrorIs(t, ty.RemoveProperty(ctx, "t", "p"), space.ErrUnsupported)
	assert.ErrorIs(t, ty.PatchProperty(ctx, "t", "p", space.PropertyPatch{}), space.ErrUnsupported)

	assert.ErrorIs(t, errUnsupported("x"), space.ErrUnsupported)
}

// The tech handle's generic write surface reaches catalog-owned
// (bundle) datasets only: built-ins, system datasets and unknown
// names refuse before any store write.
func TestTechHandle_WriteFence(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "tech.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	store := spaceobjects.NewStoreWithConfig(spaceobjects.StoreConfig{
		DB: db, SpaceId: "tech",
		SystemDatasets: techspace.SystemDatasets(), DisableHistory: true,
	})
	t.Cleanup(func() { _ = store.Close() })
	ts := &techSpace{inner: &spaceImpl{id: "tech", store: store}}

	assert.ErrorIs(t, ts.SetMetadata(ctx, space.SetMetadataRequest{}), space.ErrUnsupported)
	for _, ds := range []string{"spaces", "profile", "devices", "objects", "datasets", "shortIds", "bundles", "payloads"} {
		_, err := ts.Modify(ctx, space.ModifyBatch{ObjectId: "o", Dataset: ds})
		assert.ErrorIs(t, err, space.ErrUnsupported, ds)
		_, err = ts.Upsert(ctx, space.UpsertBatch{ObjectId: "o", Dataset: ds})
		assert.ErrorIs(t, err, space.ErrUnsupported, ds)
		_, err = ts.Delete(ctx, space.DeleteBatch{ObjectId: "o", Dataset: ds})
		assert.ErrorIs(t, err, space.ErrUnsupported, ds)
		_, err = ts.ModifyMany(ctx, []space.ModifyBatch{{ObjectId: "o", Dataset: ds}})
		assert.ErrorIs(t, err, space.ErrUnsupported, ds)
	}
	// The tech index object accepts no generic writes, whatever the
	// dataset name.
	ts.inner.techIndexId = "idx"
	_, err = ts.Delete(ctx, space.DeleteBatch{ObjectId: "idx", Dataset: "entries"})
	assert.ErrorIs(t, err, space.ErrUnsupported)
	_, err = ts.Modify(ctx, space.ModifyBatch{ObjectId: "idx", Dataset: "entries"})
	assert.ErrorIs(t, err, space.ErrUnsupported)
	// A dataset the store never heard of passes through and fails as
	// unknown, not as unsupported.
	assert.NoError(t, ts.checkWrite("o", "nope"))
	// Dataset mutators need a bundle root; an object without a row is
	// not one.
	tt := techTypes{inner: newTypesAPI(ts.inner), t: ts}
	_, err = tt.AddDataset(ctx, "not-a-root", "p", space.DatasetDraft{Key: "x"})
	assert.ErrorIs(t, err, space.ErrUnsupported)
	_, err = tt.AddPart(ctx, "not-a-root", space.PartDraft{Key: "x"})
	assert.ErrorIs(t, err, space.ErrUnsupported)
	assert.ErrorIs(t, tt.RemoveDataset(ctx, "not-a-root", "d"), space.ErrUnsupported)
}

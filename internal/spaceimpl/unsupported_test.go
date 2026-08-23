package spaceimpl

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	assert.ErrorIs(t, o.Delete(ctx, "x"), space.ErrUnsupported)

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

// Tech-handle methods that refuse before touching the store.
func TestTechHandle_RefusesBeforeStore(t *testing.T) {
	ctx := context.Background()
	s := &spaceImpl{tech: true}
	assert.ErrorIs(t, s.SetMetadata(ctx, space.SetMetadataRequest{}), space.ErrUnsupported)
	b := newBundlesAPI(s)
	assert.ErrorIs(t, b.ResolveLoser(ctx, "b", "r"), space.ErrUnsupported)
	for _, ds := range []string{"spaces", "profile", "inboxCursor", "identities", "devices", "account_values", "bundles", "payloads"} {
		assert.Error(t, s.checkPublicDataset(ds), ds)
	}
	assert.NoError(t, s.checkPublicDataset("entries"))
	regular := &spaceImpl{}
	assert.NoError(t, regular.checkPublicDataset("spaces"), "system names are fenced on the tech handle only")
}

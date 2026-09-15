package types_test

import (
	"context"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
)

func staticOverlay() map[string]map[string]types.PropInfo {
	return map[string]map[string]types.PropInfo{
		"any": {
			"name":        {Id: "name", Name: "Name", Kind: schema.KindString},
			"collections": {Id: "collections", Name: "Collections", Kind: schema.KindArray},
		},
		// A registered type that owns only datasets: known, zero props.
		"editor": {},
	}
}

func TestLiveRegistry_StaticOverlayResolves(t *testing.T) {
	r := types.NewLiveRegistry(nil, staticOverlay()) // nil db: overlay-only

	k, ok := r.LookupKind("any", "name")
	require.True(t, ok)
	assert.Equal(t, schema.KindString, k)

	_, ok = r.LookupKind("any", "ghost")
	assert.False(t, ok, "unknown prop of a known type")

	assert.True(t, r.TypeKnown("any"))
	assert.True(t, r.TypeKnown("editor"), "registered type with zero props is still known")
	assert.False(t, r.TypeKnown("user"), "no overlay entry, nil db")

	props, ok := r.PropsOf("any")
	require.True(t, ok)
	assert.Len(t, props, 2)

	props, ok = r.PropsOf("editor")
	require.True(t, ok)
	assert.Empty(t, props)

	_, ok = r.PropsOf("user")
	assert.False(t, ok)
}

func TestLiveRegistry_UserTypeFallbackToDefs(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "t.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Materialise a user type's `<typeId>_properties` defs collection,
	// the same shape typetype.PropertyHandler writes.
	const userType = "userT"
	coll, err := db.Collection(ctx, userType+"_properties")
	require.NoError(t, err)
	a := &anyenc.Arena{}
	doc := a.NewObject()
	doc.Set("id", a.NewString("p1"))
	doc.Set("name", a.NewString("Score"))
	doc.Set("kind", a.NewString("number"))
	require.NoError(t, coll.Insert(ctx, doc))

	r := types.NewLiveRegistry(db, staticOverlay())

	// Built-in still resolves from the overlay.
	k, ok := r.LookupKind("any", "name")
	require.True(t, ok)
	assert.Equal(t, schema.KindString, k)

	// User type resolves from the defs collection.
	k, ok = r.LookupKind(userType, "p1")
	require.True(t, ok)
	assert.Equal(t, schema.KindNumber, k)

	_, ok = r.LookupKind(userType, "missing")
	assert.False(t, ok)

	assert.True(t, r.TypeKnown(userType))
	assert.False(t, r.TypeKnown("neverWritten"))

	props, ok := r.PropsOf(userType)
	require.True(t, ok)
	require.Len(t, props, 1)
	assert.Equal(t, "p1", props[0].Id)
	assert.Equal(t, "Score", props[0].Name)
	assert.Equal(t, schema.KindNumber, props[0].Kind)
}

package spaceobjects

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	collectiontype "github.com/anyproto/any-sync-sdk/internal/types/collection"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
)

// TestBuildStaticSchema_MetaTypeNamespace pins both halves of the xkey
// move. Without the `type` entry every type Create fails: the registry
// resolves no type for the namespace and PreValidate rejects the whole
// change. The meta-collection's namespace is there for the same reason,
// and `any` carries the two membership fields.
func TestBuildStaticSchema_MetaTypeNamespace(t *testing.T) {
	static := buildStaticSchema(nil, nil, nil)

	for _, ns := range []string{typetype.TypeId, collectiontype.TypeId} {
		meta, ok := static[ns]
		require.True(t, ok, "%s namespace must be registered", ns)
		xkey, ok := meta[typetype.FieldXKeyProp]
		require.True(t, ok, "%s must declare xkey", ns)
		assert.Equal(t, schema.KindString, xkey.Kind)
		assert.Equal(t, schema.ScopeSynced, xkey.Scope)
	}

	_, stale := static[anytype.TypeId][typetype.FieldXKeyProp]
	assert.False(t, stale, "xkey must no longer be declared on `any`")

	anyProps := static[anytype.TypeId]
	assert.Equal(t, schema.KindString, anyProps[anytype.FieldType].Kind, "one type, a scalar slot")
	assert.Equal(t, schema.KindArray, anyProps[anytype.FieldCollections].Kind)
	_, gone := anyProps["types"]
	assert.False(t, gone, "the type list is replaced by type + collections")
}

// TestBuildStaticSchema_ExternalCollections registers a collection's
// properties under its own namespace, next to the registered types'.
func TestBuildStaticSchema_ExternalCollections(t *testing.T) {
	static := buildStaticSchema(
		[]handler.Type{{Id: "movie", Properties: []handler.PropertyDecl{{Id: "year", Kind: handler.PropertyKindNumber}}}},
		[]handler.Collection{{Id: "shelf", Properties: []handler.PropertyDecl{{Id: "pos", Kind: handler.PropertyKindString}}}},
		nil)

	assert.Equal(t, schema.KindNumber, static["movie"]["year"].Kind)
	assert.Equal(t, schema.KindString, static["shelf"]["pos"].Kind)
}

// TestValidateExternalTypes_ReservedIds keeps a caller-registered type
// from taking over a built-in namespace. The ext-type loop in
// buildStaticSchema replaces by id, so a `type` registration would
// silently break every Types().Create carrying an XKey.
func TestValidateExternalTypes_ReservedIds(t *testing.T) {
	for id := range ReservedTypeIds {
		err := ValidateExternalTypes([]handler.Type{{Id: id}})
		require.Error(t, err, "registering %q must fail", id)
		assert.Contains(t, err.Error(), "reserved")
	}
	require.NoError(t, ValidateExternalTypes([]handler.Type{{Id: "chat"}}))
}

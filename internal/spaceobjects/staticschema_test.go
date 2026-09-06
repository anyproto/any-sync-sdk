package spaceobjects

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
)

// TestBuildStaticSchema_MetaTypeNamespace pins both halves of the xkey
// move. Without the `type` entry every type Create fails: the registry
// resolves no type for the namespace and PreValidate rejects the whole
// change.
func TestBuildStaticSchema_MetaTypeNamespace(t *testing.T) {
	static := buildStaticSchema(nil, nil)

	meta, ok := static[typetype.TypeId]
	require.True(t, ok, "meta-type namespace must be registered")
	xkey, ok := meta[typetype.FieldXKeyProp]
	require.True(t, ok, "meta-type must declare xkey")
	assert.Equal(t, schema.KindString, xkey.Kind)
	assert.Equal(t, schema.ScopeSynced, xkey.Scope)

	_, stale := static[anytype.TypeId][typetype.FieldXKeyProp]
	assert.False(t, stale, "xkey must no longer be declared on `any`")
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

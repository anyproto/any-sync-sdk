package typetype_test

import (
	"context"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
)

// A head declaring deleteBy none needs no creator-stamped field: the
// fold is valid and the policy reaches the compiled declaration.
func TestCompileDatasetDefs_DeleteByNoneNeedsNoStamp(t *testing.T) {
	ctrl, db := newDefsController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}
	seedPart(t, ctrl, arena)

	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v1", "c1", "head-1", true,
		headPayload(arena, "entries", map[string]any{typetype.DefFieldDeleteBy: "none", typetype.DefFieldIdRule: "user"}))))
	require.NoError(t, ctrl.ApplyChange(ctx, defsChange("v2", "c2", "f-pos", true,
		fieldPayload(arena, "head-1", "pos", "string", map[string]any{typetype.DefFieldMutableBy: "any"}))))

	compiled, err := types.CompileDatasetDefs(ctx, db, testObjectId, nil)
	require.NoError(t, err)
	require.Len(t, compiled, 1)
	assert.False(t, compiled[0].Invalid, compiled[0].InvalidReason)
	assert.Equal(t, schema.DeleteByNone, compiled[0].Schema.DeleteBy)
	assert.Equal(t, schema.IdUser, compiled[0].Schema.IdRule)
}

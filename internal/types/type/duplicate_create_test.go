package typetype_test

import (
	"context"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
)

// Two devices creating the same property record while apart — a
// bundle-declared property under its deterministic id — reach a
// replica as one create and one creation-shaped modify. Both changes
// must leave their shortId row, or the author of the second one stamps
// data writes with a shortId this replica never learns.
func TestPropertyHandler_DuplicateCreateProjectsShortId(t *testing.T) {
	ctrl := newTypeController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	const propId = "prop-deterministic"
	payload := func() crdt.Op {
		return setMulti(arena, map[string]any{
			typetype.FieldKey:  "parentId",
			typetype.FieldXKey: "parentId",
			typetype.FieldKind: "string",
			typetype.FieldName: "Parent",
		})
	}
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v1", "change-A", propId, true, payload())))
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v2", "change-B", propId, true, payload())))

	// One record, unchanged by the duplicate.
	rec := ctrl.Get(ctx, typetype.DatasetPropertyDefs, propId)
	require.NotNil(t, rec)
	assert.Equal(t, "string", rec.GetString(typetype.FieldKind))
	assert.Equal(t, "Parent", rec.GetString(typetype.FieldName))
	assert.Nil(t, rec.Get(crdt.DeletedAtField))

	// Both changes' shortId rows exist, stamped with their own version.
	for changeId, ver := range map[string]string{"change-A": "v1", "change-B": "v2"} {
		row := ctrl.Get(ctx, typetype.ShortIdsDataset, crdt.DeriveRecordId(changeId))
		require.NotNil(t, row, "shortId row of %s", changeId)
		assert.Equal(t, propId, row.GetString(typetype.ShortIdFieldPropId))
		assert.Equal(t, "string", row.GetString(typetype.ShortIdFieldKind))
		assert.Equal(t, ver, row.GetString("_ver", "id"))
	}

	// An ordinary edit on the existing record projects nothing.
	edit := crdt.Op{Type: crdt.OpSet, Path: []string{typetype.FieldName}, Payload: arena.NewString("Parent page")}
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v3", "change-C", propId, false, edit)))
	assert.Nil(t, ctrl.Get(ctx, typetype.ShortIdsDataset, crdt.DeriveRecordId("change-C")))
	assert.Equal(t, "Parent page", ctrl.Get(ctx, typetype.DatasetPropertyDefs, propId).GetString(typetype.FieldName))
}

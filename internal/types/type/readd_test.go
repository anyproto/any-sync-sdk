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

// TestPropertyHandler_RemoveReaddMintsNewPropId proves the claim the
// §16 decision rests on: a property's kind is pinned to its propId for
// life, because "changing" a kind means remove + re-add, and re-add
// (a new changeId) mints a NEW propId. The old propId is dead forever;
// the live property carries the new kind. This is why a wrong-kind
// write to a live propId is impossible and stale values can only linger
// under dead propIds.
func TestPropertyHandler_RemoveReaddMintsNewPropId(t *testing.T) {
	ctx := context.Background()
	ctrl := newTypeController(t)
	a := &anyenc.Arena{}

	// 1. Add "year" as a string. Empty record id resolves to
	//    DeriveRecordId(changeId), which is the propId.
	const c1 = "ch-year-string"
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v1", c1, "", true,
		setMulti(a, map[string]any{typetype.FieldKey: "year", typetype.FieldKind: "string"}),
	)))
	p1 := crdt.DeriveRecordId(c1)
	rec := ctrl.Get(ctx, typetype.DatasetPropertyDefs, p1)
	require.NotNil(t, rec, "property landed under derived propId")
	assert.Equal(t, "string", rec.GetString(typetype.FieldKind))

	// 2. Remove it.
	const c2 = "ch-year-remove"
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v2", c2, p1, false,
		crdt.Op{Type: crdt.OpDelete},
	)))

	// 3. Re-add "year", now as a number. A new changeId → a new propId.
	const c3 = "ch-year-number"
	require.NoError(t, ctrl.ApplyChange(ctx, makeChange("v3", c3, "", true,
		setMulti(a, map[string]any{typetype.FieldKey: "year", typetype.FieldKind: "number"}),
	)))
	p3 := crdt.DeriveRecordId(c3)

	// The linchpin: re-add minted a different propId.
	assert.NotEqual(t, p1, p3, "re-add must mint a new propId, not reuse the dead one")

	// Only the re-added "year" is live, and it carries the new kind; the
	// old string propId is tombstoned (filtered from Records).
	var liveYear []string
	for _, r := range ctrl.Records(ctx, typetype.DatasetPropertyDefs) {
		if r.GetString(typetype.FieldKey) == "year" {
			liveYear = append(liveYear, r.GetString("id"))
		}
	}
	require.Equal(t, []string{p3}, liveYear, "exactly one live 'year', the re-added one")
	assert.Equal(t, "number", ctrl.Get(ctx, typetype.DatasetPropertyDefs, p3).GetString(typetype.FieldKind))

	// All three important changes minted distinct shortIds, all known
	// (the known-shortIds set is append-only — old versions stay known).
	s1, s2, s3 := crdt.DeriveRecordId(c1), crdt.DeriveRecordId(c2), crdt.DeriveRecordId(c3)
	for _, s := range []string{s1, s2, s3} {
		assert.NotNil(t, ctrl.Get(ctx, typetype.ShortIdsDataset, s), "shortId %s should be known", s)
	}
	assert.NotEqual(t, s1, s3, "re-add mints a new shortId")
}

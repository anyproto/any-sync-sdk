package spaceobjects

import (
	"context"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/types"
)

// gateStore opens a fresh DB-backed Store whose LiveRegistry reads the
// same DB, so we can land shortIds and watch the gate react.
func gateStore(t *testing.T) (context.Context, *Store) {
	t.Helper()
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "gate.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	store := NewStore(nil, db, nil, "spaceA", nil, nil)
	return ctx, store
}

// landShortId materialises a shortId row so KnownShortId(typeId, shortId)
// flips to true — the same effect a property-defs apply has.
func landShortId(t *testing.T, ctx context.Context, store *Store, typeId, shortId string) {
	t.Helper()
	coll, err := store.db.Collection(ctx, typeId+"_shortIds")
	require.NoError(t, err)
	a := &anyenc.Arena{}
	doc := a.NewObject()
	doc.Set("id", a.NewString(shortId))
	require.NoError(t, coll.UpsertOne(ctx, doc))
}

func countDetached(t *testing.T, ctx context.Context, store *Store) []DetachedRow {
	t.Helper()
	var rows []DetachedRow
	require.NoError(t, store.IterDetached(ctx, func(r DetachedRow) bool {
		rows = append(rows, r)
		return true
	}))
	return rows
}

func gateChange(dataVersion string) *crdt.Change {
	return &crdt.Change{
		SpaceId:     "spaceA",
		ObjectId:    "obj-X",
		ChangeId:    "ch-" + dataVersion,
		VersionId:   crdt.VersionId("v1"),
		AddSeq:      3,
		Timestamp:   100,
		DataVersion: dataVersion,
	}
}

// TestGate_Decision drives gateFor directly — the park-vs-pass logic
// that TestGate_ParkAndDrain bypasses by calling Park/Drain by hand.
func TestGate_Decision(t *testing.T) {
	t.Run("unknown shortId parks the change", func(t *testing.T) {
		ctx, store := gateStore(t)
		gate := store.gateFor("obj-X")
		ok, err := gate(ctx, gateChange("typeT:sMissing"), []byte("payload"))
		require.NoError(t, err)
		assert.False(t, ok, "gate must not pass an unknown-version change")

		rows := countDetached(t, ctx, store)
		require.Len(t, rows, 1)
		assert.Equal(t, "ch-typeT:sMissing", rows[0].ChangeId)
		require.Len(t, rows[0].Pending, 1)
		assert.Equal(t, types.DataVersionPair{TypeId: "typeT", ShortId: "sMissing"}, rows[0].Pending[0])
	})

	t.Run("known shortId passes, no park", func(t *testing.T) {
		ctx, store := gateStore(t)
		landShortId(t, ctx, store, "typeT", "sA")
		gate := store.gateFor("obj-X")
		ok, err := gate(ctx, gateChange("typeT:sA"), []byte("payload"))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Empty(t, countDetached(t, ctx, store))
	})

	t.Run("legacy/unparseable DataVersion passes through", func(t *testing.T) {
		ctx, store := gateStore(t)
		gate := store.gateFor("obj-X")
		// Handler-version strings (no colon) parse-fail → fail-open.
		ok, err := gate(ctx, gateChange("systemPropertyHandler-v1"), []byte("payload"))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Empty(t, countDetached(t, ctx, store))
	})

	t.Run("empty DataVersion passes the gate", func(t *testing.T) {
		// The empty-DataVersion reject lives in ApplyChange, not the gate;
		// the gate sees zero pairs and lets it through.
		ctx, store := gateStore(t)
		gate := store.gateFor("obj-X")
		ok, err := gate(ctx, gateChange(""), []byte("payload"))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Empty(t, countDetached(t, ctx, store))
	})

	t.Run("multi-pair parks with only the missing pair pending", func(t *testing.T) {
		ctx, store := gateStore(t)
		landShortId(t, ctx, store, "typeT", "sA") // known
		gate := store.gateFor("obj-X")
		ok, err := gate(ctx, gateChange("typeT:sA;typeU:sMissing"), []byte("payload"))
		require.NoError(t, err)
		assert.False(t, ok)

		rows := countDetached(t, ctx, store)
		require.Len(t, rows, 1)
		require.Len(t, rows[0].Pending, 1, "only the unsatisfied pair is recorded")
		assert.Equal(t, types.DataVersionPair{TypeId: "typeU", ShortId: "sMissing"}, rows[0].Pending[0])
	})
}

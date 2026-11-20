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
	store := NewStore(nil, db, nil, "spaceA", nil, nil, nil, nil)
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
		gate := store.gateFor("obj-X", nil)
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
		gate := store.gateFor("obj-X", nil)
		ok, err := gate(ctx, gateChange("typeT:sA"), []byte("payload"))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Empty(t, countDetached(t, ctx, store))
	})

	t.Run("legacy/unparseable DataVersion passes through", func(t *testing.T) {
		ctx, store := gateStore(t)
		gate := store.gateFor("obj-X", nil)
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
		gate := store.gateFor("obj-X", nil)
		ok, err := gate(ctx, gateChange(""), []byte("payload"))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Empty(t, countDetached(t, ctx, store))
	})

	t.Run("multi-pair parks with only the missing pair pending", func(t *testing.T) {
		ctx, store := gateStore(t)
		landShortId(t, ctx, store, "typeT", "sA") // known
		gate := store.gateFor("obj-X", nil)
		ok, err := gate(ctx, gateChange("typeT:sA;typeU:sMissing"), []byte("payload"))
		require.NoError(t, err)
		assert.False(t, ok)

		rows := countDetached(t, ctx, store)
		require.Len(t, rows, 1)
		require.Len(t, rows[0].Pending, 1, "only the unsatisfied pair is recorded")
		assert.Equal(t, types.DataVersionPair{TypeId: "typeU", ShortId: "sMissing"}, rows[0].Pending[0])
	})
}

// A change is parked for a definition that is not known when the gate
// looks. If the definition lands before the park commits, the wake-up
// of that landing has already found nothing to drain; the park itself
// must then ask for a drain, or the change waits for an unrelated
// definition, or a restart.
func TestGate_ParkAsksForADrainWhenItsDefinitionsLanded(t *testing.T) {
	pending := func(pairs ...types.DataVersionPair) DetachedRow {
		return DetachedRow{ChangeId: "ch1", SpaceId: "spaceA", ObjectId: "obj-X", Payload: []byte("p"), Pending: pairs}
	}
	tA := types.DataVersionPair{TypeId: "typeT", ShortId: "sA"}
	tB := types.DataVersionPair{TypeId: "typeU", ShortId: "sB"}
	// The store's drainer consumes its queue as it fills; a drainer that
	// was never started keeps the requests countable.
	gateStore := func(t *testing.T) (context.Context, *Store) {
		ctx, store := gateStore(t)
		store.drainer.Close()
		store.drainer = newDrainer(store)
		return ctx, store
	}

	t.Run("still unknown: the definition's apply will wake the drainer", func(t *testing.T) {
		ctx, store := gateStore(t)
		require.NoError(t, store.parkGated(ctx, pending(tA)))
		assert.Zero(t, store.drainer.queue.Len())
		assert.Len(t, countDetached(t, ctx, store), 1)
	})

	t.Run("landed between the lookup and the park", func(t *testing.T) {
		// The definition lands once the row is in: only a lookup made
		// after the park sees it.
		ctx, store := gateStore(t)
		store.parkedHook = func() {
			require.Len(t, countDetached(t, ctx, store), 1, "the row is committed before the lookup")
			landShortId(t, ctx, store, tA.TypeId, tA.ShortId)
		}
		require.NoError(t, store.parkGated(ctx, pending(tA)))
		assert.Equal(t, 1, store.drainer.queue.Len())
	})

	t.Run("the caller's deadline does not lose the drain", func(t *testing.T) {
		ctx, store := gateStore(t)
		ctx, cancel := context.WithCancel(ctx)
		store.parkedHook = func() {
			landShortId(t, context.Background(), store, tA.TypeId, tA.ShortId)
			cancel()
		}
		require.NoError(t, store.parkGated(ctx, pending(tA)))
		assert.Equal(t, 1, store.drainer.queue.Len())
	})

	t.Run("one of two still unknown", func(t *testing.T) {
		ctx, store := gateStore(t)
		landShortId(t, ctx, store, tA.TypeId, tA.ShortId)
		require.NoError(t, store.parkGated(ctx, pending(tA, tB)))
		assert.Zero(t, store.drainer.queue.Len())
	})

	t.Run("waiting on nothing: a stale controller", func(t *testing.T) {
		ctx, store := gateStore(t)
		require.NoError(t, store.parkGated(ctx, pending()))
		assert.Equal(t, 1, store.drainer.queue.Len())
	})
}

// A definition lookup that fails does not fail the replay: the change
// is parked as waiting on that definition, like any change whose
// definition is not known. A lookup that fails on a caller that is
// already done is the caller's failure and is returned.
func TestGate_FailedLookupParksTheChange(t *testing.T) {
	// failingLookups points the store's registry at a closed database:
	// every lookup errors, the way a lookup of a collection that is
	// still being created does, while parks go to the store's own.
	failingLookups := func(t *testing.T, ctx context.Context, store *Store) {
		t.Helper()
		closed, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "closed.db"), nil)
		require.NoError(t, err)
		require.NoError(t, closed.Close())
		store.reg = types.NewLiveRegistry(closed, nil)
		_, err = store.reg.KnownShortId(ctx, "typeT", "sA")
		require.Error(t, err, "the lookup must fail for this test to mean anything")
	}

	t.Run("lookup error", func(t *testing.T) {
		ctx, store := gateStore(t)
		failingLookups(t, ctx, store)
		gate := store.gateFor("obj-X", nil)
		ok, err := gate(ctx, gateChange("typeT:sA"), []byte("payload"))
		require.NoError(t, err)
		assert.False(t, ok)
		rows := countDetached(t, ctx, store)
		require.Len(t, rows, 1)
		assert.Equal(t, []types.DataVersionPair{{TypeId: "typeT", ShortId: "sA"}}, rows[0].Pending)
	})

	t.Run("lookup error on a caller that is done", func(t *testing.T) {
		ctx, store := gateStore(t)
		failingLookups(t, ctx, store)
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		gate := store.gateFor("obj-X", nil)
		ok, err := gate(cancelled, gateChange("typeT:sA"), []byte("payload"))
		require.Error(t, err)
		assert.False(t, ok)
		assert.Empty(t, countDetached(t, ctx, store))
	})
}

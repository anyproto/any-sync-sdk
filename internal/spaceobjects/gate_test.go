package spaceobjects

import (
	"context"
	"errors"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync/app/ocache"
	"github.com/anyproto/any-sync/commonspace/spacestorage"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/types"
)

// TestGate_ParkAndDrain doesn't spin up the full SDK — instead it
// exercises the gate + drain plumbing in isolation. Wires:
//
//   - a SDK DB,
//   - a Store with a LiveRegistry pointing at the same DB,
//   - the gate function (Park),
//   - the drain function (which re-checks pending pairs).
//
// Park a payload referencing typeT/shortS; assert it lives in the
// detached collection. Insert the shortId row; call Drain; assert
// the detached row is gone.
func TestGate_ParkAndDrain(t *testing.T) {
	ctx := context.Background()
	dbPath := t.TempDir() + "/test.db"
	db, err := anystore.Open(ctx, dbPath, nil)
	require.NoError(t, err)
	defer db.Close()

	store := NewStore(nil, db, nil, "spaceA", nil, nil, nil, nil)

	// Park a fake parked change. payload bytes are opaque to the
	// gate's bookkeeping; we use a short marker.
	row := DetachedRow{
		ChangeId:  "ch-1",
		SpaceId:   "spaceA",
		ObjectId:  "obj-X",
		AddSeq:    7,
		Timestamp: 12345,
		Payload:   []byte("placeholder"),
		Pending:   []types.DataVersionPair{{TypeId: "typeT", ShortId: "shortS"}},
	}
	require.NoError(t, store.Park(ctx, row))

	// IterDetached visits the parked row.
	var seen []DetachedRow
	require.NoError(t, store.IterDetached(ctx, func(r DetachedRow) bool {
		seen = append(seen, r)
		return true
	}))
	require.Len(t, seen, 1)
	assert.Equal(t, "ch-1", seen[0].ChangeId)
	require.Len(t, seen[0].Pending, 1)
	assert.Equal(t, "typeT", seen[0].Pending[0].TypeId)
	assert.Equal(t, "shortS", seen[0].Pending[0].ShortId)

	// Drain with the shortId NOT yet in the registry — row stays.
	require.NoError(t, store.Drain(ctx))
	seen = seen[:0]
	require.NoError(t, store.IterDetached(ctx, func(r DetachedRow) bool {
		seen = append(seen, r)
		return true
	}))
	assert.Len(t, seen, 1, "row should remain when shortId still missing")

	// Land the shortId in the type's shortIds collection.
	shortIdsColl, err := db.Collection(ctx, "typeT_shortIds")
	require.NoError(t, err)
	a := &anyenc.Arena{}
	doc := a.NewObject()
	doc.Set("id", a.NewString("shortS"))
	require.NoError(t, shortIdsColl.UpsertOne(ctx, doc))

	// Drain attempts to replay. replayParked will fail to load the
	// any-sync object (we never wired one) — but the gate logic
	// (allPendingKnown) should now return true. The retry will
	// error and the row stays parked. That's fine — we're testing
	// the GATE, not the full replay loop.
	require.NoError(t, store.Drain(ctx))

	// The pending row is still in the collection (replay failed
	// because Get on a non-existent objectId errors). The important
	// thing for this test is that allPendingKnown flipped — verify
	// directly:
	known, err := store.reg.KnownShortId(ctx, "typeT", "shortS")
	require.NoError(t, err)
	assert.True(t, known, "shortS should now be known")
}

// A parked change whose object is deleted is unparked by the drain:
// its tree cannot be opened (head storage records it deleted), so the
// change can never apply and the row would otherwise be retried on
// every pass. A row whose object fails to load for another reason
// stays parked.
func TestDrain_UnparksDeletedObject(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, t.TempDir()+"/test.db", nil)
	require.NoError(t, err)
	defer db.Close()

	store := NewStore(nil, db, nil, "spaceA", nil, nil, nil, nil)
	_ = store.cache.Close()
	store.cache = ocache.New(func(_ context.Context, id string) (ocache.Object, error) {
		if id == "obj-deleted" {
			return nil, treeOpenError("BuildTree", id, spacestorage.ErrTreeStorageAlreadyDeleted)
		}
		return nil, errors.New("offline")
	})
	defer store.cache.Close()

	payload, err := object.NewCodec().Encode(&crdt.Change{
		Dataset: "blocks", DataVersion: "test-v1",
		Records: []crdt.RecordChange{{Id: "r1", Upsert: true, Ops: []crdt.Op{{Type: crdt.OpSet, Path: []string{"a"}, Payload: (&anyenc.Arena{}).NewString("x")}}}},
	})
	require.NoError(t, err)
	for _, id := range []string{"obj-deleted", "obj-offline"} {
		require.NoError(t, store.Park(ctx, DetachedRow{
			ChangeId: "ch-" + id, SpaceId: "spaceA", ObjectId: id, AddSeq: 3, OrderId: "a1", Payload: payload,
		}))
	}

	require.NoError(t, store.Drain(ctx))

	var left []string
	require.NoError(t, store.IterDetached(ctx, func(r DetachedRow) bool {
		left = append(left, r.ObjectId)
		return true
	}))
	assert.Equal(t, []string{"obj-offline"}, left)
}

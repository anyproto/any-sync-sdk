package spaceimpl

import (
	"context"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"
)

func seedDoc(t *testing.T, ctx context.Context, coll anystore.Collection, id string) {
	t.Helper()
	a := &anyenc.Arena{}
	doc := a.NewObject()
	doc.Set("id", a.NewString(id))
	require.NoError(t, coll.UpsertOne(ctx, doc))
}

func mustColl(t *testing.T, ctx context.Context, db anystore.DB, name string) anystore.Collection {
	t.Helper()
	coll, err := db.Collection(ctx, name)
	require.NoError(t, err)
	return coll
}

// TestDropSpaceCollections asserts a space's own collections — the
// shared objects collection, the detached collection, and every
// per-object dataset (including type-owned ones) — are dropped, while a
// sibling space's collections and the shared _meta survive. A second
// run is a no-op.
func TestDropSpaceCollections(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "sdk.db"), nil)
	require.NoError(t, err)
	defer db.Close()

	const spaceId = "spaceA.1"
	const otherSpace = "spaceB.2"

	// Space A: objects collection with two object ids, their datasets,
	// a type-owned shortIds collection, and the detached collection.
	objsA := mustColl(t, ctx, db, spaceId+"_objects")
	seedDoc(t, ctx, objsA, "objX")
	seedDoc(t, ctx, objsA, "typeT")
	seedDoc(t, ctx, mustColl(t, ctx, db, "objX_blocks"), "rec1")
	seedDoc(t, ctx, mustColl(t, ctx, db, "typeT_shortIds"), "sid1")
	seedDoc(t, ctx, mustColl(t, ctx, db, spaceId+"__detached"), "chg1")

	// Sibling space + shared meta that must be preserved.
	seedDoc(t, ctx, mustColl(t, ctx, db, otherSpace+"_objects"), "objY")
	seedDoc(t, ctx, mustColl(t, ctx, db, "objY_blocks"), "rec2")
	seedDoc(t, ctx, mustColl(t, ctx, db, "_meta"), "watermark")

	s := &Service{db: db}
	require.NoError(t, s.dropSpaceCollections(ctx, spaceId))

	names, err := db.GetCollectionNames(ctx)
	require.NoError(t, err)
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}

	// Space A collections gone.
	for _, n := range []string{spaceId + "_objects", spaceId + "__detached", "objX_blocks", "typeT_shortIds"} {
		assert.False(t, got[n], "expected %s dropped", n)
	}
	// Sibling space + shared meta survive.
	for _, n := range []string{otherSpace + "_objects", "objY_blocks", "_meta"} {
		assert.True(t, got[n], "expected %s preserved", n)
	}

	// Idempotent: a second pass with nothing left to drop succeeds.
	require.NoError(t, s.dropSpaceCollections(ctx, spaceId))
}

func TestRemotelyGone(t *testing.T) {
	gone := []coordinatorproto.SpaceStatus{
		coordinatorproto.SpaceStatus_SpaceStatusPendingDeletion,
		coordinatorproto.SpaceStatus_SpaceStatusDeletionStarted,
		coordinatorproto.SpaceStatus_SpaceStatusDeleted,
		coordinatorproto.SpaceStatus_SpaceStatusNotExists,
	}
	for _, st := range gone {
		assert.True(t, remotelyGone(st), "status %v should be gone", st)
	}
	assert.False(t, remotelyGone(coordinatorproto.SpaceStatus_SpaceStatusCreated))
}

// TestDecideReconcile exhaustively covers the reconcile decision: who
// gets a coordinator SpaceDelete, who gets offloaded, and — critically —
// who is left alone (so deletes aren't re-sent and member-owned spaces
// aren't deleted network-wide).
func TestDecideReconcile(t *testing.T) {
	owner := coordinatorproto.SpacePermissions_SpacePermissionsOwner
	reader := coordinatorproto.SpacePermissions_SpacePermissionsUnknown
	st := func(s coordinatorproto.SpaceStatus, p coordinatorproto.SpacePermissions) *coordinatorproto.SpaceStatusPayload {
		return &coordinatorproto.SpaceStatusPayload{Status: s, Permissions: p}
	}
	C := coordinatorproto.SpaceStatus_SpaceStatusCreated
	P := coordinatorproto.SpaceStatus_SpaceStatusPendingDeletion
	D := coordinatorproto.SpaceStatus_SpaceStatusDeleted
	NX := coordinatorproto.SpaceStatus_SpaceStatusNotExists

	cases := []struct {
		name           string
		locallyDeleted bool
		st             *coordinatorproto.SpaceStatusPayload
		want           reconcileAction
	}{
		{"owner deleted locally, still active -> send", true, st(C, owner), actionSendDelete},
		{"owner deleted locally, already pending -> none (no resend)", true, st(P, owner), actionNone},
		{"owner deleted locally, already deleted -> none", true, st(D, owner), actionNone},
		{"member deleted locally, active -> none (cannot delete)", true, st(C, reader), actionNone},
		{"active not-deleted owner -> none", false, st(C, owner), actionNone},
		{"inbound pending, not deleted -> offload", false, st(P, owner), actionOffload},
		{"inbound deleted, not deleted -> offload", false, st(D, reader), actionOffload},
		{"inbound not-exists, not deleted -> offload", false, st(NX, owner), actionOffload},
		{"nil status -> none", true, nil, actionNone},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, decideReconcile(c.locallyDeleted, c.st))
		})
	}
}

func TestOwnedByObject(t *testing.T) {
	ids := map[string]struct{}{"objX": {}, "typeT": {}}
	assert.True(t, ownedByObject("objX_blocks", ids))
	assert.True(t, ownedByObject("typeT_shortIds", ids))
	assert.False(t, ownedByObject("objY_blocks", ids))
	assert.False(t, ownedByObject("noUnderscore", ids))
	assert.False(t, ownedByObject("_meta", ids))
}

package spaceimpl

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync/coordinator/coordinatorproto"

	"github.com/anyproto/any-sync-sdk/internal/techspace"
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

// TestDropSpaceCollectionsChunkedCommits pins the sweep's chunked-tx
// shape on a BARE ctx: commits are one per chunk of offloadDropChunk
// drops plus one meta chunk plus one final objects chunk — never one
// per drop and never one for the whole sweep. Seeds offloadDropChunk+2
// droppable collections so the chunk boundary itself is exercised.
func TestDropSpaceCollectionsChunkedCommits(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "sdk.db"), nil)
	require.NoError(t, err)
	defer db.Close()

	const spaceId = "spaceA.1"
	mustColl(t, ctx, db, spaceId+"_objects") // empty: no per-object colls
	// Realistic name shape: `<spaceId>_<objectId-like>_<dataset>`.
	drops := offloadDropChunk + 2
	for i := 0; i < drops; i++ {
		mustColl(t, ctx, db, fmt.Sprintf("%s_obj%04dabcdef_blocks", spaceId, i))
	}
	meta := mustColl(t, ctx, db, "_meta")
	seedDoc(t, ctx, meta, "w1") // survives: not this space's row

	var chunks []int
	sweepChunkCommitted = func(n int) { chunks = append(chunks, n) }
	defer func() { sweepChunkCommitted = nil }()

	s := &Service{db: db}
	require.NoError(t, s.dropSpaceCollections(ctx, spaceId))

	// 1 meta chunk FIRST + ceil(drops/chunk)=2 drop chunks + 1 objects chunk.
	assert.Equal(t, []int{1, offloadDropChunk, 2, 1}, chunks)

	names, err := db.GetCollectionNames(ctx)
	require.NoError(t, err)
	for _, n := range names {
		assert.False(t, strings.HasPrefix(n, spaceId+"_"), "expected %s dropped", n)
	}
}

// TestDropSpaceCollectionsCallerTxJoin pins the caller-tx degradation:
// with a caller tx on ctx every chunk savepoint-joins it instead of
// committing, so the caller keeps rollback authority — rolling its tx
// back restores every collection — and a later committed bare-ctx run
// still drops them all (the rollback un-closed the handles cleanly).
func TestDropSpaceCollectionsCallerTxJoin(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "sdk.db"), nil)
	require.NoError(t, err)
	defer db.Close()

	const spaceId = "spaceA.1"
	objs := mustColl(t, ctx, db, spaceId+"_objects")
	seedDoc(t, ctx, objs, "objX")
	seedDoc(t, ctx, mustColl(t, ctx, db, "objX_blocks"), "rec1")
	seedDoc(t, ctx, mustColl(t, ctx, db, spaceId+"__detached"), "chg1")

	s := &Service{db: db}
	swept := []string{spaceId + "_objects", spaceId + "__detached", "objX_blocks"}

	tx, err := db.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, s.dropSpaceCollections(tx.Context(), spaceId))
	require.NoError(t, tx.Rollback())

	names, err := db.GetCollectionNames(ctx)
	require.NoError(t, err)
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}
	for _, n := range swept {
		assert.True(t, got[n], "expected %s restored by caller-tx rollback", n)
	}

	require.NoError(t, s.dropSpaceCollections(ctx, spaceId))
	names, err = db.GetCollectionNames(ctx)
	require.NoError(t, err)
	for _, n := range names {
		for _, dropped := range swept {
			assert.NotEqual(t, dropped, n, "expected %s dropped after committed sweep", n)
		}
	}
}

// seedFailureSpace seeds one space (objects roster, a per-object
// dataset, the detached collection) plus its _meta watermark row.
func seedFailureSpace(t *testing.T, ctx context.Context, db anystore.DB, spaceId string) {
	t.Helper()
	objs := mustColl(t, ctx, db, spaceId+"_objects")
	seedDoc(t, ctx, objs, "objX")
	seedDoc(t, ctx, mustColl(t, ctx, db, "objX_blocks"), "rec1")
	seedDoc(t, ctx, mustColl(t, ctx, db, spaceId+"__detached"), "chg1")
	meta := mustColl(t, ctx, db, "_meta")
	a := &anyenc.Arena{}
	doc := a.NewObject()
	doc.Set("id", a.NewString("objX"))
	doc.Set("sp", a.NewString(spaceId))
	require.NoError(t, meta.UpsertOne(ctx, doc))
}

// TestDropSpaceCollectionsMetaPurgeFirst pins the meta-dies-first
// invariant: with the DB closed after the FIRST committed chunk, the
// first drop chunk fails — and on disk the meta purge has already
// committed (watermark row gone) while every collection remains. The
// converging direction: a re-materialized space finds no stale
// watermark, so cold-restore replays; the leaked collections go to the
// retry / startup GC.
func TestDropSpaceCollectionsMetaPurgeFirst(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sdk.db")
	db, err := anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	const spaceId = "spaceA.1"
	seedFailureSpace(t, ctx, db, spaceId)

	sweepChunkCommitted = func(int) { _ = db.Close() }
	defer func() { sweepChunkCommitted = nil }()

	s := &Service{db: db}
	require.Error(t, s.dropSpaceCollections(ctx, spaceId))
	sweepChunkCommitted = nil

	db, err = anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	defer db.Close()
	names, err := db.GetCollectionNames(ctx)
	require.NoError(t, err)
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}
	for _, n := range []string{spaceId + "_objects", spaceId + "__detached", "objX_blocks"} {
		assert.True(t, got[n], "expected %s still present (no drop chunk committed)", n)
	}
	meta := mustColl(t, ctx, db, "_meta")
	_, err = meta.FindId(ctx, "objX")
	assert.ErrorIs(t, err, anystore.ErrDocNotFound, "expected watermark purged before any drop")
}

// TestDropSpaceCollectionsChunkFailure pins incremental progress plus
// the objects-last invariant: with the DB closed after the SECOND
// committed chunk (meta purge, then the drop chunk), the final objects
// chunk fails — the error propagates while the committed chunks stay
// durable, and `<spaceId>_objects` survives so the retry can
// re-enumerate. A clean re-run finishes the sweep.
func TestDropSpaceCollectionsChunkFailure(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sdk.db")
	db, err := anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	const spaceId = "spaceA.1"
	seedFailureSpace(t, ctx, db, spaceId)

	commits := 0
	sweepChunkCommitted = func(int) {
		commits++
		if commits == 2 {
			_ = db.Close()
		}
	}
	defer func() { sweepChunkCommitted = nil }()

	s := &Service{db: db}
	require.Error(t, s.dropSpaceCollections(ctx, spaceId))
	sweepChunkCommitted = nil
	require.Equal(t, 2, commits)

	// Reopen: meta purge + drop chunk persisted; objects kept for retry.
	db, err = anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	defer db.Close()
	names, err := db.GetCollectionNames(ctx)
	require.NoError(t, err)
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}
	assert.False(t, got["objX_blocks"], "expected drop chunk committed")
	assert.False(t, got[spaceId+"__detached"], "expected drop chunk committed")
	assert.True(t, got[spaceId+"_objects"], "expected objects kept for re-enumeration")
	meta := mustColl(t, ctx, db, "_meta")
	_, err = meta.FindId(ctx, "objX")
	assert.ErrorIs(t, err, anystore.ErrDocNotFound, "expected watermark purged first")

	// Retry completes the sweep.
	s = &Service{db: db}
	require.NoError(t, s.dropSpaceCollections(ctx, spaceId))
	names, err = db.GetCollectionNames(ctx)
	require.NoError(t, err)
	for _, n := range names {
		assert.NotEqual(t, spaceId+"_objects", n)
	}
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

// TestShouldPruneReadState pins the prune gate on SYNCED removal
// markers: a device-local declined-join tombstone (LocalStatus) must
// never drive the account-wide destructive watermark.
func TestShouldPruneReadState(t *testing.T) {
	prune := []techspace.SpaceIndexRecord{
		{Id: "deleted", RemoteStatus: techspace.StatusDeleted},
		{Id: "one-to-one", RemoteStatus: techspace.OneToOneDeletedStatus},
		{Id: "guest", RemoteStatus: techspace.GuestDeletedRemoteStatus},
	}
	keep := []techspace.SpaceIndexRecord{
		{Id: "active"},
		{Id: "declined-join", LocalStatus: techspace.StatusDeleted},
		{Id: "pending", RemoteStatus: techspace.InvitePendingRemoteStatus},
	}
	for _, r := range prune {
		require.True(t, shouldPruneReadState(r), r.Id)
	}
	for _, r := range keep {
		require.False(t, shouldPruneReadState(r), r.Id)
	}
}

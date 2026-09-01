package spaceimpl

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// testCid mints a deterministic valid CIDv1 from seed — collection
// owners must be shape-valid for the sweep to classify them.
func testCid(t *testing.T, seed string) string {
	t.Helper()
	h, err := mh.Sum([]byte(seed), mh.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, h).String()
}

func collNames(t *testing.T, ctx context.Context, db anystore.DB) map[string]bool {
	t.Helper()
	names, err := db.GetCollectionNames(ctx)
	require.NoError(t, err)
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}
	return got
}

func TestSweepOrphans(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "sdk.db"), nil)
	require.NoError(t, err)
	defer db.Close()

	liveSpace := testCid(t, "live-space") + ".1"
	deadSpace := testCid(t, "dead-space") + ".2"
	ghostSpace := testCid(t, "ghost-space") + ".3"    // collections but no tech row: kept (no positive tombstone)
	ghostObj := testCid(t, "ghost-obj")               // only the ghost's roster claims it: kept
	emptyDeadSpace := testCid(t, "empty-dead") + ".4" // dead; meta-only leak, no collections
	liveObj := testCid(t, "live-obj")                 // in liveSpace's objects
	legacyObj := testCid(t, "legacy-obj")             // in liveSpace's objects; pre-scoping meta (no sp)
	prescopeObj := testCid(t, "prescope-obj")         // pre-scoping meta, NO roster row: uncertainty → live
	crossObj := testCid(t, "cross-obj")               // meta claims deadSpace but live roster has it
	baseObj := testCid(t, "base-only-obj")            // no objects row; meta claims liveSpace
	purgedObj := testCid(t, "purged-obj")             // pre-drop purge leak: meta del=true
	orphanObj := testCid(t, "orphan-obj")             // no objects row, no meta
	deadSpaceObj := testCid(t, "dead-space-obj")      // in deadSpace's objects

	// Live space: objects roster + detached + a live object's dataset.
	objsLive := mustColl(t, ctx, db, liveSpace+"_objects")
	seedDoc(t, ctx, objsLive, liveObj)
	seedDoc(t, ctx, objsLive, legacyObj)
	seedDoc(t, ctx, objsLive, crossObj)
	seedDoc(t, ctx, mustColl(t, ctx, db, liveSpace+"__detached"), "chg1")
	seedDoc(t, ctx, mustColl(t, ctx, db, liveObj+"_editor_blocks"), "rec1")
	seedDoc(t, ctx, mustColl(t, ctx, db, legacyObj+"_editor_blocks"), "rec1b")
	seedDoc(t, ctx, mustColl(t, ctx, db, prescopeObj+"_editor_blocks"), "rec1c")
	seedDoc(t, ctx, mustColl(t, ctx, db, crossObj+"_editor_blocks"), "rec1d")
	seedDoc(t, ctx, mustColl(t, ctx, db, baseObj+"_chat_messages"), "rec2")

	// Leaks: purged object (history pair), fully unknown object.
	seedDoc(t, ctx, mustColl(t, ctx, db, purgedObj+"_program_source"), "rec3")
	seedDoc(t, ctx, mustColl(t, ctx, db, purgedObj+"__history"), "chg2")
	seedDoc(t, ctx, mustColl(t, ctx, db, orphanObj+"_agent_debug_log"), "rec4")
	seedDoc(t, ctx, mustColl(t, ctx, db, orphanObj+"__history"), "chg3")

	// Dead space: interrupted offload left everything behind.
	objsDead := mustColl(t, ctx, db, deadSpace+"_objects")
	seedDoc(t, ctx, objsDead, deadSpaceObj)
	seedDoc(t, ctx, mustColl(t, ctx, db, deadSpaceObj+"_editor_blocks"), "rec5")

	// Ghost space: shell with no tech-space row at all — absence of
	// evidence, kept; its roster shields its objects like a live one.
	seedDoc(t, ctx, mustColl(t, ctx, db, ghostSpace+"_objects"), ghostObj)
	seedDoc(t, ctx, mustColl(t, ctx, db, ghostObj+"_editor_blocks"), "rec6")

	// Fixed collections the shape check must never touch.
	metaColl := mustColl(t, ctx, db, crdt.MetaCollectionName)
	seedDoc(t, ctx, mustColl(t, ctx, db, "_history_traces"), "tr1")
	seedDoc(t, ctx, mustColl(t, ctx, db, "_read_state"), "rs1")
	seedDoc(t, ctx, mustColl(t, ctx, db, "files_local"), "f1")

	// Meta rows: base-dataset-only live claim, purged marker, dead-space
	// object watermark, dead-space space row.
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, baseObj, 3, 3, nil, liveSpace))
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, legacyObj, 2, 2, nil, ""))
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, prescopeObj, 2, 2, nil, ""))
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, crossObj, 2, 2, nil, deadSpace))
	require.NoError(t, crdt.PersistDeletionMark(ctx, metaColl, purgedObj, liveSpace, 7))
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, deadSpaceObj, 4, 4, nil, deadSpace))
	require.NoError(t, crdt.PersistSpaceMaxAddSeq(ctx, metaColl, deadSpace, 9))
	// Meta-only leak: a dead space whose collections are already gone
	// (crash between an offload's drops and its meta purge) must still
	// lose its watermark rows.
	emptyDeadObj := testCid(t, "empty-dead-obj")
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, emptyDeadObj, 5, 5, nil, emptyDeadSpace))
	require.NoError(t, crdt.PersistSpaceMaxAddSeq(ctx, metaColl, emptyDeadSpace, 6))

	s := &Service{db: db}
	liveSet := map[string]struct{}{liveSpace: {}}
	deadSet := map[string]struct{}{deadSpace: {}, emptyDeadSpace: {}}
	require.NoError(t, s.sweepOrphans(ctx, liveSet, deadSet))

	got := collNames(t, ctx, db)
	for _, n := range []string{
		liveSpace + "_objects", liveSpace + "__detached",
		liveObj + "_editor_blocks", legacyObj + "_editor_blocks",
		prescopeObj + "_editor_blocks", crossObj + "_editor_blocks",
		baseObj + "_chat_messages",
		ghostSpace + "_objects", ghostObj + "_editor_blocks",
		crdt.MetaCollectionName, "_history_traces", "_read_state", "files_local",
	} {
		assert.True(t, got[n], "expected %s preserved", n)
	}
	for _, n := range []string{
		purgedObj + "_program_source", purgedObj + "__history",
		orphanObj + "_agent_debug_log", orphanObj + "__history",
		deadSpace + "_objects", deadSpaceObj + "_editor_blocks",
	} {
		assert.False(t, got[n], "expected %s dropped", n)
	}

	// Meta: purge marker survives (change-feed deletion signal); dead
	// space's watermarks — object row and space:<id> row — are gone;
	// the live base-dataset-only claim survives.
	_, err = metaColl.FindId(ctx, purgedObj)
	assert.NoError(t, err, "purged-object del marker must survive")
	_, err = metaColl.FindId(ctx, baseObj)
	assert.NoError(t, err, "live base-dataset-only meta must survive")
	_, err = metaColl.FindId(ctx, deadSpaceObj)
	assert.True(t, errors.Is(err, anystore.ErrDocNotFound), "dead-space object meta must be purged")
	_, err = metaColl.FindId(ctx, crdt.SpaceMetaKey(deadSpace))
	assert.True(t, errors.Is(err, anystore.ErrDocNotFound), "dead-space space meta row must be purged")
	_, err = metaColl.FindId(ctx, emptyDeadObj)
	assert.True(t, errors.Is(err, anystore.ErrDocNotFound), "meta-only leak: dead-space object meta must be purged")
	_, err = metaColl.FindId(ctx, crdt.SpaceMetaKey(emptyDeadSpace))
	assert.True(t, errors.Is(err, anystore.ErrDocNotFound), "meta-only leak: space meta row must be purged")

	// Idempotent: a second pass with nothing left succeeds.
	require.NoError(t, s.sweepOrphans(ctx, liveSet, deadSet))
}

func TestCollectionOwner(t *testing.T) {
	objId := testCid(t, "obj")
	spId := testCid(t, "sp") + ".2f"
	cases := []struct {
		name  string
		owner string
		kind  ownerKind
	}{
		{objId + "_chat_messages", objId, ownerObject},
		{objId + "__history", objId, ownerObject},
		{spId + "_objects", spId, ownerSpace},
		{spId + "__detached", spId, ownerSpace},
		{"_meta", "", ownerNone},
		{"_history_traces", "", ownerNone},
		{"files_local", "", ownerNone},
		{"noprefix", "", ownerNone},
		// consumer-tagged collections (SDK.Store contract): the segment
		// before the first "_" is a bare tag, never a content id.
		{"l_a_scratch", "", ownerNone},
		{"l_s_" + spId + "_cache", "", ownerNone},
	}
	for _, c := range cases {
		owner, kind := collectionOwner(c.name)
		assert.Equal(t, c.kind, kind, c.name)
		assert.Equal(t, c.owner, owner, c.name)
	}
}

package spaceobjects

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/readstate"
)

// A mark on an object that never arrived keeps its seq in a read-state
// row only. When a purge drops that row and the process restarts, the
// next change can be stamped at that seq: a feed started in between
// must still return it.
func TestUnreadSince_PresentSurvivesUnstampedPurge(t *testing.T) {
	ctx, s := purgeStore(t)
	s.applySeqs = crdt.NewApplySeqAllocator(s.seedApplySeq)
	metaColl, err := s.metaCollection(ctx)
	require.NoError(t, err)
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "live", 5, 5, nil, s.spaceId))
	require.NoError(t, s.EnsureApplySeq(ctx))
	notArrived := func(context.Context, string, string) ([]string, string, bool, error) { return nil, "", false, nil }
	s.readState = readstate.New(s.db, s.spaceId, s.applySeqs.Next, notArrived, nil)
	require.NoError(t, s.readState.Open(ctx))

	require.NoError(t, s.readState.WriteTx(ctx, func(txCtx context.Context) error {
		_, err := s.readState.MergeHeads(txCtx, "ghost", []string{"h1"})
		return err
	}))
	none, gen, present, err := s.UnreadSince(ctx, "", 0, 0)
	require.NoError(t, err)
	require.Empty(t, none)
	stateMax, err := readstate.MaxStateSeq(ctx, s.db, s.spaceId)
	require.NoError(t, err)
	require.Greater(t, stateMax, present, "the mark's seq lives in the read state only")

	require.NoError(t, s.purgeObject(ctx, "ghost"))
	s.applySeqs = crdt.NewApplySeqAllocator(s.seedApplySeq) // restart
	next, err := s.applySeqs.Next(ctx)
	require.NoError(t, err)
	// The next apply: its seq on the object's _meta row and its entry.
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "live", 9, next, nil, s.spaceId))
	require.NoError(t, s.readState.TrackChange(ctx, readstate.Track{
		ObjectId: "live", ChangeId: "c9", VersionId: "v9", AddSeq: 9, ApplySeq: next,
		RecordIds: []string{"r9"}, Tags: []string{"message"}, Tracked: true,
	}))

	got, _, resume, err := s.UnreadSince(ctx, gen, present, 0)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "c9", got[0].ChangeId)
	require.Equal(t, next, resume)
}

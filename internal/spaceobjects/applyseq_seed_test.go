package spaceobjects

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/readstate"
)

// The allocator's seed covers every table a sequence is persisted in: a
// read mark's stateSeq lives only in the read-state table, and a seed
// below it would stamp later applies under a consumer's read-state
// cursor.
func TestApplySeq_SeedCoversReadState(t *testing.T) {
	ctx, s := purgeStore(t)
	s.applySeqs = crdt.NewApplySeqAllocator(s.seedApplySeq)

	metaColl, err := s.metaCollection(ctx)
	require.NoError(t, err)
	require.NoError(t, crdt.PersistMeta(ctx, metaColl, "obj1", 10, 100, nil, s.spaceId))

	rs := readstate.New(s.db, s.spaceId, func(context.Context) (uint64, error) { return 250, nil }, nil)
	require.NoError(t, rs.TrackChange(ctx, readstate.Track{
		ObjectId: "obj1", ChangeId: "c1", VersionId: "v01", AddSeq: 1, ApplySeq: 100,
		RecordIds: []string{"r1"}, Tags: []string{"message"}, Tracked: true,
	}))
	_, err = rs.MarkReadUpTo(ctx, "obj1", "")
	require.NoError(t, err)
	// A later row with a lower stateSeq must not win.
	require.NoError(t, rs.TrackChange(ctx, readstate.Track{
		ObjectId: "obj2", ChangeId: "c2", VersionId: "v02", AddSeq: 2, ApplySeq: 120,
		RecordIds: []string{"r2"}, Tags: []string{"message"}, Tracked: true,
	}))

	next, err := s.applySeqs.Next(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(251), next, "seeded past the mark's stateSeq, not the objects' watermark")
}

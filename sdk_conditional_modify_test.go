package anysyncsdk

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestModify_IfUnchangedSince drives the conditional write through the
// public Space API on an offline store: one conditional batch updates
// and deletes against the stamp the caller read, a write against an
// older read is refused and writes nothing, each write's ApplySeq
// chains the next, and a precondition on the shared objects dataset is
// a validation error.
func TestModify_IfUnchangedSince(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sdk, err := Open(ctx, wmConfig(t, dataDir), wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "CAS"})
	require.NoError(t, err)
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: "wm-blocks-type"})
	require.NoError(t, err)

	set := func(id string, n int) space.RecordModify {
		return space.RecordModify{Id: id, Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: "n", Value: n}}}
	}
	batch := func(ifUnchangedSince *uint64, records ...space.RecordModify) space.ModifyBatch {
		return space.ModifyBatch{ObjectId: objId, Dataset: wmDataset, IfUnchangedSince: ifUnchangedSince, Records: records}
	}
	// The documented read for the precondition: tombstones included.
	highestStamp := func() uint64 {
		docs, err := sp.Query(objId, wmDataset).Projection(space.ProjectionOpts{IncludeDeleted: true}).All(ctx)
		require.NoError(t, err)
		var seq uint64
		for _, d := range docs {
			seq = max(seq, uint64(d.GetInt("_applySeq")))
		}
		return seq
	}

	seed, err := sp.Modify(ctx, batch(nil, set("a", 1), set("b", 1)))
	require.NoError(t, err)
	require.NotZero(t, seed.ApplySeq)
	read := highestStamp()
	require.Equal(t, seed.ApplySeq, read)

	res, err := sp.Modify(ctx, batch(&read, set("a", 2), space.RecordModify{Id: "b", Ops: []space.Op{{Type: space.OpDelete}}}))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)
	assert.Equal(t, res.ApplySeq, highestStamp(), "the write's stamp, the delete's included, is the dataset's highest")

	_, err = sp.Modify(ctx, batch(&read, set("a", 3)))
	require.ErrorIs(t, err, space.ErrPreconditionFailed, "the dataset moved past the older read")
	a, err := sp.Query(objId, wmDataset).Filter(`{"id": "a"}`).One(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, a.GetInt("n"), "a refused write writes nothing")
	_, err = sp.Query(objId, wmDataset).Filter(`{"id": "b"}`).One(ctx)
	require.ErrorIs(t, err, space.ErrNotFound, "the delete rode the conditional batch")

	_, err = sp.Modify(ctx, batch(&res.ApplySeq, set("a", 3)))
	require.NoError(t, err, "a write's ApplySeq is the next write's precondition")

	_, err = sp.Modify(ctx, space.ModifyBatch{ObjectId: objId, Dataset: "objects", IfUnchangedSince: &read,
		Records: []space.RecordModify{{Id: objId, Ops: []space.Op{{Type: space.OpSet, Path: "x", Value: 1}}}}})
	require.ErrorIs(t, err, handler.ErrValidation, "the shared objects dataset takes no precondition")
}

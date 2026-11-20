package anysyncsdk

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// ReadState().UnreadSince pages what became unread on this device, in
// that order, across the space's objects. Offline, single account: the
// `agent` stamp makes the account's own messages count unread.
func TestReadTracking_UnreadSince(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cfg := rsConfig(t, dataDir)
	sdk, err := Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "RS"})
	require.NoError(t, err)
	spaceId := sp.Id()
	chatA, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: rsType})
	require.NoError(t, err)
	chatB, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: rsType})
	require.NoError(t, err)

	send := func(sp space.Space, objId, id string) {
		t.Helper()
		_, err := sp.Modify(ctx, space.ModifyBatch{
			ObjectId: objId, Dataset: rsDataset,
			Records: []space.RecordModify{{Id: id, Upsert: true, Ops: []space.Op{
				{Type: space.OpSet, Path: "text", Value: id},
				{Type: space.OpSet, Path: rsAgent, Value: true},
			}}},
		})
		require.NoError(t, err)
	}
	since := func(sp space.Space, cur space.ReadCursor, limit int) ([]string, space.ReadCursor) {
		t.Helper()
		changes, next, err := sp.ReadState().UnreadSince(ctx, cur, limit)
		require.NoError(t, err)
		ids := make([]string, 0, len(changes))
		var last uint64
		for _, ch := range changes {
			require.Greater(t, ch.StateSeq, last, "ascending by StateSeq")
			last = ch.StateSeq
			ids = append(ids, ch.RecordIds...)
		}
		return ids, next
	}

	// Unread before the first call is not news: the zero cursor starts
	// at the present.
	send(sp, chatA, "old")
	ids, start := since(sp, space.ReadCursor{}, 0)
	require.Empty(t, ids)
	gen, err := sp.ReadState().Generation(ctx)
	require.NoError(t, err)
	require.Equal(t, gen, start.Generation)

	send(sp, chatA, "a1")
	send(sp, chatB, "b1")
	send(sp, chatA, "a2")
	ids, next := since(sp, start, 0)
	require.Equal(t, []string{"a1", "b1", "a2"}, ids, "arrival order across objects")
	ids, same := since(sp, next, 0)
	require.Empty(t, ids)
	require.Equal(t, next, same, "an empty page keeps the cursor")

	// Paging on the returned cursor.
	ids, page := since(sp, start, 2)
	require.Equal(t, []string{"a1", "b1"}, ids)
	ids, page = since(sp, page, 2)
	require.Equal(t, []string{"a2"}, ids)
	require.Equal(t, next, page)

	// A cursor from another generation starts at the present too.
	ids, fresh := since(sp, space.ReadCursor{Generation: "stale", StateSeq: 1}, 0)
	require.Empty(t, ids)
	require.Equal(t, gen, fresh.Generation)
	ids, _ = since(sp, fresh, 0)
	require.Empty(t, ids)

	// Read changes drop out; the rest keep their place.
	require.NoError(t, sp.ReadState().MarkReadUpTo(ctx, chatA, ""))
	ids, _ = since(sp, start, 0)
	require.Equal(t, []string{"b1"}, ids)

	// A restart keeps the axis; a handler-version rebuild re-applies the
	// records and adds nothing.
	applySeq := func(sp space.Space, id string) int {
		t.Helper()
		docs, err := sp.Query(chatB, rsDataset).All(ctx)
		require.NoError(t, err)
		for _, d := range docs {
			if string(d.GetStringBytes("id")) == id {
				return d.GetInt("_applySeq")
			}
		}
		return 0
	}
	applied := applySeq(sp, "b1")
	require.NoError(t, sdk.Close())
	cfg.Types[0].Datasets[0].HandlerVersion = 2
	sdk, err = Open(ctx, cfg, wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	sp, err = sdk.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)
	require.Greater(t, applySeq(sp, "b1"), applied, "the rebuild re-applied the record")
	ids, _ = since(sp, start, 0)
	require.Equal(t, []string{"b1"}, ids)
	ids, _ = since(sp, next, 0)
	require.Empty(t, ids, "nothing became unread across the restart and rebuild")

	send(sp, chatB, "b2")
	ids, _ = since(sp, next, 0)
	require.Equal(t, []string{"b2"}, ids)
}

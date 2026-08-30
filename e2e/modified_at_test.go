package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/space"
)

// receiveRecordFor drains the subscription until an event carries a
// SubRecord for id (the space-wide objects subscription also sees
// unrelated rows, e.g. background type registration). Nil on timeout.
func receiveRecordFor(t *testing.T, sub space.QuerySubscription, id string, timeout time.Duration) (*space.SubRecord, string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, ""
		}
		ctx, cancel := context.WithTimeout(context.Background(), remaining)
		ev, err := sub.Events().WaitOne(ctx)
		cancel()
		if err != nil {
			return nil, ""
		}
		if rec, where := subRecordFor(ev, id); rec != nil {
			return rec, where
		}
	}
}

// TestE2E_ModifiedAt_BumpsOnDatasetWrite: the objects row's modifiedAt
// is the object's recency mark — every synced write to one of the
// object's datasets moves it, not only writes to the row itself, and
// a live `objects` subscription sees each bump as an update of the
// row. A fresh object's first write also moves the stamp through the
// type attach, so the assertion is that every later write moves it
// too — and that a subscriber on the dataset itself still gets its own
// event.
func TestE2E_ModifiedAt_BumpsOnDatasetWrite(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Types:   []handler.Type{newNotesType()},
	}, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "ModifiedAt"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Spaces().Create: %v", err)
	}

	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{"notes-type"}})
	require.NoError(t, err)

	modifiedAt := func() int64 {
		row, qerr := sp.QueryObjects().Filter(map[string]any{"id": objId}).One(ctx)
		require.NoError(t, qerr)
		require.NotNil(t, row, "objects row present")
		v := row.Get("modifiedAt")
		require.NotNil(t, v, "modifiedAt present")
		require.Equal(t, anyenc.TypeDateTime, v.Type())
		ms, merr := v.DateTimeMillis()
		require.NoError(t, merr)
		assert.Equal(t, sdk.Account().Id(), row.GetString("modifiedBy"), "modifiedBy names this account")
		return ms
	}

	res, err := sp.QueryObjects().Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = res.Sub.Close() })
	notesSub, err := sp.Query(objId, notesDataset).Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = notesSub.Sub.Close() })

	writeNote := func(text string) {
		_, werr := sp.Modify(ctx, space.ModifyBatch{
			ObjectId: objId,
			Dataset:  notesDataset,
			Records: []space.RecordModify{{
				Id:     "rec-1",
				Upsert: true,
				Ops:    []space.Op{{Type: space.OpSet, Path: "text", Value: text}},
			}},
		})
		require.NoError(t, werr)
	}

	// Change timestamps have second resolution: space the writes out
	// so each stamp is distinguishable from the previous one.
	before := modifiedAt()
	for i, text := range []string{"first", "second", "third"} {
		time.Sleep(1100 * time.Millisecond)
		writeNote(text)

		after := modifiedAt()
		assert.Greater(t, after, before, "dataset write %d must move modifiedAt", i+1)
		before = after

		rec, where := receiveRecordFor(t, res.Sub, objId, 3*time.Second)
		require.NotNil(t, rec, "objects subscription must see the row after dataset write %d", i+1)
		assert.Equal(t, "updated", where)
		stamps := map[string]int{}
		for _, op := range rec.Ops {
			if len(op.Path) == 1 {
				stamps[op.Path[0]]++
			}
		}
		assert.Equal(t, map[string]int{"modifiedAt": 1, "modifiedBy": 1}, stamps, "update carries exactly one op per stamp")
		require.NotNil(t, rec.Doc)
		ms, merr := rec.Doc.Get("modifiedAt").DateTimeMillis()
		require.NoError(t, merr)
		assert.Equal(t, after, ms, "event Doc carries the post-apply stamp")
		assert.Equal(t, sdk.Account().Id(), rec.Doc.GetString("modifiedBy"))

		noteRec, _ := receiveRecordFor(t, notesSub.Sub, "rec-1", 3*time.Second)
		require.NotNil(t, noteRec, "the dataset's own subscriber sees write %d", i+1)
		assert.Equal(t, text, noteRec.Doc.GetString("text"))
	}

	// The notes record itself carries no object-level stamps.
	note, err := sp.Query(objId, notesDataset).Filter(map[string]any{"id": "rec-1"}).One(ctx)
	require.NoError(t, err)
	require.NotNil(t, note)
	assert.Nil(t, note.Get("modifiedAt"))
	assert.Nil(t, note.Get("modifiedBy"))
}

// TestE2E_ModifiedAt_ConvergesAcrossDevices: the stamp travels with the
// dataset change — a second device replaying device A's dataset writes
// bumps its own objects row, and its live `objects` subscription sees
// the row updated with the converged stamp.
func TestE2E_ModifiedAt_ConvergesAcrossDevices(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("two-device e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	providerA := newFixedSeedProvider(t)
	providerB := sameAccountFreshDevice(t, providerA)

	sdkA, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Types:   []handler.Type{newNotesType()},
	}, providerA)
	require.NoError(t, err, "device A: Open")
	t.Cleanup(func() { _ = sdkA.Close() })

	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "ModifiedAtSync"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("device A: Spaces().Create: %v", err)
	}
	spaceId := spA.Id()
	objId, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{"notes-type"}})
	require.NoError(t, err)

	modifiedAt := func(sp space.Space) (int64, bool) {
		row, qerr := sp.QueryObjects().Filter(map[string]any{"id": objId}).One(ctx)
		if qerr != nil || row == nil || row.Get("modifiedAt") == nil {
			return 0, false
		}
		if row.GetString("modifiedBy") != sdkA.Account().Id() {
			return 0, false
		}
		ms, merr := row.Get("modifiedAt").DateTimeMillis()
		return ms, merr == nil
	}
	writeNote := func(text string) {
		_, werr := spA.Modify(ctx, space.ModifyBatch{
			ObjectId: objId,
			Dataset:  notesDataset,
			Records: []space.RecordModify{{
				Id:     "rec-1",
				Upsert: true,
				Ops:    []space.Op{{Type: space.OpSet, Path: "text", Value: text}},
			}},
		})
		require.NoError(t, werr)
	}
	writeNote("seed")

	sdkB, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Types:   []handler.Type{newNotesType()},
	}, providerB)
	require.NoError(t, err, "device B: Open")
	t.Cleanup(func() { _ = sdkB.Close() })

	var spB space.Space
	require.Eventually(t, func() bool {
		list, lerr := sdkB.Spaces().List(ctx)
		if lerr != nil {
			return false
		}
		for _, info := range list {
			if info.Id == spaceId {
				spB, lerr = sdkB.Spaces().Get(ctx, spaceId)
				return lerr == nil
			}
		}
		return false
	}, 3*time.Minute, 3*time.Second, "device B: space never appeared")

	// B's cold restore lands the row with the seed write's stamp.
	seedA, ok := modifiedAt(spA)
	require.True(t, ok)
	require.Eventually(t, func() bool {
		_ = spB.SyncHeads(ctx)
		got, ok := modifiedAt(spB)
		return ok && got == seedA
	}, 2*time.Minute, 3*time.Second, "device B: row never converged the seed stamp")

	resB, err := spB.QueryObjects().Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = resB.Sub.Close() })

	time.Sleep(1100 * time.Millisecond)
	writeNote("second")
	time.Sleep(1100 * time.Millisecond)
	writeNote("third")
	finalA, ok := modifiedAt(spA)
	require.True(t, ok)
	require.Greater(t, finalA, seedA)

	require.Eventually(t, func() bool {
		_ = spB.SyncHeads(ctx)
		got, ok := modifiedAt(spB)
		return ok && got == finalA
	}, 2*time.Minute, 3*time.Second, "device B: row never converged the dataset writes' stamp")

	// The inbound batch reaches B's objects subscription as an update
	// of the row carrying the converged stamp (one event per batch,
	// however many changes it replayed).
	deadline := time.Now().Add(30 * time.Second)
	for {
		rec, where := receiveRecordFor(t, resB.Sub, objId, time.Until(deadline))
		require.NotNil(t, rec, "device B: objects subscription never saw the row update")
		assert.Equal(t, "updated", where)
		ms, merr := rec.Doc.Get("modifiedAt").DateTimeMillis()
		require.NoError(t, merr)
		if ms == finalA {
			break
		}
	}
}

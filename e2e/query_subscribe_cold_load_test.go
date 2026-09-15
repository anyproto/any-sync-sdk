package e2e

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_QuerySubscribe_ColdObjectLoad: a windowed per-object Subscribe
// on an object this device has never materialised, while another live
// subscription already arms the space's engine. The object load (remote
// fetch + replay of every stored change through the apply hook) has to
// run before the engine lock: a load started or joined from inside the
// snapshot callback deadlocks the engine against its own OnApply and
// freezes the whole space.
//
// Device A finishes writing before device B opens, so the tree reaches
// B in one piece and its first load replays every change. B subscribes
// the instant its space loads: headsync's first round has just started
// pulling the tree, so the Subscribe either starts that load itself or
// joins it in flight. A hang here is the regression; a deadlocked
// device B is left unclosed (its engine lock never frees, so Close
// would wedge too) and the test reports instead.
func TestE2E_QuerySubscribe_ColdObjectLoad(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("cold-load e2e is slow (~1min); rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	providerA := newFixedSeedProvider(t)
	providerB := sameAccountFreshDevice(t, providerA)

	// ---- Device A: one object, one change per record ----
	sdkA, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Types:   []handler.Type{newNotesType()},
	}, providerA)
	require.NoError(t, err, "device A: Open")
	t.Cleanup(func() { _ = sdkA.Close() })

	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "ColdLoad"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("device A: Spaces().Create: %v", err)
	}
	spaceId := spA.Id()

	objId, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Type: "notes-type"})
	require.NoError(t, err, "device A: Objects().Create")
	// Separate Modify calls: one DAG change each, so device B's cold load
	// replays them one by one through the apply hook.
	const records = 200
	for i := 0; i < records; i++ {
		_, err = spA.Modify(ctx, space.ModifyBatch{
			ObjectId: objId,
			Dataset:  notesDataset,
			Records: []space.RecordModify{{
				Id:     fmt.Sprintf("rec-%03d", i),
				Upsert: true,
				Ops:    []space.Op{{Type: space.OpSet, Path: "text", Value: fmt.Sprintf("note %03d", i)}},
			}},
		})
		require.NoError(t, err, "device A: seed record %d", i)
	}
	_ = sdkA.Spaces().SyncSpaceList(ctx)
	require.NoError(t, spA.SyncHeads(ctx), "device A: push the tree to the node")

	// ---- Device B: fresh DB, same account ----
	sdkB, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Types:   []handler.Type{newNotesType()},
	}, providerB)
	require.NoError(t, err, "device B: Open")
	hung := false
	t.Cleanup(func() {
		if !hung {
			_ = sdkB.Close()
		}
	})

	// Tight poll on Get: the subscribe below has to follow the space
	// load within milliseconds, ahead of headsync's object pull. The
	// tech-space kick is throttled; only the Get needs the cadence.
	var spB space.Space
	polls := 0
	require.True(t, waitFor(ctx, 90*time.Second, 10*time.Millisecond, func() bool {
		spB, err = sdkB.Spaces().Get(ctx, spaceId)
		if err != nil && polls%25 == 0 {
			_ = sdkB.Spaces().SyncSpaceList(ctx)
		}
		polls++
		return err == nil
	}), "device B: Spaces().Get(%s) never succeeded: %v", spaceId, err)

	// A live subscription arms the apply hook; with none registered
	// OnApply returns before touching the engine lock.
	sharedRes, err := spB.QueryObjects().Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err, "device B: QueryObjects().Subscribe")
	t.Cleanup(func() {
		if !hung {
			_ = sharedRes.Sub.Close()
		}
	})

	// The subscribe under test. Device B has never materialised the
	// object: Store.Get fetches the tree (or joins headsync's load of it)
	// and replays every change.
	type outcome struct {
		res *space.QueryResult
		err error
	}
	// A joined load can surface the loader's own cancellation (ocache
	// shares the first caller's ctx); that is a retry, not a verdict.
	var got outcome
	for attempt := 0; ; attempt++ {
		done := make(chan outcome, 1)
		go func() {
			res, err := spB.Query(objId, notesDataset).
				Sort("text").Limit(10).
				Subscribe(ctx, space.QueryOpts{IncludeTotal: true})
			done <- outcome{res, err}
		}()
		select {
		case got = <-done:
		case <-time.After(60 * time.Second):
			hung = true
			t.Fatalf("device B: per-object Subscribe never returned — object load deadlocked against the subscribe engine")
		}
		if errors.Is(got.err, context.Canceled) && ctx.Err() == nil && attempt < 3 {
			continue
		}
		break
	}
	require.NoError(t, got.err, "device B: Query(obj, notes).Subscribe")
	t.Cleanup(func() { _ = got.res.Sub.Close() })
	t.Logf("device B: cold-load subscribe returned initial=%d total=%d", len(got.res.Initial), got.res.Total)

	// The fetched tree is whatever the node held at that moment; changes
	// still in flight from device A land through the push path as live
	// events. The window is bounded by the limit either way.
	assert.LessOrEqual(t, len(got.res.Initial), 10, "window must not exceed the limit")
	require.True(t, waitFor(ctx, 30*time.Second, 50*time.Millisecond, func() bool {
		n, err := spB.Query(objId, notesDataset).Count(ctx)
		return err == nil && n == records
	}), "device B never converged to %d records", records)

	// The engine lock is free again: a write inside the window applies
	// and reaches the new subscription. Matched on content, so Added
	// events queued by a late fill of the window cannot stand in for it.
	const edited = "note 000 edited"
	_, err = spB.Modify(ctx, space.ModifyBatch{
		ObjectId: objId,
		Dataset:  notesDataset,
		Records: []space.RecordModify{{
			Id:  "rec-000",
			Ops: []space.Op{{Type: space.OpSet, Path: "text", Value: edited}},
		}},
	})
	require.NoError(t, err, "device B: write after cold-load subscribe")
	require.True(t, waitFor(ctx, 5*time.Second, 0, func() bool {
		ev := receiveOne(t, got.res.Sub, 5*time.Second)
		rec, _ := subRecordFor(ev, "rec-000")
		return rec != nil && rec.Doc != nil && rec.Doc.GetString("text") == edited
	}), "windowed sub never delivered the post-subscribe write")
}

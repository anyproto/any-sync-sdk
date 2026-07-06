package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/cheggaaa/mb/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/space"
)

// receiveOne blocks until one SubscriptionEvent arrives on sub or
// the timeout fires. Fails the test on timeout.
func receiveOne(t *testing.T, sub space.QuerySubscription, timeout time.Duration) space.SubscriptionEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ev, err := sub.Events().WaitOne(ctx)
	if err != nil {
		t.Fatalf("WaitOne: %v", err)
	}
	return ev
}

// assertNoSubEvent asserts the mailbox stays quiet for window. Short
// windows are fine — engine.OnApply runs synchronously inside
// afterApply, so anything that was going to land has by the time the
// trigger write returns.
func assertNoSubEvent(t *testing.T, sub space.QuerySubscription, window time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	msgs, err := sub.Events().Wait(ctx)
	if errors.Is(err, context.DeadlineExceeded) {
		return
	}
	if errors.Is(err, mb.ErrClosed) {
		t.Fatalf("mailbox unexpectedly closed during quiet window")
	}
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if len(msgs) > 0 {
		t.Fatalf("expected no event, got %+v", msgs)
	}
}

// subRecordFor finds the SubRecord with the given id across Added /
// Updated. Returns (nil, "") when absent.
func subRecordFor(ev space.SubscriptionEvent, id string) (*space.SubRecord, string) {
	for i := range ev.Added {
		if ev.Added[i].Id == id {
			return &ev.Added[i], "added"
		}
	}
	for i := range ev.Updated {
		if ev.Updated[i].Id == id {
			return &ev.Updated[i], "updated"
		}
	}
	return nil, ""
}

// TestSDK_QuerySubscribe_RoutingAndIsolation walks the live-query
// subscription contract: a shared-objects subscription receives every
// property change in the space; a per-(objectId, "objects")
// subscription receives only events for that exact object;
// mismatched (wrong object, wrong dataset) subscriptions stay silent;
// closing one subscription leaves the others working.
func TestSDK_QuerySubscribe_RoutingAndIsolation(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Sub"})
	require.NoError(t, err)

	typeId, titleProp := setupMovieType(t, ctx, sp)

	objectId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		Types: []string{typeId},
	})
	require.NoError(t, err)

	// All setup writes above happened before any subscriber was
	// registered — the engine's HasSubscribers gate skipped them, so
	// nothing is queued.

	sharedRes, err := sp.QueryObjects().Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sharedRes.Sub.Close() })

	explicitRes, err := sp.Query(objectId, "objects").Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = explicitRes.Sub.Close() })

	// Wrong-object subscription on same dataset — must stay silent
	// when objectId's properties change.
	wrongObjRes, err := sp.Query("no-such-object", "objects").Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = wrongObjRes.Sub.Close() })

	// Wrong-dataset subscription on right object — must stay silent
	// when we write to `objects` only.
	wrongDsRes, err := sp.Query(objectId, "no-such-dataset").Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = wrongDsRes.Sub.Close() })

	// Trigger one apply on the `objects` dataset.
	_, err = sp.Properties().Set(ctx, objectId, typeId, map[string]any{
		titleProp: "Casablanca",
	})
	require.NoError(t, err)

	sharedEv := receiveOne(t, sharedRes.Sub, 2*time.Second)
	assert.NotEmpty(t, sharedEv.VersionId, "VersionId must be populated on the wire")
	rec, where := subRecordFor(sharedEv, objectId)
	require.NotNil(t, rec, "shared sub missed the property write (in: %s)", where)
	assertTitle(t, rec, typeId, titleProp, "Casablanca")

	explicitEv := receiveOne(t, explicitRes.Sub, 2*time.Second)
	assert.Equal(t, sharedEv.VersionId, explicitEv.VersionId,
		"shared and explicit subs must see the same VersionId for one change")
	rec2, _ := subRecordFor(explicitEv, objectId)
	require.NotNil(t, rec2)
	assertTitle(t, rec2, typeId, titleProp, "Casablanca")

	// Mismatched subs received nothing.
	assertNoSubEvent(t, wrongObjRes.Sub, 100*time.Millisecond)
	assertNoSubEvent(t, wrongDsRes.Sub, 100*time.Millisecond)

	// Closing one sub must not break the others. shared stops; a
	// subsequent write still feeds explicit.
	require.NoError(t, sharedRes.Sub.Close())

	_, err = sp.Properties().Set(ctx, objectId, typeId, map[string]any{
		titleProp: "Vertigo",
	})
	require.NoError(t, err)

	explicitEv2 := receiveOne(t, explicitRes.Sub, 2*time.Second)
	rec3, _ := subRecordFor(explicitEv2, objectId)
	require.NotNil(t, rec3)
	assertTitle(t, rec3, typeId, titleProp, "Vertigo")

	// Closed subscription's mailbox returns ErrClosed on subsequent Wait.
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer closeCancel()
	_, err = sharedRes.Sub.Events().Wait(closeCtx)
	assert.ErrorIs(t, err, mb.ErrClosed, "shared sub mailbox should be closed after Close")
	assert.NoError(t, sharedRes.Sub.Err(), "user-initiated Close must leave Err() nil")
}

// TestSDK_QuerySubscribe_CreateEmitsAutoFields catches the regression
// where a freshly-created record's auto-stamped fields (author /
// createdAt / spaceId) never reached subscribers. The dispatcher used
// to project only ch.Records[i].Ops, missing every field the apply
// layer stamped via sink.Derive. A viewer must see no difference
// between user-supplied and auto fields — same wire — so a fresh row
// reconstructs in one event.
func TestSDK_QuerySubscribe_CreateEmitsAutoFields(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "AutoFields"})
	require.NoError(t, err)

	typeId, titleProp := setupMovieType(t, ctx, sp)

	// Subscribe BEFORE creating the object so the create-time write
	// into the per-space `objects` dataset arrives as an Added event.
	res, err := sp.QueryObjects().Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = res.Sub.Close() })

	objectId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		Types: []string{typeId},
	})
	require.NoError(t, err)

	ev := receiveOne(t, res.Sub, 2*time.Second)

	rec, where := subRecordFor(ev, objectId)
	require.NotNil(t, rec, "create event has no SubRecord for objectId")
	assert.Equal(t, "added", where, "create on a fresh row must arrive as Added (not Updated)")
	require.NotNil(t, rec.Doc, "Added SubRecord must carry the post-apply doc")

	// SystemPropertiesHandler.BeforeCreate stamps these via sink.Derive;
	// they must be present on Doc so consumers see a fully-formed record.
	assert.NotNil(t, rec.Doc.Get("author"),
		"create Doc missing author — viewer can't tell who created the record")
	assert.NotNil(t, rec.Doc.Get("createdAt"),
		"create Doc missing createdAt — viewer can't sort/display creation time")
	assert.NotNil(t, rec.Doc.Get("spaceId"),
		"create Doc missing spaceId — viewer can't route across spaces")

	// VersionId on the wire carries the per-change DAG order; required
	// for fence-and-replay consumers.
	assert.NotEmpty(t, ev.VersionId)

	// Now write a property value on the SAME row. This is an UPDATE
	// (BeforeCreate doesn't fire on an existing row); the SubRecord
	// arrives in Updated, not Added.
	_, err = sp.Properties().Set(ctx, objectId, typeId, map[string]any{
		titleProp: "Casablanca",
	})
	require.NoError(t, err)

	upd := receiveOne(t, res.Sub, 2*time.Second)
	urec, uwhere := subRecordFor(upd, objectId)
	require.NotNil(t, urec)
	assert.Equal(t, "updated", uwhere, "second write on the same row must arrive as Updated, not Added")
	assertTitle(t, urec, typeId, titleProp, "Casablanca")

	// Auto stamps must still be present on the Updated Doc (they're
	// persisted on the row), but they should NOT appear in Ops (no
	// re-stamp on update).
	assert.NotNil(t, urec.Doc.Get("author"))
	for _, op := range urec.Ops {
		assert.NotEqual(t, []string{"author"}, op.Path, "update must not re-emit author op")
		assert.NotEqual(t, []string{"createdAt"}, op.Path, "update must not re-emit createdAt op")
	}
}

// TestSDK_QuerySubscribe_DeleteEmitsRemoved covers object deletion end
// to end: Objects.Delete writes a CRDT delete op on the object's
// `objects` record before tearing down the any-sync tree. Subscribers
// see a Removed entry for the row, and a follow-up QueryObjects no
// longer returns it.
func TestSDK_QuerySubscribe_DeleteEmitsRemoved(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "DeleteSub"})
	require.NoError(t, err)

	typeId, _ := setupMovieType(t, ctx, sp)

	objectId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		Types: []string{typeId},
	})
	require.NoError(t, err)

	// Subscribe AFTER Create so the create-time writes are gated out by
	// HasSubscribers — the only event we expect is the delete (as a
	// Removed). The initial snapshot DOES include the row.
	res, err := sp.QueryObjects().Subscribe(ctx, space.QueryOpts{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = res.Sub.Close() })
	require.NotEmpty(t, res.Initial, "Initial must include the just-created object")

	require.NoError(t, sp.Objects().Delete(ctx, objectId))

	ev := receiveOne(t, res.Sub, 2*time.Second)
	assert.Contains(t, ev.Removed, space.RemovedRecord{Id: objectId, Reason: space.RemoveDeleted},
		"delete must emit Removed for the objectId with RemoveDeleted")
	assert.Empty(t, ev.Added)
	assert.Empty(t, ev.Updated)

	// The deleted object is gone from QueryObjects too — its row was
	// purged from local state (no tombstone is retained).
	rows, err := sp.QueryObjects().All(ctx)
	require.NoError(t, err)
	for _, row := range rows {
		if id := row.Get(crdt.IdField); id != nil {
			assert.NotEqual(t, objectId, string(id.GetStringBytes()),
				"deleted object still returned by QueryObjects")
		}
	}
}

// assertTitle reads the title property off rec.Doc at the canonical
// path and asserts it equals want.
func assertTitle(t *testing.T, rec *space.SubRecord, typeId, titleProp, want string) {
	t.Helper()
	require.NotNil(t, rec.Doc)
	v := rec.Doc.Get(typeId, titleProp)
	require.NotNil(t, v, "doc has no %s.%s: %s", typeId, titleProp, rec.Doc.String())
	assert.Equal(t, want, string(v.GetStringBytes()))
}

// kept to silence "declared but not used" on anyenc when future tests
// inspect raw values — used by helpers below.
var _ = anyenc.TypeObject

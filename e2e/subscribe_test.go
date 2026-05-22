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

// mustReceive blocks until one event arrives on the subscription's
// mailbox or the timeout fires. Fails the test on timeout.
func mustReceive(t *testing.T, sub space.Subscription, timeout time.Duration) space.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ev, err := sub.Mailbox().WaitOne(ctx)
	if err != nil {
		t.Fatalf("WaitOne: %v", err)
	}
	return ev
}

// assertNoEvent asserts the mailbox stays quiet for window. A short
// window is fine — the dispatcher fires synchronously inside
// AfterApply, so anything that was going to land has by the time the
// trigger write returns.
func assertNoEvent(t *testing.T, sub space.Subscription, window time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	msgs, err := sub.Mailbox().Wait(ctx)
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

// TestSDK_Subscribe walks the v1 subscription contract end-to-end:
// SubscribeProperties is a per-space firehose; Subscribe is an
// explicit (objectId, dataset) listener; mismatched pairs receive
// nothing; closing one sub leaves the others working.
func TestSDK_Subscribe(t *testing.T) {
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

	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Movie"})
	require.NoError(t, err)
	titleProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title",
		XKey: "title",
		Kind: space.PropertyKindString,
	})
	require.NoError(t, err)

	objectId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		Types: []string{typeId},
	})
	require.NoError(t, err)

	// All setup writes above happened with no subscribers in place,
	// so the dispatcher's HasSubscribers gate skipped them. Nothing
	// is queued.

	propsSub, err := sp.SubscribeProperties(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = propsSub.Close() })

	explicitSub, err := sp.Subscribe(ctx, objectId, "objects")
	require.NoError(t, err)
	t.Cleanup(func() { _ = explicitSub.Close() })

	// Wrong-object subscription on same dataset — must stay silent
	// when objectId's properties change.
	wrongObjSub, err := sp.Subscribe(ctx, "no-such-object", "objects")
	require.NoError(t, err)
	t.Cleanup(func() { _ = wrongObjSub.Close() })

	// Wrong-dataset subscription on right object — must stay silent
	// when we write to `objects` only.
	wrongDsSub, err := sp.Subscribe(ctx, objectId, "no-such-dataset")
	require.NoError(t, err)
	t.Cleanup(func() { _ = wrongDsSub.Close() })

	// Argument validation — empty objectId / dataset rejected.
	_, err = sp.Subscribe(ctx, "", "objects")
	assert.Error(t, err)
	_, err = sp.Subscribe(ctx, objectId, "")
	assert.Error(t, err)

	// Trigger one apply on the `objects` dataset.
	_, err = sp.Properties().SetBase(ctx, objectId, typeId, map[string]any{
		titleProp: "Casablanca",
	})
	require.NoError(t, err)

	propsEv := mustReceive(t, propsSub, 2*time.Second)
	assert.Equal(t, sp.Id(), propsEv.SpaceId)
	assert.Equal(t, objectId, propsEv.ObjectId)
	assert.Equal(t, "objects", propsEv.Dataset)
	// Wire-shape contract: every event carries the per-change
	// VersionId and the projected $set/$unset records. The property
	// firehose must produce the same shape as an explicit subscription
	// — same change, same projection, just different routing.
	assert.NotEmpty(t, propsEv.VersionId, "VersionId must be populated")
	assertProjectedSet(t, propsEv, objectId, []string{typeId, titleProp}, "Casablanca")

	explicitEv := mustReceive(t, explicitSub, 2*time.Second)
	assert.Equal(t, objectId, explicitEv.ObjectId)
	assert.Equal(t, "objects", explicitEv.Dataset)
	assert.Equal(t, propsEv.VersionId, explicitEv.VersionId,
		"explicit and firehose subs must see the same VersionId for one change")
	assertProjectedSet(t, explicitEv, objectId, []string{typeId, titleProp}, "Casablanca")

	// Mismatched subs received nothing.
	assertNoEvent(t, wrongObjSub, 100*time.Millisecond)
	assertNoEvent(t, wrongDsSub, 100*time.Millisecond)

	// Closing one sub must not break the others. propsSub stops; a
	// subsequent write still feeds explicitSub.
	require.NoError(t, propsSub.Close())

	_, err = sp.Properties().SetBase(ctx, objectId, typeId, map[string]any{
		titleProp: "Vertigo",
	})
	require.NoError(t, err)

	explicitEv2 := mustReceive(t, explicitSub, 2*time.Second)
	assert.Equal(t, objectId, explicitEv2.ObjectId)

	// Wire-shape on the second write reaches explicitSub with the
	// new value — confirms the projection still tracks post-apply
	// state across multiple changes on the same record.
	assertProjectedSet(t, explicitEv2, objectId, []string{typeId, titleProp}, "Vertigo")

	// Closed subscription's mailbox must return ErrClosed.
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer closeCancel()
	_, err = propsSub.Mailbox().Wait(closeCtx)
	assert.ErrorIs(t, err, mb.ErrClosed, "propsSub.Mailbox() should be closed after Close")
}

// assertProjectedSet checks that ev carries a record (id = rowId)
// with a $set whose effective target is `path` and whose value
// string-equals wantValue. Handles both wire forms the SDK ships:
//
//   - single-field: Op{Type=$set, Path=path, Payload=value}
//   - multi-field:  Op{Type=$set, Path=[], Payload={"a.b.c": value, ...}}
//
// Tolerates extra ops the SDK may emit (auto-stamps like createdAt /
// updatedAt) — we only require the user-visible (path, value) to be
// present.
func assertProjectedSet(t *testing.T, ev space.Event, rowId string, path []string, wantValue string) {
	t.Helper()
	if len(ev.Records) == 0 {
		t.Fatalf("event has no records: %+v", ev)
	}
	var rec *space.EventRecord
	for i := range ev.Records {
		if ev.Records[i].Id == rowId {
			rec = &ev.Records[i]
			break
		}
	}
	if rec == nil {
		t.Fatalf("no record for id %s in event: %+v", rowId, ev.Records)
	}
	dotted := joinPath(path)
	for _, op := range rec.Ops {
		if op.Type != crdt.OpSet {
			continue
		}
		val := opValueForPath(op, path, dotted)
		if val == nil {
			continue
		}
		got := string(val.GetStringBytes())
		if got == wantValue {
			return
		}
		t.Fatalf("payload for path %v = %q, want %q", path, got, wantValue)
	}
	t.Fatalf("no $set %v=%q op in record %s: %+v", path, wantValue, rowId, rec.Ops)
}

// opValueForPath resolves the effective value an op writes at the
// requested path, accounting for the multi-field form. Returns nil
// when the op doesn't target this path.
func opValueForPath(op space.EventOp, path []string, dotted string) *anyenc.Value {
	if len(op.Path) > 0 {
		if pathsEqual(op.Path, path) {
			return op.Payload
		}
		return nil
	}
	// Multi-field form: payload is an object keyed by dotted paths.
	if op.Payload == nil {
		return nil
	}
	return op.Payload.Get(dotted)
}

func joinPath(p []string) string {
	if len(p) == 0 {
		return ""
	}
	out := p[0]
	for _, seg := range p[1:] {
		out += "." + seg
	}
	return out
}

func pathsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// collectSetPaths flattens every $set in the EventRecord for rowId
// into a dotted-path set. Handles both wire shapes — single-field
// (Path=[…], Payload=value) and multi-field (Path=[], Payload=object
// keyed by dotted paths). Used to assert auto-field stamps land on
// the wire on create.
func collectSetPaths(t *testing.T, ev space.Event, rowId string) map[string]struct{} {
	t.Helper()
	out := make(map[string]struct{})
	for i := range ev.Records {
		if ev.Records[i].Id != rowId {
			continue
		}
		for _, op := range ev.Records[i].Ops {
			if op.Type != crdt.OpSet {
				continue
			}
			if len(op.Path) > 0 {
				out[joinPath(op.Path)] = struct{}{}
				continue
			}
			if op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
				continue
			}
			obj, _ := op.Payload.Object()
			obj.Visit(func(k []byte, _ *anyenc.Value) {
				out[string(k)] = struct{}{}
			})
		}
	}
	return out
}

// TestSDK_Subscribe_CreateEmitsAutoFields catches the regression where
// a freshly-created record's auto-stamped fields (author / createdAt /
// spaceId / _ver.id) never reached subscribers. The dispatcher used to
// project only ch.Records[i].Ops, missing every field the apply layer
// stamped via sink.Derive or directly onto the storage value. A viewer
// must see no difference between user-supplied and auto fields — same
// $set shape, same wire — so a fresh row reconstructs in one event.
func TestSDK_Subscribe_CreateEmitsAutoFields(t *testing.T) {
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

	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Movie"})
	require.NoError(t, err)
	titleProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title",
		XKey: "title",
		Kind: space.PropertyKindString,
	})
	require.NoError(t, err)

	// Subscribe BEFORE creating the object so the create-time write
	// into the per-space `objects` dataset fires as a CREATE event
	// (not an UPDATE — that's the path that exercises BeforeCreate
	// stamping and the _ver.id synthetic op).
	propsSub, err := sp.SubscribeProperties(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = propsSub.Close() })

	objectId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		Types: []string{typeId},
	})
	require.NoError(t, err)

	ev := mustReceive(t, propsSub, 2*time.Second)
	assert.Equal(t, objectId, ev.ObjectId)
	assert.Equal(t, "objects", ev.Dataset)

	paths := collectSetPaths(t, ev, objectId)

	// SystemPropertiesHandler.BeforeCreate stamps these via sink.Derive.
	assert.Contains(t, paths, "author",
		"create event missing $set author — viewer can't tell who created the record")
	assert.Contains(t, paths, "createdAt",
		"create event missing $set createdAt — viewer can't sort/display creation time")
	assert.Contains(t, paths, "spaceId",
		"create event missing $set spaceId — viewer can't route across spaces")

	// recordModifier.Modify stamps this directly onto storage; we
	// surface it via a synthetic derived op so it rides the same wire.
	assert.Contains(t, paths, "_ver.id",
		"create event missing $set _ver.id — chat-message-style queries rely on this sort key")

	// Now write a property value on the SAME row. This is an UPDATE
	// (BeforeCreate doesn't fire on an existing row); derived stamps
	// must NOT re-emit, or viewers would see noisy churn on every edit.
	_, err = sp.Properties().SetBase(ctx, objectId, typeId, map[string]any{
		titleProp: "Casablanca",
	})
	require.NoError(t, err)

	upd := mustReceive(t, propsSub, 2*time.Second)
	updPaths := collectSetPaths(t, upd, objectId)
	assert.NotContains(t, updPaths, "author",
		"update must not re-stamp author")
	assert.NotContains(t, updPaths, "createdAt",
		"update must not re-stamp createdAt")
	assert.NotContains(t, updPaths, "spaceId",
		"update must not re-stamp spaceId")
	assert.NotContains(t, updPaths, "_ver.id",
		"update must not re-stamp _ver.id — that marker is creation-only")
	// And the user-supplied write still lands on the wire.
	assertProjectedSet(t, upd, objectId, []string{typeId, titleProp}, "Casablanca")
}

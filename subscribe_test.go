package anysyncsdk_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cheggaaa/mb/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
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
	confPath := filepath.Join("..", "test-etc", "staging.yml")
	yaml, err := os.ReadFile(confPath)
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

	explicitEv := mustReceive(t, explicitSub, 2*time.Second)
	assert.Equal(t, objectId, explicitEv.ObjectId)
	assert.Equal(t, "objects", explicitEv.Dataset)

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

	// Closed subscription's mailbox must return ErrClosed.
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer closeCancel()
	_, err = propsSub.Mailbox().Wait(closeCtx)
	assert.ErrorIs(t, err, mb.ErrClosed, "propsSub.Mailbox() should be closed after Close")
}

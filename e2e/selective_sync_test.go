package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_SelectiveSyncPayloads covers SYN-18 end-to-end: a selective
// participant (modeling the filenode-v2 broker) configured with
// Sync.TreeTypes = ["payloads"] head-syncs a whole space but downloads
// and materializes ONLY the payloads trees.
//
// Device A is a regular full-sync device that builds the space: a user
// type, two content objects, and payloads rows under one of them.
// Device B opens with the same account keys and the selective config:
//
//   - the payloads rows must converge and unseal (selected type);
//   - the content/type trees must NOT materialize — no `objects` rows,
//     skip markers recorded, no local tree storage;
//   - a payloads row registered while B is online must flow through the
//     live head-update path and converge too, while content writes keep
//     being skipped.
//
// The local network's nodes may run an any-sync without the probe flag;
// the client then falls back to aborting each classify-fetch after its
// first batch, so this test exercises the compat path as well.
func TestE2E_SelectiveSyncPayloads(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available at %s: %v", confPath, err)
	}
	if testing.Short() {
		t.Skip("selective-sync e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	provider := newFixedSeedProvider(t)

	// Device A: full sync, builds the space.
	cfgA := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	sdkA, err := anysyncsdk.Open(ctx, cfgA, provider)
	require.NoError(t, err, "device A: Open")
	t.Cleanup(func() { _ = sdkA.Close() })

	sp, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "Selective"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}

	typeId, propId := setupMovieType(t, ctx, sp)
	ownerId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_, err = sp.Properties().Set(ctx, ownerId, typeId, map[string]any{propId: "Owner"})
	require.NoError(t, err)
	otherId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_, err = sp.Properties().Set(ctx, otherId, typeId, map[string]any{propId: "Bystander"})
	require.NoError(t, err)

	pa := payloadsSurface(t, sp)
	inlineBytes := []byte("selective inline file")
	fileIds, objId, err := pa.RegisterFiles(ctx, ownerId, []spaceimpl.RegisterFileOpts{
		{RootCid: "bafyselectiveroot", Size: 4242, Enc: payloads.EncPayload{
			Key:    []byte("wrapped-key-0123456789abcdef0123"),
			Name:   "photo.png",
			SHA256: []byte("0123456789abcdef0123456789abcdef"),
			Mime:   "image/png",
		}},
		{Size: int64(len(inlineBytes)), Enc: payloads.EncPayload{Name: "note.txt", Mime: "text/plain", Inline: inlineBytes}},
	})
	require.NoError(t, err)
	require.Len(t, fileIds, 2)

	// Push heads so device B doesn't race A's periodic sync.
	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = sp.SyncHeads(ctx)

	// Device B: same account, fresh DataDir, selective sync.
	cfgB := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Sync:    config.Sync{TreeTypes: []string{payloads.ChangeType}},
	}
	sdkB, err := anysyncsdk.Open(ctx, cfgB, provider)
	require.NoError(t, err, "device B: Open")
	t.Cleanup(func() { _ = sdkB.Close() })

	// Tech space is exempt from filtering — the space list converges.
	if !waitFor(ctx, 90*time.Second, 250*time.Millisecond, func() bool {
		_ = sdkB.Spaces().SyncSpaceList(ctx)
		list, err := sdkB.Spaces().List(ctx)
		if err != nil {
			return false
		}
		return findSpace(list, sp.Id()) != nil
	}) {
		t.Fatal("device B never saw the space in its tech-space")
	}

	spB, err := sdkB.Spaces().Get(ctx, sp.Id())
	require.NoError(t, err)
	paB := payloadsSurface(t, spB)

	// The payloads rows converge. The child tree can transiently fail
	// with ErrParentNotFound until the owner's heads-only stub lands
	// (same sync round or the next), hence the tolerant wait.
	var rowsB []payloads.Row
	if !waitFor(ctx, 150*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		rowsB, err = paB.ListRows(ctx, ownerId)
		if err != nil && !errors.Is(err, space.ErrNotFound) {
			t.Logf("device B ListRows error: %v", err)
			return false
		}
		return len(rowsB) == 2
	}) {
		t.Fatalf("device B never converged to 2 payloads rows (got %d)", len(rowsB))
	}
	byId := map[string]payloads.Row{}
	for _, r := range rowsB {
		byId[r.Id] = r
	}
	require.Contains(t, byId, fileIds[0])
	require.Contains(t, byId, fileIds[1])
	assert.Equal(t, "bafyselectiveroot", byId[fileIds[0]].RootCid)
	assert.False(t, byId[fileIds[0]].Sealed, "same account must unseal enc")
	assert.Equal(t, inlineBytes, byId[fileIds[1]].Enc.Inline)

	// No content materialized. The space-index object is exempt by
	// construction — every device derives it locally on space open, it
	// never arrives through the filtered fetch path — so the check is
	// "none of A's content trees", not "empty".
	contentIds := []string{ownerId, otherId, typeId}
	assertNoContent := func(when string) {
		t.Helper()
		docs, qerr := spB.QueryObjects().All(ctx)
		require.NoError(t, qerr)
		for _, d := range docs {
			id := d.GetString("id")
			for _, banned := range contentIds {
				assert.NotEqual(t, banned, id,
					"%s: selective device materialized content tree %s", when, banned)
			}
		}
	}
	assertNoContent("after payloads converged")

	// Skip markers recorded for every non-payloads tree we know about.
	svcB, ok := sdkB.Spaces().(*spaceimpl.Service)
	require.True(t, ok, "Spaces() must be the spaceimpl service")
	storeB := svcB.StoreFor(sp.Id())
	for _, id := range []string{ownerId, otherId, typeId} {
		id := id
		if !waitFor(ctx, 60*time.Second, 500*time.Millisecond, func() bool {
			_ = spB.SyncHeads(ctx)
			skipped, serr := storeB.IsTreeSkipped(ctx, id)
			return serr == nil && skipped
		}) {
			t.Fatalf("tree %s never got a skip marker on device B", id)
		}
	}

	// The payloads tree is locally present; the owner tree is not.
	_, err = spB.Debug().Object(ctx, objId)
	require.NoError(t, err, "payloads tree must be materialized on device B")
	_, err = spB.Debug().Object(ctx, ownerId)
	require.Error(t, err, "owner tree must not be loadable on device B")

	// Live phase: A registers another file while B is online — the
	// update flows through the head-update path (PullFilter lets the
	// payloads tree through) and converges on B.
	liveBytes := []byte("live inline file")
	liveId, _, err := pa.RegisterFile(ctx, ownerId, spaceimpl.RegisterFileOpts{
		Size: int64(len(liveBytes)),
		Enc:  payloads.EncPayload{Name: "live.txt", Mime: "text/plain", Inline: liveBytes},
	})
	require.NoError(t, err)
	// A content write in the same breath — must keep being skipped.
	_, err = sp.Properties().Set(ctx, otherId, typeId, map[string]any{propId: "Bystander-2"})
	require.NoError(t, err)
	_ = sp.SyncHeads(ctx)

	if !waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		rows, lerr := paB.ListRows(ctx, ownerId)
		if lerr != nil {
			return false
		}
		for _, r := range rows {
			if r.Id == liveId {
				return true
			}
		}
		return false
	}) {
		t.Fatal("device B never saw the live-registered payloads row")
	}

	// Still no content trees on B.
	assertNoContent("after live phase")
}

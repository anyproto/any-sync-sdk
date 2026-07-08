package e2e

import (
	"bytes"
	"context"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/files/gc"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_FilesV2_Delete proves Files.Delete against a live local
// network: the synced row removal (a second device sees the file
// disappear, in both directions), the variant cascade, the survival of
// unrelated files, and local byte reclamation (the deleted file's CAR
// goes unreferenced and the safety sweep deletes it once past grace).
func TestE2E_FilesV2_Delete(t *testing.T) {
	t.Parallel()
	netYaml, _, _ := loadLocalFilesV2Network(t)
	if testing.Short() {
		t.Skip("files-v2 e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	provider := newFixedSeedProvider(t)

	cfgA := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: netYaml},
	}
	sdkA, err := anysyncsdk.Open(ctx, cfgA, provider)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdkA.Close() })

	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "FilesV2Delete"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}
	typeId, _ := setupMovieType(t, ctx, spA)
	owner1, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	owner2, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = spA.SyncHeads(ctx)

	// Original (S3 tier) + inline thumbnail variant + an unrelated
	// sibling on owner1, plus one file on owner2.
	original := make([]byte, 1_200_000)
	mrand.New(mrand.NewSource(40)).Read(original)
	origInfo, err := spA.Files().Attach(ctx, owner1, bytes.NewReader(original),
		space.AttachOpts{Name: "victim.raw", Mime: "image/x-raw"})
	require.NoError(t, err)
	require.True(t, origInfo.Durable)
	thumbInfo, err := spA.Files().Attach(ctx, owner1, bytes.NewReader([]byte("thumb bytes")),
		space.AttachOpts{Name: "victim-thumb.jpg", Variant: "thumbnail", VariantOf: origInfo.FileId})
	require.NoError(t, err)
	survivor := make([]byte, 300_000)
	mrand.New(mrand.NewSource(41)).Read(survivor)
	survivorInfo, err := spA.Files().Attach(ctx, owner1, bytes.NewReader(survivor),
		space.AttachOpts{Name: "survivor.bin"})
	require.NoError(t, err)
	require.True(t, survivorInfo.Durable)
	otherInfo, err := spA.Files().Attach(ctx, owner2, bytes.NewReader([]byte("other object's file")),
		space.AttachOpts{Name: "other.txt"})
	require.NoError(t, err)
	_ = spA.SyncHeads(ctx)

	// Unknown ids are a typed miss, same as every other per-file verb.
	require.ErrorIs(t, spA.Files().Delete(ctx, "no-such-file"), space.ErrNotFound)

	cacheBefore, err := sdkA.FileCacheSize(ctx)
	require.NoError(t, err)
	require.Positive(t, cacheBefore)

	// Delete the original: the variant goes with it, siblings survive.
	require.NoError(t, spA.Files().Delete(ctx, origInfo.FileId))
	_, err = spA.Files().Get(ctx, origInfo.FileId)
	require.ErrorIs(t, err, space.ErrNotFound)
	_, err = spA.Files().Get(ctx, thumbInfo.FileId)
	require.ErrorIs(t, err, space.ErrNotFound, "variants cascade with their original")
	_, err = spA.Files().Get(ctx, survivorInfo.FileId)
	require.NoError(t, err)
	_, err = spA.Files().Get(ctx, otherInfo.FileId)
	require.NoError(t, err)
	owner1Files, err := spA.Files().List(ctx, space.FileListOpts{ObjectId: owner1})
	require.NoError(t, err)
	require.Len(t, owner1Files, 1)
	stats, err := spA.Files().Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, stats.Total)

	// A second delete of the same id is a miss, not a silent no-op.
	require.ErrorIs(t, spA.Files().Delete(ctx, origInfo.FileId), space.ErrNotFound)

	// Local reclamation: the deleted file's CAR is unreferenced now;
	// the safety sweep deletes it once past grace (shrunk to zero
	// here). The survivor's CAR keeps its bytes.
	prevGrace := gc.UnreferencedGrace
	gc.UnreferencedGrace = 0
	t.Cleanup(func() { gc.UnreferencedGrace = prevGrace })
	require.NoError(t, sdkA.SweepFileCache(ctx))
	cacheAfter, err := sdkA.FileCacheSize(ctx)
	require.NoError(t, err)
	assert.Less(t, cacheAfter, cacheBefore, "the deleted CAR is reclaimed")
	assert.Positive(t, cacheAfter, "the survivor keeps its bytes")

	// Second device: the deletion is a synced fact — B materializes the
	// space with the victim rows already tombstoned.
	_ = spA.SyncHeads(ctx)
	cfgB := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: netYaml},
	}
	sdkB, err := anysyncsdk.Open(ctx, cfgB, provider)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdkB.Close() })
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = sdkB.Spaces().SyncSpaceList(ctx)
		list, err := sdkB.Spaces().List(ctx)
		if err != nil {
			return false
		}
		for _, info := range list {
			if info.Id == spA.Id() {
				return true
			}
		}
		return false
	}), "device B never saw the space")
	spB, err := sdkB.Spaces().Get(ctx, spA.Id())
	require.NoError(t, err)
	require.True(t, waitFor(ctx, 120*time.Second, time.Second, func() bool {
		_ = spB.SyncHeads(ctx)
		if _, err := spB.Files().Get(ctx, survivorInfo.FileId); err != nil {
			return false // rows not synced in yet
		}
		_, err := spB.Files().Get(ctx, origInfo.FileId)
		return err != nil
	}), "device B never converged on the deletion")
	_, err = spB.Files().Get(ctx, origInfo.FileId)
	require.ErrorIs(t, err, space.ErrNotFound)
	_, err = spB.Files().Get(ctx, thumbInfo.FileId)
	require.ErrorIs(t, err, space.ErrNotFound)

	// And the reverse direction: B deletes, A applies the remote remove.
	require.NoError(t, spB.Files().Delete(ctx, otherInfo.FileId))
	_ = spB.SyncHeads(ctx)
	require.True(t, waitFor(ctx, 120*time.Second, time.Second, func() bool {
		_ = spA.SyncHeads(ctx)
		_, err := spA.Files().Get(ctx, otherInfo.FileId)
		return err != nil
	}), "device A never applied B's deletion")
	_, err = spA.Files().Get(ctx, otherInfo.FileId)
	require.ErrorIs(t, err, space.ErrNotFound)

	t.Logf("files-v2 delete e2e OK: space=%s deleted=%s cascade=%s cache %dB→%dB",
		spA.Id(), origInfo.FileId, thumbInfo.FileId, cacheBefore, cacheAfter)
}

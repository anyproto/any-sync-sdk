package e2e

import (
	"bytes"
	"context"
	"io"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_FilesV2_OffloadAndFreeUp proves the SYN-26 cycle against a
// live local network: pin → offload (bytes drop, file stays) →
// transparent refetch on Open → SDK-level FreeUpFileCache reclaims the
// cache again, byte-accurately.
func TestE2E_FilesV2_OffloadAndFreeUp(t *testing.T) {
	netYaml, fileV2Peer, _ := loadLocalFilesV2Network(t)
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

	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "FilesV2Offload"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}
	typeId, _ := setupMovieType(t, ctx, spA)
	ownerId, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = spA.SyncHeads(ctx)

	content := make([]byte, 2_000_000)
	mrand.New(mrand.NewSource(26)).Read(content)
	fi, err := spA.Files().Attach(ctx, ownerId, bytes.NewReader(content),
		space.AttachOpts{Name: "evictme.bin", Mime: "application/octet-stream"})
	require.NoError(t, err)
	require.True(t, fi.Durable)
	_ = spA.SyncHeads(ctx)

	cdn := newBlobCDN(t, ctx, sdkA, fileV2Peer, spA.Id())
	t.Cleanup(cdn.Close)
	cfgB := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: netYaml},
		Files:   config.Files{PublicReadBaseUrl: cdn.URL},
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
		st, err := spB.Files().Status(ctx, fi.FileId)
		return err == nil && st.State == space.FileStateDurable
	}), "device B never derived durable")

	// Pin to a full local copy; the cache accounts for it.
	require.NoError(t, spB.Files().Pin(ctx, fi.FileId))
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		st, err := spB.Files().Status(ctx, fi.FileId)
		return err == nil && st.Cached
	}), "pin never completed")
	sizeAfterPin, err := sdkB.FileCacheSize(ctx)
	require.NoError(t, err)
	require.Positive(t, sizeAfterPin)

	// Offload: bytes drop, the file stays, Status flips Cached off.
	require.NoError(t, spB.Files().Offload(ctx, fi.FileId))
	st, err := spB.Files().Status(ctx, fi.FileId)
	require.NoError(t, err)
	assert.Equal(t, space.FileStateDurable, st.State)
	assert.False(t, st.Cached)
	sizeAfterOffload, err := sdkB.FileCacheSize(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), sizeAfterOffload, "the CAR was the whole cache")

	// Transparent refetch: Open works as if nothing happened.
	fr, err := spB.Files().Open(ctx, fi.FileId, space.VariantOriginal)
	require.NoError(t, err)
	got, err := io.ReadAll(fr)
	require.NoError(t, err)
	require.NoError(t, fr.Close())
	require.True(t, bytes.Equal(content, got), "refetched content byte-equal")

	// SDK-level budget sweep reclaims the refetched copy again.
	sizeRefetched, err := sdkB.FileCacheSize(ctx)
	require.NoError(t, err)
	require.Positive(t, sizeRefetched)
	freed, err := sdkB.FreeUpFileCache(ctx, sizeRefetched)
	require.NoError(t, err)
	assert.Equal(t, sizeRefetched, freed, "everything is durable ⇒ everything evictable")
	sizeFinal, err := sdkB.FileCacheSize(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), sizeFinal)

	// And still readable after the sweep (refetch again, partially).
	fr2, err := spB.Files().Open(ctx, fi.FileId, space.VariantOriginal)
	require.NoError(t, err)
	head := make([]byte, 4096)
	_, err = io.ReadFull(fr2, head)
	require.NoError(t, err)
	require.NoError(t, fr2.Close())
	assert.Equal(t, content[:4096], head)

	t.Logf("files-v2 offload e2e OK: space=%s file=%s cache pin=%dB freed=%dB cdnRequests=%d",
		spA.Id(), fi.FileId, sizeAfterPin, freed, cdn.Requests())
}

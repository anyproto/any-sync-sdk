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
	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_FilesV2_Surface proves the SYN-30 surface against a live
// local network: variants as sibling rows resolved through
// Open(originalId, variant), the typed List (per-object fast path +
// flat + limit), and the generic Query over one object's rows.
func TestE2E_FilesV2_Surface(t *testing.T) {
	t.Parallel()
	netYaml, _, _ := loadLocalFilesV2Network(t)
	if testing.Short() {
		t.Skip("files-v2 e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: netYaml},
	}
	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "FilesV2Surface"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}
	typeId, _ := setupMovieType(t, ctx, sp)
	owner1, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)
	owner2, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)
	_ = sdk.Spaces().SyncSpaceList(ctx)
	_ = sp.SyncHeads(ctx)

	// --- Original + two variants on owner1, one plain file on owner2.
	original := make([]byte, 1_200_000)
	mrand.New(mrand.NewSource(30)).Read(original)
	thumb := []byte("tiny thumbnail bytes (inline tier)")
	preview := make([]byte, 200_000)
	mrand.New(mrand.NewSource(31)).Read(preview)

	origInfo, err := sp.Files().Attach(ctx, owner1, bytes.NewReader(original),
		space.AttachOpts{Name: "photo.raw", Mime: "image/x-raw"})
	require.NoError(t, err)
	thumbInfo, err := sp.Files().Attach(ctx, owner1, bytes.NewReader(thumb),
		space.AttachOpts{Name: "photo-thumb.jpg", Mime: "image/jpeg", Variant: "thumbnail", VariantOf: origInfo.FileId})
	require.NoError(t, err)
	require.True(t, thumbInfo.Inline, "a tiny variant rides the inline tier")
	require.Equal(t, space.Variant("thumbnail"), thumbInfo.Variant)
	_, err = sp.Files().Attach(ctx, owner1, bytes.NewReader(preview),
		space.AttachOpts{Name: "photo-preview.jpg", Mime: "image/jpeg", Variant: "preview", VariantOf: origInfo.FileId})
	require.NoError(t, err)
	otherInfo, err := sp.Files().Attach(ctx, owner2, bytes.NewReader([]byte("unrelated file")),
		space.AttachOpts{Name: "other.txt"})
	require.NoError(t, err)

	// --- Variant contract enforcement.
	_, err = sp.Files().Attach(ctx, owner1, bytes.NewReader(thumb),
		space.AttachOpts{Variant: "thumbnail"})
	require.ErrorIs(t, err, space.ErrFileVariantInvalid)
	_, err = sp.Files().Attach(ctx, owner2, bytes.NewReader(thumb),
		space.AttachOpts{Variant: "thumbnail", VariantOf: origInfo.FileId})
	require.ErrorIs(t, err, space.ErrFileVariantInvalid, "variants bind to the original's object")

	// --- Open by (originalId, variant).
	fr, err := sp.Files().Open(ctx, origInfo.FileId, "thumbnail")
	require.NoError(t, err)
	gotThumb, err := io.ReadAll(fr)
	require.NoError(t, err)
	require.NoError(t, fr.Close())
	assert.Equal(t, thumb, gotThumb)

	fr, err = sp.Files().Open(ctx, origInfo.FileId, "preview")
	require.NoError(t, err)
	gotPreview, err := io.ReadAll(fr)
	require.NoError(t, err)
	require.NoError(t, fr.Close())
	assert.Equal(t, preview, gotPreview)

	_, err = sp.Files().Open(ctx, origInfo.FileId, "nonexistent")
	require.ErrorIs(t, err, space.ErrNotFound)

	// Get on the variant's own fileId exposes the tags.
	thumbGet, err := sp.Files().Get(ctx, thumbInfo.FileId)
	require.NoError(t, err)
	assert.Equal(t, space.Variant("thumbnail"), thumbGet.Variant)
	assert.Equal(t, origInfo.FileId, thumbGet.VariantOf)

	// --- List: per-object fast path, flat, limit.
	owner1Files, err := sp.Files().List(ctx, space.FileListOpts{ObjectId: owner1})
	require.NoError(t, err)
	require.Len(t, owner1Files, 3)
	all, err := sp.Files().List(ctx, space.FileListOpts{})
	require.NoError(t, err)
	require.Len(t, all, 4)
	for _, fi := range all {
		switch fi.FileId {
		case otherInfo.FileId:
			assert.Equal(t, owner2, fi.ObjectId)
			assert.Equal(t, "other.txt", fi.Name)
		case origInfo.FileId:
			assert.Equal(t, owner1, fi.ObjectId)
			assert.True(t, fi.Cached)
		}
	}
	limited, err := sp.Files().List(ctx, space.FileListOpts{Limit: 2})
	require.NoError(t, err)
	require.Len(t, limited, 2)

	// --- Generic Query over one object's rows: cleartext fields
	// filterable (rootCid presence = S3-tier rows).
	q, err := sp.Files().Query(owner1)
	require.NoError(t, err)
	count, err := q.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
	q, err = sp.Files().Query(owner1)
	require.NoError(t, err)
	withRoot, err := q.Filter(map[string]any{payloads.FieldRootCid: map[string]any{"$exists": true}}).All(ctx)
	require.NoError(t, err)
	assert.Len(t, withRoot, 2, "original + preview are S3-tier; the thumbnail is inline")

	// A never-attached object has no files dataset yet: typed
	// ErrNotFound, not a broken query.
	owner3, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)
	_, err = sp.Files().Query(owner3)
	require.ErrorIs(t, err, space.ErrNotFound)

	t.Logf("files-v2 surface e2e OK: space=%s original=%s", sp.Id(), origInfo.FileId)
}

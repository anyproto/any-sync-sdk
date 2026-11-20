package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	mrand "math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonfile/fileproto/fileprotov2"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_FilesV2_SDKAttach proves the SYN-27 upload path end-to-end
// through the public SDK API against a live local network:
//
//  1. Files().Attach on a multi-megabyte reader → encrypt → UnixFS DAG
//     → local CARv2 → payloads row, returning before the backup; the
//     queue then drives broker Upload → presigned PUT → RequestSign →
//     receipt verified → networkSign on the row;
//  2. the stored S3 object is byte-identical to the local CARv2
//     (RequestDownload → GET → compare — locked decision 2);
//  3. the inline tier (< 4 KiB) never touches the broker;
//  4. attaching the same content to another object BINDs: same
//     rootCid, donor's receipt reused, durable with no second upload.
func TestE2E_FilesV2_SDKAttach(t *testing.T) {
	t.Parallel()
	netYaml, fileV2Peers, _ := loadLocalFilesV2Network(t)
	if testing.Short() {
		t.Skip("files-v2 e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	dataDir := t.TempDir()
	cfg := config.Config{
		Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: netYaml},
	}
	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err, "Open")
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "FilesV2SDK"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}
	typeId, _ := setupMovieType(t, ctx, sp)
	ownerId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)

	// Push the space to the tree nodes BEFORE attaching, so the broker's
	// lazy space activation (space pull + ACL) can succeed inside
	// Attach's durable-phase retry window.
	_ = sdk.Spaces().SyncSpaceList(ctx)
	_ = sp.SyncHeads(ctx)

	// --- 1) multi-block file through the public API.
	content := make([]byte, 2_500_000) // > 2 chunks at the 1 MiB leaf size
	mrand.New(mrand.NewSource(27)).Read(content)
	wantSha := sha256.Sum256(content)

	info, err := sp.Files().Attach(ctx, ownerId, bytes.NewReader(content),
		space.AttachOpts{Name: "big.bin", Mime: "application/octet-stream"})
	require.NoError(t, err, "Attach")
	require.False(t, info.Durable, "Attach returns before the backup runs")
	require.False(t, info.Inline)
	require.NotEmpty(t, info.RootCid)
	require.EqualValues(t, len(content), info.Size)
	require.Equal(t, ownerId, info.ObjectId)

	waitFileDurable(t, ctx, sp, info.FileId)
	pa := payloadsSurface(t, sp)
	row, err := pa.GetRow(ctx, ownerId, info.FileId)
	require.NoError(t, err)
	require.Equal(t, info.RootCid, row.RootCid)
	require.NotEmpty(t, row.NetworkSign, "receipt recorded on the row")
	require.False(t, row.Sealed)
	require.Len(t, row.Enc.Key, 32)
	require.Equal(t, wantSha[:], row.Enc.SHA256)
	require.Equal(t, "big.bin", row.Enc.Name)

	// --- 2) stored object == local CARv2, byte for byte.
	localCar, err := os.ReadFile(carPathFor(dataDir, sp.Id(), info.RootCid))
	require.NoError(t, err, "local CARv2 must exist")

	rootBytes := mustCidBytes(t, info.RootCid)
	var dlResp *fileprotov2.RequestDownloadResponse
	require.NoError(t, doFileV2(ctx, sdk, fileV2Peers, func(cl fileprotov2.DRPCFileV2Client) error {
		var err error
		dlResp, err = cl.RequestDownload(ctx, &fileprotov2.RequestDownloadRequest{
			SpaceId: sp.Id(), RootCids: [][]byte{rootBytes},
		})
		return err
	}))
	require.Len(t, dlResp.Results, 1)
	require.Equal(t, fileprotov2.ErrCode_Ok, dlResp.Results[0].Code)
	stored := httpGet(t, ctx, dlResp.Results[0].Download.Url)
	assert.Equal(t, localCar, stored, "S3 object must be the local CARv2 verbatim")

	// --- 3) inline tier: no rootCid, no broker, durable by construction.
	small := []byte("inline tier payload for " + sp.Id())
	inlineInfo, err := sp.Files().Attach(ctx, ownerId, bytes.NewReader(small),
		space.AttachOpts{Name: "note.txt", Mime: "text/plain"})
	require.NoError(t, err)
	require.True(t, inlineInfo.Inline)
	require.True(t, inlineInfo.Durable)
	require.Empty(t, inlineInfo.RootCid)
	inlineRow, err := pa.GetRow(ctx, ownerId, inlineInfo.FileId)
	require.NoError(t, err)
	require.True(t, inlineRow.Inline())
	require.Equal(t, small, inlineRow.Enc.Inline)

	// --- 4) BIND: same content on a second object reuses everything.
	owner2, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)
	bindStart := time.Now()
	bindInfo, err := sp.Files().Attach(ctx, owner2, bytes.NewReader(content),
		space.AttachOpts{Name: "copy-of-big.bin", Mime: "application/octet-stream"})
	require.NoError(t, err)
	require.Equal(t, info.RootCid, bindInfo.RootCid, "dedup BIND shares the root")
	require.NotEqual(t, info.FileId, bindInfo.FileId)
	require.True(t, bindInfo.Durable, "BIND inherits the donor receipt")
	assert.Less(t, time.Since(bindStart), 30*time.Second, "BIND must not re-upload")
	bindRow, err := pa.GetRow(ctx, owner2, bindInfo.FileId)
	require.NoError(t, err)
	require.Equal(t, row.Enc.Key, bindRow.Enc.Key, "file key shared")
	require.Equal(t, row.NetworkSign, bindRow.NetworkSign)
	require.Equal(t, "copy-of-big.bin", bindRow.Enc.Name)

	t.Logf("files-v2 SDK e2e OK: space=%s file=%s root=%s car=%dB",
		sp.Id(), info.FileId, info.RootCid, len(localCar))
}

// carPathFor mirrors the store layout: <DataDir>/files/<spaceId>/<2-char
// shard>/<rootCid>.car.
// waitFileDurable blocks until the queue-driven backup of fileId lands
// its receipt.
func waitFileDurable(t *testing.T, ctx context.Context, sp space.Space, fileId string) {
	t.Helper()
	require.True(t, waitFor(ctx, 120*time.Second, 200*time.Millisecond, func() bool {
		st, err := sp.Files().Status(ctx, fileId)
		return err == nil && st.State == space.FileStateDurable
	}), "file %s never became durable", fileId)
}

func carPathFor(dataDir, spaceId, rootCid string) string {
	return filepath.Join(dataDir, "files", spaceId, rootCid[len(rootCid)-2:], rootCid+".car")
}

func mustCidBytes(t *testing.T, c string) []byte {
	t.Helper()
	root, err := cid.Decode(c)
	require.NoError(t, err)
	return root.Bytes()
}

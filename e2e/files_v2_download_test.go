package e2e

import (
	"bytes"
	"context"
	"errors"
	"io"
	mrand "math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonfile/fileproto/fileprotov2"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_FilesV2_SDKDownload proves the SYN-28 download path across
// devices against a live local network: device A attaches a durable
// file; device B (same account, fresh storage) cold-syncs the space,
// resolves the payloads row, and reads the content through
// Files().Open — local store miss → sparse seed → HTTP Range fetch →
// cid-verify → CFB decrypt — including a targeted seek.
//
// The local network's object store is private (no publicReadBaseUrl),
// so the test fronts it with a Range-capable HTTP server that plays
// the CDN role: it resolves each /blob/{spaceId}/{rootCid} once via
// RequestDownload (as infra would front the bucket) and serves the
// bytes. Device B is pointed at it via cfg.Files.PublicReadBaseUrl —
// the same override a private deployment would use. B's SDK performs
// plain ranged GETs only: zero broker round-trips on the read path.
func TestE2E_FilesV2_SDKDownload(t *testing.T) {
	t.Parallel()
	netYaml, fileV2Peers, _ := loadLocalFilesV2Network(t)
	if testing.Short() {
		t.Skip("files-v2 e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	provider := newFixedSeedProvider(t)

	// --- Device A: create, attach, make durable.
	cfgA := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: netYaml},
	}
	sdkA, err := anysyncsdk.Open(ctx, cfgA, provider)
	require.NoError(t, err, "device A: Open")
	t.Cleanup(func() { _ = sdkA.Close() })

	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "FilesV2Download"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}
	typeId, _ := setupMovieType(t, ctx, spA)
	ownerId, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)
	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = spA.SyncHeads(ctx)

	content := make([]byte, 2_600_000)
	mrand.New(mrand.NewSource(28)).Read(content)
	fileInfo, err := spA.Files().Attach(ctx, ownerId, bytes.NewReader(content),
		space.AttachOpts{Name: "movie.bin", Mime: "application/octet-stream"})
	require.NoError(t, err, "device A: Attach")
	require.True(t, fileInfo.Durable, "file must be durable before B can read it publicly")

	inlineContent := []byte("inline for download e2e " + spA.Id())
	inlineInfo, err := spA.Files().Attach(ctx, ownerId, bytes.NewReader(inlineContent),
		space.AttachOpts{Name: "note.txt", Mime: "text/plain"})
	require.NoError(t, err)
	_ = spA.SyncHeads(ctx)

	// --- Synthetic CDN over the private object store.
	cdn := newBlobCDN(t, ctx, sdkA, fileV2Peers, spA.Id())
	t.Cleanup(cdn.Close)

	// --- Device B: fresh storage, same account, CDN as public base.
	cfgB := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: netYaml},
		Files:   config.Files{PublicReadBaseUrl: cdn.URL},
	}
	sdkB, err := anysyncsdk.Open(ctx, cfgB, provider)
	require.NoError(t, err, "device B: Open")
	t.Cleanup(func() { _ = sdkB.Close() })

	// Cold sync: the space appears, then the payloads row resolves.
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
	spB := getSpaceEventually(ctx, t, sdkB, spA.Id())

	var gotInfo space.FileInfo
	require.True(t, waitFor(ctx, 120*time.Second, time.Second, func() bool {
		_ = spB.SyncHeads(ctx)
		gotInfo, err = spB.Files().Get(ctx, fileInfo.FileId)
		return err == nil && gotInfo.Durable
	}), "device B never saw the durable payloads row")
	require.Equal(t, fileInfo.RootCid, gotInfo.RootCid)
	require.Equal(t, ownerId, gotInfo.ObjectId, "row carries the parent objectId")
	require.Equal(t, "movie.bin", gotInfo.Name, "sealed meta unseals on the same account")
	require.False(t, gotInfo.Cached, "nothing local before the first read")

	// --- Full read through the public path.
	fr, err := spB.Files().Open(ctx, fileInfo.FileId, space.VariantOriginal)
	require.NoError(t, err, "device B: Open")
	require.EqualValues(t, len(content), fr.Size())
	got, err := io.ReadAll(fr)
	require.NoError(t, err)
	require.NoError(t, fr.Close())
	require.True(t, bytes.Equal(content, got), "cross-device byte equality")

	// --- Targeted seek on a fresh handle (mostly local now; any hole
	// still resolves through the CDN).
	fr2, err := spB.Files().Open(ctx, fileInfo.FileId, space.VariantOriginal)
	require.NoError(t, err)
	off := int64(1_900_000)
	_, err = fr2.Seek(off, io.SeekStart)
	require.NoError(t, err)
	buf := make([]byte, 64<<10)
	_, err = io.ReadFull(fr2, buf)
	require.NoError(t, err)
	require.NoError(t, fr2.Close())
	require.True(t, bytes.Equal(content[off:off+int64(len(buf))], buf), "seek read")

	// --- Inline file needs no CDN at all.
	require.True(t, waitFor(ctx, 60*time.Second, time.Second, func() bool {
		_, err := spB.Files().Get(ctx, inlineInfo.FileId)
		return err == nil
	}), "device B never saw the inline row")
	fr3, err := spB.Files().Open(ctx, inlineInfo.FileId, space.VariantOriginal)
	require.NoError(t, err)
	gotInline, err := io.ReadAll(fr3)
	require.NoError(t, err)
	require.NoError(t, fr3.Close())
	require.Equal(t, inlineContent, gotInline)

	t.Logf("files-v2 download e2e OK: space=%s file=%s cdnRequests=%d",
		spA.Id(), fileInfo.FileId, cdn.Requests())
}

// blobCDN fronts the network's private object store with the public
// /blob/{spaceId}/{rootCid} read surface (Range-capable), resolving
// each object once via RequestDownload.
type blobCDN struct {
	*httptest.Server
	mu       sync.Mutex
	cache    map[string][]byte
	requests int
}

func newBlobCDN(t *testing.T, ctx context.Context, sdk *anysyncsdk.SDK, fileV2Peers []string, spaceId string) *blobCDN {
	c := &blobCDN{cache: map[string][]byte{}}
	c.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
		if len(parts) != 3 || parts[0] != "blob" || parts[1] != spaceId {
			http.NotFound(w, r)
			return
		}
		c.mu.Lock()
		c.requests++
		data, ok := c.cache[parts[2]]
		c.mu.Unlock()
		if !ok {
			root, err := cid.Decode(parts[2])
			if err != nil {
				http.NotFound(w, r)
				return
			}
			data, err = c.resolve(ctx, sdk, fileV2Peers, spaceId, root)
			if err != nil {
				t.Logf("blobCDN: resolve %s: %v", parts[2], err)
				http.NotFound(w, r)
				return
			}
			c.mu.Lock()
			c.cache[parts[2]] = data
			c.mu.Unlock()
		}
		http.ServeContent(w, r, "", time.Unix(0, 0), bytes.NewReader(data))
	}))
	return c
}

func (c *blobCDN) resolve(ctx context.Context, sdk *anysyncsdk.SDK, peers []string, spaceId string, root cid.Cid) ([]byte, error) {
	var resp *fileprotov2.RequestDownloadResponse
	err := doFileV2(ctx, sdk, peers, func(cl fileprotov2.DRPCFileV2Client) error {
		var err error
		resp, err = cl.RequestDownload(ctx, &fileprotov2.RequestDownloadRequest{
			SpaceId: spaceId, RootCids: [][]byte{root.Bytes()},
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Results) != 1 || resp.Results[0].Code != fileprotov2.ErrCode_Ok {
		return nil, errors.New("blobCDN: RequestDownload refused")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, resp.Results[0].Download.Url, nil)
	if err != nil {
		return nil, err
	}
	hr, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer hr.Body.Close()
	if hr.StatusCode != http.StatusOK {
		return nil, errors.New("blobCDN: signed GET " + hr.Status)
	}
	return io.ReadAll(hr.Body)
}

func (c *blobCDN) Requests() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.requests
}

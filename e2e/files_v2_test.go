package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonfile/fileproto/fileprotov2"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"gopkg.in/yaml.v3"
	"storj.io/drpc"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// loadLocalFilesV2Network gates the files-v2 e2e on a LOCAL network:
// it needs a running any-sync-filenode2 (fileV2 broker), which staging
// does not serve yet. Run with ANYSYNC_E2E_LOCAL=1 and the local
// network descriptor at e2e/local.yml (override the path with
// ANYSYNC_E2E_LOCAL_NETWORK) — see local-infra's README for the
// one-command local network.
func loadLocalFilesV2Network(t *testing.T) (yamlBytes []byte, fileV2PeerId, networkId string) {
	t.Helper()
	if os.Getenv("ANYSYNC_E2E_LOCAL") != "1" {
		t.Skip("files-v2 e2e: set ANYSYNC_E2E_LOCAL=1 (and provide e2e/local.yml) to run against a local network")
	}
	path := os.Getenv("ANYSYNC_E2E_LOCAL_NETWORK")
	if path == "" {
		path = "local.yml"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("files-v2 e2e: local network config not readable at %s: %v", path, err)
	}
	var conf struct {
		NetworkId string `yaml:"networkId"`
		Nodes     []struct {
			PeerId string   `yaml:"peerId"`
			Types  []string `yaml:"types"`
		} `yaml:"nodes"`
	}
	require.NoError(t, yaml.Unmarshal(data, &conf))
	for _, n := range conf.Nodes {
		for _, typ := range n.Types {
			if typ == "fileV2" {
				return data, n.PeerId, conf.NetworkId
			}
		}
	}
	t.Fatalf("files-v2 e2e: no fileV2 node in %s", path)
	return nil, "", ""
}

// TestE2E_FilesV2 proves the v2 files flow end-to-end against a live
// local network (client SDK + coordinator + consensus + tree nodes +
// any-sync-filenode2):
//
//  1. a fresh account creates a space and registers two S3-backed
//     payload rows (real CIDv1 roots over the actual bytes);
//  2. the client dials the fileV2 broker over drpc (identity-bearing
//     connection via the SDK pool): Info (local objstore provider ⇒
//     empty publicReadBaseUrl), Upload → presigned PUT of the bytes,
//     RequestSign → NetworkSignReceipt verified against the fleet
//     signing pubkey, recorded on the rows via SetNetworkSigns;
//  3. SpaceInfo converges to durable usage AND filesCount>0 — files
//     are counted from rows the broker's spaceindex observed through
//     selective sync (client → tree nodes → broker), so this proves
//     the whole pipeline, not just the direct RPCs;
//  4. RequestDownload → signed GET → bytes round-trip equal.
func TestE2E_FilesV2(t *testing.T) {
	netYaml, fileV2Peer, networkId := loadLocalFilesV2Network(t)
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
	require.NoError(t, err, "Open")
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "FilesV2"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}

	// The owner carries a type binding so its tree has content (see
	// TestE2E_Payloads for why a root-only owner never reaches the node).
	typeId, _ := setupMovieType(t, ctx, sp)
	ownerId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)

	// Two payloads with REAL CIDv1 roots computed over the bytes.
	blobA := []byte("files-v2 e2e payload A for space " + sp.Id())
	blobB := bytes.Repeat([]byte("files-v2 e2e payload B block."), 1024) // ~29 KiB
	cidA, cidB := rawCidV1(t, blobA), rawCidV1(t, blobB)
	shaA, shaB := sha256.Sum256(blobA), sha256.Sum256(blobB)

	pa := payloadsSurface(t, sp)
	fileIds, _, err := pa.RegisterFiles(ctx, ownerId, []spaceimpl.RegisterFileOpts{
		{RootCid: cidA.String(), Size: int64(len(blobA)), Enc: payloads.EncPayload{
			Key: []byte("wrapped-key-0123456789abcdef0123"), Name: "a.bin", SHA256: shaA[:], Mime: "application/octet-stream",
		}},
		{RootCid: cidB.String(), Size: int64(len(blobB)), Enc: payloads.EncPayload{
			Key: []byte("wrapped-key-abcdef0123456789abcd"), Name: "b.bin", SHA256: shaB[:], Mime: "application/octet-stream",
		}},
	})
	require.NoError(t, err)
	require.Len(t, fileIds, 2)

	// Push the space + rows so the broker's SpacePull finds them on the
	// tree nodes (Create pushes lazily; don't race the periodic sync).
	_ = sdk.Spaces().SyncSpaceList(ctx)
	_ = sp.SyncHeads(ctx)

	// --- Info: static node metadata. The advertised public-read base
	// depends on the node's objstore configuration (empty for a purely
	// private provider, the bucket URL when blob/* is public-read) —
	// both are valid deployments, so just surface what this network
	// advertises.
	var info *fileprotov2.InfoResponse
	require.True(t, waitFor(ctx, 60*time.Second, time.Second, func() bool {
		callErr := doFileV2(ctx, sdk, fileV2Peer, func(cl fileprotov2.DRPCFileV2Client) error {
			var err error
			info, err = cl.Info(ctx, &fileprotov2.InfoRequest{})
			return err
		})
		if callErr != nil {
			t.Logf("Info: %v", callErr)
		}
		return callErr == nil
	}), "filenode2 Info never answered")
	t.Logf("filenode2 publicReadBaseUrl=%q", info.PublicReadBaseUrl)

	// --- Upload: retried until the broker activates the space (its
	// embedded SDK pulls the space from the tree nodes and the ACL from
	// consensus on first request; per-item ErrUnexpected is retryable).
	items := []*fileprotov2.UploadRequestItem{
		{RootCid: cidA.Bytes(), Size: uint64(len(blobA))},
		{RootCid: cidB.Bytes(), Size: uint64(len(blobB))},
	}
	uploads := map[string]*fileprotov2.PresignedUpload{}
	require.True(t, waitFor(ctx, 180*time.Second, 2*time.Second, func() bool {
		var resp *fileprotov2.UploadResponse
		callErr := doFileV2(ctx, sdk, fileV2Peer, func(cl fileprotov2.DRPCFileV2Client) error {
			var err error
			resp, err = cl.Upload(ctx, &fileprotov2.UploadRequest{SpaceId: sp.Id(), Items: items})
			return err
		})
		if callErr != nil {
			t.Logf("Upload rpc: %v", callErr)
			return false
		}
		for _, res := range resp.Results {
			if res.Code != fileprotov2.ErrCode_Ok {
				t.Logf("Upload item not ready: %v", res.Code)
				return false
			}
		}
		for _, res := range resp.Results {
			uploads[string(res.RootCid)] = res.Upload
		}
		return true
	}), "Upload never authorized (broker space activation + ACL)")
	require.Len(t, uploads, 2)

	// --- Presigned PUT: exact bytes, Content-Length under the cap.
	httpPut(t, ctx, uploads[string(cidA.Bytes())], blobA)
	httpPut(t, ctx, uploads[string(cidB.Bytes())], blobB)

	// --- RequestSign: receipts for both roots, verified against the
	// fleet signing pubkey (single-node fleet: signingKey == the fileV2
	// node's peer key, so the pubkey is embedded in its peerId).
	var signResp *fileprotov2.RequestSignResponse
	require.NoError(t, doFileV2(ctx, sdk, fileV2Peer, func(cl fileprotov2.DRPCFileV2Client) error {
		var err error
		signResp, err = cl.RequestSign(ctx, &fileprotov2.RequestSignRequest{
			SpaceId:  sp.Id(),
			RootCids: [][]byte{cidA.Bytes(), cidB.Bytes()},
		})
		return err
	}))
	require.Len(t, signResp.Results, 2)

	fleetPub, err := crypto.DecodePeerId(fileV2Peer)
	require.NoError(t, err)

	wantSizes := map[string]uint64{
		string(cidA.Bytes()): uint64(len(blobA)),
		string(cidB.Bytes()): uint64(len(blobB)),
	}
	signs := map[string]string{} // rootCid bytes -> networkSign row value
	for i, res := range signResp.Results {
		require.Equalf(t, fileprotov2.ErrCode_Ok, res.Code, "sign item %d", i)
		require.NotNil(t, res.Receipt)
		ok, err := fleetPub.Verify(res.Receipt.ReceiptPayload, res.Receipt.Signature)
		require.NoError(t, err)
		require.True(t, ok, "receipt signature must verify against the fleet signing pubkey")
		rp := parseReceiptPayload(t, res.Receipt.ReceiptPayload)
		assert.Equal(t, networkId, rp.networkId, "receipt networkId")
		assert.Equal(t, sp.Id(), rp.spaceId, "receipt spaceId")
		assert.Equal(t, res.RootCid, rp.rootCid, "receipt rootCid")
		assert.Equal(t, wantSizes[string(res.RootCid)], rp.size, "receipt size = HEAD-measured PUT size")
		assert.Equal(t, fileV2Peer, rp.signerPeerId, "receipt signerPeerId")
		assert.InDelta(t, float64(time.Now().Unix()), float64(rp.signedAt), 600, "receipt signedAt")
		signs[string(res.RootCid)] = rp.signerPeerId + "/" + base64.StdEncoding.EncodeToString(res.Receipt.Signature)
	}

	// Record the receipts on the rows and push, so the broker's index
	// observes the signs through selective sync.
	require.NoError(t, pa.SetNetworkSigns(ctx, ownerId, map[string]string{
		fileIds[0]: signs[string(cidA.Bytes())],
		fileIds[1]: signs[string(cidB.Bytes())],
	}))
	_ = sp.SyncHeads(ctx)

	// --- SpaceInfo: durable usage covers both blobs AND filesCount sees
	// the rows — filesCount only comes from payload rows the broker's
	// spaceindex ingested via selective sync (client → tree nodes →
	// broker), so this is the pipeline proof.
	wantDurable := uint64(len(blobA) + len(blobB))
	var quota *fileprotov2.SpaceQuota
	lastLog := time.Now()
	require.True(t, waitFor(ctx, 240*time.Second, 2*time.Second, func() bool {
		var resp *fileprotov2.SpaceInfoResponse
		callErr := doFileV2(ctx, sdk, fileV2Peer, func(cl fileprotov2.DRPCFileV2Client) error {
			var err error
			resp, err = cl.SpaceInfo(ctx, &fileprotov2.SpaceInfoRequest{SpaceIds: []string{sp.Id()}})
			return err
		})
		if callErr != nil || len(resp.Results) != 1 {
			return false
		}
		res := resp.Results[0]
		if res.Code != fileprotov2.ErrCode_Ok || res.Quota == nil {
			return false
		}
		quota = res.Quota
		if time.Since(lastLog) > 10*time.Second {
			lastLog = time.Now()
			t.Logf("SpaceInfo progress: durable=%d inflight=%d files=%d limit=%d",
				quota.DurableUsageBytes, quota.InflightUsageBytes, quota.FilesCount, quota.LimitBytes)
		}
		return quota.DurableUsageBytes >= wantDurable && quota.FilesCount >= 2
	}), "SpaceInfo never converged to durable usage + synced rows")
	assert.Equal(t, wantDurable, quota.DurableUsageBytes, "durable usage = sum of the two PUT bodies")
	assert.Positive(t, quota.LimitBytes)

	// --- RequestDownload → signed GET → byte-for-byte round-trip.
	var dlResp *fileprotov2.RequestDownloadResponse
	require.NoError(t, doFileV2(ctx, sdk, fileV2Peer, func(cl fileprotov2.DRPCFileV2Client) error {
		var err error
		dlResp, err = cl.RequestDownload(ctx, &fileprotov2.RequestDownloadRequest{
			SpaceId:  sp.Id(),
			RootCids: [][]byte{cidA.Bytes(), cidB.Bytes()},
		})
		return err
	}))
	require.Len(t, dlResp.Results, 2)
	for _, res := range dlResp.Results {
		require.Equal(t, fileprotov2.ErrCode_Ok, res.Code)
		require.NotNil(t, res.Download)
		require.NotEmpty(t, res.Download.Url)
		got := httpGet(t, ctx, res.Download.Url)
		switch string(res.RootCid) {
		case string(cidA.Bytes()):
			assert.Equal(t, blobA, got, "blob A round-trip")
		case string(cidB.Bytes()):
			assert.Equal(t, blobB, got, "blob B round-trip")
		default:
			t.Fatalf("download result for unknown rootCid")
		}
	}

	t.Logf("files-v2 e2e OK: space=%s files=%v durable=%dB", sp.Id(), fileIds, quota.DurableUsageBytes)
}

// doFileV2 dials the fileV2 broker through the SDK's peer pool (the
// connection carries the account identity, which the broker checks
// against the space ACL) and runs fn with a FileV2 drpc client.
func doFileV2(ctx context.Context, sdk *anysyncsdk.SDK, peerId string, fn func(cl fileprotov2.DRPCFileV2Client) error) error {
	pr, err := sdk.PoolInternal().Get(ctx, peerId)
	if err != nil {
		return err
	}
	return pr.DoDrpc(ctx, func(conn drpc.Conn) error {
		return fn(fileprotov2.NewDRPCFileV2Client(conn))
	})
}

// rawCidV1 computes the CIDv1 (raw codec, sha2-256) of data — the real
// root a client would advertise for a single-block payload.
func rawCidV1(t *testing.T, data []byte) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum(data, multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, mh)
}

// httpPut performs the presigned upload: exact body, Content-Length
// under the presign cap, provider headers sent verbatim.
func httpPut(t *testing.T, ctx context.Context, up *fileprotov2.PresignedUpload, body []byte) {
	t.Helper()
	require.NotNil(t, up)
	require.NotEmpty(t, up.Url)
	require.LessOrEqual(t, uint64(len(body)), up.MaxContentLength, "body must fit the presigned cap")
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, up.Url, bytes.NewReader(body))
	require.NoError(t, err)
	req.ContentLength = int64(len(body))
	for _, f := range up.Fields {
		req.Header.Set(f.Key, f.Value)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	require.Truef(t, resp.StatusCode >= 200 && resp.StatusCode < 300,
		"PUT %s: %d %s", up.Url, resp.StatusCode, string(respBody))
}

// httpGet fetches a signed download URL and returns the body.
func httpGet(t *testing.T, ctx context.Context, url string) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, resp.StatusCode, "GET %s: %s", url, string(body))
	return body
}

// receiptPayload mirrors filenode2's receiptproto.ReceiptPayload wire
// contract (verify-then-unmarshal; the payload bytes are opaque and the
// signature is over them exactly).
type receiptPayload struct {
	networkId    string // 1
	spaceId      string // 2
	rootCid      []byte // 3
	size         uint64 // 4
	signedAt     int64  // 5
	signerPeerId string // 6
}

// parseReceiptPayload decodes the receipt payload with protowire —
// field-number-stable, so the e2e needs no dependency on the broker
// module for its codegen types.
func parseReceiptPayload(t *testing.T, b []byte) receiptPayload {
	t.Helper()
	var r receiptPayload
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		require.NoError(t, protowire.ParseError(n))
		b = b[n:]
		switch typ {
		case protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			require.NoError(t, protowire.ParseError(n))
			b = b[n:]
			switch num {
			case 1:
				r.networkId = string(v)
			case 2:
				r.spaceId = string(v)
			case 3:
				r.rootCid = append([]byte(nil), v...)
			case 6:
				r.signerPeerId = string(v)
			default:
				t.Fatalf("unexpected receipt bytes field %d", num)
			}
		case protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			require.NoError(t, protowire.ParseError(n))
			b = b[n:]
			switch num {
			case 4:
				r.size = v
			case 5:
				r.signedAt = int64(v)
			default:
				t.Fatalf("unexpected receipt varint field %d", num)
			}
		default:
			t.Fatalf("unexpected receipt wire type %v (field %d)", typ, num)
		}
	}
	return r
}

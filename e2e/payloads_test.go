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

// payloadsSurface reaches the SDK-internal payloads API on a space
// handle (not part of the public space.Space interface — SYN-30 adds
// the public Files surface; the public READ-ONLY view is
// space.Space.Payloads).
func payloadsSurface(t *testing.T, sp space.Space) *spaceimpl.PayloadsAPI {
	t.Helper()
	pa, ok := sp.(interface{ PayloadsInternal() *spaceimpl.PayloadsAPI })
	require.True(t, ok, "space impl must expose the internal Payloads API")
	return pa.PayloadsInternal()
}

// TestE2E_Payloads covers SYN-21 end-to-end on a real network: the
// per-owner payloads object is created lazily, file rows (S3-backed +
// inline) register and materialize, the sealed enc opens for a keyed
// reader, networkSign records on signed-able rows only, the public
// Modify surface is fenced, and a second device of the same account
// cold-syncs the rows and unseals them.
func TestE2E_Payloads(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available at %s: %v", confPath, err)
	}
	if testing.Short() {
		t.Skip("payloads e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	provider := newFixedSeedProvider(t)

	cfgA := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	sdkA, err := anysyncsdk.Open(ctx, cfgA, provider)
	require.NoError(t, err, "device A: Open")
	t.Cleanup(func() { _ = sdkA.Close() })

	sp, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "Payloads"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}

	// The owner carries a type binding so its tree has content. A
	// root-only (never-written) tree is excluded from the headsync
	// diff (any-sync DiffManager treats empty roots as old-diff-only),
	// so a bare owner never reaches the node — and the node then
	// refuses the payloads CHILD tree forever with ErrParentNotFound
	// (objecttree.CreateStorage hard-requires the parent). Flagged as
	// an any-sync gap; real owners always have content.
	typeId, _ := setupMovieType(t, ctx, sp)
	ownerId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)

	pa := payloadsSurface(t, sp)

	// Lazy creation: reads never create the payloads object.
	rows, err := pa.ListRows(ctx, ownerId)
	require.NoError(t, err)
	assert.Empty(t, rows, "owner with no files has no payloads rows")
	_, err = pa.GetRow(ctx, ownerId, "nope")
	assert.ErrorIs(t, err, space.ErrNotFound)
	expectObjId, err := pa.ObjectId(ctx, ownerId)
	require.NoError(t, err)

	// Register an S3-backed row and an inline row — as ONE batch
	// change (the bulk-paste shape: N files, one DAG entry).
	encBig := payloads.EncPayload{
		Key:    []byte("wrapped-key-0123456789abcdef0123"),
		Name:   "photo.png",
		SHA256: []byte("0123456789abcdef0123456789abcdef"),
		Mime:   "image/png",
	}
	inlineBytes := []byte("tiny inline file contents")
	fileIds, objId, err := pa.RegisterFiles(ctx, ownerId, []spaceimpl.RegisterFileOpts{
		{RootCid: "bafyphotoroot", Size: 123456, Enc: encBig},
		{Size: int64(len(inlineBytes)), Enc: payloads.EncPayload{Name: "note.txt", Mime: "text/plain", Inline: inlineBytes}},
	})
	require.NoError(t, err)
	require.Len(t, fileIds, 2)
	fileBig, fileInline := fileIds[0], fileIds[1]
	assert.Equal(t, expectObjId, objId, "lazily-created object must land on the pre-resolved deterministic id")
	assert.NotEqual(t, fileBig, fileInline)

	// Inline XOR is enforced pre-seal.
	_, _, err = pa.RegisterFile(ctx, ownerId, spaceimpl.RegisterFileOpts{
		Size: payloads.InlineMaxSize,
		Enc:  payloads.EncPayload{Inline: make([]byte, payloads.InlineMaxSize)},
	})
	assert.Error(t, err, "inline at/over the cutoff must be rejected")
	_, _, err = pa.RegisterFile(ctx, ownerId, spaceimpl.RegisterFileOpts{
		Size:        3,
		NetworkSign: "net/sig",
		Enc:         payloads.EncPayload{Inline: []byte("abc")},
	})
	assert.Error(t, err, "inline rows are never signed")

	// Keyed read: rows unseal.
	row, err := pa.GetRow(ctx, ownerId, fileBig)
	require.NoError(t, err)
	assert.False(t, row.Sealed)
	assert.Equal(t, "bafyphotoroot", row.RootCid)
	assert.Equal(t, int64(123456), row.Size)
	assert.Equal(t, sdkA.Account().Id(), row.Author)
	assert.Equal(t, encBig, row.Enc)

	inlineRow, err := pa.GetRow(ctx, ownerId, fileInline)
	require.NoError(t, err)
	assert.True(t, inlineRow.Inline())
	assert.Equal(t, inlineBytes, inlineRow.Enc.Inline)
	assert.Empty(t, inlineRow.NetworkSign)

	// networkSign: records on the S3-backed row, refused on inline.
	require.NoError(t, pa.SetNetworkSign(ctx, ownerId, fileBig, "net1/sig-of-bafyphotoroot"))
	row, err = pa.GetRow(ctx, ownerId, fileBig)
	require.NoError(t, err)
	assert.Equal(t, "net1/sig-of-bafyphotoroot", row.NetworkSign)
	assert.Error(t, pa.SetNetworkSign(ctx, ownerId, fileInline, "net1/sig"),
		"inline rows are never signed")

	// The public Modify surface is fenced off the payloads dataset.
	_, err = sp.Modify(ctx, space.ModifyBatch{
		ObjectId: objId,
		Dataset:  payloads.Dataset,
		Records:  []space.RecordModify{{Id: fileBig, Ops: []space.Op{{Type: "$set", Path: "size", Value: 1}}}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SDK-internal")

	t.Logf("ids: space=%s owner=%s payloadsObj=%s fileBig=%s fileInline=%s",
		sp.Id(), ownerId, objId, fileBig, fileInline)

	// Push heads so device B doesn't race A's periodic sync.
	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = sp.SyncHeads(ctx)

	// Device B: same account, fresh DataDir — the cold-sync + keyed
	// materialization acceptance.
	cfgB := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	sdkB, err := anysyncsdk.Open(ctx, cfgB, provider)
	require.NoError(t, err, "device B: Open")
	t.Cleanup(func() { _ = sdkB.Close() })

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

	objIdB, err := paB.ObjectId(ctx, ownerId)
	require.NoError(t, err)
	assert.Equal(t, objId, objIdB, "derived payloads id must agree across devices")

	var rowsB []payloads.Row
	lastLog := time.Now()
	if !waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		if time.Since(lastLog) > 10*time.Second {
			lastLog = time.Now()
			ownerSeen := queryHasObject(t, ctx, spB, ownerId)
			_, dbgErr := spB.Debug().Object(ctx, objId)
			t.Logf("device B progress: ownerMaterialized=%v payloadsTreeDebug=%v", ownerSeen, dbgErr)
		}
		rowsB, err = paB.ListRows(ctx, ownerId)
		if err != nil && !errors.Is(err, space.ErrNotFound) {
			t.Logf("device B ListRows error: %v", err)
			return false
		}
		if len(rowsB) != 2 {
			return false
		}
		for _, r := range rowsB {
			if r.NetworkSign == "" && !r.Inline() {
				return false // the sign change hasn't replayed yet
			}
		}
		return true
	}) {
		t.Fatalf("device B never converged to 2 payloads rows (got %d)", len(rowsB))
	}

	byId := map[string]payloads.Row{}
	for _, r := range rowsB {
		byId[r.Id] = r
	}
	gotBig, ok := byId[fileBig]
	require.True(t, ok)
	assert.False(t, gotBig.Sealed, "device B holds the key and must unseal")
	assert.Equal(t, encBig, gotBig.Enc)
	assert.Equal(t, "net1/sig-of-bafyphotoroot", gotBig.NetworkSign)
	assert.Equal(t, sdkA.Account().Id(), gotBig.Author)

	gotInline, ok := byId[fileInline]
	require.True(t, ok)
	assert.Equal(t, inlineBytes, gotInline.Enc.Inline)
}

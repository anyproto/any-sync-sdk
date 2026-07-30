package e2e

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_FilesV2_DerivedOwnerAttach is the files-v2 acceptance for
// attaching a file to a DERIVED owner object.
//
// A derived owner's payloads object cannot be parented to it: any-sync
// rejects a derived object as a tree parent (ErrDerivedParent). Before the
// fix, Attach/RegisterFiles derived the payloads child parented to the
// owner and failed on a derived owner. The fix derives it UNPARENTED with
// the ownerId folded into the seed (payloads.DerivedOwnerSeed).
//
// The owner here is TYPELESS on purpose. A content-less derived object is
// excluded from the headsync diff (any-sync headsync/diffmanager.go: a
// derived root whose only head is its own id is skipped), so it never
// syncs to a second device — but the UNPARENTED payloads object carries a
// change (the file row) and syncs on its own. That exercises the hardest
// case: on device B the payloads tree is present while the owner tree is
// absent, so owner-keyed reads must resolve the derived (unparented) shape
// WITHOUT the owner rather than degrade to no-rows.
func TestE2E_FilesV2_DerivedOwnerAttach(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available at %s: %v", confPath, err)
	}
	if testing.Short() {
		t.Skip("derived-owner files e2e is slow; rerun without -short")
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

	sp, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "DerivedOwnerFiles"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}

	// A DERIVED owner (Objects().Derive → store.Derive → IsDerived root),
	// deliberately typeless (no content of its own).
	ownerId, err := sp.Objects().Derive(ctx, space.DeriveObjectOpts{
		Seed: []byte("derived-owner-for-files"),
	})
	require.NoError(t, err)

	pa := payloadsSurface(t, sp)

	// Read paths degrade to no-rows when the owner genuinely has no files
	// (a never-created owner id, and the derived owner before its first
	// attach): nothing is present, so HasTree is false for both.
	const absentOwner = "bafyabsentownernonexistent000000000000000000000000000000"
	absentRows, err := pa.ListRows(ctx, absentOwner)
	require.NoError(t, err)
	assert.Empty(t, absentRows, "absent owner → no rows")
	_, err = pa.GetRow(ctx, absentOwner, "nope")
	assert.ErrorIs(t, err, space.ErrNotFound, "absent owner → ErrNotFound")

	rows, err := pa.ListRows(ctx, ownerId)
	require.NoError(t, err)
	assert.Empty(t, rows, "derived owner with no files has no payloads rows")

	expectObjId, err := pa.ObjectId(ctx, ownerId)
	require.NoError(t, err)
	require.NotEmpty(t, expectObjId)

	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = sp.SyncHeads(ctx)

	// The core fix: attach an inline file to the DERIVED owner. Inline
	// keeps the assertion on the payloads mechanism (no broker/S3). Pre-fix
	// this failed with ErrDerivedParent inside RegisterFiles' Derive.
	inline := []byte("derived-owner inline file contents")
	info, err := sp.Files().Attach(ctx, ownerId, bytes.NewReader(inline),
		space.AttachOpts{Name: "derived.txt", Mime: "text/plain"})
	require.NoError(t, err, "attach to a derived owner must succeed (no ErrDerivedParent)")
	assert.True(t, info.Inline)
	assert.Equal(t, ownerId, info.ObjectId)

	objId, err := pa.ObjectId(ctx, ownerId)
	require.NoError(t, err)
	assert.Equal(t, expectObjId, objId, "payloads id stable across the attach")

	// Round-trip on device A (owner present here).
	row, err := pa.GetRow(ctx, ownerId, info.FileId)
	require.NoError(t, err)
	assert.False(t, row.Sealed)
	assert.True(t, row.Inline())
	assert.Equal(t, inline, row.Enc.Inline)
	assert.Equal(t, ownerId, row.ObjectId)
	assert.Equal(t, sdkA.Account().Id(), row.Author)

	// The unparented payloads object is discoverable by its (unchanged)
	// changeType — selective sync / head-sync / the filenode see it like
	// any payloads object, parented or not.
	listed, err := sp.Payloads().ListObjects(ctx)
	require.NoError(t, err)
	assert.Contains(t, listed, objId, "unparented payloads object lists by changeType")

	t.Logf("ids: space=%s derivedOwner=%s payloadsObj=%s file=%s",
		sp.Id(), ownerId, objId, info.FileId)

	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = sp.SyncHeads(ctx)

	// Device B: same account, fresh DataDir. The typeless owner tree never
	// syncs (excluded from the headsync diff), but the unparented payloads
	// object does. B must therefore resolve owner-keyed reads via the
	// derived (unparented) shape WITHOUT ever seeing the owner tree.
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

	spB := getSpaceEventually(ctx, t, sdkB, sp.Id())
	paB := payloadsSurface(t, spB)

	// Owner-keyed read on B resolves the unparented payloads object even
	// though the owner tree is absent — the fix. Without it, existingObjectId
	// short-circuits to no-rows and this never converges.
	var rowsB []payloads.Row
	if !waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		rowsB, err = paB.ListRows(ctx, ownerId)
		if err != nil && !errors.Is(err, space.ErrNotFound) {
			t.Logf("device B ListRows error: %v", err)
			return false
		}
		return len(rowsB) == 1
	}) {
		t.Fatalf("device B never resolved the derived-owner payloads row via the owner (got %d) — "+
			"owner tree is absent, so this fails if owner-keyed reads don't probe the unparented shape", len(rowsB))
	}

	gotB := rowsB[0]
	assert.Equal(t, info.FileId, gotB.Id)
	assert.False(t, gotB.Sealed, "device B holds the key and must unseal")
	assert.Equal(t, inline, gotB.Enc.Inline)
	assert.Equal(t, ownerId, gotB.ObjectId)
	assert.Equal(t, sdkA.Account().Id(), gotB.Author)

	// GetRow via the owner and the owner-agnostic FindRow (public Get) must
	// agree — the same live row is reachable both ways on B.
	gotByOwner, err := paB.GetRow(ctx, ownerId, info.FileId)
	require.NoError(t, err)
	assert.Equal(t, inline, gotByOwner.Enc.Inline)

	gotByFileId, err := spB.Files().Get(ctx, info.FileId)
	require.NoError(t, err)
	assert.Equal(t, ownerId, gotByFileId.ObjectId)
	assert.Equal(t, info.FileId, gotByFileId.FileId)

	// ObjectId on B resolves the same unparented payloads id A wrote to,
	// with the owner tree never present — determinism across peers.
	objIdB, err := paB.ObjectId(ctx, ownerId)
	require.NoError(t, err)
	assert.Equal(t, objId, objIdB, "derived-owner payloads id agrees across devices")
}

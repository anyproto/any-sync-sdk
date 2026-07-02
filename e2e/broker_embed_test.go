package e2e

import (
	"context"
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

// TestE2E_BrokerEmbed covers SYN-42 end-to-end: the filenode-v2 broker
// embedding surface. Account A (a regular member device) creates a
// space and registers payload rows. Account B — a DIFFERENT account,
// never a member and holding no keys — opens the SDK in Headless mode
// with selective sync (payloads only), Tracks A's spaceId, Gets it (the
// bare spaceId is enough: any-sync SpacePull bootstraps storage from
// the responsible nodes), and reads the cleartext row fields through
// the public PayloadsView. Then B Evicts the space (close WITHOUT
// delete) and Gets it again — the rows reopen from local disk.
func TestE2E_BrokerEmbed(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available at %s: %v", confPath, err)
	}
	if testing.Short() {
		t.Skip("broker-embed e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Account A: a regular full-sync member device that builds the space.
	cfgA := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	sdkA, err := anysyncsdk.Open(ctx, cfgA, newFixedSeedProvider(t))
	require.NoError(t, err, "account A: Open")
	t.Cleanup(func() { _ = sdkA.Close() })

	sp, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "BrokerSource"})
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

	pa := payloadsSurface(t, sp)
	fileIds, objId, err := pa.RegisterFiles(ctx, ownerId, []spaceimpl.RegisterFileOpts{
		{RootCid: "bafybrokerroot1", Size: 1111, Enc: payloads.EncPayload{
			Key:    []byte("wrapped-key-0123456789abcdef0123"),
			Name:   "one.bin",
			SHA256: []byte("0123456789abcdef0123456789abcdef"),
			Mime:   "application/octet-stream",
		}},
		{RootCid: "bafybrokerroot2", Size: 2222, Enc: payloads.EncPayload{
			Key:    []byte("wrapped-key-abcdef0123456789abcd"),
			Name:   "two.bin",
			SHA256: []byte("abcdef0123456789abcdef0123456789"),
			Mime:   "application/octet-stream",
		}},
	})
	require.NoError(t, err)
	require.Len(t, fileIds, 2)
	signedId, unsignedId := fileIds[0], fileIds[1]
	require.NoError(t, pa.SetNetworkSigns(ctx, ownerId, map[string]string{
		signedId: "net1/sig-of-bafybrokerroot1",
	}))

	// Push heads so the broker doesn't race A's periodic sync.
	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = sp.SyncHeads(ctx)

	// Account B: a different account, headless broker, payloads-only sync.
	cfgB := config.Config{
		Storage:  config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network:  config.Network{NodeConfYAML: yaml},
		Sync:     config.Sync{TreeTypes: []string{payloads.ChangeType}},
		Headless: true,
	}
	sdkB, err := anysyncsdk.Open(ctx, cfgB, newFixedSeedProvider(t))
	require.NoError(t, err, "account B: Open (headless)")
	t.Cleanup(func() { _ = sdkB.Close() })
	require.NotEqual(t, sdkA.Account().Id(), sdkB.Account().Id(),
		"the broker must run under its own account")

	// Track is local registry work; Get is what bootstraps the space via
	// SpacePull — tolerate the window before A's push lands on the nodes.
	require.NoError(t, sdkB.Spaces().Track(ctx, sp.Id()))
	require.NoError(t, sdkB.Spaces().Track(ctx, sp.Id()), "Track must be idempotent")
	var spB space.Space
	if !waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		spB, err = sdkB.Spaces().Get(ctx, sp.Id())
		if err != nil {
			t.Logf("broker Get not ready yet: %v", err)
		}
		return err == nil
	}) {
		t.Fatalf("broker never bootstrapped the tracked space: %v", err)
	}

	// The payloads object materializes (selected type) and both rows
	// converge through the public read-only view. The child tree can
	// transiently fail until the owner's heads-only stub lands, hence
	// the tolerant wait (mirrors TestE2E_SelectiveSyncPayloads).
	assertRows := func(rows []space.PayloadRow) {
		t.Helper()
		byId := map[string]space.PayloadRow{}
		for _, r := range rows {
			byId[r.FileId] = r
		}
		require.Contains(t, byId, signedId)
		require.Contains(t, byId, unsignedId)
		assert.Equal(t, "bafybrokerroot1", byId[signedId].RootCid)
		assert.Equal(t, int64(1111), byId[signedId].Size)
		assert.Equal(t, "net1/sig-of-bafybrokerroot1", byId[signedId].NetworkSign)
		assert.Equal(t, "bafybrokerroot2", byId[unsignedId].RootCid)
		assert.Equal(t, int64(2222), byId[unsignedId].Size)
		assert.Empty(t, byId[unsignedId].NetworkSign, "only the signed row carries a receipt")
		for id, r := range byId {
			assert.Equal(t, sdkA.Account().Id(), r.Author, "row %s: author must be account A", id)
			assert.True(t, r.Sealed, "row %s: a keyless broker must see the row sealed", id)
		}
	}

	var rows []space.PayloadRow
	if !waitFor(ctx, 150*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		objIds, lerr := spB.Payloads().ListObjects(ctx)
		if lerr != nil || !containsString(objIds, objId) {
			return false
		}
		rows, lerr = spB.Payloads().ListRows(ctx, objId)
		if lerr != nil {
			t.Logf("broker ListRows error: %v", lerr)
			return false
		}
		return len(rows) == 2
	}) {
		t.Fatalf("broker never converged to 2 payloads rows (got %d)", len(rows))
	}
	assertRows(rows)

	// Selective sync held: A's content trees did not materialize.
	docs, err := spB.QueryObjects().All(ctx)
	require.NoError(t, err)
	for _, d := range docs {
		id := d.GetString("id")
		assert.NotEqual(t, ownerId, id, "broker materialized the owner content tree")
		assert.NotEqual(t, typeId, id, "broker materialized the type tree")
	}

	// Evict = close WITHOUT delete: disk state stays, a later Get
	// reopens from local storage and the rows read back immediately.
	require.NoError(t, sdkB.Spaces().Evict(ctx, sp.Id()))
	require.NoError(t, sdkB.Spaces().Evict(ctx, sp.Id()), "Evict must be idempotent")
	spB, err = sdkB.Spaces().Get(ctx, sp.Id())
	require.NoError(t, err, "Get after Evict must reopen from disk")
	objIds, err := spB.Payloads().ListObjects(ctx)
	require.NoError(t, err)
	require.Contains(t, objIds, objId, "payloads object must survive Evict")
	rows, err = spB.Payloads().ListRows(ctx, objId)
	require.NoError(t, err)
	require.Len(t, rows, 2, "rows must be readable after Evict + Get")
	assertRows(rows)
}

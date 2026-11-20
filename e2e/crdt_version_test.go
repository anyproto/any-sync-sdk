package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_CRDTVersionMark: the first Open of an account stamps the
// tech space with this SDK's CRDT version, the mark reads back through
// the tech handle as a system dataset (readable, never writable
// through the generic surface), a re-Open finds it equal and leaves
// it alone, and a second device restoring the account sees the same
// mark. The refusal of a NEWER mark cannot be driven from one binary
// (an SDK never writes above its own version) and is covered by the
// handler and verdict unit tests.
func TestE2E_CRDTVersionMark(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	provider := newFixedSeedProvider(t)
	dirA := t.TempDir()
	open := func(dir, label string) *anysyncsdk.SDK {
		sdk, err := anysyncsdk.Open(ctx, config.Config{
			Storage: config.Storage{DataDir: dir, Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}, provider)
		require.NoError(t, err, "%s: Open", label)
		return sdk
	}

	sdkA := open(dirA, "device A")
	st := sdkA.CRDTVersion()
	assert.Equal(t, space.CRDTVersion, st.Supported)
	assert.Equal(t, space.CRDTVersion, st.Stored, "first open stamps the mark")
	assert.False(t, st.Newer)

	tech, err := sdkA.Spaces().Get(ctx, sdkA.TechSpaceId())
	if err != nil && isNoNetworkErr(err) {
		t.Skipf("network unreachable: %v", err)
	}
	require.NoError(t, err)
	indexId := tech.SpaceIndexObjectId()
	rows, err := tech.Query(indexId, "crdtVersion").All(ctx)
	require.NoError(t, err)
	require.Len(t, rows, 1, "one mark record")
	assert.Equal(t, "crdtVersion", rows[0].GetString("id"))
	assert.Equal(t, float64(space.CRDTVersion), rows[0].GetFloat64("version"))

	// A system dataset: the generic write surface refuses it.
	_, err = tech.Modify(ctx, space.ModifyBatch{
		ObjectId: indexId, Dataset: "crdtVersion",
		Records: []space.RecordModify{{Id: "crdtVersion", Ops: []space.Op{{Type: "$set", Path: "version", Value: 99}}}},
	})
	require.ErrorIs(t, err, space.ErrUnsupported)
	assert.False(t, sdkA.CRDTVersion().Newer)

	// A synced write on a regular space passes the gate.
	sp, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "CRDTVersion"})
	if err != nil && isNoNetworkErr(err) {
		t.Skipf("network unreachable on space create: %v", err)
	}
	require.NoError(t, err)
	_, err = sp.Objects().Create(ctx, space.CreateObjectOpts{Type: markerTypeId(t, ctx, sp, "Doc")})
	require.NoError(t, err)

	// Re-open on the same data: equal mark, nothing rewritten.
	require.NoError(t, sdkA.Close())
	sdkA = open(dirA, "device A again")
	assert.Equal(t, space.CRDTVersion, sdkA.CRDTVersion().Stored)
	rows, err = tech.Query(indexId, "crdtVersion").All(ctx)
	if err == nil {
		assert.Len(t, rows, 1)
	}

	// A second device restoring the account converges on the mark.
	sdkB := open(t.TempDir(), "device B")
	defer func() { _ = sdkB.Close() }()
	require.NoError(t, sdkB.Spaces().WaitListSynced(ctx))
	assert.Equal(t, space.CRDTVersion, sdkB.CRDTVersion().Stored)
	assert.False(t, sdkB.CRDTVersion().Newer)
	require.NoError(t, sdkA.Close())
}

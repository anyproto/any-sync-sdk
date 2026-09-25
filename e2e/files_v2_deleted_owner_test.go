package e2e

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// untouchedReader fails the test if the attach reads a byte of it.
type untouchedReader struct{ t *testing.T }

func (r untouchedReader) Read([]byte) (int, error) {
	r.t.Error("attach read the body of a refused upload")
	return 0, context.Canceled
}

// TestE2E_FilesV2_AttachToDeletedOwner: an attach to a deleted owner is
// refused with ErrObjectDeleted before the body is read — whether the
// object never had files, had some, or is a derived child cascade-
// deleted with its created parent (whose payloads object has no parent
// gate of its own).
func TestE2E_FilesV2_AttachToDeletedOwner(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available at %s: %v", confPath, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	sdk, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "AttachToDeletedOwner"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Create: %v", err)
	}
	typeId := markerTypeId(t, ctx, sp, "FileHolder")

	bare, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)
	withFiles, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)
	_, err = sp.Files().Attach(ctx, withFiles, bytes.NewReader([]byte("before the delete")), space.AttachOpts{Name: "a.txt"})
	require.NoError(t, err)
	root, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)
	child, err := sp.Objects().Derive(ctx, space.DeriveObjectOpts{Seed: []byte("child"), Type: typeId, ParentId: root})
	require.NoError(t, err)

	require.NoError(t, sp.Objects().Delete(ctx, bare))
	require.NoError(t, sp.Objects().Delete(ctx, withFiles))
	require.NoError(t, sp.Objects().Delete(ctx, root))

	for name, id := range map[string]string{"never had files": bare, "had files": withFiles, "cascade-deleted child": child} {
		_, err := sp.Files().Attach(ctx, id, untouchedReader{t}, space.AttachOpts{Name: "late.bin"})
		require.ErrorIs(t, err, space.ErrObjectDeleted, name)
	}
}

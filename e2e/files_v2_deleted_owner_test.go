package e2e

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cheggaaa/mb/v3"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/payloads"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
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
// gate of its own). Registration refuses the same way, for an owner
// deleted while its upload was spooling, and the deleted owners' files
// stop listing, resolving and counting at once — the derived child's
// too, though its payloads object outlives it. Live file queries on
// the deleted owners close with ErrObjectDeleted, and their file index
// entries go with them.
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
	attached, err := sp.Files().Attach(ctx, withFiles, bytes.NewReader([]byte("before the delete")), space.AttachOpts{Name: "a.txt"})
	require.NoError(t, err)
	root, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
	require.NoError(t, err)
	child, err := sp.Objects().Derive(ctx, space.DeriveObjectOpts{Seed: []byte("child"), Type: typeId, ParentId: root})
	require.NoError(t, err)
	childFile, err := sp.Files().Attach(ctx, child, bytes.NewReader([]byte("child's file")), space.AttachOpts{Name: "c.txt"})
	require.NoError(t, err)
	childPayloads, err := payloadsSurface(t, sp).ObjectId(ctx, child)
	require.NoError(t, err)
	pa := payloadsSurface(t, sp)

	// Live file queries and index entries, to be ended by the deletes.
	subs := map[string]space.QuerySubscription{}
	for name, id := range map[string]string{"had files": withFiles, "cascade-deleted child": child} {
		q, err := sp.Files().Query(id)
		require.NoError(t, err, name)
		res, err := q.Subscribe(ctx, space.QueryOpts{})
		require.NoError(t, err, name)
		require.Len(t, res.Initial, 1, name)
		subs[name] = res.Sub
	}
	for name, f := range map[string]space.FileInfo{"had files": attached, "cascade-deleted child": childFile} {
		_, ok, err := pa.IndexedPayloadsObject(ctx, f.FileId)
		require.NoError(t, err)
		require.True(t, ok, "%s: the attach indexed the file", name)
	}

	require.NoError(t, sp.Objects().Delete(ctx, bare))
	require.NoError(t, sp.Objects().Delete(ctx, withFiles))
	require.NoError(t, sp.Objects().Delete(ctx, root))

	for name, sub := range subs {
		waitCtx, cancelWait := context.WithTimeout(ctx, 30*time.Second)
		for {
			_, err := sub.Events().WaitOne(waitCtx)
			if err != nil {
				require.True(t, errors.Is(err, mb.ErrClosed), "%s: the subscription must close, got %v", name, err)
				break
			}
		}
		cancelWait()
		require.ErrorIs(t, sub.Err(), space.ErrObjectDeleted, "%s: close reason", name)
		require.NoError(t, sub.Close())
	}
	for name, f := range map[string]space.FileInfo{"had files": attached, "cascade-deleted child": childFile} {
		require.Eventually(t, func() bool {
			_, ok, err := pa.IndexedPayloadsObject(ctx, f.FileId)
			return err == nil && !ok
		}, 30*time.Second, 100*time.Millisecond, "%s: the file index entry must go with the owner", name)
	}
	late := []byte("spooled before the delete")
	for name, id := range map[string]string{"never had files": bare, "had files": withFiles, "cascade-deleted child": child} {
		_, err := sp.Files().Attach(ctx, id, untouchedReader{t}, space.AttachOpts{Name: "late.bin"})
		require.ErrorIs(t, err, space.ErrObjectDeleted, name)
		_, _, err = pa.RegisterFile(ctx, id, spaceimpl.RegisterFileOpts{Size: int64(len(late)), Enc: payloads.EncPayload{Inline: late}})
		require.ErrorIs(t, err, space.ErrObjectDeleted, name)
	}

	for name, f := range map[string]space.FileInfo{"had files": attached, "cascade-deleted child": childFile} {
		files, err := sp.Files().List(ctx, space.FileListOpts{ObjectId: f.ObjectId})
		require.NoError(t, err)
		require.Empty(t, files, "%s: a deleted owner lists no files", name)
		_, err = sp.Files().Get(ctx, f.FileId)
		require.ErrorIs(t, err, space.ErrNotFound, "%s: a deleted owner's file no longer resolves", name)
	}
	all, err := sp.Files().List(ctx, space.FileListOpts{})
	require.NoError(t, err)
	require.Empty(t, all, "the space lists no files of deleted owners")
	rows, err := sp.Payloads().ListRows(ctx, childPayloads)
	require.NoError(t, err)
	require.Empty(t, rows, "the payloads view skips a deleted owner's rows")
	stats, err := sp.Files().Stats(ctx)
	require.NoError(t, err)
	require.Zero(t, stats.Total, "stats count no files of deleted owners")
}

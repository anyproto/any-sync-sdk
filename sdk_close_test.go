package anysyncsdk

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/anyproto/any-sync/app/ocache"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/auth"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestClose_ReleasesDataDirFiles pins that Close leaves no file under
// the data dir open: Windows cannot remove an open file, so a leaked
// handle breaks space deletion and any host that cleans the dir up. A
// clean Close also clears every durability sentinel, so the next Open
// skips the quick-check an unclean shutdown needs.
func TestClose_ReleasesDataDirFiles(t *testing.T) {
	yaml, err := os.ReadFile("e2e/local.yml")
	if err != nil {
		t.Skipf("nodeconf not available: %v", err)
	}
	dataDir := t.TempDir()
	provider, err := auth.NewFileProvider(auth.FileProviderConfig{
		Path: filepath.Join(dataDir, "wallet.key"),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sdk, err := Open(ctx, config.Config{
		Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}, provider)
	require.NoError(t, err)
	select {
	case <-sdk.BootstrapDone():
	case <-ctx.Done():
		t.Fatal("bootstrap did not complete")
	}
	require.NoError(t, sdk.Close())

	require.Empty(t, openFilesUnder(t, dataDir), "files still open after Close")
	sentinels, err := filepath.Glob(filepath.Join(dataDir, "anysync", "*.db.lock"))
	require.NoError(t, err)
	require.Empty(t, sentinels, "dirty sentinels after a clean Close")
	require.NoError(t, os.RemoveAll(dataDir))
}

// TestClose_ClosesLoadedSpaceStores pins that Close stops the per-space
// Store of every space still loaded: its drainer, read-state
// materializer and reindex sweep write the SDK db, so they must be
// joined before the db closes. A late storeFor gets the closed store.
func TestClose_ClosesLoadedSpaceStores(t *testing.T) {
	yaml, err := os.ReadFile("e2e/local.yml")
	if err != nil {
		t.Skipf("nodeconf not available: %v", err)
	}
	dataDir := t.TempDir()
	provider, err := auth.NewFileProvider(auth.FileProviderConfig{
		Path: filepath.Join(dataDir, "wallet.key"),
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sdk, err := Open(ctx, config.Config{
		Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}, provider)
	require.NoError(t, err)
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "close"})
	require.NoError(t, err)
	store := sdk.spaces.StoreFor(sp.Id())
	_, err = store.Get(ctx, sp.SpaceIndexObjectId())
	require.NoError(t, err, "the store serves objects while the SDK is open")

	require.NoError(t, sdk.Close())

	_, err = store.Get(ctx, sp.SpaceIndexObjectId())
	require.ErrorIs(t, err, ocache.ErrClosed, "Close must close a loaded space's store")
	require.Same(t, store, sdk.spaces.StoreFor(sp.Id()), "a late storeFor must not build a fresh store")

	// A space first touched after Close gets a store born closed.
	late := sdk.spaces.StoreFor("space-first-seen-after-close")
	_, err = late.Get(ctx, "any-object")
	require.ErrorIs(t, err, ocache.ErrClosed, "a store built after Close must be closed")
}

// openFilesUnder lists this process's open files below dir. Only Linux
// exposes them; elsewhere the caller's RemoveAll is the check.
func openFilesUnder(t *testing.T, dir string) []string {
	t.Helper()
	if runtime.GOOS != "linux" {
		return nil
	}
	fds, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	var out []string
	for _, fd := range fds {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
		if err == nil && strings.HasPrefix(target, dir+string(filepath.Separator)) {
			out = append(out, target)
		}
	}
	return out
}

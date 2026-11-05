package e2e

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/settings"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_BundlesUninstall covers Uninstall on a forked install.
//
//  1. Device B installs two bundles while apart from the network,
//     after A installed the same ids: two created roots per bundle,
//     which the registry resolves to one winner and one loser.
//  2. Uninstall on A deletes the loser and the winner; both devices
//     read the bundle as uninstalled, and a fresh install lists no
//     loser, so nothing of the old install can be merged back.
//  3. Deleting the winner object alone leaves the loser claimed and
//     the next install inherits it; Uninstall on the dead row sweeps
//     it.
//  4. An unknown bundle is ErrBundleUnknown; a derived install is
//     refused with any-sync's derived-delete error and stays.
func TestE2E_BundlesUninstall(t *testing.T) {
	t.Parallel()
	netYAML, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("bundles e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	provider := newFixedSeedProvider(t)
	open := func(label, dataDir string, nodeConf []byte) *anysyncsdk.SDK {
		cfg := config.Config{
			Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: nodeConf},
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, provider)
		require.NoError(t, err, "%s: Open", label)
		return sdk
	}
	declared := func(id string) space.EnsureBundleRequest {
		return space.EnsureBundleRequest{Id: id, Name: id, Parts: []space.PartDraft{articlesPart()}}
	}
	const resetId, staleId = "reset/v1", "stale/v1"
	ids := []string{resetId, staleId}

	// --- A creates the space; B restores it with an empty registry. ---
	sdkA := open("device A", t.TempDir(), netYAML)
	defer func() { _ = sdkA.Close() }()
	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "UninstallHome"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("device A: Spaces().Create: %v", err)
	}
	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = spA.SyncHeads(ctx)

	dirB := t.TempDir()
	sdkB := open("device B", dirB, netYAML)
	waitCtx, waitCancel := context.WithTimeout(ctx, 90*time.Second)
	require.NoError(t, sdkB.Spaces().WaitListSynced(waitCtx), "device B: WaitListSynced")
	waitCancel()
	var spB space.Space
	require.True(t, waitFor(ctx, 30*time.Second, 250*time.Millisecond, func() bool {
		spB, err = sdkB.Spaces().Get(ctx, spA.Id())
		return err == nil
	}), "device B: Spaces().Get(%s) never succeeded: %v", spA.Id(), err)
	waitCtx, waitCancel = context.WithTimeout(ctx, 90*time.Second)
	require.NoError(t, spB.WaitIndexSynced(waitCtx), "device B: WaitIndexSynced")
	waitCancel()
	_, err = spB.Bundles().Get(ctx, resetId)
	require.ErrorIs(t, err, space.ErrBundleUnknown, "device B restored before any install")
	require.NoError(t, sdkB.Close(), "device B: Close")

	// --- A installs both; B installs both while apart. ---
	rootA := map[string]string{}
	for _, id := range ids {
		bd, did, err := spA.Bundles().Ensure(ctx, declared(id))
		require.NoError(t, err, "device A: Ensure(%s)", id)
		require.True(t, did, "device A: first Ensure(%s) must install", id)
		rootA[id] = bd.RootId
	}
	_ = spA.SyncHeads(ctx)

	restore := parkStoredNodeconf(t, dirB)
	sdkB = open("device B apart", dirB, deadNetwork(t, netYAML))
	spB, err = sdkB.Spaces().Get(ctx, spA.Id())
	require.NoError(t, err, "device B apart: Spaces().Get")
	for _, id := range ids {
		bd, did, err := spB.Bundles().Ensure(ctx, declared(id))
		require.NoError(t, err, "device B apart: Ensure(%s)", id)
		require.True(t, did, "device B apart from A must mint its own %s root", id)
		require.NotEqual(t, rootA[id], bd.RootId, "%s: B minted A's root while apart", id)
	}
	require.NoError(t, sdkB.Close(), "device B apart: Close")
	restore()

	// --- B back online: each fork converges on both devices. ---
	sdkB = open("device B", dirB, netYAML)
	defer func() { _ = sdkB.Close() }()
	spB, err = sdkB.Spaces().Get(ctx, spA.Id())
	require.NoError(t, err, "device B: Spaces().Get after reopen")

	converged := func(id string) space.Bundle {
		var a, b space.Bundle
		var aerr, berr error
		require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
			_ = spA.SyncHeads(ctx)
			_ = spB.SyncHeads(ctx)
			a, aerr = spA.Bundles().Get(ctx, id)
			b, berr = spB.Bundles().Get(ctx, id)
			return aerr == nil && berr == nil && a.RootId != "" && a.RootId == b.RootId &&
				len(a.Roots) == 2 && len(b.Roots) == 2
		}), "%s never converged: A=%+v(%v) B=%+v(%v)", id, a, aerr, b, berr)
		require.Len(t, a.Losers, 1, "%s: the fork must list one loser", id)
		return a
	}
	// Uninstall deletes what is local; both trees of the fork must have
	// reached A before it runs, or the test would measure sync timing.
	heldByA := func(roots []string) bool {
		for _, id := range roots {
			if spA.SyncStatus().Object(id).State != space.SyncStateSynced {
				return false
			}
		}
		return true
	}

	// --- reset/v1: Uninstall deletes the loser and the winner. ---
	reset := converged(resetId)
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = spA.SyncHeads(ctx)
		return heldByA(reset.Roots)
	}), "device A never held both reset/v1 trees")
	require.NoError(t, spA.Bundles().Uninstall(ctx, resetId), "device A: Uninstall(reset/v1)")
	_, err = spA.Bundles().Get(ctx, resetId)
	require.ErrorIs(t, err, space.ErrBundleUnknown, "device A: uninstalled")
	require.NoError(t, spA.Bundles().Uninstall(ctx, resetId), "Uninstall is idempotent")
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		_, err := spB.Bundles().Get(ctx, resetId)
		return errors.Is(err, space.ErrBundleUnknown)
	}), "device B never read reset/v1 as uninstalled")

	fresh, did, err := spA.Bundles().Ensure(ctx, declared(resetId))
	require.NoError(t, err, "device A: reinstall reset/v1")
	require.True(t, did, "a reinstall mints a fresh root")
	require.NotContains(t, reset.Roots, fresh.RootId)
	require.Empty(t, fresh.Losers, "nothing of the old install survives to be merged back")
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = spA.SyncHeads(ctx)
		_ = spB.SyncHeads(ctx)
		b, err := spB.Bundles().Get(ctx, resetId)
		return err == nil && b.RootId == fresh.RootId && len(b.Losers) == 0
	}), "device B never adopted the fresh reset/v1 root without losers")

	// --- stale/v1: a deleted winner alone leaves the loser claimed. ---
	stale := converged(staleId)
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = spA.SyncHeads(ctx)
		return heldByA(stale.Roots)
	}), "device A never held both stale/v1 trees")
	require.NoError(t, spA.Objects().Delete(ctx, stale.RootId), "device A: delete the winner object")
	_, err = spA.Bundles().Get(ctx, staleId)
	require.ErrorIs(t, err, space.ErrBundleUnknown, "a dead winner reads as uninstalled")
	inherited, did, err := spA.Bundles().Ensure(ctx, declared(staleId))
	require.NoError(t, err, "device A: reinstall stale/v1 after the object delete")
	require.True(t, did)
	require.Equal(t, stale.Losers, inherited.Losers, "the next install inherits the loser the object delete left")

	// Uninstall takes the inherited loser with the fresh winner.
	require.NoError(t, spA.Bundles().Uninstall(ctx, staleId), "device A: Uninstall(stale/v1)")
	clean, did, err := spA.Bundles().Ensure(ctx, declared(staleId))
	require.NoError(t, err, "device A: reinstall stale/v1 after Uninstall")
	require.True(t, did)
	require.Empty(t, clean.Losers, "the loser is gone with the install")
	require.Len(t, clean.Roots, 4, "every claim stays listed")

	// --- Refusals. ---
	require.ErrorIs(t, spA.Bundles().Uninstall(ctx, "nope/v1"), space.ErrBundleUnknown)
	derived, _, err := spA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "fixed/v1", Name: "Fixed", DerivedRoot: true, Parts: []space.PartDraft{articlesPart()},
	})
	require.NoError(t, err, "device A: Ensure(fixed/v1, derived)")
	require.True(t, derived.Derived)
	require.ErrorIs(t, spA.Bundles().Uninstall(ctx, "fixed/v1"), settings.ErrCantDeleteDerivedObject)
	still, err := spA.Bundles().Get(ctx, "fixed/v1")
	require.NoError(t, err, "a refused uninstall deletes nothing")
	require.Equal(t, derived.RootId, still.RootId)
}

// deadNetwork is the nodeconf with every node address unreachable: a
// device opened on it holds its data and cannot sync.
func deadNetwork(t *testing.T, online []byte) []byte {
	t.Helper()
	var conf map[string]any
	require.NoError(t, yaml.Unmarshal(online, &conf), "parse nodeconf")
	nodes, _ := conf["nodes"].([]any)
	for _, n := range nodes {
		n.(map[string]any)["addresses"] = []string{"127.0.0.1:1"}
	}
	dead, err := yaml.Marshal(conf)
	require.NoError(t, err, "marshal nodeconf")
	return dead
}

// parkStoredNodeconf moves the nodeconf any-sync stored in the data
// dir aside, since a stored copy beats the configured one, and returns
// the restore.
func parkStoredNodeconf(t *testing.T, dataDir string) (restore func()) {
	t.Helper()
	var parked []string
	_ = filepath.WalkDir(dataDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if filepath.Base(filepath.Dir(path)) == "nodeconf" && strings.HasSuffix(path, ".yml") {
			require.NoError(t, os.Rename(path, path+".parked"), "park stored nodeconf")
			parked = append(parked, path)
		}
		return nil
	})
	return func() {
		for _, path := range parked {
			_ = os.Rename(path+".parked", path)
		}
	}
}

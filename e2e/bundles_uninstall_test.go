package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonspace/settings"
	"github.com/stretchr/testify/require"

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
//     the next install inherits it; Uninstall on that install sweeps
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

	// Budgeted from the waits below: two convergences, five syncs and
	// the restore gates, each at its own cap.
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
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

	forgetStoredNodeConf(t, dirB)
	sdkB = open("device B apart", dirB, nodeConfUnreachable(t, netYAML))
	spB, err = sdkB.Spaces().Get(ctx, spA.Id())
	require.NoError(t, err, "device B apart: Spaces().Get")
	for _, id := range ids {
		bd, did, err := spB.Bundles().Ensure(ctx, declared(id))
		require.NoError(t, err, "device B apart: Ensure(%s)", id)
		require.True(t, did, "device B apart from A must mint its own %s root", id)
		require.NotEqual(t, rootA[id], bd.RootId, "%s: B minted A's root while apart", id)
	}
	require.NoError(t, sdkB.Close(), "device B apart: Close")

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
	// Uninstall refuses, deleting nothing, until every tree of the
	// install is local. A root minted on the other device arrives after
	// the row that names it, so the call is retried the way a client
	// retries a 409; anything but the two not-local sentinels is final.
	notLocal := func(err error) bool {
		return errors.Is(err, space.ErrLoserNotSynced) || errors.Is(err, space.ErrBundleRootNotSynced)
	}
	uninstall := func(id string) {
		var uerr error
		refused := 0
		require.True(t, waitFor(ctx, 90*time.Second, time.Second, func() bool {
			_ = spA.SyncHeads(ctx)
			uerr = spA.Bundles().Uninstall(ctx, id)
			if notLocal(uerr) {
				refused++
				return false
			}
			return true
		}), "device A: Uninstall(%s) never held every tree: %v", id, uerr)
		require.NoError(t, uerr, "device A: Uninstall(%s)", id)
		t.Logf("%s: uninstall refused %d time(s) before every tree was local", id, refused)
	}

	// --- reset/v1: Uninstall deletes the loser and the winner. ---
	reset := converged(resetId)
	uninstall(resetId)
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
	// The winner may be B's root, which deletes only once it is local:
	// wait until A reads it, then the delete must go through.
	var gerr error
	require.True(t, waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = spA.SyncHeads(ctx)
		_, gerr = spA.Objects().Get(ctx, stale.RootId)
		return gerr == nil
	}), "device A: the winner object never arrived: %v", gerr)
	require.NoError(t, spA.Objects().Delete(ctx, stale.RootId), "device A: delete the winner object")
	_, err = spA.Bundles().Get(ctx, staleId)
	require.ErrorIs(t, err, space.ErrBundleUnknown, "a dead winner reads as uninstalled")
	inherited, did, err := spA.Bundles().Ensure(ctx, declared(staleId))
	require.NoError(t, err, "device A: reinstall stale/v1 after the object delete")
	require.True(t, did)
	require.Equal(t, stale.Losers, inherited.Losers, "the next install inherits the loser the object delete left")

	// Uninstall takes the inherited loser with the fresh winner.
	uninstall(staleId)
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
	derr := spA.Bundles().Uninstall(ctx, "fixed/v1")
	require.ErrorIs(t, derr, space.ErrBundleDerived)
	require.ErrorIs(t, derr, settings.ErrCantDeleteDerivedObject, "the any-sync sentinel rides along")
	still, err := spA.Bundles().Get(ctx, "fixed/v1")
	require.NoError(t, err, "a refused uninstall deletes nothing")
	require.Equal(t, derived.RootId, still.RootId)
}

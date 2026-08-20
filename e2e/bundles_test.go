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
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_BundlesConvergeAndRestore covers the bundles registry
// across devices of one account:
//
//  1. Device A installs a bundle (Ensure creates the root and
//     registers it); a second Ensure adopts without creating.
//  2. Device B (fresh DataDir, same keys) restores via the intended
//     gate sequence — WaitListSynced → open → WaitIndexSynced — and
//     adopts A's winner without creating anything.
//  3. A and B race an install of a second bundle. Whatever
//     interleaving the network produced (adopt or genuine conflict),
//     both devices converge on the same winner; when a conflict
//     happened, ResolveLoser deletes the losing root and Losers
//     drains on both devices.
func TestE2E_BundlesConvergeAndRestore(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("bundles e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	provider := newFixedSeedProvider(t)

	newDevice := func(label string) *anysyncsdk.SDK {
		cfg := config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, provider)
		require.NoError(t, err, "%s: Open", label)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}

	newRootVia := func(sp space.Space) func(ctx context.Context) (string, error) {
		return func(ctx context.Context) (string, error) {
			return sp.Objects().Create(ctx, space.CreateObjectOpts{})
		}
	}
	// mustAdopt fails the test if Ensure reaches NewRoot — the adopt
	// path must never create.
	mustAdopt := func(label string) func(ctx context.Context) (string, error) {
		return func(ctx context.Context) (string, error) {
			return "", errors.New(label + ": NewRoot called on adopt path")
		}
	}

	// --- Device A: create the space, install bao/v1. ---
	sdkA := newDevice("device A")
	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "BundleHome"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("device A: Spaces().Create: %v", err)
	}

	installed, err := spA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "bao/v1", Name: "Bao", NewRoot: newRootVia(spA),
	})
	require.NoError(t, err, "device A: Ensure(bao/v1)")
	require.NotEmpty(t, installed.RootId)
	require.Equal(t, []string{installed.RootId}, installed.Roots)
	require.Empty(t, installed.Losers)

	adopted, err := spA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "bao/v1", NewRoot: mustAdopt("device A"),
	})
	require.NoError(t, err, "device A: re-Ensure must adopt")
	require.Equal(t, installed.RootId, adopted.RootId)

	// Push before B pulls (see cold_sync_test for why).
	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = spA.SyncHeads(ctx)

	// --- Device B: restore through the intended gate sequence. ---
	sdkB := newDevice("device B")
	require.Equal(t, sdkA.Account().Id(), sdkB.Account().Id())

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

	// After the index gate, the registry is readable and Ensure adopts.
	got, err := spB.Bundles().Get(ctx, "bao/v1")
	require.NoError(t, err, "device B: Bundles().Get after WaitIndexSynced")
	require.Equal(t, installed.RootId, got.RootId)

	gotEnsure, err := spB.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "bao/v1", NewRoot: mustAdopt("device B"),
	})
	require.NoError(t, err, "device B: Ensure must adopt")
	require.Equal(t, installed.RootId, gotEnsure.RootId)

	// --- Concurrent install race on a second bundle. ---
	// Both devices Ensure without a prior sync kick. Depending on how
	// realtime sync interleaves, one device may adopt the other's
	// record (no conflict) or both create roots (conflict). Both
	// outcomes are valid; the invariant is deterministic convergence.
	resA, errA := spA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "race/v1", Name: "Race", NewRoot: newRootVia(spA),
	})
	resB, errB := spB.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "race/v1", Name: "Race", NewRoot: newRootVia(spB),
	})
	require.NoError(t, errA, "device A: Ensure(race/v1)")
	require.NoError(t, errB, "device B: Ensure(race/v1)")
	t.Logf("race installs: A→%s B→%s", resA.RootId, resB.RootId)

	// Converge: same winner on both devices, every claimed root listed.
	var winA, winB space.Bundle
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = spA.SyncHeads(ctx)
		_ = spB.SyncHeads(ctx)
		winA, errA = spA.Bundles().Get(ctx, "race/v1")
		winB, errB = spB.Bundles().Get(ctx, "race/v1")
		return errA == nil && errB == nil &&
			winA.RootId != "" && winA.RootId == winB.RootId &&
			len(winA.Roots) == len(winB.Roots)
	}), "race/v1 never converged: A=%+v(%v) B=%+v(%v)", winA, errA, winB, errB)
	assert.ElementsMatch(t, winA.Roots, winB.Roots)

	// Conflict cleanup, when the race produced one.
	if len(winA.Losers) > 0 {
		t.Logf("race produced a conflict: winner=%s losers=%v", winA.RootId, winA.Losers)
		for _, loser := range winA.Losers {
			// The loser's tree usually lives on the other device; the
			// pull-on-demand inside ResolveLoser can transiently fail
			// while the node hasn't received it yet — retry.
			var rerr error
			require.True(t, waitFor(ctx, 60*time.Second, time.Second, func() bool {
				rerr = spA.Bundles().ResolveLoser(ctx, "race/v1", loser)
				return rerr == nil
			}), "device A: ResolveLoser(%s) never succeeded: %v", loser, rerr)
			// Idempotent re-resolve.
			require.NoError(t, spA.Bundles().ResolveLoser(ctx, "race/v1", loser))
		}
		require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
			_ = spA.SyncHeads(ctx)
			_ = spB.SyncHeads(ctx)
			a, aerr := spA.Bundles().Get(ctx, "race/v1")
			b, berr := spB.Bundles().Get(ctx, "race/v1")
			return aerr == nil && berr == nil && len(a.Losers) == 0 && len(b.Losers) == 0
		}), "losers never drained after ResolveLoser")
	} else {
		t.Logf("race resolved as adopt (no conflict): winner=%s", winA.RootId)
	}

	// ResolveLoser must refuse the winner.
	err = spA.Bundles().ResolveLoser(ctx, "race/v1", winA.RootId)
	require.ErrorIs(t, err, space.ErrBundleNotLoser)

	// Unknown bundle stays unknown.
	_, err = spB.Bundles().Get(ctx, "nope/v1")
	require.ErrorIs(t, err, space.ErrBundleUnknown)
}

// TestE2E_BundlesDerivedRoot covers the fork-proof install: a bundle
// whose root is DERIVED from its id instead of created.
//
//  1. The root id is computable before anything exists, from the
//     bundle id alone — no registry read, no network.
//  2. Installing registers exactly that id; a second device computes
//     the same one and its own install claims the same root, so the
//     claim set never grows past one and there is nothing to resolve.
//  3. Adoption still wins: a later Ensure asking for a created root
//     adopts the derived install rather than forking it.
func TestE2E_BundlesDerivedRoot(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("bundles e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	provider := newFixedSeedProvider(t)
	newDevice := func(label string) *anysyncsdk.SDK {
		cfg := config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, provider)
		require.NoError(t, err, "%s: Open", label)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}

	const bundleId = "derived-chat/v1"

	sdkA := newDevice("device A")
	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "DerivedBundleHome"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("device A: Spaces().Create: %v", err)
	}

	// The id exists before the install does.
	wantRoot, err := spA.Bundles().DerivedRootId(ctx, bundleId)
	require.NoError(t, err, "device A: DerivedRootId")
	require.NotEmpty(t, wantRoot)
	_, err = spA.Bundles().Get(ctx, bundleId)
	require.ErrorIs(t, err, space.ErrBundleUnknown, "computing the id must not install anything")

	inst, err := spA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: bundleId, Name: "Chat", DerivedRoot: true,
	})
	require.NoError(t, err, "device A: Ensure(derived)")
	require.Equal(t, wantRoot, inst.RootId, "installed root must be the canonical derived id")
	require.True(t, inst.Derived)
	require.Equal(t, []string{wantRoot}, inst.Roots)
	require.Empty(t, inst.Losers)

	// A created-root request cannot fork a live install.
	adopted, err := spA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: bundleId,
		NewRoot: func(context.Context) (string, error) {
			return "", errors.New("device A: NewRoot called on adopt path")
		},
	})
	require.NoError(t, err, "device A: re-Ensure must adopt")
	require.Equal(t, wantRoot, adopted.RootId)
	require.True(t, adopted.Derived)

	// Push before B pulls (see cold_sync_test for why).
	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = spA.SyncHeads(ctx)

	// --- Device B: same account, fresh storage. ---
	sdkB := newDevice("device B")
	waitCtx, waitCancel := context.WithTimeout(ctx, 90*time.Second)
	require.NoError(t, sdkB.Spaces().WaitListSynced(waitCtx), "device B: WaitListSynced")
	waitCancel()

	var spB space.Space
	require.True(t, waitFor(ctx, 30*time.Second, 250*time.Millisecond, func() bool {
		spB, err = sdkB.Spaces().Get(ctx, spA.Id())
		return err == nil
	}), "device B: Spaces().Get(%s) never succeeded: %v", spA.Id(), err)

	// Computed from the bundle id, not read from the registry — B has
	// run no convergence gate at this point.
	gotRoot, err := spB.Bundles().DerivedRootId(ctx, bundleId)
	require.NoError(t, err, "device B: DerivedRootId")
	require.Equal(t, wantRoot, gotRoot, "the derived root id must not depend on the device")

	// The offline-1-1 shape: install with no WaitIndexSynced. Whether
	// B's registry has converged or not, it lands on the same root.
	resB, err := spB.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: bundleId, Name: "Chat", DerivedRoot: true,
	})
	require.NoError(t, err, "device B: Ensure(derived) without a convergence gate")
	require.Equal(t, wantRoot, resB.RootId)
	require.True(t, resB.Derived)
	require.Equal(t, []string{wantRoot}, resB.Roots, "a second install must not claim a second root")
	require.Empty(t, resB.Losers)

	// Converge: one root, no conflict, on both devices.
	var a, b space.Bundle
	var aerr, berr error
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = spA.SyncHeads(ctx)
		_ = spB.SyncHeads(ctx)
		a, aerr = spA.Bundles().Get(ctx, bundleId)
		b, berr = spB.Bundles().Get(ctx, bundleId)
		return aerr == nil && berr == nil && a.RootId == wantRoot && b.RootId == wantRoot &&
			len(a.Roots) == 1 && len(b.Roots) == 1
	}), "derived install never converged: A=%+v(%v) B=%+v(%v)", a, aerr, b, berr)
	assert.True(t, a.Derived && b.Derived)
	assert.Empty(t, a.Losers)
	assert.Empty(t, b.Losers)

	// The derived root is permanent: the tree cannot be deleted, which
	// is the price of never forking.
	require.Error(t, spA.Objects().Delete(ctx, wantRoot), "a derived bundle root must not be deletable")

	// A derived root cannot be a PARENT (any-sync: a derived object is
	// not a valid ParentId), so a derived install's setup objects hang
	// off it by seed instead — which converges the same way and needs
	// no cascade, the root being undeletable anyway. Pinned because
	// both halves of the rule are permanent.
	_, err = spA.Objects().Derive(ctx, space.DeriveObjectOpts{Seed: []byte("inbox"), ParentId: wantRoot})
	require.Error(t, err, "a derived root must not be usable as a parent")

	childA, err := spA.Objects().Derive(ctx, space.DeriveObjectOpts{Seed: []byte(wantRoot + "/inbox")})
	require.NoError(t, err)
	childB, err := spB.Objects().Derive(ctx, space.DeriveObjectOpts{Seed: []byte(wantRoot + "/inbox")})
	require.NoError(t, err)
	require.Equal(t, childA, childB)
}

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

	installed, didInstall, err := spA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "bao/v1", Name: "Bao", NewRoot: newRootVia(spA),
	})
	require.NoError(t, err, "device A: Ensure(bao/v1)")
	require.True(t, didInstall, "first Ensure must register the install")
	require.NotEmpty(t, installed.RootId)
	require.Equal(t, []string{installed.RootId}, installed.Roots)
	require.Empty(t, installed.Losers)

	adopted, didInstall, err := spA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "bao/v1", NewRoot: mustAdopt("device A"),
	})
	require.NoError(t, err, "device A: re-Ensure must adopt")
	require.False(t, didInstall, "adopting must not report an install")
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

	gotEnsure, _, err := spB.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "bao/v1", NewRoot: mustAdopt("device B"),
	})
	require.NoError(t, err, "device B: Ensure must adopt")
	require.Equal(t, installed.RootId, gotEnsure.RootId)

	// --- Concurrent install race on a second bundle. ---
	// Both devices Ensure without a prior sync kick. Depending on how
	// realtime sync interleaves, one device may adopt the other's
	// record (no conflict) or both create roots (conflict). Both
	// outcomes are valid; the invariant is deterministic convergence.
	resA, _, errA := spA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "race/v1", Name: "Race", NewRoot: newRootVia(spA),
	})
	resB, _, errB := spB.Bundles().Ensure(ctx, space.EnsureBundleRequest{
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

	// The bundle declares a FULL type: a part, two properties with
	// handles, a layout and a weight. Both devices send the same
	// request; the property ids derive from (root, handle), so the
	// two installs mint one column per handle.
	declaredType := func() space.EnsureBundleRequest {
		return space.EnsureBundleRequest{
			Id: bundleId, Name: "Chat", DerivedRoot: true,
			RootProperties: map[string]map[string]any{"any": {"description": "seeded"}},
			Parts:          []space.PartDraft{articlesPart()},
			Properties: []space.PropertyDraft{
				{XKey: "parentId", Name: "Parent", Kind: space.PropertyKindString},
				{XKey: "pos", Name: "Position", Kind: space.PropertyKindString, XFormat: map[string]any{"type": "text"}},
			},
			Layout: map[string]any{"type": "chat"},
			Weight: 5,
		}
	}
	inst, didInstall, err := spA.Bundles().Ensure(ctx, declaredType())
	require.NoError(t, err, "device A: Ensure(derived)")
	require.True(t, didInstall, "first derived Ensure must register the install")
	require.Equal(t, wantRoot, inst.RootId, "installed root must be the canonical derived id")
	require.True(t, inst.Derived)
	require.Equal(t, []string{wantRoot}, inst.Roots)
	require.Empty(t, inst.Losers)

	// A root declaring parts is a type implementing itself: the
	// declaration is discoverable under typeId = rootId and records
	// land on the root through the generic upsert path.
	defs, err := spA.Types().Datasets(ctx, wantRoot)
	require.NoError(t, err, "device A: Types().Datasets(root)")
	require.Len(t, defs, 1)
	require.Equal(t, "articles", defs[0].Key)
	articlesColl := defs[0].Collection
	require.Equal(t, wantRoot+"_articles", articlesColl)
	upRes, err := spA.Upsert(ctx, space.UpsertBatch{
		ObjectId: wantRoot, Dataset: articlesColl,
		Records: []space.UpsertRecord{{Id: "a-1", Fields: map[string]any{"title": "One"}}},
	})
	require.NoError(t, err, "device A: Upsert on the bundle root")
	require.Equal(t, 1, upRes.Created)
	require.Empty(t, upRes.Rejections)

	// The type's metadata and properties: weight / layout on the root,
	// one definition per handle, listed (Hidden was not asked for), and
	// usable on an object carrying the root as its type.
	rootInfo, err := spA.Types().Get(ctx, wantRoot)
	require.NoError(t, err)
	assert.Equal(t, 5, rootInfo.Weight)
	assert.Equal(t, map[string]any{"type": "chat"}, rootInfo.Layout)
	assert.False(t, rootInfo.Hidden, "hidden is explicit — a declared type stays listed")
	propsA, err := spA.Types().Properties(ctx, wantRoot)
	require.NoError(t, err)
	require.Len(t, propsA, 2)
	propIdsA := map[string]string{}
	for _, p := range propsA {
		propIdsA[p.XKey] = p.Id
	}
	require.Len(t, propIdsA, 2)
	assert.Equal(t, space.PropertyKindString, propsA[0].Kind)
	carrier, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{wantRoot}})
	require.NoError(t, err)
	_, err = spA.Properties().Set(ctx, carrier, wantRoot, map[string]any{propIdsA["parentId"]: "any://o/x"})
	require.NoError(t, err, "a bundle-declared property takes values on a carrier")

	// Re-Ensure adopts and declares nothing twice: same ids, same count.
	_, didInstall, err = spA.Bundles().Ensure(ctx, declaredType())
	require.NoError(t, err)
	require.False(t, didInstall)
	propsA, err = spA.Types().Properties(ctx, wantRoot)
	require.NoError(t, err)
	require.Len(t, propsA, 2, "adopt must not redeclare a property")

	// Initial property values land on the derived root — a created
	// root gets them from Objects().Create, a derived one from the
	// seeding write Ensure runs before it registers anything.
	props, err := spA.Properties().Get(ctx, wantRoot)
	require.NoError(t, err)
	require.NotNil(t, props)
	require.Equal(t, "seeded", string(props.Get("any", "description").GetStringBytes()))

	// A created-root request cannot fork a live install.
	adopted, didInstall, err := spA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: bundleId,
		NewRoot: func(context.Context) (string, error) {
			return "", errors.New("device A: NewRoot called on adopt path")
		},
	})
	require.NoError(t, err, "device A: re-Ensure must adopt")
	require.False(t, didInstall, "adopting must not report an install")
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
	// B's registry has converged or not, it lands on the same root —
	// and on the same property ids.
	resB, _, err := spB.Bundles().Ensure(ctx, declaredType())
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

	// Both devices declared `articles`; the definitions converge to one
	// per name and A's record reaches B.
	var defsB []space.DatasetDef
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = spA.SyncHeads(ctx)
		_ = spB.SyncHeads(ctx)
		defsA, aerr := spA.Types().Datasets(ctx, wantRoot)
		defsB, berr = spB.Types().Datasets(ctx, wantRoot)
		if aerr != nil || berr != nil || len(defsA) != 1 || len(defsB) != 1 || defsA[0].Id != defsB[0].Id {
			return false
		}
		n, qerr := spB.Query(wantRoot, articlesColl).Count(ctx)
		return qerr == nil && n == 1
	}), "bundle datasets never converged: B defs=%+v", defsB)

	// Both devices declared the properties blind; the deterministic ids
	// make them ONE definition per handle, not two columns.
	var propsB []space.PropertyDef
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = spA.SyncHeads(ctx)
		_ = spB.SyncHeads(ctx)
		var perr error
		propsB, perr = spB.Types().Properties(ctx, wantRoot)
		if perr != nil || len(propsB) != 2 {
			return false
		}
		for _, p := range propsB {
			if propIdsA[p.XKey] != p.Id {
				return false
			}
		}
		return true
	}), "bundle properties never converged: B props=%+v", propsB)
	infoB, err := spB.Types().Get(ctx, wantRoot)
	require.NoError(t, err)
	assert.Equal(t, 5, infoB.Weight)
	assert.Equal(t, map[string]any{"type": "chat"}, infoB.Layout)

	// Data written AFTER both installs crosses in both directions: each
	// device stamps the root's newest schema state, which includes the
	// property the other device created under the same id — every
	// replica must know that change's shortId, or the write parks.
	upB, err := spB.Upsert(ctx, space.UpsertBatch{
		ObjectId: wantRoot, Dataset: articlesColl,
		Records: []space.UpsertRecord{{Id: "a-2", Fields: map[string]any{"title": "Two"}}},
	})
	require.NoError(t, err)
	require.Equal(t, 1, upB.Created)
	upA, err := spA.Upsert(ctx, space.UpsertBatch{
		ObjectId: wantRoot, Dataset: articlesColl,
		Records: []space.UpsertRecord{{Id: "a-3", Fields: map[string]any{"title": "Three"}}},
	})
	require.NoError(t, err)
	require.Equal(t, 1, upA.Created)
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = spA.SyncHeads(ctx)
		_ = spB.SyncHeads(ctx)
		na, aerr := spA.Query(wantRoot, articlesColl).Count(ctx)
		nb, berr := spB.Query(wantRoot, articlesColl).Count(ctx)
		return aerr == nil && berr == nil && na == 3 && nb == 3
	}), "records written after both installs never crossed")

	// Adopt never patches the type's metadata: asking for Hidden on a
	// listed install changes nothing.
	hiddenReq := declaredType()
	hiddenReq.Hidden = true
	_, didInstall, err = spA.Bundles().Ensure(ctx, hiddenReq)
	require.NoError(t, err)
	require.False(t, didInstall)
	rootInfo, err = spA.Types().Get(ctx, wantRoot)
	require.NoError(t, err)
	assert.False(t, rootInfo.Hidden, "adopt must not patch hidden")

	// A declaration the gate refuses mints nothing: type metadata without
	// parts or properties, and a layout that cannot be encoded.
	_, _, err = spA.Bundles().Ensure(ctx, space.EnsureBundleRequest{Id: "bare/v1", DerivedRoot: true, Layout: map[string]any{"type": "page"}})
	require.ErrorIs(t, err, space.ErrBundleBadRequest)
	_, _, err = spA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "bare/v1", DerivedRoot: true, Parts: []space.PartDraft{articlesPart()},
		Layout: map[string]any{"type": func() {}},
	})
	require.ErrorIs(t, err, space.ErrBundleBadRequest)
	_, err = spA.Bundles().Get(ctx, "bare/v1")
	require.ErrorIs(t, err, space.ErrBundleUnknown, "a refused declaration must mint nothing")

	// A removed property stays removed through later Ensures: the
	// tombstone keeps the deterministic id. A property added through
	// the type API gets an ordinary id.
	require.NoError(t, spA.Types().RemoveProperty(ctx, wantRoot, propIdsA["pos"]))
	_, _, err = spA.Bundles().Ensure(ctx, declaredType())
	require.NoError(t, err)
	propsA, err = spA.Types().Properties(ctx, wantRoot)
	require.NoError(t, err)
	require.Len(t, propsA, 1, "Ensure must not resurrect a removed property")
	extra, err := spA.Types().AddProperty(ctx, wantRoot, space.PropertyDraft{XKey: "extra", Kind: space.PropertyKindNumber})
	require.NoError(t, err)
	require.NotEqual(t, propIdsA["pos"], extra)

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

// TestE2E_BundlesSelfTypedCreatedRoot covers the SDK-minted created
// root of a type-declaring request. The root's first change carries
// its types, name, type metadata and seeded values together, so an
// install is root + 3 changes (objects, properties, datasets); XKey
// reads back as the type's handle; RootTypes / RootProperties land on
// a created root; an XKey alone is a marker type (root + 1 change)
// objects can carry; a derived root gets the same one-change stamp.
func TestE2E_BundlesSelfTypedCreatedRoot(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	provider := newFixedSeedProvider(t)
	sdk, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}, provider)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "SelfTypedRoot"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Spaces().Create: %v", err)
	}

	changesByDataset := func(objectId string) map[string]int {
		t.Helper()
		list, err := sp.History().ListChanges(ctx, objectId, space.HistoryFilter{}, 0, "")
		require.NoError(t, err, "ListChanges(%s)", objectId)
		out := map[string]int{}
		for _, c := range list.Changes {
			out[c.Dataset]++
		}
		return out
	}
	typesOf := func(objectId string) []string {
		t.Helper()
		row, err := sp.Properties().Get(ctx, objectId)
		require.NoError(t, err)
		var out []string
		for _, v := range row.GetArray("any", "types") {
			out = append(out, string(v.GetStringBytes()))
		}
		return out
	}

	// A user type whose id and property the bundle root carries as an
	// extra type with a seeded value — the miniapp shape.
	movieType, titleProp := setupMovieType(t, ctx, sp)

	req := space.EnsureBundleRequest{
		Id: "wiki/v1", Name: "Wiki", XKey: "wiki", Hidden: true,
		RootTypes:      []string{movieType},
		RootProperties: map[string]map[string]any{movieType: {titleProp: "seeded"}},
		Properties:     []space.PropertyDraft{{XKey: "parentId", Name: "Parent", Kind: space.PropertyKindString}},
		Parts:          []space.PartDraft{articlesPart()},
		Layout:         map[string]any{"type": "page"},
	}
	inst, didInstall, err := sp.Bundles().Ensure(ctx, req)
	require.NoError(t, err, "Ensure(created, self-typed)")
	require.True(t, didInstall)
	require.False(t, inst.Derived)
	root := inst.RootId

	info, err := sp.Types().Get(ctx, root)
	require.NoError(t, err)
	assert.Equal(t, "wiki", info.XKey, "XKey is the type's handle")
	assert.True(t, info.Hidden)
	assert.Equal(t, map[string]any{"type": "page"}, info.Layout)
	assert.Equal(t, "Wiki", info.Name)

	assert.ElementsMatch(t, []string{"__type__", root, movieType}, typesOf(root),
		"marker, self and the root types, attached in the stamp")
	row, err := sp.Properties().Get(ctx, root)
	require.NoError(t, err)
	assert.Equal(t, "seeded", string(row.Get(movieType, titleProp).GetStringBytes()),
		"RootProperties seed a created root")

	// Root + 3: the stamp (objects), the definitions (properties), the
	// parts (datasets) — no attach-then-name chatter.
	assert.Equal(t, map[string]int{"objects": 1, "properties": 1, "datasets": 1}, changesByDataset(root))

	props, err := sp.Types().Properties(ctx, root)
	require.NoError(t, err)
	require.Len(t, props, 1)
	assert.Equal(t, "parentId", props[0].XKey)
	defs, err := sp.Types().Datasets(ctx, root)
	require.NoError(t, err)
	require.Len(t, defs, 1)

	// Adopt writes nothing.
	_, didInstall, err = sp.Bundles().Ensure(ctx, req)
	require.NoError(t, err)
	require.False(t, didInstall)
	assert.Equal(t, map[string]int{"objects": 1, "properties": 1, "datasets": 1}, changesByDataset(root),
		"adopt must not add a change")

	// A marker type: an XKey alone, no columns, no parts. Root + 1.
	flag, didInstall, err := sp.Bundles().Ensure(ctx, space.EnsureBundleRequest{Id: "flag/v1", Name: "Flag", XKey: "flag"})
	require.NoError(t, err, "Ensure(marker type)")
	require.True(t, didInstall)
	flagInfo, err := sp.Types().Get(ctx, flag.RootId)
	require.NoError(t, err)
	assert.Equal(t, "flag", flagInfo.XKey)
	assert.ElementsMatch(t, []string{"__type__", flag.RootId}, typesOf(flag.RootId))
	assert.Equal(t, map[string]int{"objects": 1}, changesByDataset(flag.RootId))
	carrier, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{flag.RootId}})
	require.NoError(t, err)
	assert.Contains(t, typesOf(carrier), flag.RootId, "objects carry a marker type")

	// A derived root gets the same one-change stamp: types, name and
	// seeded values together, then its declarations.
	der, didInstall, err := sp.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "chat/v1", Name: "Chat", DerivedRoot: true, XKey: "general_chat", Hidden: true,
		RootProperties: map[string]map[string]any{"any": {"description": "seeded"}},
		Parts:          []space.PartDraft{articlesPart()},
	})
	require.NoError(t, err, "Ensure(derived)")
	require.True(t, didInstall)
	require.True(t, der.Derived)
	assert.ElementsMatch(t, []string{"__type__", der.RootId}, typesOf(der.RootId), "`any` is never attached")
	drow, err := sp.Properties().Get(ctx, der.RootId)
	require.NoError(t, err)
	assert.Equal(t, "seeded", string(drow.Get("any", "description").GetStringBytes()))
	dinfo, err := sp.Types().Get(ctx, der.RootId)
	require.NoError(t, err)
	assert.Equal(t, "general_chat", dinfo.XKey)
	assert.Equal(t, map[string]int{"objects": 1, "datasets": 1}, changesByDataset(der.RootId))

	// A caller-minted root that already carries a name still gets its
	// types: the stamp attaches what the row lacks whatever the name
	// says (an install writes the request's name; an adopt never
	// renames).
	named, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		InitialProperties: map[string]map[string]any{"any": {"name": "Named by the caller"}},
	})
	require.NoError(t, err)
	nb, didInstall, err := sp.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "named/v1", Name: "Other", XKey: "named",
		NewRoot: func(context.Context) (string, error) { return named, nil },
	})
	require.NoError(t, err, "Ensure(NewRoot, named)")
	require.True(t, didInstall)
	require.Equal(t, named, nb.RootId)
	assert.ElementsMatch(t, []string{"__type__", named}, typesOf(named), "a named NewRoot is still self-typed")
	ninfo, err := sp.Types().Get(ctx, named)
	require.NoError(t, err)
	assert.Equal(t, "named", ninfo.XKey)
	assert.Equal(t, "Other", ninfo.Name, "the install writes the request's name — the documented $set")

	// A request that gains a root type and a handle after the install
	// reaches the existing root on adopt: attached and filled, the
	// rest untouched, seeds never re-written.
	plain, didInstall, err := sp.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "plain/v1", Name: "Plain", Parts: []space.PartDraft{articlesPart()}, Weight: 7,
	})
	require.NoError(t, err)
	require.True(t, didInstall)
	assert.ElementsMatch(t, []string{"__type__", plain.RootId}, typesOf(plain.RootId))
	grown, didInstall, err := sp.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "plain/v1", Name: "Renamed", Parts: []space.PartDraft{articlesPart()}, Weight: 9,
		XKey: "plain", RootTypes: []string{movieType},
		RootProperties: map[string]map[string]any{movieType: {titleProp: "late seed"}},
	})
	require.NoError(t, err, "adopt with a gained type and handle")
	require.False(t, didInstall)
	require.Equal(t, plain.RootId, grown.RootId)
	assert.ElementsMatch(t, []string{"__type__", plain.RootId, movieType}, typesOf(plain.RootId), "the gained root type is attached on adopt")
	pinfo, err := sp.Types().Get(ctx, plain.RootId)
	require.NoError(t, err)
	assert.Equal(t, "plain", pinfo.XKey, "an absent handle is filled on adopt")
	assert.Equal(t, "Plain", pinfo.Name, "adopt never renames")
	assert.Equal(t, 7, pinfo.Weight, "adopt never patches metadata")
	prow, err := sp.Properties().Get(ctx, plain.RootId)
	require.NoError(t, err)
	assert.Nil(t, prow.Get(movieType, titleProp), "adopt never seeds")
	assert.Equal(t, map[string]int{"objects": 2, "datasets": 1}, changesByDataset(plain.RootId),
		"the heal is one more objects change; a third Ensure adds none")
	_, _, err = sp.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "plain/v1", Parts: []space.PartDraft{articlesPart()}, XKey: "plain", RootTypes: []string{movieType},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"objects": 2, "datasets": 1}, changesByDataset(plain.RootId))

	// A second device of the account adopts the created self-typed
	// root: the same root, the same handle, the same property ids.
	_ = sdk.Spaces().SyncSpaceList(ctx)
	_ = sp.SyncHeads(ctx)
	sdkB, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}, provider)
	require.NoError(t, err, "device B: Open")
	t.Cleanup(func() { _ = sdkB.Close() })
	waitCtx, waitCancel := context.WithTimeout(ctx, 90*time.Second)
	require.NoError(t, sdkB.Spaces().WaitListSynced(waitCtx), "device B: WaitListSynced")
	waitCancel()
	var spB space.Space
	require.True(t, waitFor(ctx, 30*time.Second, 250*time.Millisecond, func() bool {
		spB, err = sdkB.Spaces().Get(ctx, sp.Id())
		return err == nil
	}), "device B: Spaces().Get never succeeded: %v", err)
	waitCtx, waitCancel = context.WithTimeout(ctx, 90*time.Second)
	require.NoError(t, spB.WaitIndexSynced(waitCtx), "device B: WaitIndexSynced")
	waitCancel()
	var adoptedB space.Bundle
	require.True(t, waitFor(ctx, 60*time.Second, 500*time.Millisecond, func() bool {
		b, installedB, err := spB.Bundles().Ensure(ctx, req)
		if err != nil || installedB {
			return false
		}
		adoptedB = b
		if _, err := spB.Types().Get(ctx, b.RootId); err != nil {
			return false
		}
		return true
	}), "device B never adopted the created self-typed root")
	require.Equal(t, root, adoptedB.RootId)
	infoB, err := spB.Types().Get(ctx, root)
	require.NoError(t, err)
	assert.Equal(t, "wiki", infoB.XKey)
	propsB, err := spB.Types().Properties(ctx, root)
	require.NoError(t, err)
	require.Len(t, propsB, 1)
	assert.Equal(t, props[0].Id, propsB[0].Id, "the property id is the same on both devices")
}

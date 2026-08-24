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

// entriesDatasetDraft is the shape of an account-level bundle dataset:
// user-minted ids, one required field, creator / create-time stamps.
func entriesDatasetDraft() space.DatasetDraft {
	return space.DatasetDraft{
		Name:      "entries",
		IdRule:    space.IdUser,
		IdPattern: "^(any://o/.+|f:[A-Za-z0-9_-]{1,64})$",
		IdMaxLen:  256,
		Fields: []space.DatasetFieldDraft{
			{Key: "title", Kind: space.PropertyKindString, Required: true},
			{Key: "parentId", Kind: space.PropertyKindString, MutableBy: space.MutableByAnyone},
			{Key: "creator", Stamp: space.StampCreator},
			{Key: "createdAt", Stamp: space.StampCreateTime},
		},
	}
}

func openTechDevice(t *testing.T, ctx context.Context, yaml []byte, provider *fixedSeedProvider, label string) *anysyncsdk.SDK {
	t.Helper()
	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	sdk, err := anysyncsdk.Open(ctx, cfg, provider)
	require.NoError(t, err, "%s: Open", label)
	t.Cleanup(func() { _ = sdk.Close() })
	return sdk
}

// TestE2E_TechBundle_EntriesConvergeAndRestore covers account-level
// bundles on the tech space:
//
//  1. The tech handle: reachable by id, restricted — lifecycle
//     surfaces refuse, reads of the system datasets work, writes to
//     them do not.
//  2. Device A installs favorites/v1 with an `entries` dataset: the
//     root is a type implementing itself, the declaration is
//     discoverable, records land through the generic upsert path;
//     re-Ensure adopts without declaring twice.
//  3. Device B (fresh DataDir, same account) restores through
//     WaitListSynced → tech handle → Bundles().Get, reads A's
//     records and writes its own, which converge back to A.
func TestE2E_TechBundle_EntriesConvergeAndRestore(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("tech bundles e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	const bundleId = "favorites/v1"
	provider := newFixedSeedProvider(t)

	// --- Device A. ---
	sdkA := openTechDevice(t, ctx, yaml, provider, "device A")
	techId := sdkA.TechSpaceId()
	require.NotEmpty(t, techId)

	techA, err := sdkA.Spaces().Get(ctx, techId)
	require.NoError(t, err, "device A: Spaces().Get(tech)")
	require.Equal(t, techId, techA.Id())
	info := techA.Info()
	assert.Equal(t, "any.techspace", info.SpaceType)
	assert.Equal(t, sdkA.Account().Id(), info.Author)
	assert.True(t, info.Derived)
	assert.Equal(t, sdkA.Spaces().SpaceIndexObjectId(), techA.SpaceIndexObjectId())

	// Never listed; never deletable.
	list, err := sdkA.Spaces().List(ctx)
	require.NoError(t, err)
	for _, si := range list {
		assert.NotEqual(t, techId, si.Id, "tech space must not appear in the space list")
	}
	require.ErrorIs(t, sdkA.Spaces().Delete(ctx, techId), space.ErrIsTechSpace)

	// Restricted surfaces.
	_, err = techA.Objects().Create(ctx, space.CreateObjectOpts{})
	require.ErrorIs(t, err, space.ErrUnsupported)
	_, err = techA.Members().List(ctx)
	require.ErrorIs(t, err, space.ErrUnsupported)
	require.ErrorIs(t, techA.SetMetadata(ctx, space.SetMetadataRequest{}), space.ErrUnsupported)
	_, err = techA.Types().Create(ctx, space.TypeCreateParams{})
	require.ErrorIs(t, err, space.ErrUnsupported)

	// System datasets: readable through the generic surface, not
	// writable.
	_, err = techA.Query(techA.SpaceIndexObjectId(), "spaces").All(ctx)
	require.NoError(t, err, "device A: generic read of the spaces registry")
	_, err = techA.Upsert(ctx, space.UpsertBatch{
		ObjectId: techA.SpaceIndexObjectId(), Dataset: "spaces",
		Records: []space.UpsertRecord{{Id: "x", Fields: map[string]any{"name": "nope"}}},
	})
	require.Error(t, err, "device A: system datasets are typed-API-only")

	// Bundles must declare datasets; roots are minted by Ensure only.
	_, _, err = techA.Bundles().Ensure(ctx, space.EnsureBundleRequest{Id: bundleId, DerivedRoot: true})
	require.ErrorIs(t, err, space.ErrBundleBadRequest, "tech bundle without Datasets")
	_, _, err = techA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: bundleId, NewRoot: func(context.Context) (string, error) { return "r", nil },
		Datasets: []space.DatasetDraft{entriesDatasetDraft()},
	})
	require.ErrorIs(t, err, space.ErrBundleBadRequest, "tech bundle with a caller-created root")

	wantRoot, err := techA.Bundles().DerivedRootId(ctx, bundleId)
	require.NoError(t, err)

	inst, didInstall, err := techA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: bundleId, Name: "Favorites", DerivedRoot: true,
		Datasets: []space.DatasetDraft{entriesDatasetDraft()},
	})
	require.NoError(t, err, "device A: Ensure(favorites/v1)")
	require.True(t, didInstall)
	require.Equal(t, wantRoot, inst.RootId)
	require.True(t, inst.Derived)

	// The root is a type implementing itself.
	row, err := techA.Objects().Get(ctx, wantRoot)
	require.NoError(t, err, "device A: Objects().Get(root)")
	var types []string
	for _, v := range row.GetArray("any", "types") {
		types = append(types, string(v.GetStringBytes()))
	}
	assert.Contains(t, types, "__type__")
	assert.Contains(t, types, wantRoot)
	ti, err := techA.Types().Get(ctx, wantRoot)
	require.NoError(t, err, "device A: Types().Get(root)")
	assert.Equal(t, "Favorites", ti.Name)
	defs, err := techA.Types().Datasets(ctx, wantRoot)
	require.NoError(t, err)
	require.Len(t, defs, 1)
	require.Equal(t, "entries", defs[0].Name)
	var discovered bool
	for _, ds := range techA.Datasets() {
		if ds.Name == "entries" {
			discovered = true
			assert.Equal(t, wantRoot, ds.TypeId)
		}
	}
	assert.True(t, discovered, "Datasets() lists the bundle dataset under typeId = rootId")

	// Records through the generic path.
	res, err := techA.Upsert(ctx, space.UpsertBatch{
		ObjectId: wantRoot, Dataset: "entries",
		Records: []space.UpsertRecord{
			{Id: "any://o/one", Fields: map[string]any{"title": "One"}},
			{Id: "f:folder", Fields: map[string]any{"title": "Folder"}},
		},
	})
	require.NoError(t, err, "device A: Upsert(entries)")
	require.Equal(t, 2, res.Created)
	require.Empty(t, res.Rejections)
	one, err := techA.Query(wantRoot, "entries").Filter(map[string]any{"id": "any://o/one"}).One(ctx)
	require.NoError(t, err)
	assert.Equal(t, sdkA.Account().Id(), string(one.GetStringBytes("creator")))

	// Re-Ensure adopts, declares nothing twice and keeps the name.
	adopted, didInstall, err := techA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: bundleId, DerivedRoot: true, Datasets: []space.DatasetDraft{entriesDatasetDraft()},
	})
	require.NoError(t, err)
	require.False(t, didInstall)
	require.Equal(t, wantRoot, adopted.RootId)
	defs, err = techA.Types().Datasets(ctx, wantRoot)
	require.NoError(t, err)
	require.Len(t, defs, 1, "adopt must not redeclare an existing name")
	ti, err = techA.Types().Get(ctx, wantRoot)
	require.NoError(t, err)
	assert.Equal(t, "Favorites", ti.Name, "adopt must not rename the root")

	// Dataset names are unique per space: another bundle claiming
	// `entries` is refused before its root exists.
	otherRoot, err := techA.Bundles().DerivedRootId(ctx, "other/v1")
	require.NoError(t, err)
	_, _, err = techA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "other/v1", DerivedRoot: true, Datasets: []space.DatasetDraft{entriesDatasetDraft()},
	})
	require.ErrorIs(t, err, space.ErrBundleBadRequest, "owned dataset name")
	_, err = techA.Objects().Get(ctx, otherRoot)
	require.ErrorIs(t, err, space.ErrNotFound, "a refused install must mint no root")
	_, _, err = techA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "other/v1", DerivedRoot: true, Datasets: []space.DatasetDraft{{Name: "spaces", IdRule: space.IdUser}},
	})
	require.ErrorIs(t, err, space.ErrBundleBadRequest, "reserved dataset name")

	// Created-root install: no DerivedRoot, no NewRoot — Ensure mints
	// the root, stamps it as its own type, declares. Deletable, so it
	// is the ordinary shape for app installs.
	pinsDraft := entriesDatasetDraft()
	pinsDraft.Name = "pins"
	pins, didInstall, err := techA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "pins/v1", Name: "Pins",
		Datasets: []space.DatasetDraft{pinsDraft},
	})
	require.NoError(t, err, "device A: Ensure(pins/v1) created root")
	require.True(t, didInstall)
	require.False(t, pins.Derived)
	require.NotEmpty(t, pins.RootId)
	prow, err := techA.Objects().Get(ctx, pins.RootId)
	require.NoError(t, err)
	var ptypes []string
	for _, v := range prow.GetArray("any", "types") {
		ptypes = append(ptypes, string(v.GetStringBytes()))
	}
	assert.Contains(t, ptypes, "__type__")
	assert.Contains(t, ptypes, pins.RootId, "created root implements itself")
	_, err = techA.Upsert(ctx, space.UpsertBatch{
		ObjectId: pins.RootId, Dataset: "pins",
		Records: []space.UpsertRecord{{Id: "any://o/two", Fields: map[string]any{"title": "Two"}}},
	})
	require.NoError(t, err, "device A: records on the created root")
	padopt, didInstall, err := techA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "pins/v1", Datasets: []space.DatasetDraft{pinsDraft},
	})
	require.NoError(t, err)
	require.False(t, didInstall, "re-ensure adopts")
	require.Equal(t, pins.RootId, padopt.RootId)
	// ResolveLoser is wired (the winner is refused as a loser, not
	// ErrUnsupported); uninstall = delete the created winner, after
	// which the id reads as uninstalled and a fresh install works.
	err = techA.Bundles().ResolveLoser(ctx, "pins/v1", pins.RootId)
	require.Error(t, err)
	require.NotErrorIs(t, err, space.ErrUnsupported, "ResolveLoser must be wired on the tech handle")
	require.NoError(t, techA.Objects().Delete(ctx, pins.RootId), "uninstall: delete the created winner")
	_, err = techA.Bundles().Get(ctx, "pins/v1")
	require.ErrorIs(t, err, space.ErrBundleUnknown, "deleted winner reads as uninstalled")
	pins2, didInstall, err := techA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "pins/v1", Name: "Pins",
		Datasets: []space.DatasetDraft{pinsDraft},
	})
	require.NoError(t, err, "reinstall after uninstall — the deleted root's dataset name is released")
	require.True(t, didInstall)
	require.NotEqual(t, pins.RootId, pins2.RootId, "reinstall mints a fresh root")
	require.Error(t, techA.Objects().Delete(ctx, wantRoot),
		"a DERIVED bundle root stays undeletable (any-sync refuses derived deletion)")

	// Dataset mutators reach bundle roots only.
	_, err = techA.Types().AddDataset(ctx, techA.SpaceIndexObjectId(), entriesDatasetDraft())
	require.ErrorIs(t, err, space.ErrUnsupported)
	_, err = techA.Modify(ctx, space.ModifyBatch{ObjectId: wantRoot, Dataset: "objects"})
	require.ErrorIs(t, err, space.ErrUnsupported, "type-system built-ins are not writable generically")

	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = techA.SyncHeads(ctx)

	// --- Device B: fresh storage, same account. ---
	sdkB := openTechDevice(t, ctx, yaml, sameAccountFreshDevice(t, provider), "device B")
	require.Equal(t, techId, sdkB.TechSpaceId(), "the tech space id is a function of the account")

	waitCtx, waitCancel := context.WithTimeout(ctx, 90*time.Second)
	require.NoError(t, sdkB.Spaces().WaitListSynced(waitCtx), "device B: WaitListSynced")
	waitCancel()
	techB, err := sdkB.Spaces().Get(ctx, techId)
	require.NoError(t, err)
	waitCtx, waitCancel = context.WithTimeout(ctx, 90*time.Second)
	require.NoError(t, techB.WaitIndexSynced(waitCtx), "device B: WaitIndexSynced on the tech handle")
	waitCancel()

	var got space.Bundle
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = techB.SyncHeads(ctx)
		got, err = techB.Bundles().Get(ctx, bundleId)
		return err == nil
	}), "device B: Bundles().Get never succeeded: %v", err)
	require.Equal(t, wantRoot, got.RootId)

	// A's records reach B through the root tree.
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = techB.SyncHeads(ctx)
		n, qerr := techB.Query(wantRoot, "entries").Count(ctx)
		return qerr == nil && n == 2
	}), "device B: entries never arrived")

	// B writes; A converges.
	resB, err := techB.Upsert(ctx, space.UpsertBatch{
		ObjectId: wantRoot, Dataset: "entries",
		Records: []space.UpsertRecord{{Id: "any://o/two", Fields: map[string]any{"title": "Two", "parentId": "f:folder"}}},
	})
	require.NoError(t, err, "device B: Upsert(entries)")
	require.Equal(t, 1, resB.Created)
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = techB.SyncHeads(ctx)
		_ = techA.SyncHeads(ctx)
		n, qerr := techA.Query(wantRoot, "entries").Count(ctx)
		return qerr == nil && n == 3
	}), "device A: B's entry never arrived")
}

// TestE2E_TechBundle_AdoptReconcilesDatasets: two devices install the
// same tech bundle with different dataset lists. Whether B's Ensure
// ran before A's root tree reached it (both declare `entries`, the
// duplicate heads converge to one) or after (B adopts A's
// declarations and adds `tags` through Types().AddDataset), both end
// with exactly one definition per name, the same on both devices.
func TestE2E_TechBundle_AdoptReconcilesDatasets(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("tech bundles e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	const bundleId = "pins/v1"
	provider := newFixedSeedProvider(t)
	tags := space.DatasetDraft{
		Name:   "tags",
		IdRule: space.IdUser,
		Fields: []space.DatasetFieldDraft{{Key: "label", Kind: space.PropertyKindString, Required: true}},
	}

	sdkA := openTechDevice(t, ctx, yaml, provider, "device A")
	techA, err := sdkA.Spaces().Get(ctx, sdkA.TechSpaceId())
	require.NoError(t, err)
	sdkB := openTechDevice(t, ctx, yaml, sameAccountFreshDevice(t, provider), "device B")
	techB, err := sdkB.Spaces().Get(ctx, sdkB.TechSpaceId())
	require.NoError(t, err)

	instA, _, err := techA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: bundleId, DerivedRoot: true, Datasets: []space.DatasetDraft{entriesDatasetDraft()},
	})
	require.NoError(t, err, "device A: Ensure")
	instB, _, err := techB.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: bundleId, DerivedRoot: true, Datasets: []space.DatasetDraft{entriesDatasetDraft(), tags},
	})
	require.NoError(t, err, "device B: Ensure")
	require.Equal(t, instA.RootId, instB.RootId, "derived root is device-independent")
	root := instA.RootId

	names := func(defs []space.DatasetDef) map[string]string {
		out := map[string]string{}
		for _, d := range defs {
			out[d.Name] = d.Id
		}
		return out
	}
	// Adopted A's declarations (tree arrived first): evolve explicitly.
	defsB, err := techB.Types().Datasets(ctx, root)
	require.NoError(t, err)
	if _, ok := names(defsB)["tags"]; !ok {
		_, err = techB.Types().AddDataset(ctx, root, tags)
		require.NoError(t, err, "device B: AddDataset(tags) after adopting")
	}

	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = sdkB.Spaces().SyncSpaceList(ctx)
	var defsA []space.DatasetDef
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = techA.SyncHeads(ctx)
		_ = techB.SyncHeads(ctx)
		var aerr, berr error
		defsA, aerr = techA.Types().Datasets(ctx, root)
		defsB, berr = techB.Types().Datasets(ctx, root)
		if aerr != nil || berr != nil || len(defsA) != 2 || len(defsB) != 2 {
			return false
		}
		na, nb := names(defsA), names(defsB)
		return na["entries"] != "" && na["entries"] == nb["entries"] && na["tags"] != "" && na["tags"] == nb["tags"]
	}), "definitions never converged: A=%+v B=%+v", defsA, defsB)

	// A re-Ensure with the union declares nothing new.
	_, _, err = techA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: bundleId, DerivedRoot: true, Datasets: []space.DatasetDraft{entriesDatasetDraft(), tags},
	})
	require.NoError(t, err)
	defsA, err = techA.Types().Datasets(ctx, root)
	require.NoError(t, err)
	require.Len(t, defsA, 2)

	// A removed dataset stays removed through later Ensures.
	require.NoError(t, techA.Types().RemoveDataset(ctx, root, names(defsA)["tags"]))
	_, _, err = techA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: bundleId, DerivedRoot: true, Datasets: []space.DatasetDraft{entriesDatasetDraft(), tags},
	})
	require.NoError(t, err)
	defsA, err = techA.Types().Datasets(ctx, root)
	require.NoError(t, err)
	require.Len(t, defsA, 1, "Ensure must not resurrect a removed dataset")
	require.NoError(t, techA.Types().RemoveDataset(ctx, root, names(defsA)["entries"]))
	_, _, err = techA.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: bundleId, DerivedRoot: true, Datasets: []space.DatasetDraft{entriesDatasetDraft(), tags},
	})
	require.NoError(t, err)
	defsA, err = techA.Types().Datasets(ctx, root)
	require.NoError(t, err)
	require.Empty(t, defsA)
	// Explicit re-add is the way back.
	_, err = techA.Types().AddDataset(ctx, root, tags)
	require.NoError(t, err)

	// Both datasets are writable on both devices.
	res, err := techA.Upsert(ctx, space.UpsertBatch{
		ObjectId: root, Dataset: "tags",
		Records: []space.UpsertRecord{{Id: "t1", Fields: map[string]any{"label": "work"}}},
	})
	require.NoError(t, err)
	require.Equal(t, 1, res.Created)
	require.True(t, waitFor(ctx, 90*time.Second, 500*time.Millisecond, func() bool {
		_ = techA.SyncHeads(ctx)
		_ = techB.SyncHeads(ctx)
		n, qerr := techB.Query(root, "tags").Count(ctx)
		return qerr == nil && n == 1
	}), "device B: tag never arrived")
}

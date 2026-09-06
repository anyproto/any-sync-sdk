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

// TestE2E_TypeHiddenAndMeta: a type's `hidden` flag and its per-key
// `meta` bag round-trip through Create / Patch / Get, meta patches are
// per key (set one, unset another, the rest untouched), a bad key or
// value is refused, and a self-typed bundle root is hidden by
// construction.
func TestE2E_TypeHiddenAndMeta(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "TypeFlags"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatal(err)
	}

	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{
		Name: "Draft", XKey: "draft", Hidden: true,
		Meta: map[string]any{"index": "none", "rank": 3, "beta": true},
	})
	require.NoError(t, err)
	info, err := sp.Types().Get(ctx, typeId)
	require.NoError(t, err)
	assert.True(t, info.Hidden)
	assert.Equal(t, map[string]any{"index": "none", "rank": float64(3), "beta": true}, info.Meta)

	// Per-key patch: set one, unset one, leave one; unhide.
	hidden := false
	require.NoError(t, sp.Types().Patch(ctx, typeId, space.TypePatch{
		Hidden: &hidden,
		Meta:   map[string]any{"index": "basic", "beta": nil},
	}))
	info, err = sp.Types().Get(ctx, typeId)
	require.NoError(t, err)
	assert.False(t, info.Hidden)
	assert.Equal(t, map[string]any{"index": "basic", "rank": float64(3)}, info.Meta)

	// Shape rules.
	err = sp.Types().Patch(ctx, typeId, space.TypePatch{Meta: map[string]any{"a.b": "x"}})
	require.ErrorIs(t, err, space.ErrInvalidFieldValue)
	err = sp.Types().Patch(ctx, typeId, space.TypePatch{Meta: map[string]any{"obj": map[string]any{"x": 1}}})
	require.ErrorIs(t, err, space.ErrInvalidFieldValue)

	// A self-typed bundle root is hidden when the install says so — a
	// root hosting only its records — and listed when it does not: a
	// declared type objects carry.
	b, _, err := sp.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "notes/v1", Name: "Notes", Hidden: true,
		Parts: []space.PartDraft{{Key: "entries", Datasets: []space.DatasetDraft{entriesDatasetDraft()}}},
	})
	require.NoError(t, err)
	root, err := sp.Types().Get(ctx, b.RootId)
	require.NoError(t, err)
	assert.True(t, root.Hidden, "bundle root type must be hidden when asked")
	pageReq := space.EnsureBundleRequest{
		Id: "page/v1", Name: "Page", Layout: map[string]any{"type": "page"},
		Parts:      []space.PartDraft{{Key: "notes", Datasets: []space.DatasetDraft{entriesDatasetDraft()}}},
		Properties: []space.PropertyDraft{{XKey: "cover", Kind: space.PropertyKindString}},
	}
	page, didInstall, err := sp.Bundles().Ensure(ctx, pageReq)
	require.NoError(t, err)
	require.True(t, didInstall)
	pageInfo, err := sp.Types().Get(ctx, page.RootId)
	require.NoError(t, err)
	assert.False(t, pageInfo.Hidden, "a declared type stays listed unless the install hides it")
	// A created root declaring properties: adopt keeps the definition.
	pageProps, err := sp.Types().Properties(ctx, page.RootId)
	require.NoError(t, err)
	require.Len(t, pageProps, 1)
	_, didInstall, err = sp.Bundles().Ensure(ctx, pageReq)
	require.NoError(t, err)
	require.False(t, didInstall)
	again, err := sp.Types().Properties(ctx, page.RootId)
	require.NoError(t, err)
	require.Len(t, again, 1, "adopt must not redeclare a property on a created root")
	assert.Equal(t, pageProps[0].Id, again[0].Id)
	list, err := sp.Types().List(ctx)
	require.NoError(t, err)
	var sawRoot, sawDraft, sawPage bool
	for _, ti := range list {
		if ti.Id == b.RootId {
			sawRoot = ti.Hidden
		}
		if ti.Id == typeId {
			sawDraft = true
		}
		if ti.Id == page.RootId {
			sawPage = !ti.Hidden
		}
	}
	assert.True(t, sawRoot, "List carries hidden types with the flag set")
	assert.True(t, sawDraft)
	assert.True(t, sawPage)
}

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

// TestSDK_PropertyFormat exercises the property `format` surface:
//
//   - AddProperty with a links format (Kind omitted) defaults Kind to
//     array and persists the format object.
//   - The format survives the Properties() read-back, filter as its
//     JSON text.
//   - Format/kind mismatches and the reserved `tags` format are
//     rejected at AddProperty.
//   - UpdatePropertyMeta mutates the ui/filter leaves; format.type
//     stays pinned; formatless properties reject format-leaf updates.
func TestSDK_PropertyFormat(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Fmt"})
	require.NoError(t, err)
	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Page"})
	require.NoError(t, err)

	// Links format, Kind omitted — defaulted to array.
	relatedProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Related",
		Format: &space.PropertyFormatDraft{
			Type:   space.FormatLinks,
			UI:     "multiselect",
			Filter: map[string]any{"type": map[string]any{"$in": []string{"page"}}},
		},
	})
	require.NoError(t, err)

	// Datetime format with an explicit matching kind.
	dueProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Due",
		Kind: space.PropertyKindString,
		Format: &space.PropertyFormatDraft{
			Type: space.FormatDatetime,
		},
	})
	require.NoError(t, err)

	// Formatless property for the negative UpdatePropertyMeta case.
	titleProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title", Kind: space.PropertyKindString,
	})
	require.NoError(t, err)

	// Rejections: kind mismatch and the reserved tags format.
	_, err = sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Broken", Kind: space.PropertyKindString,
		Format: &space.PropertyFormatDraft{Type: space.FormatLinks},
	})
	require.Error(t, err, "links format with string kind must be rejected")
	_, err = sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name:   "Tagged",
		Format: &space.PropertyFormatDraft{Type: space.FormatTags},
	})
	require.Error(t, err, "tags format is reserved until the tag table lands")

	// Read-back.
	defs, err := sp.Types().Properties(ctx, typeId)
	require.NoError(t, err)
	byId := map[string]space.PropertyDef{}
	for _, d := range defs {
		byId[d.Id] = d
	}

	related := byId[relatedProp]
	assert.Equal(t, space.PropertyKindArray, related.Kind, "kind defaulted from links format")
	require.NotNil(t, related.Format)
	assert.Equal(t, space.FormatLinks, related.Format.Type)
	assert.Equal(t, "multiselect", related.Format.UI)
	assert.JSONEq(t, `{"type":{"$in":["page"]}}`, related.Format.Filter)

	due := byId[dueProp]
	require.NotNil(t, due.Format)
	assert.Equal(t, space.FormatDatetime, due.Format.Type)
	assert.Empty(t, due.Format.UI)

	assert.Nil(t, byId[titleProp].Format, "formatless property reads back nil Format")

	// Mutate the ui/filter leaves.
	newUI := "select"
	newFilter := `{"type":"person"}`
	require.NoError(t, sp.Types().UpdatePropertyMeta(ctx, typeId, relatedProp, space.PropertyMetaUpdate{
		FormatUI:     &newUI,
		FormatFilter: &newFilter,
	}))
	defs, err = sp.Types().Properties(ctx, typeId)
	require.NoError(t, err)
	for _, d := range defs {
		if d.Id != relatedProp {
			continue
		}
		require.NotNil(t, d.Format)
		assert.Equal(t, space.FormatLinks, d.Format.Type, "format.type pinned across meta updates")
		assert.Equal(t, "select", d.Format.UI)
		assert.Equal(t, `{"type":"person"}`, d.Format.Filter)
	}

	// Format leaves on a formatless property are rejected.
	err = sp.Types().UpdatePropertyMeta(ctx, typeId, titleProp, space.PropertyMetaUpdate{FormatUI: &newUI})
	require.Error(t, err)

	// Unknown propId errors rather than silently no-oping.
	err = sp.Types().UpdatePropertyMeta(ctx, typeId, "no-such-prop", space.PropertyMetaUpdate{Name: &newUI})
	require.Error(t, err)
}

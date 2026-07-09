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
//   - PatchProperty mutates the ui/filter leaves; format.type
//     stays pinned; formatless properties reject format-leaf updates.
func TestSDK_PropertyFormat(t *testing.T) {
	t.Parallel()
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

	// Mutate the ui/filter leaves via a generic patch.
	require.NoError(t, sp.Types().PatchProperty(ctx, typeId, relatedProp, space.PropertyPatch{
		Set: map[string]any{"format.ui": "select", "format.filter": `{"type":"person"}`},
	}))
	defs, err = sp.Types().Properties(ctx, typeId)
	require.NoError(t, err)
	for _, d := range defs {
		if d.Id != relatedProp {
			continue
		}
		require.NotNil(t, d.Format)
		assert.Equal(t, space.FormatLinks, d.Format.Type, "format.type pinned across patches")
		assert.Equal(t, "select", d.Format.UI)
		assert.Equal(t, `{"type":"person"}`, d.Format.Filter)
	}

	// Rename the property (a top-level leaf).
	require.NoError(t, sp.Types().PatchProperty(ctx, typeId, titleProp, space.PropertyPatch{
		Set: map[string]any{"name": "Heading"},
	}))

	// Pinned paths are rejected as a whole — kind stays immutable and no
	// partial write lands.
	err = sp.Types().PatchProperty(ctx, typeId, titleProp, space.PropertyPatch{
		Set: map[string]any{"name": "X", "kind": "number"},
	})
	require.ErrorIs(t, err, space.ErrPinnedField)

	// Format leaves on a formatless property are rejected.
	err = sp.Types().PatchProperty(ctx, typeId, titleProp, space.PropertyPatch{
		Set: map[string]any{"format.ui": "select"},
	})
	require.Error(t, err)

	// Unknown propId errors rather than silently no-oping.
	err = sp.Types().PatchProperty(ctx, typeId, "no-such-prop", space.PropertyPatch{
		Set: map[string]any{"name": "Nope"},
	})
	require.Error(t, err)

	testSelectOptions(t, ctx, sp)
}

// testSelectOptions exercises the select/multiselect option surface:
// create a multiselect property, add options as string leaves (atomic
// multi-field patch), read them back, recolor/reorder/rename single
// leaves, delete an option subtree, and re-add the same key.
func testSelectOptions(t *testing.T, ctx context.Context, sp space.Space) {
	t.Helper()
	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Task"})
	require.NoError(t, err)

	// multiselect defaults Kind to array; select would default to string.
	tagsProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name:   "Tags",
		Format: &space.PropertyFormatDraft{Type: space.FormatMultiselect, UI: "multiselect"},
	})
	require.NoError(t, err)

	// Add an option atomically (name+color+pos in one change).
	require.NoError(t, sp.Types().PatchProperty(ctx, typeId, tagsProp, space.PropertyPatch{
		Set: map[string]any{
			"format.options.high.name":  "High",
			"format.options.high.color": "red",
			"format.options.high.pos":   "a0",
		},
	}))
	// Recolor + reorder single leaves; add a second option.
	require.NoError(t, sp.Types().PatchProperty(ctx, typeId, tagsProp, space.PropertyPatch{
		Set: map[string]any{
			"format.options.high.color": "crimson",
			"format.options.low.name":   "Low",
			"format.options.low.color":  "blue",
			"format.options.low.pos":    "a1",
		},
	}))

	optsOf := func(propId string) map[string]space.PropertyOption {
		defs, err := sp.Types().Properties(ctx, typeId)
		require.NoError(t, err)
		for _, d := range defs {
			if d.Id == propId {
				require.NotNil(t, d.Format)
				return d.Format.Options
			}
		}
		t.Fatalf("prop %s not found", propId)
		return nil
	}

	opts := optsOf(tagsProp)
	require.Len(t, opts, 2)
	assert.Equal(t, "High", opts["high"].Name)
	assert.Equal(t, "crimson", opts["high"].Color)
	assert.Equal(t, "a0", opts["high"].Pos)
	assert.Equal(t, "Low", opts["low"].Name)

	// Delete an option subtree, then re-add the same key (no tombstone —
	// it's a field $unset, not a record delete).
	require.NoError(t, sp.Types().PatchProperty(ctx, typeId, tagsProp, space.PropertyPatch{
		Unset: []string{"format.options.high"},
	}))
	opts = optsOf(tagsProp)
	require.Len(t, opts, 1)
	_, gone := opts["high"]
	assert.False(t, gone, "deleted option removed")

	require.NoError(t, sp.Types().PatchProperty(ctx, typeId, tagsProp, space.PropertyPatch{
		Set: map[string]any{"format.options.high.name": "Highest"},
	}))
	opts = optsOf(tagsProp)
	assert.Equal(t, "Highest", opts["high"].Name, "re-added option after delete")

	// Whole-option object writes are rejected — options are string leaves.
	err = sp.Types().PatchProperty(ctx, typeId, tagsProp, space.PropertyPatch{
		Set: map[string]any{"format.options.mid": map[string]any{"name": "Mid"}},
	})
	require.ErrorIs(t, err, space.ErrPinnedField)
}

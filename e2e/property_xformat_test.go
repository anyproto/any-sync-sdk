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

// TestSDK_PropertyXFormat exercises the property descriptor surface:
//
//   - AddProperty persists the `x-format` bag as one object and it
//     reads back through Properties() with plain Go values.
//   - Kind is always explicit — nothing is defaulted from the bag.
//   - PatchProperty mutates any path under x-format, the slug
//     included, with any value type; option subtrees add and remove
//     per path; pinned paths still fail the whole patch.
//   - x-key is a mutable handle.
func TestSDK_PropertyXFormat(t *testing.T) {
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

	relatedProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Related", XKey: "related",
		Kind:  space.PropertyKindArray,
		Items: &space.PropertyDraft{Kind: space.PropertyKindString},
		XFormat: map[string]any{
			"type": "relation", "icon": "link", "pos": "a1",
			"config":   map[string]any{"multiple": true},
			"relation": map[string]any{"targetTypes": []string{"page"}, "filter": `{"type":{"$in":["page"]}}`},
		},
	})
	require.NoError(t, err)

	dueProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Due", Kind: space.PropertyKindDatetime,
		XFormat: map[string]any{"type": "date"},
	})
	require.NoError(t, err)

	// A bare property: no descriptor at all.
	titleProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title", Kind: space.PropertyKindString,
	})
	require.NoError(t, err)

	// Kind is never defaulted.
	_, err = sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "NoKind", XFormat: map[string]any{"type": "relation"},
	})
	require.Error(t, err, "kind is required regardless of the descriptor")

	// Read-back.
	defs, err := sp.Types().Properties(ctx, typeId)
	require.NoError(t, err)
	byId := map[string]space.PropertyDef{}
	for _, d := range defs {
		byId[d.Id] = d
	}

	related := byId[relatedProp]
	assert.Equal(t, space.PropertyKindArray, related.Kind)
	assert.Equal(t, "related", related.XKey)
	require.NotNil(t, related.XFormat)
	assert.Equal(t, "relation", related.XFormat["type"])
	assert.Equal(t, map[string]any{"multiple": true}, related.XFormat["config"])
	rel, _ := related.XFormat["relation"].(map[string]any)
	assert.Equal(t, []any{"page"}, rel["targetTypes"])
	assert.Equal(t, `{"type":{"$in":["page"]}}`, rel["filter"])

	due := byId[dueProp]
	assert.Equal(t, space.PropertyKindDatetime, due.Kind)
	assert.Equal(t, "date", due.XFormat["type"])

	assert.Nil(t, byId[titleProp].XFormat, "bare property reads back nil XFormat")

	// Any path under x-format mutates, the slug included; a descriptor
	// can be grown onto a property that had none.
	require.NoError(t, sp.Types().PatchProperty(ctx, typeId, relatedProp, space.PropertyPatch{
		Set: map[string]any{
			"x-format.config.multiple": false,
			"x-format.relation.filter": `{"type":"person"}`,
			"x-format.acme.widget":     "compact",
		},
	}))
	require.NoError(t, sp.Types().PatchProperty(ctx, typeId, dueProp, space.PropertyPatch{
		Set: map[string]any{"x-format.type": "datetime", "x-format.config.zone": "viewer"},
	}))
	require.NoError(t, sp.Types().PatchProperty(ctx, typeId, titleProp, space.PropertyPatch{
		Set: map[string]any{"x-format.type": "text", "x-key": "heading", "name": "Heading"},
	}))
	defs, err = sp.Types().Properties(ctx, typeId)
	require.NoError(t, err)
	byId = map[string]space.PropertyDef{}
	for _, d := range defs {
		byId[d.Id] = d
	}
	related = byId[relatedProp]
	assert.Equal(t, map[string]any{"multiple": false}, related.XFormat["config"])
	rel, _ = related.XFormat["relation"].(map[string]any)
	assert.Equal(t, `{"type":"person"}`, rel["filter"])
	assert.Equal(t, []any{"page"}, rel["targetTypes"], "sibling leaf untouched")
	assert.Equal(t, map[string]any{"widget": "compact"}, related.XFormat["acme"], "vendor subtree stored verbatim")
	assert.Equal(t, "datetime", byId[dueProp].XFormat["type"], "slug is mutable")
	assert.Equal(t, "text", byId[titleProp].XFormat["type"], "descriptor grown onto a bare property")
	assert.Equal(t, "heading", byId[titleProp].XKey, "x-key is mutable")
	assert.Equal(t, "Heading", byId[titleProp].Name)

	// Pinned paths are rejected as a whole — kind stays immutable and no
	// partial write lands.
	err = sp.Types().PatchProperty(ctx, typeId, titleProp, space.PropertyPatch{
		Set: map[string]any{"name": "X", "kind": "number"},
	})
	require.ErrorIs(t, err, space.ErrPinnedField)

	// Unknown propId errors rather than silently no-oping.
	err = sp.Types().PatchProperty(ctx, typeId, "no-such-prop", space.PropertyPatch{
		Set: map[string]any{"name": "Nope"},
	})
	require.Error(t, err)

	testXFormatOptions(t, ctx, sp)
}

// testXFormatOptions exercises the option surface as x-format paths:
// add options as leaves (atomic multi-field patch), read them back,
// recolor/reorder/rename single leaves, delete an option subtree, and
// re-add the same key.
func testXFormatOptions(t *testing.T, ctx context.Context, sp space.Space) {
	t.Helper()
	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Task"})
	require.NoError(t, err)

	tagsProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Tags", Kind: space.PropertyKindArray,
		Items:   &space.PropertyDraft{Kind: space.PropertyKindString},
		XFormat: map[string]any{"type": "choice", "config": map[string]any{"multiple": true}},
	})
	require.NoError(t, err)

	require.NoError(t, sp.Types().PatchProperty(ctx, typeId, tagsProp, space.PropertyPatch{
		Set: map[string]any{
			"x-format.options.high.name":  "High",
			"x-format.options.high.color": "red",
			"x-format.options.high.pos":   "a0",
		},
	}))
	require.NoError(t, sp.Types().PatchProperty(ctx, typeId, tagsProp, space.PropertyPatch{
		Set: map[string]any{
			"x-format.options.high.color": "crimson",
			"x-format.options.low.name":   "Low",
			"x-format.options.low.color":  "blue",
			"x-format.options.low.pos":    "a1",
		},
	}))

	optsOf := func(propId string) map[string]any {
		defs, err := sp.Types().Properties(ctx, typeId)
		require.NoError(t, err)
		for _, d := range defs {
			if d.Id == propId {
				require.NotNil(t, d.XFormat)
				opts, _ := d.XFormat["options"].(map[string]any)
				return opts
			}
		}
		t.Fatalf("prop %s not found", propId)
		return nil
	}
	opt := func(opts map[string]any, key string) map[string]any {
		o, _ := opts[key].(map[string]any)
		return o
	}

	opts := optsOf(tagsProp)
	require.Len(t, opts, 2)
	assert.Equal(t, "High", opt(opts, "high")["name"])
	assert.Equal(t, "crimson", opt(opts, "high")["color"])
	assert.Equal(t, "a0", opt(opts, "high")["pos"])
	assert.Equal(t, "Low", opt(opts, "low")["name"])

	// Delete an option subtree, then re-add the same key (no tombstone —
	// it's a field $unset, not a record delete).
	require.NoError(t, sp.Types().PatchProperty(ctx, typeId, tagsProp, space.PropertyPatch{
		Unset: []string{"x-format.options.high"},
	}))
	opts = optsOf(tagsProp)
	require.Len(t, opts, 1)
	_, gone := opts["high"]
	assert.False(t, gone, "deleted option removed")

	require.NoError(t, sp.Types().PatchProperty(ctx, typeId, tagsProp, space.PropertyPatch{
		Set: map[string]any{"x-format.options.high.name": "Highest"},
	}))
	opts = optsOf(tagsProp)
	assert.Equal(t, "Highest", opt(opts, "high")["name"], "re-added option after delete")
}

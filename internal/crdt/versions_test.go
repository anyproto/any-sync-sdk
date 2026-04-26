package crdt

import (
	"testing"

	"github.com/anyproto/any-store/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildRecord creates a record with the given _ver tree (described as a
// Go-side tree of either VersionId or map[string]any). Field values
// themselves are not part of the tests in this file — we exercise _ver
// lookup/update only.
func buildRecord(t *testing.T, arena *anyenc.Arena, oTree any) *anyenc.Value {
	t.Helper()
	rec := arena.NewObject()
	rec.Set(IdField, arena.NewString("rec"))
	if oTree != nil {
		rec.Set(VersionsKey, treeToValue(arena, oTree))
	}
	return rec
}

func treeToValue(arena *anyenc.Arena, tree any) *anyenc.Value {
	switch t := tree.(type) {
	case VersionId:
		return arena.NewString(string(t))
	case string:
		return arena.NewString(t)
	case map[string]any:
		obj := arena.NewObject()
		// Iterate deterministically by inserting in stable order.
		for k, v := range t {
			obj.Set(k, treeToValue(arena, v))
		}
		return obj
	}
	return nil
}

func TestGetRecordVersion_NoVersionsMap(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := arena.NewObject()
	rec.Set(IdField, arena.NewString("rec"))

	assert.Equal(t, VersionId(""), GetRecordVersion(rec, "name"))
}

func TestGetRecordVersion_StringDefault(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := buildRecord(t, arena, map[string]any{
		defaultKey: "v1",
	})
	// Any field falls back to the default.
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "name"))
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "id"))
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "tags"))
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "meta", "color"))
}

func TestGetRecordVersion_ExplicitOverridesDefault(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := buildRecord(t, arena, map[string]any{
		defaultKey: "v1",
		"name":     "v3",
		"meta": map[string]any{
			defaultKey: "v2",
			"color":    "v5",
		},
	})

	assert.Equal(t, VersionId("v3"), GetRecordVersion(rec, "name"))
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "tags")) // root default
	assert.Equal(t, VersionId("v5"), GetRecordVersion(rec, "meta", "color"))
	assert.Equal(t, VersionId("v2"), GetRecordVersion(rec, "meta", "size")) // meta default
}

func TestGetRecordVersion_StringSubtreeIsCollapsedAncestor(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := buildRecord(t, arena, map[string]any{
		"meta": "v10", // collapsed: everything under meta is v10
	})

	assert.Equal(t, VersionId("v10"), GetRecordVersion(rec, "meta", "color"))
	assert.Equal(t, VersionId("v10"), GetRecordVersion(rec, "meta", "size", "deep"))
	assert.Equal(t, VersionId("v10"), GetRecordVersion(rec, "meta"))
}

func TestGetRecordVersion_SubtreeReturnsMaxLeaf(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := buildRecord(t, arena, map[string]any{
		"meta": map[string]any{
			"color": "v5",
			"size":  "v9",
			"sub": map[string]any{
				"deep": "v3",
			},
		},
	})

	// Looking up the subtree itself yields the max leaf version.
	assert.Equal(t, VersionId("v9"), GetRecordVersion(rec, "meta"))
}

func TestSetRecordVersion_FreshRecord(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := arena.NewObject()
	rec.Set(IdField, arena.NewString("rec"))

	SetRecordVersion(arena, rec, "v5", "name")

	assert.Equal(t, VersionId("v5"), GetRecordVersion(rec, "name"))
}

func TestSetRecordVersion_ExpandsCollapsedAncestor(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := buildRecord(t, arena, map[string]any{
		defaultKey: "v1",
		"meta":     "v10",
	})

	// Write a finer entry under meta. The collapsed "v10" must expand into
	// an object that retains v10 as the meta-level default for siblings.
	SetRecordVersion(arena, rec, "v15", "meta", "color")

	assert.Equal(t, VersionId("v15"), GetRecordVersion(rec, "meta", "color"))
	// Other meta siblings still inherit v10, NOT root v1.
	assert.Equal(t, VersionId("v10"), GetRecordVersion(rec, "meta", "size"))
	assert.Equal(t, VersionId("v10"), GetRecordVersion(rec, "meta", "anything"))
	// Root-level non-meta still falls through to root default.
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "tags"))
}

func TestSetRecordVersion_ExpandsTwoLevels(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := buildRecord(t, arena, map[string]any{
		"meta": "v10",
	})
	// First expand meta...
	SetRecordVersion(arena, rec, "v15", "meta", "color")
	// ...then expand color, which is now a string "v15".
	SetRecordVersion(arena, rec, "v20", "meta", "color", "bg")

	assert.Equal(t, VersionId("v20"), GetRecordVersion(rec, "meta", "color", "bg"))
	// meta.color.fg inherits the expanded color default = v15.
	assert.Equal(t, VersionId("v15"), GetRecordVersion(rec, "meta", "color", "fg"))
	// meta.size still inherits the meta default = v10.
	assert.Equal(t, VersionId("v10"), GetRecordVersion(rec, "meta", "size"))
}

func TestSetRecordVersion_OverwriteSubtreeWithLeaf(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := buildRecord(t, arena, map[string]any{
		"meta": map[string]any{
			"color": "v5",
			"size":  "v9",
		},
	})

	// $set("meta", ...) at v20 — the leaf write overwrites the whole subtree
	// with a single string.
	SetRecordVersion(arena, rec, "v20", "meta")

	assert.Equal(t, VersionId("v20"), GetRecordVersion(rec, "meta"))
	assert.Equal(t, VersionId("v20"), GetRecordVersion(rec, "meta", "color"))
	assert.Equal(t, VersionId("v20"), GetRecordVersion(rec, "meta", "size"))
}

func TestSetRecordVersion_CreatesVersionsMapIfMissing(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := arena.NewObject()
	rec.Set(IdField, arena.NewString("rec"))

	SetRecordVersion(arena, rec, "v1", "name")

	o := rec.Get(VersionsKey)
	require.NotNil(t, o)
	require.Equal(t, anyenc.TypeObject, o.Type())
}

func TestIsCollapsible(t *testing.T) {
	arena := &anyenc.Arena{}

	// Uniform object → collapsible.
	uniform := treeToValue(arena, map[string]any{
		defaultKey: "v5",
		"name":     "v5",
		"color":    "v5",
	})
	v, ok := IsCollapsible(uniform)
	assert.True(t, ok)
	assert.Equal(t, VersionId("v5"), v)

	// Mixed → not.
	mixed := treeToValue(arena, map[string]any{
		"name":  "v5",
		"color": "v6",
	})
	_, ok = IsCollapsible(mixed)
	assert.False(t, ok)

	// Nested object → not (because not all values are strings).
	nested := treeToValue(arena, map[string]any{
		"name": map[string]any{"sub": "v5"},
	})
	_, ok = IsCollapsible(nested)
	assert.False(t, ok)
}

func TestCompareVersion(t *testing.T) {
	assert.Equal(t, 0, CompareVersion("", ""))
	assert.Equal(t, -1, CompareVersion("", "a"))
	assert.Equal(t, 1, CompareVersion("a", ""))
	assert.Equal(t, -1, CompareVersion("a", "b"))
	assert.Equal(t, 1, CompareVersion("b", "a"))
	assert.Equal(t, 0, CompareVersion("abc", "abc"))
}

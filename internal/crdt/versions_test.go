package crdt

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
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

	// Uniform but WITHOUT a defaultKey → not collapsible. The node never
	// claimed authority over unenumerated siblings; collapsing would
	// invent a version for never-written fields and break convergence.
	uniformNoDefault := treeToValue(arena, map[string]any{
		"name":  "v5",
		"color": "v5",
	})
	_, ok = IsCollapsible(uniformNoDefault)
	assert.False(t, ok)
}

// ----------------------------------------------------------------------------
// compactVersions
// ----------------------------------------------------------------------------

func TestCompact_NoopOnEmpty(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := arena.NewObject()
	rec.Set(IdField, arena.NewString("rec"))
	compactVersions(arena, rec)
	// No _ver to compact — must not create one.
	assert.Nil(t, rec.Get(VersionsKey))
}

func TestCompact_NoopOnCanonicalRoot(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := buildRecord(t, arena, map[string]any{
		IdField:    "v1",
		defaultKey: "v1",
	})
	compactVersions(arena, rec)
	// Already in canonical {id, *} shape — leave untouched.
	o := rec.Get(VersionsKey)
	require.NotNil(t, o)
	assert.Equal(t, VersionId("v1"), VersionId(o.Get(IdField).GetStringBytes()))
	assert.Equal(t, VersionId("v1"), VersionId(o.Get(defaultKey).GetStringBytes()))
	assert.Equal(t, 2, o.GetObject().Len())
}

func TestCompact_NoopOnTombstone(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := arena.NewObject()
	rec.Set(IdField, arena.NewString("rec"))
	rec.Set(DeletedAtField, arena.NewNumberInt(1715000000))
	// Even a "weird" non-canonical _ver on a tombstone must be left alone —
	// tombstones are sticky and the modifier never re-enters their _ver.
	rec.Set(VersionsKey, treeToValue(arena, map[string]any{
		IdField:  "v1",
		"name":   "v9",
		"author": "v9",
	}))
	compactVersions(arena, rec)
	o := rec.Get(VersionsKey)
	require.NotNil(t, o)
	assert.Equal(t, 3, o.GetObject().Len(), "tombstone _ver must not be touched")
}

func TestCompact_NoRootFactor_SharedVersionsStayExplicit(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := buildRecord(t, arena, map[string]any{
		IdField:    "v1",
		"changeId": "v1",
		"kind":     "v1",
		"propId":   "v1",
	})
	compactVersions(arena, rec)
	// Shared versions are NOT factored into `*` — no write ever claimed
	// authority over unenumerated fields, so inventing a default would
	// gate out concurrent older writes to fresh fields (divergence).
	o := rec.Get(VersionsKey)
	require.NotNil(t, o)
	assert.Nil(t, o.Get(defaultKey))
	assert.Equal(t, VersionId("v1"), VersionId(o.Get(IdField).GetStringBytes()))
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "changeId"))
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "kind"))
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "propId"))
	// Never-written fields stay unversioned.
	assert.Equal(t, VersionId(""), GetRecordVersion(rec, "anyNewField"))
}

func TestCompact_NoRootFactor_OutlierUnaffected(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := buildRecord(t, arena, map[string]any{
		IdField:     "v0",
		"author":    "v1",
		"createdAt": "v1",
		"spaceId":   "v1",
		"name":      "v9",
	})
	compactVersions(arena, rec)
	o := rec.Get(VersionsKey)
	require.NotNil(t, o)
	// Nothing factored; every field keeps its explicit version.
	assert.Nil(t, o.Get(defaultKey))
	assert.Equal(t, VersionId("v0"), VersionId(o.Get(IdField).GetStringBytes()))
	assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "author"))
	assert.Equal(t, VersionId("v9"), GetRecordVersion(rec, "name"))
	assert.Equal(t, VersionId(""), GetRecordVersion(rec, "newField"))
}

func TestCompact_NoRootFactorOnSingleNonIdSibling(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := buildRecord(t, arena, map[string]any{
		IdField: "v1",
		"name":  "v2",
	})
	compactVersions(arena, rec)
	// Only one non-id sibling — no factoring (would change semantics for
	// unenumerated fields without any supporting evidence).
	o := rec.Get(VersionsKey)
	require.NotNil(t, o)
	assert.Nil(t, o.Get(defaultKey))
	assert.Equal(t, VersionId("v2"), VersionId(o.Get("name").GetStringBytes()))
}

func TestCompact_RootStar_DropsOnlyEntriesEqualToDefault(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := buildRecord(t, arena, map[string]any{
		IdField:    "v1",
		defaultKey: "v5",
		"a":        "v9",
		"b":        "v9",
		"c":        "v5", // equal to the default — redundant, lookup unchanged
	})
	compactVersions(arena, rec)
	o := rec.Get(VersionsKey)
	require.NotNil(t, o)
	assert.Equal(t, VersionId("v5"), VersionId(o.Get(defaultKey).GetStringBytes()))
	// a/b differ from the default and must stay explicit.
	assert.NotNil(t, o.Get("a"))
	assert.NotNil(t, o.Get("b"))
	// c equals the default — dropped, but its lookup falls back to `*`.
	assert.Nil(t, o.Get("c"))
	assert.Equal(t, VersionId("v5"), GetRecordVersion(rec, "c"))
}

func TestCompact_SubtreeAllSameWithoutStarStaysExpanded(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := buildRecord(t, arena, map[string]any{
		IdField: "v1",
		"nav": map[string]any{
			"type":     "v5",
			"parentId": "v5",
			"pos":      "v5",
		},
	})
	compactVersions(arena, rec)
	o := rec.Get(VersionsKey)
	require.NotNil(t, o)
	// No `*` at nav — the three leaf writes never claimed nav's other
	// keys, so collapsing to a string (which WOULD claim them) is lossy
	// and forbidden.
	nav := o.Get("nav")
	require.NotNil(t, nav)
	assert.Equal(t, anyenc.TypeObject, nav.Type())
	assert.Equal(t, VersionId("v5"), GetRecordVersion(rec, "nav", "type"))
	assert.Equal(t, VersionId("v5"), GetRecordVersion(rec, "nav", "parentId"))
	assert.Equal(t, VersionId("v5"), GetRecordVersion(rec, "nav", "pos"))
	assert.Equal(t, VersionId(""), GetRecordVersion(rec, "nav", "unwritten"))
}

func TestCompact_SubtreeMixedNotCollapsed(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := buildRecord(t, arena, map[string]any{
		IdField: "v1",
		"any": map[string]any{
			"types": "v5",
			"name":  "v3", // mixed — must stay expanded
		},
	})
	compactVersions(arena, rec)
	any := rec.Get(VersionsKey).Get("any")
	require.NotNil(t, any)
	assert.Equal(t, anyenc.TypeObject, any.Type())
	assert.Equal(t, VersionId("v5"), GetRecordVersion(rec, "any", "types"))
	assert.Equal(t, VersionId("v3"), GetRecordVersion(rec, "any", "name"))
}

func TestCompact_SubtreeSingleEntryNotCollapsed(t *testing.T) {
	arena := &anyenc.Arena{}
	// Spec §3.2 example: $set "a.b.c" at v5 → _ver: {a:{b:{c:v5}}}.
	// Single-entry subtrees with no defaultKey must NOT collapse upward —
	// that would falsely claim authority over unenumerated siblings.
	rec := buildRecord(t, arena, map[string]any{
		"a": map[string]any{
			"b": map[string]any{
				"c": "v5",
			},
		},
	})
	compactVersions(arena, rec)
	a := rec.Get(VersionsKey).Get("a")
	require.NotNil(t, a)
	assert.Equal(t, anyenc.TypeObject, a.Type())
	b := a.Get("b")
	require.NotNil(t, b)
	assert.Equal(t, anyenc.TypeObject, b.Type())
	assert.Equal(t, VersionId("v5"), GetRecordVersion(rec, "a", "b", "c"))
	assert.Equal(t, VersionId(""), GetRecordVersion(rec, "a", "b", "other"))
}

func TestCompact_SubtreeWithStarCollapsesEvenSingleEntry(t *testing.T) {
	arena := &anyenc.Arena{}
	// {*: v5, color: v5} is lossless to "v5" — the `*` already claimed
	// authority for all siblings.
	rec := buildRecord(t, arena, map[string]any{
		"meta": map[string]any{
			defaultKey: "v5",
			"color":    "v5",
		},
	})
	compactVersions(arena, rec)
	meta := rec.Get(VersionsKey).Get("meta")
	require.NotNil(t, meta)
	assert.Equal(t, anyenc.TypeString, meta.Type())
	assert.Equal(t, VersionId("v5"), VersionId(meta.GetStringBytes()))
}

func TestCompact_BottomUpCascades(t *testing.T) {
	arena := &anyenc.Arena{}
	// Deep nesting with `*` claims at every level: each subtree collapses
	// to its default, then the parent (all entries equal to ITS default)
	// collapses too.
	rec := buildRecord(t, arena, map[string]any{
		IdField: "v1",
		"outer": map[string]any{
			defaultKey: "v5",
			"x":        map[string]any{defaultKey: "v5", "a": "v5", "b": "v5"},
			"y":        map[string]any{defaultKey: "v5", "a": "v5", "b": "v5"},
		},
	})
	compactVersions(arena, rec)
	outer := rec.Get(VersionsKey).Get("outer")
	require.NotNil(t, outer)
	assert.Equal(t, anyenc.TypeString, outer.Type())
	assert.Equal(t, VersionId("v5"), VersionId(outer.GetStringBytes()))
	// Without the `*` claims, the same shape must stay fully expanded.
	rec2 := buildRecord(t, arena, map[string]any{
		IdField: "v1",
		"outer": map[string]any{
			"x": map[string]any{"a": "v5", "b": "v5"},
			"y": map[string]any{"a": "v5", "b": "v5"},
		},
	})
	compactVersions(arena, rec2)
	outer2 := rec2.Get(VersionsKey).Get("outer")
	require.NotNil(t, outer2)
	assert.Equal(t, anyenc.TypeObject, outer2.Type())
	assert.Equal(t, anyenc.TypeObject, outer2.Get("x").Type())
}

func TestCompact_Idempotent(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := buildRecord(t, arena, map[string]any{
		IdField:    "v1",
		"a":        "v1",
		"b":        "v1",
		"c":        "v1",
		"someBig":  "v9",
		"someBig2": "v9",
		"nav": map[string]any{
			"type":     "v5",
			"parentId": "v5",
		},
	})
	compactVersions(arena, rec)
	first := encodeVer(t, rec)
	compactVersions(arena, rec)
	compactVersions(arena, rec)
	second := encodeVer(t, rec)
	assert.Equal(t, first, second, "compactVersions must be idempotent")
}

// encodeVer marshals a record's _ver to bytes for equality comparison.
func encodeVer(t *testing.T, rec *anyenc.Value) []byte {
	t.Helper()
	o := rec.Get(VersionsKey)
	if o == nil {
		return nil
	}
	return o.MarshalTo(nil)
}

func TestCompact_PreservesIdAlwaysExplicit(t *testing.T) {
	arena := &anyenc.Arena{}
	rec := buildRecord(t, arena, map[string]any{
		IdField: "v1",
		"a":     "v1",
		"b":     "v1",
	})
	compactVersions(arena, rec)
	o := rec.Get(VersionsKey)
	require.NotNil(t, o)
	// _ver.id stays as an explicit string entry, never factored into `*`.
	idVer := o.Get(IdField)
	require.NotNil(t, idVer)
	assert.Equal(t, anyenc.TypeString, idVer.Type())
	assert.Equal(t, VersionId("v1"), VersionId(idVer.GetStringBytes()))
}

func TestCompareVersion(t *testing.T) {
	assert.Equal(t, 0, CompareVersion("", ""))
	assert.Equal(t, -1, CompareVersion("", "a"))
	assert.Equal(t, 1, CompareVersion("a", ""))
	assert.Equal(t, -1, CompareVersion("a", "b"))
	assert.Equal(t, 1, CompareVersion("b", "a"))
	assert.Equal(t, 0, CompareVersion("abc", "abc"))
}

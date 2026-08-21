package schema

import (
	"encoding/json"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataset_MarshalJSONSchema(t *testing.T) {
	ds := Dataset{
		Fields: []Field{
			{Id: "name", Name: "Name", Schema: Leaf(KindString), Scope: ScopeSynced},
			{Id: "localStatus", Name: "Local status", Schema: Leaf(KindString), Scope: ScopeLocal},
			{Id: "createdAt", Schema: Leaf(KindNumber), Scope: ScopeDerived},
			{Id: "tags", Schema: &Schema{Kind: KindArray, Items: Leaf(KindString)}, Scope: ScopeSynced},
		},
		Dynamic: false,
	}
	raw, err := json.Marshal(ds)
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(raw, &doc))

	assert.Equal(t, "object", doc["type"])
	assert.Equal(t, false, doc["additionalProperties"])

	props := doc["properties"].(map[string]any)

	name := props["name"].(map[string]any)
	assert.Equal(t, "string", name["type"])
	assert.Equal(t, "Name", name["title"])
	assert.Equal(t, "synced", name["x-scope"])

	assert.Equal(t, "local", props["localStatus"].(map[string]any)["x-scope"])
	assert.Equal(t, "derived", props["createdAt"].(map[string]any)["x-scope"])

	tags := props["tags"].(map[string]any)
	assert.Equal(t, "array", tags["type"])
	assert.Equal(t, "string", tags["items"].(map[string]any)["type"])
}

func TestDataset_MarshalBehavioralAnnotations(t *testing.T) {
	ds := Dataset{
		Fields: []Field{
			{Id: "title", Schema: Leaf(KindString), Required: true},
			{Id: "body", Schema: Leaf(KindString), MutableBy: MutableByAuthor},
			{Id: "note", Schema: Leaf(KindString), MutableBy: MutableByAnyone},
			{Id: "creator", Stamp: StampCreator, Scope: ScopeDerived},
			{Id: "modifiedAt", Stamp: StampModifyTime, Scope: ScopeDerived},
		},
		DeleteBy:  DeleteByAuthor,
		IdRule:    IdUser,
		IdPattern: `[a-z]+`,
		IdMaxLen:  32,
		Search:    &SearchFields{Title: "title", Text: []string{"body"}, Scope: "articles"},
	}
	raw, err := json.Marshal(ds)
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(raw, &doc))

	assert.ElementsMatch(t, []any{"title"}, doc["required"])
	assert.Equal(t, "author", doc["x-delete-by"])
	assert.Equal(t, "user", doc["x-id"])
	assert.Equal(t, "[a-z]+", doc["x-id-pattern"])
	assert.Equal(t, float64(32), doc["x-id-max-length"])
	search := doc["x-search"].(map[string]any)
	assert.Equal(t, "title", search["title"])
	assert.Equal(t, "body", search["text"], "a single text key canonicalizes to the bare string")
	assert.Equal(t, "articles", search["scope"])

	// An empty scope is omitted, like every other default.
	rawNoScope, err := json.Marshal(Dataset{Search: &SearchFields{Title: "title"}})
	require.NoError(t, err)
	var docNoScope map[string]any
	require.NoError(t, json.Unmarshal(rawNoScope, &docNoScope))
	_, hasScope := docNoScope["x-search"].(map[string]any)["scope"]
	assert.False(t, hasScope)

	props := doc["properties"].(map[string]any)
	assert.Equal(t, "author", props["body"].(map[string]any)["x-mutable-by"])
	assert.Equal(t, "any", props["note"].(map[string]any)["x-mutable-by"])
	assert.Equal(t, "creator", props["creator"].(map[string]any)["x-stamp"])
	assert.Equal(t, "modifyTime", props["modifiedAt"].(map[string]any)["x-stamp"])

	// Defaults are omitted.
	title := props["title"].(map[string]any)
	_, has := title["x-mutable-by"]
	assert.False(t, has)
	_, has = title["x-stamp"]
	assert.False(t, has)
}

func TestDataset_MarshalMultiFieldSearchText(t *testing.T) {
	raw, err := json.Marshal(Dataset{Search: &SearchFields{Title: "subject", Text: []string{"body", "notes"}}})
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(raw, &doc))
	search := doc["x-search"].(map[string]any)
	assert.Equal(t, []any{"body", "notes"}, search["text"])

	// No text mapping — the key is omitted like every other default.
	raw, err = json.Marshal(Dataset{Search: &SearchFields{Title: "subject"}})
	require.NoError(t, err)
	doc = nil
	require.NoError(t, json.Unmarshal(raw, &doc))
	_, hasText := doc["x-search"].(map[string]any)["text"]
	assert.False(t, hasText)
}

func TestSearchTextFromAnyenc(t *testing.T) {
	arena := &anyenc.Arena{}
	assert.Nil(t, SearchTextFromAnyenc(nil))
	assert.Nil(t, SearchTextFromAnyenc(arena.NewString("")), "empty string means no mapping")
	assert.Equal(t, []string{"body"}, SearchTextFromAnyenc(arena.NewString("body")))

	arr := arena.NewArray()
	arr.SetArrayItem(0, arena.NewString("body"))
	arr.SetArrayItem(1, arena.NewString("notes"))
	assert.Equal(t, []string{"body", "notes"}, SearchTextFromAnyenc(arr))

	assert.Nil(t, SearchTextFromAnyenc(arena.NewArray()), "empty array means no mapping")
	assert.Nil(t, SearchTextFromAnyenc(arena.NewNumberInt(1)), "non-string/array shapes are ignored")

	// Non-string entries ride as "" so ValidateSearchText flags them.
	bad := arena.NewArray()
	bad.SetArrayItem(0, arena.NewNumberInt(1))
	assert.Equal(t, []string{""}, SearchTextFromAnyenc(bad))
}

func TestSearchTextToAnyenc_Canonicalizes(t *testing.T) {
	arena := &anyenc.Arena{}
	assert.Nil(t, SearchTextToAnyenc(arena, nil))

	single := SearchTextToAnyenc(arena, []string{"body"})
	require.NotNil(t, single)
	assert.Equal(t, anyenc.TypeString, single.Type(), "a single key encodes as the bare string")
	assert.Equal(t, "body", string(single.GetStringBytes()))

	multi := SearchTextToAnyenc(arena, []string{"body", "notes"})
	require.NotNil(t, multi)
	assert.Equal(t, anyenc.TypeArray, multi.Type())

	// Round-trip through the parser is identity for canonical forms.
	assert.Equal(t, []string{"body"}, SearchTextFromAnyenc(single))
	assert.Equal(t, []string{"body", "notes"}, SearchTextFromAnyenc(multi))
	// And the encoder collapses the non-canonical one-element array.
	oneElem := arena.NewArray()
	oneElem.SetArrayItem(0, arena.NewString("body"))
	assert.Equal(t, anyenc.TypeString, SearchTextToAnyenc(arena, SearchTextFromAnyenc(oneElem)).Type())
}

func TestDataset_MarshalOmitsDefaultBehavior(t *testing.T) {
	raw, err := json.Marshal(Dataset{Fields: []Field{{Id: "a", Schema: Leaf(KindString)}}})
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(raw, &doc))
	for _, k := range []string{"required", "x-delete-by", "x-id", "x-id-pattern", "x-id-max-length", "x-search"} {
		_, has := doc[k]
		assert.False(t, has, k)
	}
}

func TestDataset_DynamicMarshalsAdditionalProperties(t *testing.T) {
	raw, err := json.Marshal(Dataset{Dynamic: true})
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(raw, &doc))
	assert.Equal(t, true, doc["additionalProperties"])
}

func TestDataset_ScopeOf(t *testing.T) {
	ds := Dataset{Fields: []Field{{Id: "x", Scope: ScopeLocal}}}
	sc, ok := ds.ScopeOf("x")
	assert.True(t, ok)
	assert.Equal(t, ScopeLocal, sc)
	_, ok = ds.ScopeOf("nope")
	assert.False(t, ok)
}

package schema

import (
	"encoding/json"
	"testing"

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

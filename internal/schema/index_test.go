package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func indexTestDataset() Dataset {
	return Dataset{Fields: []Field{
		{Id: "ts", Schema: &Schema{Kind: KindDatetime}},
		{Id: "value", Schema: &Schema{Kind: KindNumber}},
		{Id: "label", Schema: &Schema{Kind: KindString}},
		{Id: "done", Schema: &Schema{Kind: KindBoolean}},
		{Id: "tags", Schema: &Schema{Kind: KindArray, Items: &Schema{Kind: KindString}}},
		{Id: "extra", Schema: &Schema{Kind: KindObject}},
		{Id: "free"},
	}}
}

func TestValidateIndexDecl(t *testing.T) {
	ds := indexTestDataset()
	valid := []struct {
		name   string
		idx    Index
		shared bool
	}{
		{"one field", Index{Key: "by_ts", Fields: []string{"ts"}}, false},
		{"descending", Index{Key: "by_ts", Fields: []string{"-ts"}}, false},
		{"four fields", Index{Key: "wide", Fields: []string{"label", "done", "-value", "ts"}}, false},
		{"creation order", Index{Key: "created", Fields: []string{"label", IndexPathCreated}}, false},
		{"object of a shared dataset", Index{Key: "obj", Fields: []string{"ts", IndexPathObject}}, true},
		{"sparse", Index{Key: "sp", Fields: []string{"label"}, Sparse: true}, false},
	}
	for _, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, ValidateIndexDecl(ds, tc.idx, tc.shared))
		})
	}

	refused := []struct {
		name   string
		idx    Index
		shared bool
	}{
		{"no key", Index{Fields: []string{"ts"}}, false},
		{"key is not a slug", Index{Key: "By Ts", Fields: []string{"ts"}}, false},
		{"no fields", Index{Key: "x"}, false},
		{"five fields", Index{Key: "x", Fields: []string{"ts", "value", "label", "done", IndexPathCreated}}, false},
		{"empty field", Index{Key: "x", Fields: []string{""}}, false},
		{"bare direction", Index{Key: "x", Fields: []string{"-"}}, false},
		{"double direction", Index{Key: "x", Fields: []string{"--ts"}}, false},
		{"one path twice", Index{Key: "x", Fields: []string{"ts", "-ts"}}, false},
		{"undeclared field", Index{Key: "x", Fields: []string{"missing"}}, false},
		{"array field", Index{Key: "x", Fields: []string{"tags"}}, false},
		{"object field", Index{Key: "x", Fields: []string{"extra"}}, false},
		{"unconstrained field", Index{Key: "x", Fields: []string{"free"}}, false},
		{"record id", Index{Key: "x", Fields: []string{"id"}}, false},
		{"another protocol path", Index{Key: "x", Fields: []string{"_applySeq"}}, false},
		{"object of a per-object dataset", Index{Key: "x", Fields: []string{IndexPathObject}}, false},
	}
	for _, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorIs(t, ValidateIndexDecl(ds, tc.idx, tc.shared), ErrDecl)
		})
	}
}

// The store name follows what is indexed, not the key.
func TestIndex_StoreName(t *testing.T) {
	a := Index{Key: "a", Fields: []string{"ts", "-value"}}
	b := Index{Key: "b", Fields: []string{"ts", "-value"}}
	assert.Equal(t, "dx_ts,-value", a.StoreName())
	assert.Equal(t, a.StoreName(), b.StoreName())
	assert.NotEqual(t, a.StoreName(), Index{Key: "a", Fields: []string{"ts", "value"}}.StoreName())
	assert.NotEqual(t, a.StoreName(), Index{Key: "a", Fields: []string{"-value", "ts"}}.StoreName())
	assert.Equal(t, "dx_ts,-value~sparse", Index{Key: "a", Fields: []string{"ts", "-value"}, Sparse: true}.StoreName())
}

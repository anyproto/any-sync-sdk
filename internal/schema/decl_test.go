package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func creatorField() Field {
	return Field{Id: "creator", Stamp: StampCreator}
}

func TestValidateDatasetDecl_OK(t *testing.T) {
	ds := Dataset{
		Fields: []Field{
			{Id: "title", Schema: Leaf(KindString), Required: true},
			{Id: "body", Schema: Leaf(KindString), MutableBy: MutableByAuthor},
			creatorField(),
			{Id: "createdAt", Stamp: StampCreateTime},
			{Id: "modifiedAt", Stamp: StampModifyTime},
		},
		DeleteBy:  DeleteByAuthor,
		IdRule:    IdUser,
		IdPattern: `[a-z0-9-]+`,
		IdMaxLen:  64,
		Search:    &SearchFields{Title: "title", Text: []string{"body", "summary"}},
	}
	require.NoError(t, ValidateDatasetDecl(ds))

	// A single-key mapping and a title-only mapping (nil Text) pass too.
	require.NoError(t, ValidateDatasetDecl(Dataset{Search: &SearchFields{Text: []string{"body"}}}))
	require.NoError(t, ValidateDatasetDecl(Dataset{Search: &SearchFields{Title: "title"}}))
}

func TestValidateDatasetDecl_Rejections(t *testing.T) {
	cases := []struct {
		name string
		ds   Dataset
	}{
		{"empty field id", Dataset{Fields: []Field{{Id: ""}}}},
		{"dotted field id", Dataset{Fields: []Field{{Id: "a.b"}}}},
		{"duplicate field id", Dataset{Fields: []Field{{Id: "a"}, {Id: "a"}}}},
		{"stamp with synced scope", Dataset{Fields: []Field{{Id: "c", Stamp: StampCreator, Scope: ScopeSynced}}}},
		{"duplicate stamp kind", Dataset{Fields: []Field{{Id: "a", Stamp: StampCreator}, {Id: "b", Stamp: StampCreator}}}},
		{"required stamped field", Dataset{Fields: []Field{{Id: "c", Stamp: StampCreateTime, Required: true}}}},
		{"mutable stamped field", Dataset{Fields: []Field{{Id: "c", Stamp: StampCreateTime, MutableBy: MutableByAnyone}}}},
		{"required local field", Dataset{Fields: []Field{{Id: "c", Required: true, Scope: ScopeLocal}}}},
		{"author-mutable without creator stamp", Dataset{Fields: []Field{{Id: "b", MutableBy: MutableByAuthor}}}},
		{"deleteBy author without creator stamp", Dataset{Fields: []Field{{Id: "b"}}, DeleteBy: DeleteByAuthor}},
		{"id constraints under auto rule", Dataset{IdPattern: "x+"}},
		{"bad id pattern", Dataset{IdRule: IdUser, IdPattern: "("}},
		{"negative id max length", Dataset{IdRule: IdUser, IdMaxLen: -1}},
		{"empty search text array", Dataset{Search: &SearchFields{Title: "t", Text: []string{}}}},
		{"empty search text key", Dataset{Search: &SearchFields{Text: []string{"body", ""}}}},
		{"duplicate search text key", Dataset{Search: &SearchFields{Text: []string{"body", "body"}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateDatasetDecl(tc.ds)
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrDecl)
		})
	}
}

func TestCompileIdPattern_Defaults(t *testing.T) {
	re, maxLen, err := CompileIdPattern(Dataset{IdRule: IdUser})
	require.NoError(t, err)
	assert.Equal(t, DefaultIdMaxLen, maxLen)
	assert.True(t, re.MatchString("row_1:a-b.c"))
	assert.False(t, re.MatchString("has space"))
	assert.False(t, re.MatchString(""))
}

func TestCompileIdPattern_FullMatchAnchoring(t *testing.T) {
	re, maxLen, err := CompileIdPattern(Dataset{IdRule: IdUser, IdPattern: `[a-z]+`, IdMaxLen: 8})
	require.NoError(t, err)
	assert.Equal(t, 8, maxLen)
	assert.True(t, re.MatchString("abc"))
	// Pattern must match the WHOLE id, not a substring.
	assert.False(t, re.MatchString("abc1"))
	assert.False(t, re.MatchString("1abc"))
}

func TestParseLabels_RoundTrip(t *testing.T) {
	for _, m := range []Mutability{MutableNever, MutableByAuthor, MutableByAnyone} {
		got, ok := ParseMutability(m.String())
		require.True(t, ok)
		assert.Equal(t, m, got)
	}
	for _, s := range []Stamp{StampNone, StampCreator, StampCreateTime, StampModifyTime} {
		got, ok := ParseStamp(s.String())
		require.True(t, ok)
		assert.Equal(t, s, got)
	}
	for _, r := range []IdRule{IdAuto, IdUser} {
		got, ok := ParseIdRule(r.String())
		require.True(t, ok)
		assert.Equal(t, r, got)
	}
	for _, p := range []DeletePolicy{DeleteByAnyone, DeleteByAuthor} {
		got, ok := ParseDeletePolicy(p.String())
		require.True(t, ok)
		assert.Equal(t, p, got)
	}
	_, ok := ParseMutability("bogus")
	assert.False(t, ok)
	_, ok = ParseStamp("bogus")
	assert.False(t, ok)
	_, ok = ParseIdRule("bogus")
	assert.False(t, ok)
	_, ok = ParseDeletePolicy("bogus")
	assert.False(t, ok)
}

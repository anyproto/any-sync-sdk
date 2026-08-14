package spaceimpl

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/space"
)

func testUpserter(identity string) *upserter {
	decl := &schema.Dataset{
		Fields: []schema.Field{
			{Id: "title", Schema: schema.Leaf(schema.KindString)}, // write-once
			{Id: "body", Schema: schema.Leaf(schema.KindString), MutableBy: schema.MutableByAuthor},
			{Id: "note", Schema: schema.Leaf(schema.KindString), MutableBy: schema.MutableByAnyone},
			{Id: "creator", Stamp: schema.StampCreator, Scope: schema.ScopeDerived},
		},
		IdRule: schema.IdUser,
	}
	u := &upserter{decl: decl, rules: map[string]*schema.Field{}, identity: identity}
	for i := range decl.Fields {
		f := &decl.Fields[i]
		u.rules[f.Id] = f
		if f.Stamp == schema.StampCreator {
			u.creatorField = f.Id
		}
	}
	return u
}

func storedRecord(a *anyenc.Arena, fields map[string]string) *anyenc.Value {
	obj := a.NewObject()
	for k, v := range fields {
		obj.Set(k, a.NewString(v))
	}
	return obj
}

func desired(a *anyenc.Arena, fields map[string]string) map[string]*anyenc.Value {
	out := make(map[string]*anyenc.Value, len(fields))
	for k, v := range fields {
		out[k] = a.NewString(v)
	}
	return out
}

func TestUpsertDiff_SkipsIdenticalRecord(t *testing.T) {
	a := &anyenc.Arena{}
	u := testUpserter("me")
	before := storedRecord(a, map[string]string{"title": "t", "note": "n", "creator": "me"})
	mod, err := u.diffRecord("r1", desired(a, map[string]string{"title": "t", "note": "n"}), before)
	require.NoError(t, err)
	assert.Nil(t, mod, "identical record emits nothing")
}

func TestUpsertDiff_EmitsSinglePathSetsForChangedMutable(t *testing.T) {
	a := &anyenc.Arena{}
	u := testUpserter("me")
	before := storedRecord(a, map[string]string{"title": "t", "note": "old", "creator": "me"})
	mod, err := u.diffRecord("r1", desired(a, map[string]string{"title": "t", "note": "new"}), before)
	require.NoError(t, err)
	require.NotNil(t, mod)
	assert.False(t, mod.Upsert)
	require.Len(t, mod.Ops, 1)
	assert.Equal(t, space.OpSet, mod.Ops[0].Type)
	assert.Equal(t, "note", mod.Ops[0].Path)
}

func TestUpsertDiff_ImmutableChangeRejectsRecord(t *testing.T) {
	a := &anyenc.Arena{}
	u := testUpserter("me")
	before := storedRecord(a, map[string]string{"title": "t", "creator": "me"})
	_, err := u.diffRecord("r1", desired(a, map[string]string{"title": "CHANGED"}), before)
	require.Error(t, err)
	assert.ErrorIs(t, err, space.ErrImmutableFieldChanged)
}

func TestUpsertDiff_AuthorGate(t *testing.T) {
	a := &anyenc.Arena{}

	// Author edits pass.
	u := testUpserter("me")
	before := storedRecord(a, map[string]string{"body": "old", "creator": "me"})
	mod, err := u.diffRecord("r1", desired(a, map[string]string{"body": "new"}), before)
	require.NoError(t, err)
	require.NotNil(t, mod)

	// Non-author edits reject deterministically client-side.
	other := testUpserter("someone-else")
	_, err = other.diffRecord("r1", desired(a, map[string]string{"body": "new"}), before)
	require.Error(t, err)
	assert.ErrorIs(t, err, space.ErrUpsertNotAuthor)
}

func TestUpsertDiff_StampedFieldRejected(t *testing.T) {
	a := &anyenc.Arena{}
	u := testUpserter("me")
	before := storedRecord(a, map[string]string{"creator": "me"})
	_, err := u.diffRecord("r1", desired(a, map[string]string{"creator": "spoof"}), before)
	require.Error(t, err)
}

func TestUpsertDiff_UndeclaredField(t *testing.T) {
	a := &anyenc.Arena{}
	u := testUpserter("me")
	before := storedRecord(a, map[string]string{"creator": "me"})

	// Non-dynamic dataset: undeclared field rejects the record.
	_, err := u.diffRecord("r1", desired(a, map[string]string{"extra": "x"}), before)
	require.Error(t, err)

	// Dynamic dataset: free-keyspace fields diff as plain sets.
	u.decl.Dynamic = true
	mod, err := u.diffRecord("r1", desired(a, map[string]string{"extra": "x"}), before)
	require.NoError(t, err)
	require.NotNil(t, mod)
}

func TestUpsertDiff_WrongKindValueRejectsRecord(t *testing.T) {
	a := &anyenc.Arena{}
	u := testUpserter("me")
	before := storedRecord(a, map[string]string{"note": "old", "creator": "me"})
	_, err := u.diffRecord("r1", map[string]*anyenc.Value{"note": a.NewNumberInt(1)}, before)
	require.Error(t, err, "wrong-kind update must be a per-record rejection, not a page abort")
}

func TestUpsertFields_ReservedAndDottedRejected(t *testing.T) {
	a := &anyenc.Arena{}
	for _, bad := range []string{"id", "_ver", "a.b", ""} {
		_, err := upsertFields(a, map[string]any{bad: "x"})
		require.Error(t, err, bad)
	}
	out, err := upsertFields(a, map[string]any{"ok": "x", "n": 42})
	require.NoError(t, err)
	assert.Len(t, out, 2)
}

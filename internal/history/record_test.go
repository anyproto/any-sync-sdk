package history

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

func recordAtParams(recordId string, touched []string) RecordAtParams {
	return RecordAtParams{
		ObjectId:         testObjectId,
		Dataset:          "notes",
		RecordId:         recordId,
		Version:          "head",
		Regs:             testRegs(),
		TouchedChangeIds: touched,
	}
}

func TestRecordAtFallbackMatchesFullView(t *testing.T) {
	ctx := context.Background()
	b := newTreeBuilder(t)
	b.add("notes", upsert(t, "n1", `{"title":"first","count":1}`))
	b.add("notes", upsert(t, "n2", `{"title":"other"}`))
	b.add("notes", crdt.RecordChange{Id: "n1", Ops: []crdt.Op{{
		Type: crdt.OpSet, Path: []string{"title"}, Payload: mustVal(t, `"renamed"`),
	}}})
	b.add("tasks", upsert(t, "t1", `{"done":true}`))

	full, err := buildScratch(t, b.tree, ViewParams{
		ObjectId: testObjectId, Heads: []string{"cid-004"}, Regs: testRegs(),
	})
	require.NoError(t, err)
	want := full.Record(ctx, "notes", "n1")
	require.NotNil(t, want)

	got, err := recordFromTree(ctx, b.tree.freshTree(0), recordAtParams("n1", nil))
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, want.String(), got.String())
}

func TestRecordAtWithTouchedList(t *testing.T) {
	ctx := context.Background()
	b := newTreeBuilder(t)
	c1 := b.add("notes", upsert(t, "n1", `{"title":"first"}`))
	b.add("notes", upsert(t, "n2", `{"title":"other"}`))
	c3 := b.add("notes", crdt.RecordChange{Id: "n1", Ops: []crdt.Op{{
		Type: crdt.OpSet, Path: []string{"title"}, Payload: mustVal(t, `"renamed"`),
	}}})

	fallback, err := recordFromTree(ctx, b.tree, recordAtParams("n1", nil))
	require.NoError(t, err)

	// Exact index-fed list — decode of non-touching changes skipped.
	indexed, err := recordFromTree(ctx, b.tree.freshTree(0), recordAtParams("n1", []string{c1, c3}))
	require.NoError(t, err)
	require.NotNil(t, indexed)
	assert.Equal(t, fallback.String(), indexed.String())

	// A stale index row naming a non-touching change is re-checked and
	// ignored, not applied.
	stale, err := recordFromTree(ctx, b.tree.freshTree(0), recordAtParams("n1", []string{c1, c3, "cid-002"}))
	require.NoError(t, err)
	require.NotNil(t, stale)
	assert.Equal(t, fallback.String(), stale.String())
}

func TestRecordAtAbsentAndTombstone(t *testing.T) {
	ctx := context.Background()
	b := newTreeBuilder(t)
	b.add("notes", upsert(t, "n1", `{"title":"a"}`))
	b.add("notes", crdt.RecordChange{Id: "n1", Ops: []crdt.Op{{Type: crdt.OpDelete}}})

	got, err := recordFromTree(ctx, b.tree, recordAtParams("missing", nil))
	require.NoError(t, err)
	assert.Nil(t, got)

	dead, err := recordFromTree(ctx, b.tree.freshTree(0), recordAtParams("n1", nil))
	require.NoError(t, err)
	require.NotNil(t, dead)
	assert.NotNil(t, dead.Get(crdt.DeletedAtField))
}

func TestRecordAtEmptyTouchedListStillReChecks(t *testing.T) {
	// Empty non-nil list = "index says nothing touches this record":
	// nothing applies, record comes back nil.
	ctx := context.Background()
	b := newTreeBuilder(t)
	b.add("notes", upsert(t, "n1", `{"title":"a"}`))

	got, err := recordFromTree(ctx, b.tree, recordAtParams("n1", []string{}))
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestFilteredReplayDisabledFlag(t *testing.T) {
	regs := []crdt.HandlerReg{
		{Name: "notes", Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}, DisableFilteredReplay: true},
	}
	assert.True(t, filteredReplayDisabled(regs, "notes"))
	assert.False(t, filteredReplayDisabled(regs, "tasks"))
	assert.False(t, filteredReplayDisabled(testRegs(), "notes"))
}

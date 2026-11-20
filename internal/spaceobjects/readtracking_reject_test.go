package spaceobjects

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// The classify seam must only see ops that actually landed: a
// handler-rejected delete must not clear unread entries, a rejected
// edit must not forge or supersede mention entries. These pin the
// filtering helpers readApplyHook runs on every record.

func TestRejectedOps(t *testing.T) {
	assert.Nil(t, rejectedOps(nil))
	assert.Nil(t, rejectedOps(&crdt.ApplyResult{}))

	res := &crdt.ApplyResult{Rejections: []crdt.OpRejection{
		{RecordIndex: 0, OpIndex: 1},
		{RecordIndex: 0, OpIndex: 1}, // duplicate (multi-field salvage repeats an index)
		{RecordIndex: 2, OpIndex: -1},
	}}
	got := rejectedOps(res)
	require.Len(t, got, 2)
	assert.Contains(t, got[0], 1)
	assert.Contains(t, got[2], -1)
	assert.NotContains(t, got, 1)
}

func TestSurvivingRecord(t *testing.T) {
	rec := &crdt.RecordChange{Id: "r1", Ops: []crdt.Op{
		{Type: crdt.OpSet, Path: []string{"text"}},
		{Type: crdt.OpDelete},
		{Type: crdt.OpSet, Path: []string{"reactions", "x", "y"}},
	}}

	// No rejections: same pointer back, copy-free.
	got, ok := survivingRecord(rec, nil)
	require.True(t, ok)
	assert.Same(t, rec, got)

	// One rejected op (the delete): it must vanish from the view while
	// the original RecordChange stays untouched.
	got, ok = survivingRecord(rec, map[int]struct{}{1: {}})
	require.True(t, ok)
	require.Len(t, got.Ops, 2)
	assert.Equal(t, []string{"text"}, got.Ops[0].Path)
	assert.Equal(t, []string{"reactions", "x", "y"}, got.Ops[1].Path)
	assert.Len(t, rec.Ops, 3, "input must not be mutated")

	// Whole-record rejection (BeforeCreate/BeforeDelete): skip outright.
	_, ok = survivingRecord(rec, map[int]struct{}{-1: {}})
	assert.False(t, ok)

	// Every op rejected: nothing left to classify.
	_, ok = survivingRecord(rec, map[int]struct{}{0: {}, 1: {}, 2: {}})
	assert.False(t, ok)
}

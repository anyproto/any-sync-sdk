package spaceimpl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/space"
)

func TestBuildChange_DeleteRecords(t *testing.T) {
	del := space.Op{Type: space.OpDelete}
	set := space.Op{Type: space.OpSet, Path: "text", Value: "x"}

	ch, err := buildChange(space.ModifyBatch{Dataset: "notes", Records: []space.RecordModify{
		{Id: "keep", Ops: []space.Op{set}},
		{Id: "gone", Ops: []space.Op{del}},
	}}, "v1")
	require.NoError(t, err)
	assert.Equal(t, crdt.OpDelete, ch.Records[1].Ops[0].Type, "a delete record rides the same change")

	for name, records := range map[string][]space.RecordModify{
		"delete beside another op":          {{Id: "r", Ops: []space.Op{del, set}}},
		"delete with a path":                {{Id: "r", Ops: []space.Op{{Type: space.OpDelete, Path: "text"}}}},
		"delete with a value":               {{Id: "r", Ops: []space.Op{{Type: space.OpDelete, Value: "x"}}}},
		"delete without an id":              {{Ops: []space.Op{del}}},
		"delete as an upsert":               {{Id: "r", Upsert: true, Ops: []space.Op{del}}},
		"deleted id written by the batch":   {{Id: "r", Ops: []space.Op{del}}, {Id: "r", Ops: []space.Op{set}}},
		"written id deleted later in batch": {{Id: "r", Upsert: true, Ops: []space.Op{set}}, {Id: "r", Ops: []space.Op{del}}},
	} {
		_, err := buildChange(space.ModifyBatch{Dataset: "notes", Records: records}, "v1")
		assert.ErrorIs(t, err, crdt.ErrValidation, name)
	}
}

// The guards fire before any store access, so a zero spaceImpl
// exercises them (as in modify_scope_test.go).
func TestModify_ConditionalGuards(t *testing.T) {
	ctx := context.Background()
	s := &spaceImpl{}
	seq := uint64(7)

	t.Run("local scope: precondition rejected", func(t *testing.T) {
		b := scopedBatch(space.ScopeLocal)
		b.IfUnchangedSince = &seq
		_, err := s.Modify(ctx, b)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "IfUnchangedSince")
	})

	t.Run("local scope: delete rejected", func(t *testing.T) {
		b := scopedBatch(space.ScopeLocal)
		b.Records[0].Ops = []space.Op{{Type: space.OpDelete}}
		_, err := s.Modify(ctx, b)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot delete")
	})

	t.Run("ModifyMany: precondition rejected", func(t *testing.T) {
		b := scopedBatch(space.ScopeSynced)
		b.IfUnchangedSince = &seq
		_, err := s.ModifyMany(ctx, []space.ModifyBatch{b})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Modify only")
	})
}

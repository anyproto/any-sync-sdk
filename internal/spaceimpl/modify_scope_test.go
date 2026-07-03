package spaceimpl

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// The scope-routing guards on Modify / ModifyMany all fire before any
// store access, so a zero spaceImpl exercises them without a fixture.
// The happy local path (LocalSet apply, scope enforcement, rejections)
// is covered at the crdt layer and end-to-end by
// e2e/local_scope_records_test.go.

func scopedBatch(scope space.Scope) space.ModifyBatch {
	return space.ModifyBatch{
		ObjectId: "obj",
		Dataset:  "notes",
		Scope:    scope,
		Records: []space.RecordModify{{
			Id:  "rec-1",
			Ops: []space.Op{{Type: space.OpSet, Path: "unread", Value: true}},
		}},
	}
}

func TestModify_ScopeGuards(t *testing.T) {
	ctx := context.Background()
	s := &spaceImpl{}

	t.Run("account scope rejected", func(t *testing.T) {
		_, err := s.Modify(ctx, scopedBatch(space.ScopeAccount))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "account")
	})

	t.Run("derived scope rejected", func(t *testing.T) {
		_, err := s.Modify(ctx, scopedBatch(space.ScopeDerived))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "derived")
	})

	t.Run("local: TraceIds rejected", func(t *testing.T) {
		b := scopedBatch(space.ScopeLocal)
		b.TraceIds = []string{"trace-1"}
		_, err := s.Modify(ctx, b)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "TraceIds")
	})

	t.Run("local: empty record id rejected", func(t *testing.T) {
		b := scopedBatch(space.ScopeLocal)
		b.Records[0].Id = ""
		b.Records[0].Upsert = true
		_, err := s.Modify(ctx, b)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "explicit record ids")
	})

	t.Run("local: upsert rejected", func(t *testing.T) {
		b := scopedBatch(space.ScopeLocal)
		b.Records[0].Upsert = true
		_, err := s.Modify(ctx, b)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Upsert")
	})
}

func TestModifyMany_ScopedBatchRejected(t *testing.T) {
	ctx := context.Background()
	s := &spaceImpl{}

	for _, scope := range []space.Scope{space.ScopeLocal, space.ScopeAccount, space.ScopeDerived} {
		_, err := s.ModifyMany(ctx, []space.ModifyBatch{scopedBatch(scope)})
		require.Error(t, err, "scope %s", scope)
		assert.Contains(t, err.Error(), "synced only", "scope %s", scope)
	}
}

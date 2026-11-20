package object

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/space"
)

// MarkDeleted flips the flag under the tree lock; every write entry
// point then refuses with ErrDeleted, which matches the public
// sentinel, and a replay callback no-ops.
func TestMarkDeleted_RefusesWrites(t *testing.T) {
	tree := &closeSpyTree{}
	o := &Object{tree: tree}
	ctx := context.Background()
	ch := crdt.Change{Dataset: "chat", VersionId: "v01"}

	o.MarkDeleted()
	assert.True(t, o.deleted)
	assert.False(t, tree.locked, "MarkDeleted releases the tree lock")

	err := o.ApplyDecoded(ctx, ch)
	assert.True(t, errors.Is(err, ErrDeleted), "drain: %v", err)
	assert.True(t, errors.Is(err, space.ErrObjectDeleted))
	_, err = o.LocalSet(ctx, ch)
	assert.True(t, errors.Is(err, ErrDeleted), "local set: %v", err)
	_, err = o.InjectedSet(ctx, ch)
	assert.True(t, errors.Is(err, ErrDeleted), "injected set: %v", err)
	require.NoError(t, o.replayLocked(ctx, tree), "a replay callback on a deleted object no-ops")
	assert.False(t, tree.locked)

	// Idempotent, and Close still works afterwards.
	o.MarkDeleted()
	require.NoError(t, o.Close())
	assert.Equal(t, 1, tree.closeCalls)
}

// A deleted Object without a bound tree takes the flag without locking.
func TestMarkDeleted_NoTree(t *testing.T) {
	o := &Object{}
	o.MarkDeleted()
	assert.True(t, o.deleted)
}

// LocalWrite checks the flag after its structural validation, under
// the tree lock: a valid change on a deleted object is refused before
// anything reaches the DAG.
func TestMarkDeleted_RefusesLocalWrite(t *testing.T) {
	o, tree := newLocalWriteFixture(t, "blocks", &timestampHandler{})
	o.MarkDeleted()
	_, err := o.LocalWrite(context.Background(), crdt.Change{
		Dataset:     "blocks",
		DataVersion: "test-v1",
		Records: []crdt.RecordChange{{
			Id:     "rec-1",
			Upsert: true,
			Ops:    []crdt.Op{recordOp(t, "name", "hello")},
		}},
	})
	assert.True(t, errors.Is(err, ErrDeleted), "local write: %v", err)
	assert.Zero(t, tree.captured, "nothing reached AddContent")
}

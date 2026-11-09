package object

import (
	"context"
	"errors"
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

func setRecord(t *testing.T, id, key, val string) crdt.Change {
	t.Helper()
	return crdt.Change{
		Dataset:     "blocks",
		DataVersion: "test-v1",
		Records: []crdt.RecordChange{{
			Id:     id,
			Upsert: true,
			Ops:    []crdt.Op{recordOp(t, key, val)},
		}},
	}
}

// The request context ends while the post-AddContent apply runs. The
// change is already in the DAG, so the apply commits, the record is
// stored and the watermark passes the change.
func TestLocalWrite_ApplyOutlivesRequestCtx(t *testing.T) {
	o, _ := newLocalWriteFixture(t, "blocks", &crdt.DefaultHandler{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var hookCtxErr error
	o.ctrl.SetApplyHook(func(txCtx context.Context, _ *crdt.Change, _ []string, _ *crdt.ApplyResult) error {
		cancel()
		hookCtxErr = txCtx.Err()
		return nil
	})

	res, err := o.LocalWrite(ctx, setRecord(t, "rec-1", "name", "a"))
	require.NoError(t, err)
	assert.NoError(t, hookCtxErr, "the apply tx context must not inherit the request's cancellation")
	assert.Equal(t, "ch-1", res.ChangeId)
	assert.Equal(t, uint64(1), o.ctrl.MaxAddSeq())
	rec := o.ctrl.Get(context.Background(), "blocks", "rec-1")
	require.NotNil(t, rec)
	assert.Equal(t, "a", rec.GetString("name"))
	assert.False(t, o.applyGap)
}

// The apply after AddContent fails: the change stays in the tree above
// the watermark and the next local write replays it before its own
// apply, so the watermark never passes an unapplied change.
func TestLocalWrite_FailedApplyIsReplayedBeforeNextWrite(t *testing.T) {
	o, tree := newLocalWriteFixture(t, "blocks", &crdt.DefaultHandler{})
	failOnce := true
	o.ctrl.SetApplyHook(func(context.Context, *crdt.Change, []string, *crdt.ApplyResult) error {
		if failOnce {
			failOnce = false
			return errors.New("disk")
		}
		return nil
	})
	ctx := context.Background()

	_, err := o.LocalWrite(ctx, setRecord(t, "rec-1", "name", "a"))
	require.ErrorContains(t, err, "apply local")
	require.Len(t, tree.stored, 1, "the change is in the DAG")
	assert.Equal(t, uint64(0), o.ctrl.MaxAddSeq())
	assert.True(t, o.applyGap)
	assert.Nil(t, o.ctrl.Get(ctx, "blocks", "rec-1"))

	res, err := o.LocalWrite(ctx, setRecord(t, "rec-2", "name", "b"))
	require.NoError(t, err)
	assert.Equal(t, uint64(2), o.ctrl.MaxAddSeq())
	assert.Equal(t, "ch-2", res.ChangeId)
	assert.False(t, o.applyGap)
	for id, want := range map[string]string{"rec-1": "a", "rec-2": "b"} {
		rec := o.ctrl.Get(ctx, "blocks", id)
		require.NotNil(t, rec, id)
		assert.Equal(t, want, rec.GetString("name"), id)
	}
}

// A replay that halts on an apply error marks the gap too: a local
// write that follows catches the halted change up first.
func TestLocalWrite_HaltedReplayIsCaughtUpBeforeNextWrite(t *testing.T) {
	o, tree := newLocalWriteFixture(t, "blocks", &crdt.DefaultHandler{})
	tree.nextSeq = 6
	inbound := setRecord(t, "rec-in", "name", "peer")
	payload, err := o.codec.Encode(&inbound)
	require.NoError(t, err)
	tree.stored = append(tree.stored, &objecttree.Change{
		Id:       "ch-in",
		OrderId:  "a000000000",
		AddSeq:   5,
		Data:     payload,
		Identity: tree.root.Identity,
	})
	failOnce := true
	o.ctrl.SetApplyHook(func(context.Context, *crdt.Change, []string, *crdt.ApplyResult) error {
		if failOnce {
			failOnce = false
			return errors.New("disk")
		}
		return nil
	})
	ctx := context.Background()

	require.ErrorContains(t, o.ColdRestore(ctx), "apply ch-in")
	assert.True(t, o.applyGap)
	assert.Equal(t, uint64(0), o.ctrl.MaxAddSeq())

	_, err = o.LocalWrite(ctx, setRecord(t, "rec-1", "name", "a"))
	require.NoError(t, err)
	assert.False(t, o.applyGap)
	assert.Equal(t, uint64(6), o.ctrl.MaxAddSeq())
	for id, want := range map[string]string{"rec-in": "peer", "rec-1": "a"} {
		rec := o.ctrl.Get(ctx, "blocks", id)
		require.NotNil(t, rec, id)
		assert.Equal(t, want, rec.GetString("name"), id)
	}
}

// A catch-up that fails leaves nothing in the DAG and keeps the gap.
func TestLocalWrite_CatchUpFailureWritesNothing(t *testing.T) {
	o, tree := newLocalWriteFixture(t, "blocks", &crdt.DefaultHandler{})
	o.ctrl.SetApplyHook(func(context.Context, *crdt.Change, []string, *crdt.ApplyResult) error {
		return errors.New("disk")
	})
	ctx := context.Background()
	_, err := o.LocalWrite(ctx, setRecord(t, "rec-1", "name", "a"))
	require.ErrorContains(t, err, "apply local")

	_, err = o.LocalWrite(ctx, setRecord(t, "rec-2", "name", "b"))
	require.ErrorContains(t, err, "catch up")
	assert.Len(t, tree.stored, 1, "the second change never reaches AddContent")
	assert.True(t, o.applyGap)
	assert.Equal(t, uint64(0), o.ctrl.MaxAddSeq())
}

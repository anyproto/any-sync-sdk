package object

import (
	"context"
	"errors"
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree/updatelistener"
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

// failOnce returns an apply hook that fails its first call.
func failOnce() crdt.ApplyHook {
	failed := false
	return func(context.Context, *crdt.Change, []string, *crdt.ApplyResult) error {
		if !failed {
			failed = true
			return errors.New("disk")
		}
		return nil
	}
}

// storedInbound appends a peer change to the fake tree's storage.
func storedInbound(t *testing.T, o *Object, tree *fakeLocalWriteTree, id string, addSeq uint64, ch crdt.Change) {
	t.Helper()
	payload, err := o.codec.Encode(&ch)
	require.NoError(t, err)
	tree.stored = append(tree.stored, &objecttree.Change{
		Id:       id,
		OrderId:  "a00000000" + id[len(id)-1:],
		AddSeq:   addSeq,
		Data:     payload,
		Identity: tree.root.Identity,
	})
}

func requireRecords(t *testing.T, o *Object, want map[string]string) {
	t.Helper()
	for id, val := range want {
		rec := o.ctrl.Get(context.Background(), "blocks", id)
		require.NotNil(t, rec, id)
		assert.Equal(t, val, rec.GetString("name"), id)
	}
}

// The request context ends while the post-AddContent apply runs. The
// change is already in the DAG, so the apply commits, the record is
// stored, the watermark passes the change, and afterApply runs on a
// context that is still live.
func TestLocalWrite_ApplyOutlivesRequestCtx(t *testing.T) {
	o, _ := newLocalWriteFixture(t, "blocks", &crdt.DefaultHandler{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var hookCtxErr, afterCtxErr error
	o.ctrl.SetApplyHook(func(txCtx context.Context, _ *crdt.Change, _ []string, _ *crdt.ApplyResult) error {
		cancel()
		hookCtxErr = txCtx.Err()
		return nil
	})
	o.afterApply = func(ctx context.Context, _ *Object, _ *crdt.Change, _ *crdt.ApplyResult) {
		afterCtxErr = ctx.Err()
	}

	res, err := o.LocalWrite(ctx, setRecord(t, "rec-1", "name", "a"))
	require.NoError(t, err)
	assert.NoError(t, hookCtxErr, "the apply tx context must not inherit the request's cancellation")
	assert.NoError(t, afterCtxErr, "afterApply must not inherit the request's cancellation")
	assert.Equal(t, "ch-1", res.ChangeId)
	assert.Equal(t, uint64(1), o.ctrl.MaxAddSeq())
	requireRecords(t, o, map[string]string{"rec-1": "a"})
	assert.False(t, o.applyGap)
}

// The apply after AddContent fails: the change stays in the tree above
// the watermark and the next local write replays it before its own
// apply, so the watermark never passes an unapplied change. The store
// hears about the gap when it opens and when it closes.
func TestLocalWrite_FailedApplyIsReplayedBeforeNextWrite(t *testing.T) {
	o, tree := newLocalWriteFixture(t, "blocks", &crdt.DefaultHandler{})
	o.ctrl.SetApplyHook(failOnce())
	var gaps []bool
	o.onGap = func(gap bool) { gaps = append(gaps, gap) }
	ctx := context.Background()

	_, err := o.LocalWrite(ctx, setRecord(t, "rec-1", "name", "a"))
	require.ErrorContains(t, err, "apply local")
	require.Len(t, tree.stored, 1, "the change is in the DAG")
	assert.Equal(t, uint64(0), o.ctrl.MaxAddSeq())
	assert.True(t, o.applyGap)
	assert.Equal(t, []bool{true}, gaps)
	assert.Nil(t, o.ctrl.Get(ctx, "blocks", "rec-1"))

	res, err := o.LocalWrite(ctx, setRecord(t, "rec-2", "name", "b"))
	require.NoError(t, err)
	assert.Equal(t, uint64(2), o.ctrl.MaxAddSeq())
	assert.Equal(t, "ch-2", res.ChangeId)
	assert.False(t, o.applyGap)
	assert.Equal(t, []bool{true, false}, gaps)
	requireRecords(t, o, map[string]string{"rec-1": "a", "rec-2": "b"})
}

// An inbound update whose replay halts on an apply error marks the gap
// too: the local write that follows catches the halted change up first.
func TestLocalWrite_HaltedUpdateIsCaughtUpBeforeNextWrite(t *testing.T) {
	o, tree := newLocalWriteFixture(t, "blocks", &crdt.DefaultHandler{})
	tree.nextSeq = 6
	storedInbound(t, o, tree, "ch-in", 5, setRecord(t, "rec-in", "name", "peer"))
	o.ctrl.SetApplyHook(failOnce())
	ctx := context.Background()

	require.ErrorContains(t, o.Update(tree), "apply ch-in")
	assert.True(t, o.applyGap)
	assert.Equal(t, uint64(0), o.ctrl.MaxAddSeq())

	_, err := o.LocalWrite(ctx, setRecord(t, "rec-1", "name", "a"))
	require.NoError(t, err)
	assert.False(t, o.applyGap)
	assert.Equal(t, uint64(6), o.ctrl.MaxAddSeq())
	requireRecords(t, o, map[string]string{"rec-in": "peer", "rec-1": "a"})
}

// The catch-up runs before the precondition: a write conditioned on the
// dataset being unchanged fails when the caught-up change changed it.
func TestLocalWriteIf_CatchUpPrecedesPrecondition(t *testing.T) {
	o, _ := newLocalWriteFixture(t, "blocks", &crdt.DefaultHandler{})
	o.ctrl.SetApplySeqAllocator(crdt.NewApplySeqAllocator(nil))
	ctx := context.Background()

	first, err := o.LocalWrite(ctx, setRecord(t, "rec-1", "name", "a"))
	require.NoError(t, err)
	o.ctrl.SetApplyHook(failOnce())
	_, err = o.LocalWrite(ctx, setRecord(t, "rec-2", "name", "b"))
	require.ErrorContains(t, err, "apply local")
	require.True(t, o.applyGap)

	_, err = o.LocalWriteIf(ctx, setRecord(t, "rec-3", "name", "c"), &first.ApplySeq)
	require.ErrorIs(t, err, crdt.ErrPreconditionFailed)
	assert.False(t, o.applyGap, "the catch-up ran")
	requireRecords(t, o, map[string]string{"rec-1": "a", "rec-2": "b"})
	assert.Nil(t, o.ctrl.Get(ctx, "blocks", "rec-3"))
}

// The request context ends after a caught-up change commits: its
// afterApply and the batch's afterReplay still run on a live context.
func TestLocalWrite_CatchUpDispatchOutlivesRequestCtx(t *testing.T) {
	o, tree := newLocalWriteFixture(t, "blocks", &crdt.DefaultHandler{})
	tree.nextSeq = 6
	storedInbound(t, o, tree, "ch-in", 5, setRecord(t, "rec-in", "name", "peer"))
	o.ctrl.SetApplyHook(failOnce())
	require.Error(t, o.Update(tree))
	require.True(t, o.applyGap)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var afterErrs, replayErrs []error
	o.afterApply = func(ctx context.Context, _ *Object, ch *crdt.Change, _ *crdt.ApplyResult) {
		if ch.ChangeId == "ch-in" {
			cancel()
		}
		afterErrs = append(afterErrs, ctx.Err())
	}
	o.afterReplay = func(ctx context.Context, _ *Object) { replayErrs = append(replayErrs, ctx.Err()) }

	_, _ = o.LocalWrite(ctx, setRecord(t, "rec-1", "name", "a"))
	require.NotEmpty(t, afterErrs)
	for _, err := range afterErrs {
		assert.NoError(t, err, "afterApply ran on the request context")
	}
	assert.Equal(t, []error{nil}, replayErrs, "afterReplay ran on the request context")
	assert.False(t, o.applyGap)
	assert.GreaterOrEqual(t, o.ctrl.MaxAddSeq(), uint64(5))
	requireRecords(t, o, map[string]string{"rec-in": "peer"})
}

// A malformed inbound change can never apply: the replay skips it and
// the changes after it, so the watermark passes it and no gap is left.
func TestUpdate_SkipsInvalidInboundChange(t *testing.T) {
	o, tree := newLocalWriteFixture(t, "blocks", &crdt.DefaultHandler{})
	bad := setRecord(t, "rec-bad", "name", "x")
	bad.DataVersion = ""
	storedInbound(t, o, tree, "ch-bad", 5, bad)
	storedInbound(t, o, tree, "ch-ok", 6, setRecord(t, "rec-ok", "name", "peer"))

	require.NoError(t, o.Update(tree))
	assert.False(t, o.applyGap)
	assert.Equal(t, uint64(6), o.ctrl.MaxAddSeq())
	assert.Nil(t, o.ctrl.Get(context.Background(), "blocks", "rec-bad"))
	requireRecords(t, o, map[string]string{"rec-ok": "peer"})
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

// An AddContent that fails before anything is stored leaves no gap;
// one that stores the change and fails after it (a failed head update)
// marks the gap, and the next write replays the stored change.
func TestLocalWrite_AddContentErrorMarksGapOnlyWhenStored(t *testing.T) {
	ctx := context.Background()
	o, tree := newLocalWriteFixture(t, "blocks", &crdt.DefaultHandler{})
	tree.addErr = errors.New("context canceled")
	_, err := o.LocalWrite(ctx, setRecord(t, "rec-1", "name", "a"))
	require.ErrorContains(t, err, "AddContent")
	assert.False(t, o.applyGap, "nothing stored, nothing to catch up")

	tree.addErr = nil
	tree.storeThenErr = errors.New("head update")
	_, err = o.LocalWrite(ctx, setRecord(t, "rec-1", "name", "a"))
	require.ErrorContains(t, err, "AddContent")
	require.Len(t, tree.stored, 1)
	assert.True(t, o.applyGap)

	tree.storeThenErr = nil
	_, err = o.LocalWrite(ctx, setRecord(t, "rec-2", "name", "b"))
	require.NoError(t, err)
	assert.False(t, o.applyGap)
	requireRecords(t, o, map[string]string{"rec-1": "a", "rec-2": "b"})
}

// An object the store recorded a gap for starts marked (Config.ApplyGap
// through New), so the replay that closes the gap reports it closed.
func TestColdRestore_ClosesSeededGap(t *testing.T) {
	seed, tree := newLocalWriteFixture(t, "blocks", &crdt.DefaultHandler{})
	storedInbound(t, seed, tree, "ch-in", 5, setRecord(t, "rec-in", "name", "peer"))
	var gaps []bool
	o, err := New(Config{
		SpaceId:    seed.spaceId,
		SignKey:    seed.signKey,
		Controller: seed.ctrl,
		ApplyGap:   true,
		OnGap:      func(gap bool) { gaps = append(gaps, gap) },
	}, func(updatelistener.UpdateListener) (objecttree.ObjectTree, error) { return tree, nil })
	require.NoError(t, err)
	require.True(t, o.applyGap, "New seeds the mark from the config")

	require.NoError(t, o.ColdRestore(context.Background()))
	assert.False(t, o.applyGap)
	assert.Equal(t, []bool{false}, gaps)
	requireRecords(t, o, map[string]string{"rec-in": "peer"})
}

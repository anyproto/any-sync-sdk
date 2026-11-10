package object

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// storeInboundN appends n peer changes sharing one addSeq, each
// creating record rec-<i>.
func storeInboundN(t *testing.T, o *Object, tree *fakeLocalWriteTree, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		payload, err := o.codec.Encode(new(setRecord(t, fmt.Sprintf("rec-%d", i), "name", "v")))
		require.NoError(t, err)
		tree.stored = append(tree.stored, &objecttree.Change{
			Id:       fmt.Sprintf("ch-in-%d", i),
			OrderId:  fmt.Sprintf("a%09d", i),
			AddSeq:   7,
			Data:     payload,
			Identity: tree.root.Identity,
		})
	}
}

// A replay never applies a change while the storage cursor is open:
// the cursor holds a space db read connection, and the apply waits for
// the SDK db writer.
func TestReplay_AppliesWithCursorClosed(t *testing.T) {
	o, tree := newLocalWriteFixture(t, "blocks", &crdt.DefaultHandler{})
	storeInboundN(t, o, tree, 3)
	var order []string
	o.ctrl.SetApplyHook(func(_ context.Context, ch *crdt.Change, _ []string, _ *crdt.ApplyResult) error {
		order = append(order, ch.ChangeId)
		assert.False(t, tree.cursorOpen, "apply ran under the open cursor")
		return nil
	})

	require.NoError(t, o.Update(tree))
	assert.Equal(t, []string{"ch-in-0", "ch-in-1", "ch-in-2"}, order)
	assert.Equal(t, uint64(7), o.ctrl.MaxAddSeq())
	assert.False(t, o.applyGap)
}

// The cursor fails after yielding changes: they are applied and the
// cursor error is returned.
func TestReplay_CursorErrorAppliesBufferedChanges(t *testing.T) {
	o, tree := newLocalWriteFixture(t, "blocks", &crdt.DefaultHandler{})
	storeInboundN(t, o, tree, 2)
	tree.cursorErr = errors.New("cursor")

	require.ErrorContains(t, o.Update(tree), "cursor")
	requireRecords(t, o, map[string]string{"rec-0": "v", "rec-1": "v"})
	assert.True(t, o.applyGap)
}

// Both the cursor and an apply fail: the apply error, which halted the
// replay, is the one returned.
func TestReplay_ApplyErrorOutranksCursorError(t *testing.T) {
	o, tree := newLocalWriteFixture(t, "blocks", &crdt.DefaultHandler{})
	storeInboundN(t, o, tree, 2)
	tree.cursorErr = errors.New("cursor")
	o.ctrl.SetApplyHook(failOnce())

	require.ErrorContains(t, o.Update(tree), "apply ch-in-0")
	assert.Nil(t, o.ctrl.Get(context.Background(), "blocks", "rec-1"))
	assert.True(t, o.applyGap)
}

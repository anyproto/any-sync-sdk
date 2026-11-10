package object

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// storeInboundN appends n peer changes sharing one addSeq, each
// setting record rec-<i> to a random value of valSize bytes. The
// codec compresses a payload to no less than half of that.
func storeInboundN(t *testing.T, o *Object, tree *fakeLocalWriteTree, n, valSize int) {
	t.Helper()
	rnd := rand.New(rand.NewSource(1))
	buf := make([]byte, valSize/2)
	for i := 0; i < n; i++ {
		rnd.Read(buf)
		val := hex.EncodeToString(buf)
		payload, err := o.codec.Encode(new(setRecord(t, fmt.Sprintf("rec-%d", i), "name", val)))
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
	storeInboundN(t, o, tree, 3, 1)
	hooks := 0
	o.ctrl.SetApplyHook(func(context.Context, *crdt.Change, []string, *crdt.ApplyResult) error {
		hooks++
		assert.False(t, tree.cursorOpen, "apply ran under the open cursor")
		return nil
	})

	require.NoError(t, o.Update(tree))
	assert.Equal(t, 3, hooks)
	assert.Equal(t, 1, tree.passes)
	assert.Equal(t, uint64(7), o.ctrl.MaxAddSeq())
}

// More stored changes than one pass buffers: later passes resume past
// the last buffered change, so each change applies once, in order.
func TestReplay_ResumesAfterFullBatch(t *testing.T) {
	o, tree := newLocalWriteFixture(t, "blocks", &crdt.DefaultHandler{})
	const valSize = 64 << 10
	n := 4 * replayBatchBytes / valSize
	storeInboundN(t, o, tree, n, valSize)
	var order []string
	o.ctrl.SetApplyHook(func(_ context.Context, ch *crdt.Change, _ []string, _ *crdt.ApplyResult) error {
		order = append(order, ch.ChangeId)
		assert.False(t, tree.cursorOpen, "apply ran under the open cursor")
		return nil
	})

	require.NoError(t, o.Update(tree))
	require.Len(t, order, n)
	for i, id := range order {
		require.Equal(t, fmt.Sprintf("ch-in-%d", i), id)
	}
	assert.Greater(t, tree.passes, 1)
	assert.False(t, o.applyGap)
}

// A replay halted by an apply error stops at the failed change and
// does not start another pass.
func TestReplay_HaltStopsPasses(t *testing.T) {
	o, tree := newLocalWriteFixture(t, "blocks", &crdt.DefaultHandler{})
	const valSize = 64 << 10
	storeInboundN(t, o, tree, 4*replayBatchBytes/valSize, valSize)
	o.ctrl.SetApplyHook(failOnce())

	require.ErrorContains(t, o.Update(tree), "apply ch-in-0")
	assert.Equal(t, 1, tree.passes)
	assert.True(t, o.applyGap)
	assert.Equal(t, uint64(0), o.ctrl.MaxAddSeq())
}

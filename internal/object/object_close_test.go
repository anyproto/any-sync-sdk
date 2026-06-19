package object

import (
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree/updatelistener"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// closeSpyTree records the close/lock/listener calls Object makes on
// eviction. It embeds objecttree.ObjectTree (nil) so any method we
// forget to override panics. It also satisfies synctree.ListenerSetter
// so setListenerNilLocked takes its real branch.
type closeSpyTree struct {
	objecttree.ObjectTree
	locked        bool
	tryLockFails  bool
	closeCalls    int
	listenerNiled bool
}

func (s *closeSpyTree) Id() string { return "obj-close" }

func (s *closeSpyTree) Lock() { s.locked = true }

func (s *closeSpyTree) TryLock() bool {
	if s.tryLockFails {
		return false
	}
	s.locked = true
	return true
}

func (s *closeSpyTree) Unlock() { s.locked = false }

func (s *closeSpyTree) Close() error {
	// tree.Close must run unlocked — Object releases the lock before
	// calling it (the real synctree.Close re-acquires it).
	if s.locked {
		panic("tree.Close called while Object still holds tree.Lock")
	}
	s.closeCalls++
	return nil
}

func (s *closeSpyTree) SetListener(_ updatelistener.UpdateListener) {
	s.listenerNiled = true
}

// TestClose_ClosesTree is the regression guard for the leaked
// multiqueue receive-queue goroutines: Object.Close must close the
// underlying tree so any-sync's OnClose hook
// (objecttreebuilder.onClose → CloseReceiveQueue) fires. Detaching
// the listener alone (the prior behaviour) left the synctree open and
// the per-object goroutine parked forever.
func TestClose_ClosesTree(t *testing.T) {
	tree := &closeSpyTree{}
	o := &Object{tree: tree}

	require.NoError(t, o.Close())
	assert.Equal(t, 1, tree.closeCalls, "Close must close the underlying tree")
	assert.True(t, tree.listenerNiled, "Close must detach the listener")
	assert.True(t, o.closed)

	// Idempotent: a second Close must not re-close the tree.
	require.NoError(t, o.Close())
	assert.Equal(t, 1, tree.closeCalls, "second Close must be a no-op")
}

// TestTryClose_ClosesTree mirrors TestClose_ClosesTree for the
// ocache-GC path.
func TestTryClose_ClosesTree(t *testing.T) {
	tree := &closeSpyTree{}
	o := &Object{tree: tree}

	ok, err := o.TryClose(0)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 1, tree.closeCalls, "successful TryClose must close the tree")
	assert.True(t, tree.listenerNiled)
	assert.True(t, o.closed)

	ok, err = o.TryClose(0)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, 1, tree.closeCalls, "second TryClose must be a no-op")
}

// TestTryClose_BusyTreeDoesNotClose: when the tree is locked by an
// in-flight handler, TryClose backs off without closing or detaching.
func TestTryClose_BusyTreeDoesNotClose(t *testing.T) {
	tree := &closeSpyTree{tryLockFails: true}
	o := &Object{tree: tree}

	ok, err := o.TryClose(0)
	require.NoError(t, err)
	assert.False(t, ok, "TryClose must report not-closed when the tree is busy")
	assert.Equal(t, 0, tree.closeCalls, "busy tree must not be closed")
	assert.False(t, tree.listenerNiled, "busy tree must keep its listener")
	assert.False(t, o.closed)
}

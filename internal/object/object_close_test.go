package object

import (
	"context"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/synctree/updatelistener"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
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
	// A backed-off TryClose must not release resources either — the
	// in-flight handler is still using the controller's collections.
}

// newCloseTestObject builds an Object with a real controller over a temp
// DB, one materialised per-object collection, and a spy tree.
func newCloseTestObject(t *testing.T, onClose func()) (*Object, anystore.DB, *closeSpyTree) {
	t.Helper()
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "objclose.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctrl, err := crdt.NewController(ctx, "obj-close", db,
		crdt.HandlerReg{Name: "blocks", Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}})
	require.NoError(t, err)
	arena := &anyenc.Arena{}
	payload := arena.NewObject()
	payload.Set("name", arena.NewString("hello"))
	require.NoError(t, ctrl.ApplyChange(ctx, crdt.Change{
		ObjectId: "obj-close", Dataset: "blocks", VersionId: "v1", DataVersion: "test-v1",
		Records: []crdt.RecordChange{{Id: "r1", Upsert: true, Ops: []crdt.Op{{Type: crdt.OpSet, Payload: payload}}}},
	}))
	tree := &closeSpyTree{}
	return &Object{tree: tree, ctrl: ctrl, onClose: onClose}, db, tree
}

// TestClose_ReleasesCollectionHandles is the SYN-105 regression guard:
// eviction must close the controller's per-object collection handles
// (each pins planner sketches + caches for the process lifetime) and
// fire the store's OnClose hook exactly once.
func TestClose_ReleasesCollectionHandles(t *testing.T) {
	ctx := context.Background()
	onCloseCalls := 0
	o, db, _ := newCloseTestObject(t, func() { onCloseCalls++ })

	// The registry handle the controller cached.
	coll, err := db.OpenCollection(ctx, "obj-close_blocks")
	require.NoError(t, err)

	require.NoError(t, o.Close())
	_, err = coll.FindId(ctx, "r1")
	assert.ErrorIs(t, err, anystore.ErrCollectionClosed, "eviction must close the per-object handle")
	assert.Equal(t, 1, onCloseCalls, "OnClose must fire once")

	require.NoError(t, o.Close())
	assert.Equal(t, 1, onCloseCalls, "second Close must not re-fire OnClose")

	// Recovery: a controller read after eviction re-opens by name.
	rec := o.Controller().Get(ctx, "blocks", "r1")
	require.NotNil(t, rec, "post-eviction read must re-open by name")
	assert.Equal(t, "hello", string(rec.GetStringBytes("name")))
}

// TestTryClose_ReleasesCollectionHandles mirrors the ocache-GC path.
func TestTryClose_ReleasesCollectionHandles(t *testing.T) {
	ctx := context.Background()
	onCloseCalls := 0
	o, db, _ := newCloseTestObject(t, func() { onCloseCalls++ })

	coll, err := db.OpenCollection(ctx, "obj-close_blocks")
	require.NoError(t, err)

	ok, err := o.TryClose(0)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = coll.FindId(ctx, "r1")
	assert.ErrorIs(t, err, anystore.ErrCollectionClosed)
	assert.Equal(t, 1, onCloseCalls)
}

// TestApplyDecoded_RejectsClosedObject: a drain landing on a
// just-closed object must be rejected — post-close, an apply's
// open-by-name would CREATE (resurrect) the released
// `<objectId>_<dataset>` collection. The drainer re-resolves via a
// fresh Store.Get, so the error is its retry signal.
func TestApplyDecoded_RejectsClosedObject(t *testing.T) {
	ctx := context.Background()
	o, db, _ := newCloseTestObject(t, nil)
	require.NoError(t, o.Close())

	err := o.ApplyDecoded(ctx, crdt.Change{
		ObjectId: "obj-close", Dataset: "blocks", VersionId: "v2", DataVersion: "test-v1",
		Records: []crdt.RecordChange{{Id: "r2", Upsert: true,
			Ops: []crdt.Op{{Type: crdt.OpSet, Path: []string{"name"}}}}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "closed")

	// Nothing written: the record must not exist after reopening.
	coll, oerr := db.OpenCollection(ctx, "obj-close_blocks")
	require.NoError(t, oerr)
	_, ferr := coll.FindId(ctx, "r2")
	assert.ErrorIs(t, ferr, anystore.ErrDocNotFound)
}
